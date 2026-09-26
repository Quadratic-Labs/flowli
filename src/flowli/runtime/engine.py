"""Engine: operator API and the shared machinery the worker uses.

See specs/04-api.md section 3 and specs/05-protocols.md sections 4 and 5.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from cairndb import Timestamp

from flowli.codec import structure, unstructure
from flowli.domain import (
    ROOT_FID,
    Actor,
    Code,
    Eid,
    Entry,
    EntryType,
    Execution,
    ExecutionStatus,
    FrameRef,
    MemoTable,
    Message,
    Ports,
    Provenance,
    Sequenced,
    Site,
    Task,
    TaskKind,
    check_channel,
    execution_channel,
    new_eid,
)
from flowli.log import get_logger

from .context import WorkflowRef
from .registry import Registry

log = get_logger("flowli.engine")


@dataclass(frozen=True, slots=True)
class EngineConfig:
    exec_ttl: float = 120.0  # execution lease TTL, seconds
    task_ttl: float = 60.0  # task lease TTL (visibility timeout), seconds
    poll_interval: float = 1.0  # worker idle sleep, seconds
    default_queue: str = "default"
    code_ref: str | None = None
    nack_delay: float = 5.0  # seconds, when a worker cannot handle a task


@dataclass(frozen=True, slots=True)
class StartResult:
    """The answer of `Engine.start_result`."""

    eid: Eid
    deduplicated: bool


class UnknownExecution(Exception):
    def __init__(self, eid: Eid | str) -> None:
        super().__init__(f"unknown execution {eid}")
        self.eid = eid


class Engine:
    def __init__(
        self,
        ports: Ports,
        site: Site,
        *,
        clock: Callable[[], Timestamp] = Timestamp.now,
        config: EngineConfig | None = None,
        registry: Registry | None = None,
    ) -> None:
        self.ports = ports
        self.site = site
        self.clock = clock
        self.config = config or EngineConfig()
        self.registry = registry or Registry()

    # --- registration ----------------------------------------------------------

    def workflow(
        self, name: str, version: str
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.registry.workflow(name, version)

    # --- provenance --------------------------------------------------------------

    def provenance(
        self,
        actor: Actor,
        *,
        workflow: str = "-",
        version: str = "-",
        frame_kind: str = "root",
        frame_name: str = "engine",
        site: Site | None = None,
        attempt: int = 1,
    ) -> Provenance:
        code = Code(workflow, version, frame_kind, frame_name, self.config.code_ref)
        return Provenance(actor, site or self.site, code, attempt, self.clock())

    # --- start -------------------------------------------------------------------

    async def start(
        self,
        workflow_fn: Callable[..., Any],
        /,
        *args: Any,
        key: str | None = None,
        queue: str | None = None,
        by: Actor,
        **kwargs: Any,
    ) -> Eid:
        """Create an execution and enqueue its START task. Return the eid."""
        result = await self.start_result(
            workflow_fn, *args, key=key, queue=queue, by=by, **kwargs
        )
        return result.eid

    async def start_result(
        self,
        workflow_fn: Callable[..., Any],
        /,
        *args: Any,
        key: str | None = None,
        queue: str | None = None,
        by: Actor,
        **kwargs: Any,
    ) -> StartResult:
        """`start`, with whether the dispatch key already had a winner.

        A caller that reports back to a person needs that fact; `start` alone
        cannot tell a fresh execution from a converged one.
        """
        wf = self.registry.of(workflow_fn)
        eid = new_eid()
        if key is not None:
            won, eid = await self.ports.dispatch.claim_start(key, eid)
            if not won:
                return StartResult(eid, True)
        execution = Execution(
            eid=eid,
            workflow=wf.name,
            version=wf.version,
            args={"args": list(args), "kwargs": kwargs},
            created_by=self.provenance(by, workflow=wf.name, version=wf.version),
            dispatch_key=key,
            queue=queue or self.config.default_queue,
        )
        created = await self.create_execution(execution)
        return StartResult(execution.eid, not created)

    async def start_child(self, execution: Execution, queue: str) -> None:
        """ChildStarter. Idempotent on execution.eid."""
        if execution.queue != queue:
            execution = replace(execution, queue=queue)
        await self.create_execution(execution)

    async def create_execution(self, execution: Execution) -> bool:
        """Write the record, announce execution.created, enqueue START. Idempotent."""
        created = await self.ports.executions.create(execution)
        prov = execution.created_by
        if created:
            log.info(
                "execution_created",
                eid=str(execution.eid),
                workflow=execution.workflow,
                version=execution.version,
                queue=execution.queue,
                by=f"{prov.actor.kind}:{prov.actor.id}",
                parent=None if execution.parent is None else str(execution.parent.eid),
            )
            # unstructure(None) is None too, so this needs no None-guard of its own.
            await self.ports.control.announce(
                Entry(
                    EntryType.EXECUTION_CREATED,
                    ROOT_FID,
                    {
                        "eid": str(execution.eid),
                        "workflow": execution.workflow,
                        "version": execution.version,
                        "dispatch_key": execution.dispatch_key,
                        "parent": unstructure(execution.parent),
                        "queue": execution.queue,
                    },
                    prov,
                )
            )
        await self.enqueue(
            Task(
                queue=execution.queue,
                kind=TaskKind.START,
                target=FrameRef(execution.eid, ROOT_FID),
                reason="start",
                enqueued_by=prov,
            )
        )
        return created

    # --- tasks -------------------------------------------------------------------------

    async def enqueue(self, task: Task, *, repair: bool = False) -> bool:
        """`repair=True` also puts back a task whose enqueue marker outlived it.

        Repair paths only (the sweeper): it costs one listing more. See
        `Queue.ensure`.
        """
        port = self.ports.queue
        created = await (port.ensure(task) if repair else port.enqueue(task))
        if created:
            await self.ports.control.announce(Entry.task_enqueued(task))
        return created

    async def enqueue_resume(
        self, eid: Eid, cause: str, reason: str, by: Provenance, *, repair: bool = False
    ) -> bool:
        execution = await self.execution(eid)
        return await self.enqueue(
            Task(
                queue=execution.queue,
                kind=TaskKind.RESUME,
                target=FrameRef(eid, ROOT_FID),
                reason=reason,
                enqueued_by=by,
                key=cause,
            ),
            repair=repair,
        )

    # --- messages ----------------------------------------------------------------------

    async def signal(
        self,
        eid: Eid,
        channel: str,
        payload: Any,
        *,
        by: Actor,
        correlation: str | None = None,
    ) -> int:
        """Send on the execution-scoped channel, then enqueue a RESUME task."""
        return await self.deliver(
            eid,
            execution_channel(eid, channel),
            payload,
            by=by,
            correlation=correlation,
            reason=f"message:{channel}",
        )

    async def deliver(
        self,
        eid: Eid,
        channel: str,
        payload: Any,
        *,
        by: Actor,
        correlation: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Send on a channel given by its full name, then enqueue a RESUME task for eid."""
        prov = self.provenance(by, frame_name="signal")
        seq = await self.ports.channel.send(Message(channel, 0, payload, prov, correlation))  # pragma: no mutate
        await self.enqueue_resume(
            eid, f"message:{channel}:{seq}", reason or f"message:{channel}", prov
        )
        log.info("message_sent", eid=str(eid), channel=channel, seq=seq, by=f"{by.kind}:{by.id}")
        return seq

    async def broadcast(self, channel: str, payload: Any, *, by: Actor) -> int:
        """Send on a global channel, then enqueue one RESUME task per waiter."""
        check_channel(channel)
        prov = self.provenance(by, frame_name="broadcast")
        seq = await self.ports.channel.send(Message(channel, 0, payload, prov))  # pragma: no mutate
        for ref in await self.ports.channel.waiters(channel):
            await self.enqueue_resume(
                ref.eid, f"message:{channel}:{seq}", f"message:{channel}", prov
            )
        return seq

    async def notify_parent(
        self, execution: Execution, body: dict[str, Any], by: Provenance
    ) -> None:
        """Procedure 5: tell the parent that this child reached a terminal entry."""
        if execution.parent is None:
            return
        parent_eid = execution.parent.eid
        channel = execution.parent.child_channel
        await self.ports.channel.send(Message(channel, 0, body, by))  # pragma: no mutate
        await self.enqueue_resume(
            parent_eid, "", f"child:{execution.eid}", by
        )

    # --- cancel ------------------------------------------------------------------------

    async def cancel(self, eid: Eid, *, by: Actor) -> None:
        prov = self.provenance(by, frame_name="cancel")
        await self.ports.ownership.request_cancel(eid, prov)
        await self.enqueue_resume(eid, "cancel", "cancel", prov)
        log.info("cancel_requested", eid=str(eid), by=f"{by.kind}:{by.id}")

    # --- migrate -----------------------------------------------------------------------

    async def migrate(self, eid: Eid, version: str, *, by: Actor) -> Execution:
        """Point a live execution at another workflow version. See 05-protocols.md section 10.

        The record is replaced with CAS. A journal and control-log entry record who did it.
        A RESUME task is enqueued so a worker replays with the new code. Old memos stay
        valid when their frame ids and digests match.
        """
        before = await self.execution(eid)
        if (await self.memo(eid)).is_terminal:
            raise ValueError(f"execution {eid} is terminal: nothing to migrate")
        if before.version == version:
            return before
        after = await self.ports.executions.replace(eid, lambda e: replace(e, version=version))
        if after is None:
            raise UnknownExecution(eid)
        prov = self.provenance(by, workflow=after.workflow, version=version, frame_name="migrate")
        entry = Entry.execution_migrated(prov, before.version, version)
        await self.ports.journal.append(eid, entry)
        await self.ports.control.announce(
            Entry(entry.type, ROOT_FID, {"eid": str(eid), **entry.payload}, prov)
        )
        await self.enqueue_resume(eid, f"migrate:{version}", f"migrate:{version}", prov)
        log.info(
            "execution_migrated",
            eid=str(eid),
            from_version=before.version,
            version=version,
            by=f"{by.kind}:{by.id}",
        )
        return after

    # --- reads -------------------------------------------------------------------------

    async def execution(self, eid: Eid) -> Execution:
        execution = await self.ports.executions.read(eid)
        if execution is None:
            raise UnknownExecution(eid)
        return execution

    async def journal(self, eid: Eid) -> list[Sequenced[Entry]]:
        """The live journal, or the archived one when retention folded it."""
        entries = await self.ports.journal.read(eid)
        if entries:
            return entries
        archived = await self.ports.archive.read(eid)
        if archived is None:
            return []
        return [
            Sequenced(int(item["seq"]), structure(item["entry"], Entry))
            for item in archived.get("journal", [])
        ]

    async def archive(self, eid: Eid) -> dict[str, Any] | None:
        return await self.ports.archive.read(eid)

    async def memo(self, eid: Eid) -> MemoTable:
        return MemoTable.build(await self.ports.journal.read(eid))

    async def status(self, eid: Eid) -> ExecutionStatus:
        """Derived from the last execution.* entry of the journal."""
        await self.execution(eid)
        entries = await self.ports.journal.read(eid)
        last: str | None = None
        for s in entries:
            if s.item.type.startswith("execution.") and s.item.type != EntryType.EXECUTION_MIGRATED:
                last = s.item.type
        match last:
            case None:
                return ExecutionStatus.PENDING
            case EntryType.EXECUTION_COMPLETED:
                return ExecutionStatus.COMPLETED
            case EntryType.EXECUTION_FAILED:
                return ExecutionStatus.FAILED
            case EntryType.EXECUTION_CANCELLED:
                return ExecutionStatus.CANCELLED
            case EntryType.EXECUTION_SUSPENDED:
                return ExecutionStatus.SUSPENDED
            case _:
                return ExecutionStatus.RUNNING

    def workflow_ref(self, execution: Execution) -> WorkflowRef:
        return self.registry.get(execution.workflow, execution.version)

    # --- workers -------------------------------------------------------------------------

    def worker(self, queues: list[str] | None = None, worker_id: str | None = None) -> Any:
        from .worker import Worker

        return Worker(self, queues or [self.config.default_queue], worker_id or self.site.worker_id)

    def sweeper(self, **kwargs: Any) -> Any:
        from .sweeper import Sweeper

        return Sweeper(self, **kwargs)

    def retention(self, **kwargs: Any) -> Any:
        from .retention import Retention

        return Retention(self, **kwargs)

    @property
    def reviews(self) -> Any:
        from flowli.patterns.review import Reviews

        return Reviews(self)

"""Worker: dequeue tasks and run executions. See specs/05-protocols.md sections 1-8."""

from __future__ import annotations

import asyncio
import importlib
import inspect
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from flowli.codec import digest, unstructure
from flowli.domain import (
    ROOT_FID,
    Actor,
    Cancelled,
    ClaimedTask,
    Condition,
    Eid,
    Entry,
    Execution,
    Failed,
    FrameKind,
    FrameRef,
    Lease,
    LeaseLost,
    MemoTable,
    Message,
    NondeterminismError,
    Ports,
    Provenance,
    Site,
    Task,
    TaskKind,
    Timer,
    WorkflowNotRegistered,
    parse_eid,
)
from flowli.log import bound, get_logger

from .context import Context, Raised, Returned, Suspended, Wait
from .engine import Engine, UnknownExecution

log = get_logger("flowli.worker")


@dataclass(frozen=True, slots=True)
class Done:
    pass


@dataclass(frozen=True, slots=True)
class Retry:
    delay: timedelta


TaskResult = Done | Retry


class _RenewLoop:
    """Renews a lease every ttl/3 seconds. On LeaseLost, calls on_lost once."""

    def __init__(self, lease: Lease, ttl: float, on_lost: Callable[[], None]) -> None:
        self._lease = lease
        self._interval = ttl / 3
        self._on_lost = on_lost
        self.lost = False
        self._task: asyncio.Task[None] | None = None  # pragma: no mutate

    async def __aenter__(self) -> _RenewLoop:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self._lease.renew()
            except LeaseLost:
                self.lost = True
                self._on_lost()
                return


class Worker:
    def __init__(self, engine: Engine, queues: list[str], worker_id: str) -> None:
        self.engine = engine
        self.queues = list(queues)
        self.worker_id = worker_id
        self.actor = Actor.worker(worker_id)
        self._current: asyncio.Task[Any] | None = None  # pragma: no mutate

    @property
    def ports(self) -> Ports:
        return self.engine.ports

    # --- procedure 1: worker loop -----------------------------------------------------

    async def run_once(self) -> bool:
        """Dequeue at most one task and process it. Return True when a task was processed."""
        with bound(worker_id=self.worker_id):
            claimed = await self._dequeue()
            if claimed is None:
                return False
            await self._process(claimed)
            return True

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Loop until `stop`. One failed iteration is logged, not fatal: the bucket arbitrates,
        so a lost race or a storage hiccup is retried on the next pass."""
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                busy = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("worker_iteration_failed", worker_id=self.worker_id)
                busy = False  # pragma: no mutate
            if not busy:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self.engine.config.poll_interval)

    async def _dequeue(self) -> ClaimedTask | None:
        for queue in self.queues:
            claimed = await self.ports.queue.dequeue(
                queue, self.worker_id, self.engine.config.task_ttl
            )
            if claimed is not None:
                return claimed
        return None

    async def _process(self, claimed: ClaimedTask) -> None:
        """Run the task under its lease. A lost lease means: stop, do not ack."""
        task = claimed.task
        result: TaskResult
        with bound(task_id=task.task_id, queue=task.queue, task_kind=task.kind.value):
            log.debug("task_dequeued", reason=task.reason, eid=str(task.target.eid))
            async with _RenewLoop(
                claimed.lease, self.engine.config.task_ttl, self._cancel_current
            ) as renew:
                self._current = asyncio.ensure_future(self._dispatch(task))
                try:
                    result = await self._current
                except asyncio.CancelledError:
                    if renew.lost:
                        log.warning("task_lease_lost", eid=str(task.target.eid))
                        return
                    raise
                except LeaseLost as exc:
                    log.warning("task_lease_lost", eid=str(task.target.eid), detail=str(exc))
                    return
                finally:
                    self._current = None  # pragma: no mutate
            match result:
                case Done():
                    await self.ports.queue.ack(claimed)
                    log.debug("task_acked")
                case Retry(delay):
                    await self.ports.queue.nack(claimed, delay)
                    log.info("task_nacked", delay_s=delay.total_seconds())

    def _cancel_current(self) -> None:
        if self._current is not None and not self._current.done():
            self._current.cancel()

    async def _dispatch(self, task: Task) -> TaskResult:
        match task.kind:
            case TaskKind.START | TaskKind.RESUME:
                return await self.run_execution(task)
            case TaskKind.RUN_STEP:
                return await self.run_detached_step(task)
            case TaskKind.DELEGATE:
                log.warning("delegate_task_on_worker_queue")
                return Retry(timedelta(seconds=self.engine.config.nack_delay))
        raise AssertionError(task.kind)  # pragma: no mutate

    # --- procedure 2: run an execution ---------------------------------------------------

    async def run_execution(self, task: Task) -> TaskResult:
        eid = task.target.eid
        engine = self.engine
        lease = await self.ports.ownership.acquire(eid, self.worker_id, engine.config.exec_ttl)
        if lease is None:
            log.info("execution_owned_elsewhere", eid=str(eid))
            return Done()
        site = engine.site.with_epoch(lease.epoch)
        with bound(eid=str(eid), epoch=lease.epoch):
            return await self._run_owned(task, lease, site)

    async def _run_owned(self, task: Task, lease: Lease, site: Site) -> TaskResult:
        eid = task.target.eid
        engine = self.engine
        try:
            try:
                execution = await engine.execution(eid)
            except UnknownExecution:
                log.info("execution_unknown", hint="archived?")
                await lease.release()
                return Done()
            try:
                wf = engine.workflow_ref(execution)
            except WorkflowNotRegistered:
                log.warning(
                    "workflow_not_registered",
                    workflow=execution.workflow,
                    version=execution.version,
                )
                await lease.release()
                return Retry(timedelta(seconds=engine.config.nack_delay))

            memo = await engine.memo(eid)
            prov = engine.provenance(
                self.actor, workflow=wf.name, version=wf.version, site=site, frame_name="worker"
            )
            if memo.is_terminal:
                await lease.release()
                return Done()
            if _cancel_requested(lease.state):
                await self._cancel(execution, memo, lease, prov)
                return Done()

            if task.kind is TaskKind.START and memo.tail == 0:
                await self._record(eid, Entry.execution_started(prov, execution.args), memo, lease)
                log.info("execution_started", workflow=wf.name, version=wf.version)
            else:
                await self._record(
                    eid, Entry.execution_resumed(prov, lease.epoch, task.reason), memo, lease
                )
                log.info(
                    "execution_resumed", workflow=wf.name, version=wf.version, reason=task.reason
                )

            ctx = Context(
                eid=eid,
                workflow=wf,
                memo=memo,
                ports=self.ports,
                actor=self.actor,
                site=site,
                clock=engine.clock,
                code_ref=engine.config.code_ref,
                starter=engine,
                workflow_of=engine.registry.of,
                cancel_requested=lambda: _cancel_requested(lease.state),
            )
            args = execution.args or {}
            async with _RenewLoop(lease, engine.config.exec_ttl, self._cancel_current) as renew:
                try:
                    call_args = args.get("args", [])  # pragma: no mutate
                    call_kwargs = args.get("kwargs", {})  # pragma: no mutate
                    outcome = await ctx.run(*call_args, **call_kwargs)
                except asyncio.CancelledError:
                    if renew.lost:
                        raise LeaseLost(f"execution {eid} lease lost during run") from None
                    raise

            match outcome:
                case Returned(value):
                    await self._record(eid, Entry.execution_completed(prov, value), memo, lease)
                    await engine.notify_parent(
                        execution, {"status": "completed", "value": value}, prov
                    )
                    await lease.release({"status": "completed"})
                    log.info("execution_completed")
                case Raised(error) if isinstance(error, Cancelled):
                    await self._cancel(execution, memo, lease, prov)
                case Raised(error) if isinstance(error, NondeterminismError):
                    await self._record(
                        eid, Entry.execution_suspended(prov, [Condition.operator()]), memo, lease
                    )
                    await lease.release({"status": "suspended", "blocked": "nondeterminism"})
                    log.error("execution_blocked", reason="nondeterminism", fid=error.fid)
                case Raised(error):
                    failed = Failed.from_exception(error, retryable=False)  # pragma: no mutate
                    await self._record(eid, Entry.execution_failed(prov, failed), memo, lease)
                    await engine.notify_parent(
                        execution, {"status": "failed", "error": str(error)}, prov
                    )
                    await lease.release({"status": "failed"})
                    log.warning(
                        "execution_failed", error_type=failed.error_type, message=failed.message
                    )
                case Suspended(waits):
                    await self._suspend(execution, memo, lease, prov, waits)
            return Done()
        except LeaseLost:
            log.warning("execution_lease_lost", outcome="discarded")
            raise

    async def _record(
        self, eid: Eid, entry: Entry, memo: MemoTable, lease: Lease | None = None
    ) -> None:
        """Append an execution.* entry to the journal, fold it, and announce it.

        With a lease, first make one guarded write on it: a fenced worker gets LeaseLost
        here instead of publishing a lifecycle entry it no longer owns. Frame entries are
        not checked: the memo rule makes duplicates harmless (02-journal.md section 3).
        """
        from flowli.domain import Sequenced

        if lease is not None:
            await lease.refresh_state()
        seq = await self.ports.journal.append(eid, entry)
        memo.apply(Sequenced(seq, entry))
        await self.ports.control.announce(
            Entry(entry.type, ROOT_FID, {"eid": str(eid), **entry.payload}, entry.provenance)
        )

    async def _cancel(
        self, execution: Execution, memo: MemoTable, lease: Lease, prov: Provenance
    ) -> None:
        by = (lease.state or {}).get("cancel_requested") or unstructure(prov.actor)
        await self._record(execution.eid, Entry.execution_cancelled(prov, by), memo, lease)
        await self.engine.notify_parent(
            execution, {"status": "cancelled", "error": "cancelled"}, prov
        )
        await lease.release({"status": "cancelled"})
        log.info("execution_cancelled", by=by)

    # --- procedure 3: suspend ---------------------------------------------------------------

    async def _suspend(
        self,
        execution: Execution,
        memo: MemoTable,
        lease: Lease,
        prov: Provenance,
        waits: tuple[Wait, ...],
    ) -> None:
        eid = execution.eid
        ports = self.ports
        channels: list[str] = []
        children: list[str] = []
        for w in waits:
            ref = FrameRef(eid, w.fid)
            kind, name = Condition.parse(w.on)
            if kind == Condition.CHANNEL and name is not None:
                channels.append(name)
                await ports.channel.register_wait(name, ref)
            elif kind == Condition.CHILD and name is not None:
                children.append(name)
            if w.deadline is not None:
                await ports.timers.schedule(Timer(w.deadline, ref))

        await self._record(eid, Entry.execution_suspended(prov, [w.on for w in waits]), memo, lease)
        await lease.release({"status": "suspended"})
        log.info("execution_suspended", on=[w.on for w in waits])

        # check again after the release: close the race with senders
        for channel in channels:
            messages = await ports.channel.read(channel, after=memo.last_consumed(channel))
            if messages:
                seq = messages[0].seq
                await self.engine.enqueue_resume(
                    eid, f"message:{channel}:{seq}", f"message:{channel}", prov
                )
        for child_eid in children:
            with suppress(Exception):
                if (await self.engine.status(parse_eid(child_eid))).is_terminal:
                    await self.engine.enqueue_resume(
                        eid, f"child:{child_eid}", f"child:{child_eid}", prov
                    )

    # --- procedure 7: detached step ------------------------------------------------------------

    async def run_detached_step(self, task: Task) -> TaskResult:
        """payload: {"fn": "module:qualname", "args": [...], "kwargs": {...}, "name": str}."""
        ref = task.target
        payload = task.payload or {}
        fn = _resolve(payload["fn"])
        name = payload.get("name", fn.__name__)
        execution = await self.engine.execution(ref.eid)
        memo = await self.engine.memo(ref.eid)
        if ref.fid in memo.memos:
            return Done()
        attempt = memo.next_attempt(ref.fid)
        prov = self.engine.provenance(
            self.actor,
            workflow=execution.workflow,
            version=execution.version,
            frame_kind=FrameKind.STEP.value,
            frame_name=name,
            attempt=attempt,
        )
        args_digest = digest({"args": payload.get("args", []), "kwargs": payload.get("kwargs", {})})
        await self.ports.journal.append(
            ref.eid,
            Entry.frame_started(prov, ref.fid, FrameKind.STEP.value, name, args_digest, attempt),
        )
        body: dict[str, Any]
        try:
            result = fn(*payload.get("args", []), **payload.get("kwargs", {}))
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            failed = Failed.from_exception(exc)
            await self.ports.journal.append(
                ref.eid, Entry.frame_failed(prov, ref.fid, attempt, failed)
            )
            body = {"status": "failed", "error": unstructure(failed)}
        else:
            await self.ports.journal.append(
                ref.eid, Entry.frame_completed(prov, ref.fid, attempt, result)
            )
            body = {"status": "completed", "value": result}
        await self.ports.channel.send(Message(ref.step_channel, 0, body, prov))  # pragma: no mutate
        await self.engine.enqueue_resume(ref.eid, f"step:{ref.fid}", f"step:{ref.fid}", prov)
        log.info("detached_step_done", eid=str(ref.eid), fid=ref.fid, status=body["status"])
        return Done()


def _cancel_requested(state: Any) -> bool:
    return isinstance(state, dict) and bool(state.get("cancel_requested"))


def _resolve(path: str) -> Callable[..., Any]:
    module, _, qualname = path.partition(":")  # pragma: no mutate
    obj: Any = importlib.import_module(module)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj  # type: ignore[no-any-return]

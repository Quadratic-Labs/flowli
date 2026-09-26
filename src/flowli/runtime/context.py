"""The Context: replay a workflow coroutine over the journal.

See specs/02-journal.md section 5 and specs/04-api.md section 2.

Every public frame method allocates its frame id synchronously, then returns
an awaitable. This keeps ids stable under `gather`. The run driver detects the
moment every live frame waits and reports `Suspended`.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import random as _random
import uuid as _uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, Protocol

from cairndb import Timestamp

from flowli.codec import digest
from flowli.domain import (
    ROOT_FID,
    Actor,
    Cancelled,
    ChildFailed,
    Code,
    Condition,
    Eid,
    Entry,
    EvidenceRef,
    Execution,
    Failed,
    FrameIdAllocator,
    FrameKind,
    FrameRef,
    MemoTable,
    Message,
    NondeterminismError,
    NonRetryableError,
    Ports,
    Provenance,
    RetryPolicy,
    Sequenced,
    Site,
    StepFailed,
    Task,
    TaskKind,
    Timer,
    check_channel,
    check_queue,
    execution_channel,
)
from flowli.evidence import EvidenceWriter
from flowli.log import bound, get_logger

Scope = Literal["execution", "global"]
log = get_logger("flowli.context")
Now = Callable[[], Timestamp]

_current_fid: contextvars.ContextVar[str] = contextvars.ContextVar("flowli_fid", default=ROOT_FID)
_current_attempt: contextvars.ContextVar[int] = contextvars.ContextVar(
    "flowli_attempt", default=1
)


@dataclass(frozen=True, slots=True)
class WorkflowRef:
    name: str
    version: str
    fn: Callable[..., Any]


class ChildStarter(Protocol):
    """Creates and enqueues a child execution. Must be idempotent on execution.eid."""

    async def start_child(self, execution: Execution, queue: str) -> None: ...


@dataclass(frozen=True, slots=True)
class Wait:
    """A frame that waits. The worker registers it. See 05-protocols.md section 3."""

    fid: str
    on: str
    deadline: Timestamp | None


@dataclass(frozen=True, slots=True)
class Returned:
    value: Any


@dataclass(frozen=True, slots=True)
class Raised:
    error: BaseException


@dataclass(frozen=True, slots=True)
class Suspended:
    waits: tuple[Wait, ...]


RunOutcome = Returned | Raised | Suspended


class _Never(Exception):
    """Internal: a waiting frame never resumes inside one run."""


class Context:
    def __init__(
        self,
        *,
        eid: Eid,
        workflow: WorkflowRef,
        memo: MemoTable,
        ports: Ports,
        actor: Actor,
        site: Site,
        clock: Now,
        code_ref: str | None = None,
        starter: ChildStarter | None = None,
        workflow_of: Callable[[Callable[..., Any]], WorkflowRef] | None = None,
        cancel_requested: Callable[[], bool] = lambda: False,
        evidence: EvidenceWriter | None = None,
    ) -> None:
        self.eid = eid
        self.workflow = workflow
        self.memo = memo
        self._ports = ports
        self._actor = actor
        self._site = site
        self._clock = clock
        self._code_ref = code_ref
        self._starter = starter
        self._workflow_of = workflow_of
        self._cancel_requested = cancel_requested
        self._evidence = evidence or EvidenceWriter(ports.evidence)
        self._allocators: dict[str, FrameIdAllocator] = {}
        self._active = 0
        self._waiting: dict[str, Wait] = {}
        # Always replaced by a real Event before anything can read it (run() sets it
        # first thing, and no frame method runs except via run() -> _main()), so its
        # initial value here is never observed.
        self._idle: asyncio.Event | None = None  # pragma: no mutate

    # --- read-only helpers ----------------------------------------------------

    @property
    def fid(self) -> str:
        return _current_fid.get()

    @property
    def attempt(self) -> int:
        return _current_attempt.get()

    def cancel_requested(self) -> bool:
        return self._cancel_requested()

    # --- driver ----------------------------------------------------------------

    async def run(self, *args: Any, **kwargs: Any) -> RunOutcome:
        """Run the workflow function once. Return how it ended."""
        self._idle = asyncio.Event()
        main = asyncio.ensure_future(self._main(args, kwargs))
        idle_waiter = asyncio.ensure_future(self._idle.wait())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {main, idle_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                if main in done:
                    try:
                        return Returned(main.result())
                    except Exception as exc:
                        return Raised(exc)
                # idle signalled: let ready callbacks drain, then check again
                for _ in range(3):  # pragma: no mutate
                    await asyncio.sleep(0)
                if main.done():
                    # Unreachable given this file's own invariants: reaching here means
                    # idle_waiter (not main) was the one that resolved, which only ever
                    # happens once _maybe_idle finds a genuinely parked frame (self._waiting
                    # truthy) -- and every parked frame blocks forever on _wait()'s eternal
                    # future until *this* code cancels it below, so main cannot complete on
                    # its own in between. Kept as a defensive check, like the file's other
                    # "unreachable" asserts.
                    continue  # pragma: no mutate
                if self._active == 0 and self._waiting:
                    main.cancel()
                    with suppress(asyncio.CancelledError, _Never):
                        await main
                    return Suspended(tuple(self._waiting.values()))
                self._idle.clear()
                idle_waiter = asyncio.ensure_future(self._idle.wait())
        finally:
            idle_waiter.cancel()
            if not main.done():
                main.cancel()

    async def _main(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        _current_fid.set(ROOT_FID)
        result = self.workflow.fn(self, *args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    # --- frame methods (sync allocation, async body) -------------------------

    def step(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        name: str | None = None,
        key: str | None = None,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Awaitable[Any]:
        name = name or fn.__name__
        fid = self._allocate(name, key)
        return self._run_step(
            fid,
            FrameKind.STEP,
            name,
            retry or RetryPolicy(),
            lambda: fn(*args, **kwargs),
            {"args": list(args), "kwargs": kwargs},
        )

    def send(
        self,
        channel: str,
        payload: Any,
        *,
        correlation: str | None = None,
        scope: Scope = "execution",
        name: str = "send",
        key: str | None = None,
    ) -> Awaitable[int]:
        fid = self._allocate(name, key)
        full = self._channel_name(channel, scope)

        async def do() -> int:
            prov = self._prov(FrameKind.STEP, name, _current_attempt.get())
            # `seq=0` here is a placeholder: every Channel port overwrites it with the
            # log-assigned sequence number before the message is durable (03-ports.md
            # section 8), so no caller ever observes this literal.
            return await self._ports.channel.send(Message(full, 0, payload, prov, correlation))  # pragma: no mutate

        digest_input = {"channel": full, "payload": payload, "correlation": correlation}  # pragma: no mutate
        return self._run_step(fid, FrameKind.STEP, name, RetryPolicy(), do, digest_input)

    def announce(
        self, kind: str, payload: dict[str, Any], *, name: str = "announce", key: str | None = None
    ) -> Awaitable[int]:
        fid = self._allocate(name, key)

        async def do() -> int:
            prov = self._prov(FrameKind.STEP, name, _current_attempt.get())
            return await self._ports.control.announce(Entry.announce(prov, fid, kind, payload))

        digest_input = {"kind": kind, "payload": payload}
        return self._run_step(fid, FrameKind.STEP, name, RetryPolicy(), do, digest_input)

    def enqueue(
        self,
        queue: str,
        payload: Any,
        *,
        task_key: str,
        kind: TaskKind = TaskKind.DELEGATE,
        reason: str = "delegate",
        name: str = "enqueue",
        key: str | None = None,
    ) -> Awaitable[str]:
        """A step that puts a task on a queue. The memo is the task id.

        `task_key` is the task's dedup discriminator (see Task). `key` is this frame's key.
        """
        fid = self._allocate(name, key)
        check_queue(queue)

        async def do() -> str:
            prov = self._prov(FrameKind.STEP, name, _current_attempt.get())
            task = Task(
                queue=queue,
                kind=kind,
                target=FrameRef(self.eid, fid),
                reason=reason,
                enqueued_by=prov,
                key=task_key,
                payload=payload,
            )
            if await self._ports.queue.enqueue(task):
                await self._ports.control.announce(Entry.task_enqueued(task))
            return task.task_id

        digest_input = {"queue": queue, "key": task_key, "kind": kind.value, "payload": payload}  # pragma: no mutate
        return self._run_step(
            fid, FrameKind.STEP, name, RetryPolicy(max_attempts=3), do, digest_input
        )

    def withdraw(
        self, queue: str, task_id: str, *, name: str = "withdraw", key: str | None = None
    ) -> Awaitable[bool]:
        """A step that removes a task from a queue. The memo is whether a task was removed."""
        fid = self._allocate(name, key)
        check_queue(queue)

        async def do() -> bool:
            claimed = await self._ports.queue.take(queue, task_id, self._site.worker_id, 30.0)
            if claimed is None:
                return False
            await self._ports.queue.ack(claimed)
            return True

        digest_input = {"queue": queue, "task_id": task_id}
        return self._run_step(
            fid, FrameKind.STEP, name, RetryPolicy(max_attempts=3), do, digest_input
        )

    def receive(
        self,
        channel: str,
        *,
        timeout: timedelta | None = None,
        scope: Scope = "execution",
        name: str = "receive",
        key: str | None = None,
    ) -> Awaitable[Message | None]:
        fid = self._allocate(name, key)
        full = self._channel_name(channel, scope)
        digest_input = {
            "channel": full,
            "timeout_s": None if timeout is None else timeout.total_seconds(),
        }
        return self._run_receive(
            fid, FrameKind.RECEIVE, name, full, Condition.channel(full), timeout, digest_input
        )

    def sleep(
        self, duration: timedelta, *, name: str = "sleep", key: str | None = None
    ) -> Awaitable[None]:
        fid = self._allocate(name, key)
        return self._run_sleep(fid, name, {"duration_s": duration.total_seconds()}, duration, None)

    def sleep_until(
        self, instant: Timestamp, *, name: str = "sleep_until", key: str | None = None
    ) -> Awaitable[None]:
        fid = self._allocate(name, key)
        return self._run_sleep(fid, name, {"until": instant.to_iso()}, None, instant)

    def child(
        self,
        workflow_fn: Callable[..., Any],
        /,
        *args: Any,
        name: str | None = None,
        key: str | None = None,
        queue: str = "default",
        **kwargs: Any,
    ) -> Awaitable[Any]:
        if self._workflow_of is None:
            raise RuntimeError("Context has no workflow registry: child() is unavailable")
        wf = self._workflow_of(workflow_fn)
        # A workflow name may hold `.`, a frame name may not (01, sections 3.1
        # and 7). The function's name is a Python identifier, so it always can.
        name = name or str(getattr(workflow_fn, "__name__", None) or wf.name)
        fid = self._allocate(name, key)
        return self._run_child(fid, name, wf, list(args), kwargs, queue)

    def gather(self, *aws: Awaitable[Any]) -> Awaitable[list[Any]]:
        return asyncio.gather(*aws)

    def now(self) -> Awaitable[Timestamp]:
        fid = self._allocate("now", None)

        async def do() -> Timestamp:
            return Timestamp.from_iso(
                await self._run_step(
                    fid, FrameKind.STEP, "now", RetryPolicy(), lambda: self._clock().to_iso(), {}
                )
            )

        return do()

    def random(self) -> Awaitable[float]:
        fid = self._allocate("random", None)
        return self._run_step(fid, FrameKind.STEP, "random", RetryPolicy(), _random.random, {})

    def uuid(self) -> Awaitable[str]:
        fid = self._allocate("uuid", None)
        return self._run_step(
            fid, FrameKind.STEP, "uuid", RetryPolicy(), lambda: _uuid.uuid4().hex, {}
        )

    # --- frame bodies -----------------------------------------------------------

    async def _run_step(
        self,
        fid: str,
        kind: FrameKind,
        name: str,
        retry: RetryPolicy,
        thunk: Callable[[], Any],
        digest_input: Any,
    ) -> Any:
        args_digest = digest(digest_input)
        self._check_digest(fid, args_digest)
        memo = self.memo
        if fid in memo.memos:
            return memo.memos[fid].value

        while True:
            attempt = memo.next_attempt(fid)
            if attempt > retry.max_attempts:
                raise StepFailed(fid, memo.failures[fid][-1])
            retry_at = memo.retry_at.get(fid)
            if retry_at is not None:
                due = Timestamp.from_iso(retry_at)
                prov = self._prov(kind, name, attempt)
                on = Condition.timer(Timer(due, FrameRef(self.eid, fid)).timer_id)
                if self._clock() < due:
                    await self._wait(fid, on, due, prov, attempt)
                await self._append(Entry.frame_fulfilled(prov, fid, attempt, on, None))
            self._check_cancel()
            prov = self._prov(kind, name, attempt)
            await self._append(
                Entry.frame_started(prov, fid, kind.value, name, args_digest, attempt)
            )

            ok, outcome, exc = await self._execute(fid, attempt, thunk)
            if ok:
                await self._append(Entry.frame_completed(prov, fid, attempt, outcome))
                return outcome
            assert isinstance(outcome, Failed)

            retry_ok = retry.allows_retry(attempt, outcome.retryable)
            delay = retry.delay_after(attempt) if retry_ok else timedelta(0)
            next_at = self._clock() + delay if delay > timedelta(0) else None
            await self._append(
                Entry.frame_failed(
                    prov, fid, attempt, outcome, None if next_at is None else next_at.to_iso()
                )
            )
            if not retry_ok:
                raise StepFailed(fid, outcome) from exc
            if next_at is not None:
                on = Condition.timer(Timer(next_at, FrameRef(self.eid, fid)).timer_id)
                await self._wait(fid, on, next_at, prov, attempt)

    async def _execute(
        self, fid: str, attempt: int, thunk: Callable[[], Any]
    ) -> tuple[bool, Any, BaseException | None]:
        """Run the thunk as a live frame. Return (ok, value | Failed, exception)."""
        fid_token = _current_fid.set(fid)
        attempt_token = _current_attempt.set(attempt)
        self._active += 1
        try:
            # What the step logs is kept as evidence of this attempt and written
            # when the attempt ends (specs/03-ports.md, section 12). The
            # failure line is inside: it is the one a reader wants most.
            async with self._evidence.collecting(EvidenceRef(self.eid, fid, attempt)):
                try:
                    with bound(fid=fid, attempt=attempt):
                        log.debug("frame_started")
                        result = thunk()
                        if inspect.isawaitable(result):
                            result = await result
                        log.debug("frame_completed")
                        return True, result, None
                except Cancelled, NondeterminismError, StepFailed, asyncio.CancelledError, _Never:
                    raise
                except Exception as exc:
                    retryable = not isinstance(exc, NonRetryableError)
                    log.info(
                        "frame_failed",
                        fid=fid,
                        attempt=attempt,
                        error_type=type(exc).__name__,
                        retryable=retryable,
                    )
                    return False, Failed.from_exception(exc, retryable=retryable), exc
        finally:
            self._active -= 1
            _current_attempt.reset(attempt_token)
            _current_fid.reset(fid_token)
            self._maybe_idle()

    async def _run_receive(
        self,
        fid: str,
        kind: FrameKind,
        name: str,
        channel: str,
        on: str,
        timeout: timedelta | None,
        digest_input: Any,
    ) -> Message | None:
        args_digest = digest(digest_input)
        self._check_digest(fid, args_digest)
        memo = self.memo
        ref = FrameRef(self.eid, fid)  # pragma: no mutate
        if fid in memo.fulfilled:
            seq = memo.fulfilled[fid]
            if seq is None:
                return None
            for m in await self._ports.channel.read(channel, after=seq - 1):  # pragma: no mutate
                if m.seq == seq:
                    return m
            raise RuntimeError(f"channel {channel} lost message {seq} consumed by {fid}")

        prov = self._prov(kind, name, 1)
        if fid not in memo.digests:
            await self._append(Entry.frame_started(prov, fid, kind.value, name, args_digest, 1))

        messages = await self._ports.channel.read(channel, after=memo.last_consumed(channel))
        if messages:
            m = messages[0]
            await self._append(Entry.frame_fulfilled(prov, fid, 1, on, m.seq))
            await self._ports.channel.clear_wait(channel, ref)
            return m

        deadline: Timestamp | None = None
        if fid in memo.deadlines:
            deadline = Timestamp.from_iso(memo.deadlines[fid])
        elif timeout is not None:
            deadline = self._clock() + timeout
        if deadline is not None and self._clock() >= deadline:
            await self._append(Entry.frame_fulfilled(prov, fid, 1, on, None))
            await self._ports.channel.clear_wait(channel, ref)
            return None
        await self._wait(fid, on, deadline, prov, 1)
        raise AssertionError("unreachable")  # pragma: no mutate

    async def _run_sleep(
        self,
        fid: str,
        name: str,
        digest_input: Any,
        duration: timedelta | None,
        until: Timestamp | None,
    ) -> None:
        args_digest = digest(digest_input)
        self._check_digest(fid, args_digest)
        memo = self.memo
        if fid in memo.fulfilled:
            return None
        prov = self._prov(FrameKind.SLEEP, name, 1)
        if fid not in memo.digests:
            await self._append(
                Entry.frame_started(prov, fid, FrameKind.SLEEP.value, name, args_digest, 1)
            )
        if fid in memo.deadlines:
            due = Timestamp.from_iso(memo.deadlines[fid])
        elif until is not None:
            due = until
        else:
            assert duration is not None
            due = self._clock() + duration
        on = Condition.timer(Timer(due, FrameRef(self.eid, fid)).timer_id)
        if self._clock() >= due:
            await self._append(Entry.frame_fulfilled(prov, fid, 1, on, None))
            return None
        await self._wait(fid, on, due, prov, 1)
        raise AssertionError("unreachable")  # pragma: no mutate

    async def _run_child(
        self,
        fid: str,
        name: str,
        wf: WorkflowRef,
        args: list[Any],
        kwargs: dict[str, Any],
        queue: str,
    ) -> Any:
        ref = FrameRef(self.eid, fid)
        child_eid = ref.child_eid
        digest_input = {"workflow": wf.name, "version": wf.version, "args": args, "kwargs": kwargs}

        async def start() -> str:
            if self._starter is None:
                raise NonRetryableError("Context has no ChildStarter: child() is unavailable")
            execution = Execution(
                eid=child_eid,
                workflow=wf.name,
                version=wf.version,
                args={"args": args, "kwargs": kwargs},
                created_by=self._prov(FrameKind.STEP, "start", _current_attempt.get()),
                parent=FrameRef(self.eid, fid),
            )
            await self._starter.start_child(execution, queue)
            return str(child_eid)  # pragma: no mutate

        token = _current_fid.set(fid)
        try:
            start_fid = self._allocate("start", None)
        finally:
            _current_fid.reset(token)
        await self._run_step(
            start_fid, FrameKind.STEP, "start", RetryPolicy(max_attempts=3), start, digest_input
        )

        message = await self._run_receive(
            fid,
            FrameKind.CHILD,
            name,
            ref.child_channel,
            Condition.child(str(child_eid)),
            None,
            digest_input,
        )
        assert message is not None
        body = message.payload
        if body.get("status") != "completed":
            raise ChildFailed(child_eid, body.get("status", "unknown"), body.get("error"))
        return body.get("value")

    # --- waiting and idle detection ----------------------------------------------

    async def _wait(
        self, fid: str, on: str, deadline: Timestamp | None, prov: Provenance, attempt: int
    ) -> None:
        """Park this frame for the rest of the run. Never returns normally."""
        if self.memo.suspended.get(fid) != on:
            await self._append(
                Entry.frame_suspended(
                    prov, fid, attempt, on, None if deadline is None else deadline.to_iso()
                )
            )
        self._waiting[fid] = Wait(fid, on, deadline)
        # A diagnostic line only: nothing reads it back, so its exact fields are not
        # observable. Kept on one physical line so a single trailing pragma covers it
        # (mutmut's bare pragma only suppresses mutations on a statement's own first
        # line, which a multi-line call's inner argument lines are not).
        deadline_iso = None if deadline is None else deadline.to_iso()  # pragma: no mutate
        log.debug("frame_suspended", fid=fid, on=on, deadline=deadline_iso)  # pragma: no mutate
        self._maybe_idle()
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            raise _Never() from None
        raise AssertionError("unreachable")  # pragma: no mutate

    def _maybe_idle(self) -> None:
        if self._idle is not None and self._active == 0 and self._waiting:
            self._idle.set()

    # --- internals ------------------------------------------------------------------

    def _allocate(self, name: str, key: str | None) -> str:
        parent = _current_fid.get()
        allocator = self._allocators.get(parent)
        if allocator is None:
            allocator = self._allocators[parent] = FrameIdAllocator(parent)
        return allocator.allocate(name, key)

    def _channel_name(self, channel: str, scope: Scope) -> str:
        if scope == "execution":
            return execution_channel(self.eid, channel)
        return check_channel(channel)

    def _check_digest(self, fid: str, computed: str) -> None:
        recorded = self.memo.check_digest(fid, computed)
        if recorded is not None:
            raise NondeterminismError(fid, recorded, computed)

    def _check_cancel(self) -> None:
        if self._cancel_requested():
            raise Cancelled(f"execution {self.eid} cancelled")

    def _prov(self, kind: FrameKind, name: str, attempt: int) -> Provenance:
        code = Code(
            workflow=self.workflow.name,
            version=self.workflow.version,
            frame_kind=kind.value,
            frame_name=name,
            code_ref=self._code_ref,
        )
        return Provenance(
            actor=self._actor, site=self._site, code=code, attempt=attempt, at=self._clock()
        )

    async def _append(self, entry: Entry) -> int:
        seq = await self._ports.journal.append(self.eid, entry)
        self.memo.apply(Sequenced(seq, entry))
        return seq


__all__ = [
    "ChildStarter",
    "Context",
    "Raised",
    "Returned",
    "RunOutcome",
    "Suspended",
    "Wait",
    "WorkflowRef",
]

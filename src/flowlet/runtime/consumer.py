"""The consumer of a DELEGATE task. See docs/specs/10-agent-runner.md sections 2 to 5.

A delegate task leaves the engine. Someone outside takes it, does the work,
and answers on the reply channel. This is that someone, minus the work: the
loop, the ownership of a long attempt, recovery, and cancel.

The work is a `Handler`. An agent runner is one (`flowlet-runner`); a group
of humans behind a form is another.

Nothing here appends to a journal or writes an execution record. A consumer's
only writes are the reply message and the state of its own task lease.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol

from flowlet.domain import (
    Actor,
    ClaimedTask,
    DelegateTask,
    LeaseLost,
    Provenance,
    TaskKind,
)
from flowlet.log import bound, get_logger

log = get_logger("flowlet.consumer")

CANCEL_KEY = "cancel_requested"
HANDLE_KEY = "handle"


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """See docs/specs/10-agent-runner.md section 11."""

    queues: tuple[str, ...] = ("agents",)
    # The holder is stable and configured: a restart must reuse it, or it
    # cannot attach to what it still holds (section 4.3).
    holder: str = "consumer"
    ttl: float = 300.0
    poll_interval: float = 1.0
    grace: float = 120.0
    nack_delay: timedelta = timedelta(seconds=30)
    conflict_delay: timedelta = timedelta(seconds=10)


class Refused(Exception):
    """The consumer cannot take this task now. The task goes back on the queue."""

    def __init__(self, reason: str, delay: timedelta | None = None) -> None:
        super().__init__(reason)
        self.delay = delay


@dataclass
class Held:
    """One task this consumer owns, and the state it keeps in the lease.

    The lease document is the whole ownership model of a long attempt: it
    fences, it survives a restart, and its state carries whatever the consumer
    must find again (section 4.1).
    """

    claimed: ClaimedTask
    task: DelegateTask
    adopted: bool = False  # taken over from a dead holder, with its handle

    @property
    def lease(self) -> Any:
        return self.claimed.lease

    @property
    def task_id(self) -> str:
        return self.claimed.task.task_id

    @property
    def queue(self) -> str:
        return self.claimed.task.queue

    @property
    def state(self) -> dict[str, Any]:
        value = self.lease.state
        return dict(value) if isinstance(value, dict) else {}

    @property
    def handle(self) -> Any:
        return self.state.get(HANDLE_KEY)

    @property
    def cancel_requested(self) -> bool:
        return self.state.get(CANCEL_KEY) is not None

    async def write_handle(self, handle: Any) -> None:
        """Record the address of the work, before the work can outlive us."""
        await self.lease.update_state(lambda s: {**(s or {}), HANDLE_KEY: handle})


class Handler(Protocol):
    """The work. Every method takes the `Held` task it belongs to.

    `start` must be write-ahead: whatever it starts is addressable by the
    handle it returns, and the consumer writes that handle before the next
    heartbeat (section 4.2).
    """

    async def prepare(self, held: Held) -> None:
        """Acquire what the work needs. Raise `Refused` on a conflict."""
        ...

    async def start(self, held: Held) -> Any:
        """Start the work. Return a JSON-compatible handle."""
        ...

    async def reattach(self, held: Held, handle: Any) -> Any | None:
        """Re-attach to live work. None when it is gone. Never starts anything."""
        ...

    async def poll(self, held: Held, handle: Any) -> Any | None:
        """The reply payload when the work is done, None while it runs."""
        ...

    async def interrupt(self, held: Held, handle: Any, *, grace: float) -> Any:
        """Stop the work within `grace`, and return the reply payload to deliver."""
        ...

    async def terminate(self, held: Held, handle: Any) -> None:
        """Stop work this consumer will not wait for. Never raises."""
        ...

    async def release(self, held: Held) -> None:
        """Give back what `prepare` acquired. Never raises."""
        ...


@dataclass
class ConsumerReport:
    delivered: list[str] = field(default_factory=list)
    refused: list[str] = field(default_factory=list)
    recovered: list[str] = field(default_factory=list)
    terminated: list[str] = field(default_factory=list)


class Consumer:
    """Takes delegate tasks from queues and answers them.

    One task at a time, per the rule that one session holds one task
    (section 2). Run several consumers for more.
    """

    def __init__(
        self,
        engine: Any,
        handler: Handler,
        config: ConsumerConfig | None = None,
        *,
        actor: Actor | None = None,
    ) -> None:
        self.engine = engine
        self.handler = handler
        self.config = config or ConsumerConfig()
        self.actor = actor or Actor.worker(self.config.holder)
        self.report = ConsumerReport()

    @property
    def ports(self) -> Any:
        return self.engine.ports

    # --- the loop ---------------------------------------------------------------

    async def run_once(self) -> bool:
        """Take one task and see it through. False when every queue was empty."""
        claimed = await self._dequeue()
        if claimed is None:
            return False
        held = Held(claimed, DelegateTask.from_task_payload(claimed.task.payload))
        with bound(task_id=held.task_id, queue=held.queue, eid=str(held.task.eid)):
            await self._serve(held)
        return True

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        await self.recover()
        while not stop.is_set():
            try:
                busy = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("consumer_iteration_failed", holder=self.config.holder)
                busy = False
            if not busy:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self.config.poll_interval)

    async def _dequeue(self) -> ClaimedTask | None:
        for queue in self.config.queues:
            claimed: ClaimedTask | None = await self.ports.queue.dequeue(
                queue, self.config.holder, self.config.ttl
            )
            if claimed is None:
                continue
            if claimed.task.kind is not TaskKind.DELEGATE:
                # A start, a resume or a step belongs to a worker of the engine.
                await self.ports.queue.nack(claimed, self.config.nack_delay)
                continue
            return claimed
        return None

    # --- recovery ---------------------------------------------------------------

    async def recover(self) -> list[str]:
        """Take back the tasks this holder still owns, after a restart.

        `attach` writes nothing and does not bump the epoch: this is the same
        ownership period continuing, after an interruption of the process that
        held it (section 4.3).
        """
        taken: list[str] = []
        for queue in self.config.queues:
            for task in await self.ports.queue.pending(queue):
                claimed = await self.ports.queue.attach(
                    queue, task.task_id, self.config.holder, self.config.ttl
                )
                if claimed is None:
                    continue
                held = Held(claimed, DelegateTask.from_task_payload(claimed.task.payload))
                taken.append(held.task_id)
                self.report.recovered.append(held.task_id)
                log.info("task_recovered", task_id=held.task_id, epoch=claimed.lease.epoch)
                with bound(task_id=held.task_id, queue=queue):
                    await self._serve(held, recovering=True)
        return taken

    # --- one task ---------------------------------------------------------------

    async def _serve(self, held: Held, *, recovering: bool = False) -> None:
        inherited = held.handle
        if inherited is not None and not recovering:
            # A steal: the dead holder's handle came with the lease state. The
            # work may still be alive, and nobody else will read its answer.
            held.adopted = True
            log.info("handle_inherited", task_id=held.task_id)  # pragma: no mutate

        try:
            await self.handler.prepare(held)
        except Refused as refusal:
            await self._give_back(held, refusal)
            return

        handle = None
        try:
            if inherited is not None:
                handle = await self.handler.reattach(held, inherited)
                if handle is None:
                    # Whoever does not adopt an inherited handle must terminate
                    # it, or a hosted session runs and bills forever (section 4.4).
                    await self._terminate(held, inherited)
            if handle is None:
                handle = await self.handler.start(held)
                await held.write_handle(handle)
            payload = await self._watch(held, handle)
        except LeaseLost:
            # Fenced: the thief owns the task now. Deliver nothing, ack nothing.
            log.warning("task_lease_lost", task_id=held.task_id)  # pragma: no mutate
            await self.handler.release(held)
            return
        except Exception as exc:
            log.exception("work_failed", task_id=held.task_id)  # pragma: no mutate
            payload = {"outcome": "failed", "error": f"{type(exc).__name__}: {exc}"}
            if handle is not None:
                await self._terminate(held, handle)

        await self._answer(held, payload)

    async def _watch(self, held: Held, handle: Any) -> Any:
        """Heartbeat until the work finishes, or a cancel reaches us.

        The renew is what observes the cancel: a cooperative write into the
        task lease is absorbed by the next renew, and fences nobody (section 5).
        """
        interval = max(0.05, self.config.ttl / 3)
        elapsed = 0.0
        while True:
            payload = await self.handler.poll(held, handle)
            if payload is not None:
                return payload
            await asyncio.sleep(min(interval, 1.0))
            elapsed += min(interval, 1.0)
            if elapsed >= interval:
                elapsed = 0.0
                await held.lease.renew()  # LeaseLost propagates: we were fenced
                if held.cancel_requested:
                    log.info("cancel_seen", task_id=held.task_id)
                    return await self.handler.interrupt(held, handle, grace=self.config.grace)

    async def _answer(self, held: Held, payload: Any) -> None:
        """Deliver, then ack. The order matters (section 3).

        A consumer that acks first and stops loses the answer. One that
        delivers first and stops answers twice at worst, and the receive frame
        consumes the first message only.
        """
        try:
            await self.engine.deliver(
                held.task.eid, held.task.reply_channel, payload, by=self.actor
            )
            self.report.delivered.append(held.task_id)
            log.info("reply_delivered", task_id=held.task_id, eid=str(held.task.eid))
            await self.ports.queue.ack(held.claimed)
        except LeaseLost:
            log.warning("task_lease_lost_before_ack", task_id=held.task_id)
        finally:
            await self.handler.release(held)

    async def _give_back(self, held: Held, refusal: Refused) -> None:
        delay = refusal.delay or self.config.conflict_delay
        with suppress(LeaseLost):
            await self.ports.queue.nack(held.claimed, delay)
        self.report.refused.append(held.task_id)
        log.info("task_refused", task_id=held.task_id, reason=str(refusal))

    async def _terminate(self, held: Held, handle: Any) -> None:
        with suppress(Exception):
            await self.handler.terminate(held, handle)
        self.report.terminated.append(held.task_id)
        log.info("work_terminated", task_id=held.task_id)


async def request_cancel(engine: Any, queue: str, task_id: str, by: Actor) -> None:
    """Ask the consumer of one task to stop. It fences nobody (section 5)."""
    prov: Provenance = engine.provenance(by, frame_name="cancel")
    await engine.ports.queue.request_cancel(queue, task_id, prov)


__all__ = [
    "CANCEL_KEY",
    "HANDLE_KEY",
    "Consumer",
    "ConsumerConfig",
    "ConsumerReport",
    "Handler",
    "Held",
    "Refused",
    "request_cancel",
]

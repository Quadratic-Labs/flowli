"""Lease renewal and cooperative cancellation for running flows.

``flowlet.heartbeat()`` is the single user-facing call: inside a worker it
renews the run's lease (epoch-fenced by the cairndb engine) and observes the
run's ``cancel`` signal; outside any worker context it is a no-op, so flows
remain plain callables.

Cancellation travels out-of-band: the API sets an immutable signal object
under ``signals/<flow>/<run_id>/``, never touching the lease document.  The
lease document therefore has exactly one writer — its holder — and a failed
renewal always means the lease was fenced (stolen after expiry), never a
flag to reinterpret.
"""
import contextvars
import logging
import time

from attrs import define, field
from cairndb.core.exceptions import LeaseLost as LeaseLost  # re-export

from flowlet.models import Effect
from flowlet.repository.effects import EffectRepository
from flowlet.repository.messages import MessageRepository
from flowlet.repository.signals import CANCEL, SignalRepository
from flowlet.repository.state import StateLease
from flowlet.types import Timestamp

logger = logging.getLogger(__name__)

DEFAULT_MIN_BEAT_INTERVAL = 5.0  # seconds between effective (I/O) heartbeats


class RunCancelled(Exception):
    """Raised inside a flow when its run has been cancelled via the API.

    The worker catches this and finalizes the run as ``canceled``.  Flows
    may catch it themselves to release resources, but should re-raise.
    """


@define(kw_only=True)
class RunLease:
    """Live lease over a claimed run, owned by the executing worker.

    Attributes:
        lease: The owned StateLease; renewals and the terminal release go
            through it and are epoch-fenced.
        signals: Signal repository the cancel signal is read from.
        effects: Effect repository for exactly-once side-effect claims
            (``flowlet.effect``); optional outside worker-managed runs.
        messages: Message repository for the ordered channel
            (``flowlet.recv``); optional outside worker-managed runs.
        min_interval: Seconds below which heartbeat() calls are free no-ops,
            bounding state-store I/O regardless of call frequency.
    """

    lease: StateLease
    signals: SignalRepository
    effects: EffectRepository | None = None
    messages: MessageRepository | None = None
    min_interval: float = DEFAULT_MIN_BEAT_INTERVAL
    _last_beat: float = field(factory=time.monotonic, init=False)
    # recv ordinals per topic for THIS attempt: while the ordinal is within
    # the account's recorded consumptions, recv replays; past it, consumes.
    _recv_counts: dict[str, int] = field(factory=dict, init=False)

    def beat(self) -> dict[str, dict]:
        """Renew the lease and observe the obligation's pending signals.

        Throttled: within ``min_interval`` of the previous effective beat the
        call returns an empty mapping without any I/O.

        Returns:
            Pending signals, name → payload (``cancel``, ``interrupt``,
            controller-defined names).  Empty when none are set.

        Raises:
            LeaseLost: When the run is no longer owned by this worker.
        """
        if time.monotonic() - self._last_beat < self.min_interval:
            return {}
        self._last_beat = time.monotonic()

        self.lease.renew()
        obligation = self.lease.record.obligation
        return self.signals.list(obligation.flow_name, obligation.id)


_current_lease: contextvars.ContextVar[RunLease | None] = contextvars.ContextVar(
    "flowlet_current_lease", default=None
)


def bind_lease(lease: RunLease) -> contextvars.Token:
    """Bind *lease* as the ambient lease for the current context (worker use)."""
    return _current_lease.set(lease)


def unbind_lease(token: contextvars.Token) -> None:
    """Restore the previous ambient lease (worker use)."""
    _current_lease.reset(token)


def current_lease() -> RunLease | None:
    """Return the ambient RunLease, or None outside a worker-managed run."""
    return _current_lease.get()


def heartbeat(*, raise_on_cancel: bool = True) -> dict[str, dict]:
    """Renew the current run's lease and observe pending signals.

    Call between units of work in long-running flows.  Cheap to call often:
    actual state-store I/O happens at most once per ``min_interval``.
    Outside a worker-managed run (direct call, sync API execution, tests)
    this is a no-op returning an empty mapping.

    Args:
        raise_on_cancel: When True (default) a pending cancel signal raises
            :class:`RunCancelled`, so unmodified flows stop at their next
            heartbeat.  Pass False to receive it in the returned mapping
            and shut down gracefully.

    Returns:
        Pending signals, name → payload.  ``cancel`` and ``interrupt`` are
        kernel names; controllers may define others (steering payloads).
        Empty when nothing is pending.

    Raises:
        RunCancelled: Cancel requested and ``raise_on_cancel`` is True.
        LeaseLost: The run was reclaimed by another owner; stop immediately.

    Example:
        >>> for batch in batches:
        ...     pending = flowlet.heartbeat()
        ...     if "interrupt" in pending:
        ...         write_resume_artifact(); return
        ...     process(batch)
    """
    lease = _current_lease.get()
    if lease is None:
        return {}
    pending = lease.beat()
    if CANCEL in pending and raise_on_cancel:
        raise RunCancelled(
            f"obligation {lease.lease.record.obligation.id} cancelled"
        )
    return pending


def effect(name: str, body, *, occurrence: str = "1"):
    """Run a side-effect exactly once per occurrence, recorded in the account.

    The occurrence key is derived from the obligation and the effect's
    identity — never the attempt — so a retried attempt *converges* on the
    recorded result instead of re-firing the side effect.  A deliberate
    repetition (authorized re-send) passes a new ``occurrence``.

    Semantics are at-least-once execution, exactly-once recording (the DBOS
    step contract): in a crash window the body may run twice, so keep effect
    bodies idempotent where possible.

    Outside a worker-managed run this degrades to a plain call.

    Args:
        name: Stable effect name (e.g. ``"send_email"``).
        body: Zero-argument callable producing a JSON-safe result.
        occurrence: Occurrence discriminator; default "1".

    Returns:
        The recorded result — this execution's, or a previous one's.

    Example:
        >>> def my_flow(user_id: int):
        ...     flowlet.effect("welcome_email", lambda: send_mail(user_id))
    """
    lease = _current_lease.get()
    if lease is None or lease.effects is None:
        return body()

    record = lease.lease.record
    obligation = record.obligation
    result, produced = lease.effects.memoize(
        obligation.flow_name,
        obligation.id,
        name,
        body,
        occurrence=occurrence,
        executor=lease.lease.holder,
    )
    if produced:
        record.record_effect(
            Effect(
                name=name,
                occurrence=occurrence,
                attempt_n=len(record.attempts),
                produced_at=Timestamp.now(),
                result_ref=result,
            )
        )
        lease.lease.write(record)  # fenced account update
    return result


def recv(topic: str) -> dict | None:
    """Consume the next message on *topic*, checkpointed in the account.

    The obligation's message channel: senders append ordered messages
    (POST ``/runs/{run_id}/messages/{topic}``, or
    ``MessageRepository.send``); the running attempt consumes them in send
    order.  Each consumption is recorded in the account under the lease
    fence, so consumption is exactly-once per obligation and deterministic
    across retries: a retried attempt's recv calls first *replay* the
    recorded consumptions in order — returning the identical messages the
    failed attempt acted on — and only then consume fresh ones.

    Non-blocking: returns None when no unconsumed message is pending.  For
    a durable wait, end the attempt and park the obligation (admission or
    adjudication gates), optionally with a timer for the timeout — waiting
    is an obligation state, never a code position.

    Outside a worker-managed run this is a no-op returning None.

    Args:
        topic: Channel name (e.g. ``"steering"``).

    Returns:
        The message as ``{"id", "topic", "body", "actor", "sent_at"}``, or
        None when nothing is pending.

    Raises:
        LeaseLost: The run was reclaimed by another owner; stop immediately.
        LookupError: A recorded consumption's message object is missing
            from the store (replay is impossible — the store was tampered
            with or cleaned prematurely).

    Example:
        >>> while (msg := flowlet.recv("steering")) is not None:
        ...     apply_steering(msg["body"])
    """
    lease = _current_lease.get()
    if lease is None or lease.messages is None:
        return None

    record = lease.lease.record
    obligation = record.obligation
    seq = lease._recv_counts.get(topic, 0) + 1
    consumed = record.consumptions_for(topic)

    if seq <= len(consumed):
        # Replay: this attempt's Nth recv returns the recorded Nth message.
        entry = consumed[seq - 1]
        doc = lease.messages.read(
            obligation.flow_name, obligation.id, topic, entry.message_id
        )
        if doc is None:
            raise LookupError(
                f"consumed message {entry.message_id} on topic {topic!r} "
                "is missing from the store — cannot replay"
            )
        lease._recv_counts[topic] = seq
        return {
            "id": doc.id, "topic": topic, "body": doc.body,
            "actor": doc.actor, "sent_at": doc.sent_at,
        }

    # Fresh consume: the next message past the topic's cursor, recorded
    # under the fence before it is handed to the flow.
    after = consumed[-1].message_id if consumed else None
    fresh = lease.messages.list_topic(
        obligation.flow_name, obligation.id, topic, after=after
    )
    if not fresh:
        return None
    head = fresh[0]
    record.record_consumption(topic, head.id)
    lease.lease.write(record)  # fenced checkpoint — the recv is durable now
    lease._recv_counts[topic] = seq
    return {
        "id": head.id, "topic": topic, "body": head.body,
        "actor": head.actor, "sent_at": head.sent_at,
    }

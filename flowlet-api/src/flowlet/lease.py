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

from .models import Effect
from .repository.effects import EffectRepository
from .repository.signals import CANCEL, SignalRepository
from .repository.state import StateLease
from .types import Timestamp

logger = logging.getLogger(__name__)

DEFAULT_MIN_BEAT_INTERVAL = 5.0  # seconds between effective (I/O) heartbeats


# region @lease
# ---
# role: core
# intent: renew a running flow's lease and observe cancellation cooperatively
# description: >
#   RunLease is created by the worker at claim time and bound to a
#   contextvar for the duration of the flow call.  heartbeat() (module
#   function, exported as flowlet.heartbeat) is throttled to one effective
#   beat per min_interval: it renews the engine lease (LeaseLost propagates
#   when the run was reclaimed after expiry) and reads the cancel signal.
#   Flows that never call heartbeat() keep the original static-deadline
#   behaviour.
# rules:
#   - heartbeat() MUST be a no-op outside a worker-managed run context.
#   - LeaseLost MUST mean fenced ownership, nothing else — the lease
#     document is single-writer and renewals are never reinterpreted.
#   - RunCancelled MUST only be raised for a deliberate cancel signal.
#   - Cancellation MUST be observed from the signal object, never from the
#     lease payload.
# dependencies:
#   - state_repository
#   - signals_repository
# aliases:
#   - heartbeat
#   - run-lease
# triggers:
#   - how does a flow renew its lease
#   - how is a run cancelled
#   - how does cooperative cancellation work
# ---


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
        min_interval: Seconds below which heartbeat() calls are free no-ops,
            bounding state-store I/O regardless of call frequency.
    """

    lease: StateLease
    signals: SignalRepository
    effects: EffectRepository | None = None
    min_interval: float = DEFAULT_MIN_BEAT_INTERVAL
    _last_beat: float = field(factory=time.monotonic, init=False)

    def beat(self) -> bool:
        """Renew the lease and report whether cancellation was requested.

        Throttled: within ``min_interval`` of the previous effective beat the
        call returns False without any I/O.

        Returns:
            True when a cancel signal was observed, False otherwise.

        Raises:
            LeaseLost: When the run is no longer owned by this worker.
        """
        if time.monotonic() - self._last_beat < self.min_interval:
            return False
        self._last_beat = time.monotonic()

        self.lease.renew()
        obligation = self.lease.record.obligation
        return (
            self.signals.get(obligation.flow_name, obligation.id, CANCEL)
            is not None
        )


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


def heartbeat(*, raise_on_cancel: bool = True) -> bool:
    """Renew the current run's lease and observe cancellation.

    Call between units of work in long-running flows.  Cheap to call often:
    actual state-store I/O happens at most once per ``min_interval``.
    Outside a worker-managed run (direct call, sync API execution, tests)
    this is a no-op returning False.

    Args:
        raise_on_cancel: When True (default) a pending cancel signal raises
            :class:`RunCancelled`, so unmodified flows stop at their next
            heartbeat.  Pass False to receive the request as a return value
            and shut down gracefully.

    Returns:
        True when cancellation was requested (only with
        ``raise_on_cancel=False``), False otherwise.

    Raises:
        RunCancelled: Cancel requested and ``raise_on_cancel`` is True.
        LeaseLost: The run was reclaimed by another owner; stop immediately.

    Example:
        >>> for batch in batches:
        ...     flowlet.heartbeat()
        ...     process(batch)
    """
    lease = _current_lease.get()
    if lease is None:
        return False
    cancelled = lease.beat()
    if cancelled and raise_on_cancel:
        raise RunCancelled(
            f"obligation {lease.lease.record.obligation.id} cancelled"
        )
    return cancelled


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

# ---
# endregion

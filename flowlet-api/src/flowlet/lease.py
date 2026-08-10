"""Lease renewal and cooperative cancellation for running flows.

``flowlet.heartbeat()`` is the single user-facing call: inside a worker it
renews the run's lease deadline and observes the ``cancel_requested`` flag;
outside any worker context it is a no-op, so flows remain plain callables.

The cancellation channel is the CAS state write itself: the API sets
``cancel_requested`` with a conditional write, which bumps the state version.
The worker's next heartbeat write then fails its precondition, re-reads, and
discovers the flag — no polling channel beyond the state file is needed.
"""
import contextvars
import logging
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from attrs import define, field

from .models import RunState, RunStatus
from .types import Timestamp

if TYPE_CHECKING:
    from .repository.state import StateRepository

logger = logging.getLogger(__name__)

DEFAULT_MIN_BEAT_INTERVAL = 5.0  # seconds between effective (I/O) heartbeats


# region @lease
# ---
# role: core
# intent: renew a running flow's lease and observe cancellation cooperatively
# description: >
#   RunLease is created by the worker at claim time and bound to a
#   contextvar for the duration of the flow call.  heartbeat() (module
#   function, exported as flowlet.heartbeat) is throttled to one CAS write
#   per min_interval: a successful conditional write extends deadline_at;
#   a failed one is re-read to distinguish a cancel request (flag set by
#   the API) from lost ownership (reclaimed after lease expiry).  Flows
#   that never call heartbeat() keep the original static-deadline
#   behaviour.
# rules:
#   - heartbeat() MUST be a no-op outside a worker-managed run context.
#   - Lease renewal MUST go through a conditional state write; a lost CAS
#     MUST NOT be retried blindly — re-read and reinterpret first.
#   - RunCancelled MUST only be raised for a deliberate cancel request;
#     lost ownership raises LeaseLost.
#   - The worker MUST use lease.etag (not the claim etag) for its terminal
#     write, since every renewal advances the version.
# dependencies:
#   - models.run
#   - state_repository
#   - types.time
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


class LeaseLost(Exception):
    """Raised when the run's state no longer belongs to this worker.

    Happens when the lease expired mid-run and the run was reclaimed
    (sweeper or another worker).  The flow must stop; the worker discards
    the outcome without writing state it no longer owns.
    """


@define(kw_only=True)
class RunLease:
    """Live lease over a claimed run, owned by the executing worker.

    Attributes:
        state: The claimed RunState; ``deadline_at`` is advanced in place on
            every effective heartbeat.
        etag: Version of the last successful state write.  Terminal writes
            must use this value.
        state_repo: Repository used for conditional renewal writes.
        timeout_seconds: Lease duration; each renewal sets
            ``deadline_at = now + timeout_seconds``.
        min_interval: Seconds below which heartbeat() calls are free no-ops,
            bounding state-store I/O regardless of call frequency.
    """

    state: RunState
    etag: str
    state_repo: "StateRepository"
    timeout_seconds: int
    min_interval: float = DEFAULT_MIN_BEAT_INTERVAL
    _last_beat: float = field(factory=time.monotonic, init=False)

    def beat(self) -> bool:
        """Renew the lease and report whether cancellation was requested.

        Throttled: within ``min_interval`` of the previous effective beat the
        call returns False without any I/O.

        Returns:
            True when a cancel request was observed, False otherwise.

        Raises:
            LeaseLost: When the run is no longer owned by this worker.
        """
        if time.monotonic() - self._last_beat < self.min_interval:
            return False
        self._last_beat = time.monotonic()

        self.state.deadline_at = Timestamp(
            Timestamp.now().value + timedelta(seconds=self.timeout_seconds)
        )
        ok, new_etag = self.state_repo.write(self.state, self.etag)
        if ok:
            self.etag = new_etag
            return False
        return self._reinterpret_conflict()

    def _reinterpret_conflict(self) -> bool:
        """Decide what a failed renewal means: cancel request or lost lease.

        The only legitimate concurrent writer of an owned, unexpired run is
        the cancel endpoint, so a conflict where we still own the run means
        the flag was set; anything else means ownership is gone.

        Returns:
            True when the conflict was a cancel request.

        Raises:
            LeaseLost: When the current state is not ours anymore.
        """
        read = self.state_repo.read(self.state.flow_name, self.state.run_id)
        if read is None:
            raise LeaseLost(f"state for run {self.state.run_id} disappeared")
        current, current_etag = read
        if (
            current.status != RunStatus.running
            or current.worker_id != self.state.worker_id
            or current.attempt != self.state.attempt
        ):
            raise LeaseLost(
                f"run {self.state.run_id} reclaimed "
                f"(status={current.status}, worker={current.worker_id})"
            )
        # Still ours — adopt the concurrent write (cancel flag and version).
        self.state.cancel_requested = current.cancel_requested
        self.etag = current_etag
        if current.cancel_requested:
            return True
        # Unexpected but recoverable: retry the renewal once on the new version.
        ok, new_etag = self.state_repo.write(self.state, self.etag)
        if ok:
            self.etag = new_etag
        return False


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
        raise_on_cancel: When True (default) a pending cancel request raises
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
        raise RunCancelled(f"run {lease.state.run_id} cancelled")
    return cancelled

# ---
# endregion

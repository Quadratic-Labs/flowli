"""Account-backed job source — queue-less deployments poll the account.

Satisfies :class:`~flowlet.queue.JobQueueProtocol` with no queue
infrastructure at all: the state directory *is* the work list.

- ``enqueue`` records the obligation in the account store (put-if-absent,
  unheld) — submitting is creating the obligation, there is no message.
  A duplicate submission loses the claim and converges, exactly like a
  duplicate wake-up message in queue mode.
- ``dequeue`` scans the active state directory for a claimable obligation
  (open, unheld, past its backoff window, not paused) and synthesizes the
  wake-up entirely from the account, the way the sweeper rebuilds lost
  messages.
- ``ack`` is a no-op — there is no message to remove; resolving the run's
  state was the whole point.

Trade-offs versus a real queue: dispatch latency is the worker's poll
interval, each poll lists and reads the active state documents, and
concurrent pollers may race on the same ready obligation (the lease CAS
arbitrates — one wins, the others skip).  Correctness is untouched: it
lives in the account either way.  Per-submission ``timeout_seconds`` is
not carried (the account does not record it); the worker default applies.
"""
import logging
from collections.abc import Callable
from uuid import UUID

from attrs import define, field

from flowlet.models import FlowJob, Obligation, ObligationRecord, ObligationStatus
from flowlet.repository.signals import SignalRepository
from flowlet.repository.state import StateRepository, StateView
from flowlet.types import Timestamp

logger = logging.getLogger(__name__)


def _claimable(view: StateView, now: Timestamp) -> bool:
    """Whether a worker should claim this obligation now.

    Open and unheld, with the crash case (dead holder's in-flight attempt)
    claimable immediately — the claim transition does the crash accounting
    — and a parked retry gated by the account-derived backoff window,
    measured from the release time the lease document already records.
    """
    record = view.record
    status = record.obligation.status
    if status.is_closed() or status in (
        ObligationStatus.awaiting_review,
        ObligationStatus.held,
    ):
        return False
    if view.held(now):
        return False
    if record.open_attempt is not None or not record.attempts:
        return True
    if view.deadline_at is None:
        return True
    elapsed = (now.value - view.deadline_at.value).total_seconds()
    return elapsed >= record.backoff_seconds()


@define(slots=True, kw_only=True)
class AccountJobSource:
    """Queue-less :class:`~flowlet.queue.JobQueueProtocol` over the account store.

    Attributes:
        state_repo: State repository the obligations live in.
        signals: Optional signal repository; when given, obligations under
            a paused scope are not handed out.
        review_policy_for: Per-flow review policy stamped on
            obligations created by ``enqueue`` (queue mode stamps it at the
            worker's first claim instead); None means auto.
    """

    state_repo: StateRepository
    signals: SignalRepository | None = field(default=None)
    review_policy_for: Callable[[str], str] | None = field(default=None)

    def enqueue(self, job: FlowJob, delay: int = 0) -> UUID:  # noqa: ARG002
        """Record the obligation in the account store, put-if-absent.

        ``delay`` is ignored: a fresh obligation is claimable immediately,
        and a retry's backoff is derived from the account at ``dequeue``
        time, so the delayed-message mechanism has nothing to carry.
        """
        policy = (
            self.review_policy_for(job.flow_name)
            if self.review_policy_for is not None
            else "auto"
        )
        record = ObligationRecord(
            obligation=Obligation(
                id=job.run_id,
                flow_name=job.flow_name,
                kwargs=job.kwargs,
                parent_id=job.parent_id,
                root_id=job.root_id,
                max_retries=job.max_retries,
                review_policy=policy,
                caused_by=job.caused_by,
                created_at=Timestamp.now(),
            )
        )
        created = self.state_repo.create(job.flow_name, job.run_id, record)
        logger.info(
            "account_obligation_recorded" if created else "account_obligation_exists",
            extra={"run_id": str(job.run_id), "flow_name": job.flow_name},
        )
        return job.job_id

    def dequeue(self, timeout: int | None = None) -> FlowJob | None:  # noqa: ARG002
        """Return a wake-up for the oldest claimable obligation, or None.

        ``timeout`` is ignored: there is no message claim window — the
        worker's lease acquisition is the only claim.
        """
        now = Timestamp.now()
        candidates = [
            view
            for view in self.state_repo.list_views()
            if _claimable(view, now)
        ]
        # uuid7 run_ids: chronological order = submission order.
        candidates.sort(key=lambda view: view.record.obligation.id)
        for view in candidates:
            obligation = view.record.obligation
            if self.signals is not None and self.signals.paused_scopes(
                flow_name=obligation.flow_name,
                run_id=obligation.id,
                parent_id=obligation.parent_id,
                root_id=obligation.root_id,
            ):
                continue
            return FlowJob(
                run_id=obligation.id,
                flow_name=obligation.flow_name,
                kwargs=obligation.kwargs,
                max_retries=obligation.max_retries,
                parent_id=obligation.parent_id,
                root_id=obligation.root_id,
                caused_by="account_poll",
            )
        return None

    def ack(self, job_id: UUID) -> None:
        """No-op — there is no message; the account was already resolved."""

    def get_queue_size(self) -> int:
        """Number of currently claimable obligations (paused scopes included)."""
        now = Timestamp.now()
        return sum(
            1 for view in self.state_repo.list_views() if _claimable(view, now)
        )

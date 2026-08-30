"""Append-only run event log — the audit trail of state transitions.

Every actor that moves a run through its lifecycle (API submit, worker
claim/finalize, sweeper recovery, cancel endpoint) appends one JSON line to

    ``runs/<flow_name>/<yyyy-mm-dd>/<obligation_id>/events.jsonl``

colocated with the run's span files, so the run folder remains the complete
durable record and archiving needs no extra step.  The state snapshot stays
authoritative for execution; this log is an observability side-channel and
its writes are strictly non-throwing.
"""
import json
import logging
from typing import Any
from uuid import UUID

from attrs import define
from cairndb.storage.base import BlobStorage

from flowlet.storage import append_lines, read_lines, run_prefix
from flowlet.transitions import TransitionFeed
from flowlet.types import Timestamp

logger = logging.getLogger(__name__)


@define(slots=True, kw_only=True)
class EventLog:
    """Writer for per-run lifecycle event streams.

    Attributes:
        store: CairnDB blob store containing the ``runs/`` tree — the same
            store the span exporter writes to.
        feed: Optional account-transition feed; when configured, every
            appended event also lands on the ordered ``transitions`` named
            log so reconcilers can tail lifecycle transitions with a cursor
            instead of polling (abstractions v0.3, delta 3).  Both sinks
            are non-throwing and neither gates the other.
    """

    store: BlobStorage
    feed: "TransitionFeed | None" = None

    def append(
        self,
        *,
        flow_name: str,
        obligation_id: UUID,
        event: str,
        actor: str,
        attempt: int | None = None,
        from_status: str | None = None,
        to_status: str | None = None,
        cause: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Append one lifecycle event to the run's events file.

        Args:
            flow_name: Flow the run belongs to.
            obligation_id: The run's UUID (uuid7 — determines the date partition).
            event: Event name, e.g. ``submitted``, ``claimed``, ``completed``,
                ``retry_scheduled``, ``failed``, ``canceled``,
                ``cancel_requested``, ``requeued``.
            actor: Who caused it — ``api``, a worker id, or ``sweeper``.
            attempt: Attempt number the event applies to, when meaningful.
            from_status: Run status before the transition.
            to_status: Run status after the transition.
            cause: Short machine-readable reason (e.g. ``max_retries_exceeded``).
            details: Extra structured context (kept small).
        """
        record: dict[str, Any] = {
            "ts": Timestamp.now().to_iso(),
            "obligation_id": str(obligation_id),
            "flow_name": flow_name,
            "event": event,
            "actor": actor,
        }
        if attempt is not None:
            record["attempt"] = attempt
        if from_status is not None:
            record["from"] = from_status
        if to_status is not None:
            record["to"] = to_status
        if cause is not None:
            record["cause"] = cause
        if details:
            record["details"] = details

        key = f"{run_prefix(flow_name, obligation_id)}/events.jsonl"
        if not append_lines(self.store, key, json.dumps(record, default=str) + "\n"):
            logger.warning(
                "run_event_append_failed",
                extra={"obligation_id": str(obligation_id), "event": event},
            )
        if self.feed is not None:
            self.feed.record(record)

    def read(self, flow_name: str, obligation_id: UUID) -> list[dict[str, Any]]:
        """Read a run's lifecycle events for display, in append order.

        Audit/display use only — never a source for execution decisions.
        Malformed lines are skipped; a missing file yields an empty list.

        Args:
            flow_name: Flow the run belongs to.
            obligation_id: The run's UUID.

        Returns:
            List of event records as plain dicts.
        """
        key = f"{run_prefix(flow_name, obligation_id)}/events.jsonl"
        records: list[dict[str, Any]] = []
        for line in read_lines(self.store, key):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning(
                    "run_event_parse_error", extra={"obligation_id": str(obligation_id)}
                )
        return records

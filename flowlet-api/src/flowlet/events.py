"""Append-only run event log — the audit trail of state transitions.

Every actor that moves a run through its lifecycle (API submit, worker
claim/finalize, sweeper recovery, cancel endpoint) appends one JSON line to

    ``runs/<flow_name>/<yyyy-mm-dd>/<run_id>/events.jsonl``

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

from .repository.log import run_folder
from .storage.types import StoragePath
from .types import Timestamp

logger = logging.getLogger(__name__)


# region @events.log
# ---
# role: storage
# intent: append run lifecycle transition events to the run folder
# description: >
#   RunEventLog.append() writes one JSON line per lifecycle event
#   ({ts, event, actor, attempt, from/to status, cause, details}) into the
#   run's folder under runs/, next to its span files.  Appends go through
#   the StoragePath open("a") interface so local and blob backends behave
#   identically.  The log has no correctness role: losing it loses history,
#   never state, and every append failure is swallowed after logging.
# rules:
#   - append() MUST NOT raise; failures are logged and dropped.
#   - Events MUST be appended, never rewritten — the file is append-only.
#   - The event log MUST NOT be read to make execution decisions; the
#     RunState snapshot remains the source of truth.  read() exists for
#     display/audit only.
# dependencies:
#   - log_repository
#   - storage.types
#   - types.time
# aliases:
#   - event-log
#   - run-events
# triggers:
#   - where are run transitions recorded
#   - how do I audit a run's lifecycle
# ---


@define(slots=True, kw_only=True)
class RunEventLog:
    """Writer for per-run lifecycle event files.

    Attributes:
        base_path: Storage root containing the ``runs/`` tree — the same
            root the span exporter writes under.
    """

    base_path: StoragePath

    def append(
        self,
        *,
        flow_name: str,
        run_id: UUID,
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
            run_id: The run's UUID (uuid7 — determines the date partition).
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
            "run_id": str(run_id),
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

        try:
            file = run_folder(self.base_path, flow_name, run_id) / "events.jsonl"
            file.parent.mkdir(parents=True, exist_ok=True)
            with file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, default=str) + "\n")
        except Exception:
            logger.warning(
                "run_event_append_failed",
                extra={"run_id": str(run_id), "event": event},
                exc_info=True,
            )

    def read(self, flow_name: str, run_id: UUID) -> list[dict[str, Any]]:
        """Read a run's lifecycle events for display, in append order.

        Audit/display use only — never a source for execution decisions.
        Malformed lines are skipped; a missing file yields an empty list.

        Args:
            flow_name: Flow the run belongs to.
            run_id: The run's UUID.

        Returns:
            List of event records as plain dicts.
        """
        file = run_folder(self.base_path, flow_name, run_id) / "events.jsonl"
        try:
            raw = file.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return []
        records: list[dict[str, Any]] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning(
                    "run_event_parse_error", extra={"run_id": str(run_id)}
                )
        return records

# ---
# endregion

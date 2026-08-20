"""Durable timers as swept documents — nothing in the engine fires at time T.

A timer is a small mutable document at
``timers/<flow_name>/<run_id>/<name>.json`` declaring when it is due and
what firing means: send a named signal to the obligation, enqueue a wake-up
message, or both.  The sweeper's pass fires due timers (see
``flowlet.sweeper``); firing is idempotent by construction — signal sends
converge on the first, duplicate wake-ups are dropped by the worker state
machine — so overlapping sweepers are harmless.

This is the recovery-as-cron pattern generalized: gate timeouts, grace
budgets, notification due-dates, and stall detection are all timer
documents with different payloads.
"""
import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from attrs import define
from cairndb.storage.base import BlobStorage

from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @timers_repository
# ---
# role: storage
# intent: durable timer documents fired by the sweeper's cron pass
# description: >
#   TimerRepository.set() writes (or reschedules — timers are mutable,
#   unlike signals) a named timer for an obligation; due() lists the timers
#   whose due_at has passed; clear() removes one after firing or when the
#   waiting condition resolved early.  Firing semantics (signal and/or
#   wake-up) are declared in the document; the sweeper executes them.
# rules:
#   - Timers are best-effort schedules: latency is bounded by the sweep
#     cadence, never better.
#   - Firing MUST be idempotent: duplicate fires converge (signals are
#     claims, wake-ups are droppable).
#   - Timers MUST be cleared by the firing pass; a timer that must repeat
#     is re-set by its controller.
# dependencies:
#   - types.time
# aliases:
#   - timers
#   - durable-timers
# triggers:
#   - how do gate timeouts work
#   - how are grace budgets enforced
# ---


@define(slots=True, kw_only=True)
class TimerDoc:
    """A parsed timer document.

    Attributes:
        flow_name: Flow the obligation belongs to.
        run_id: The obligation the timer concerns.
        name: Timer name (unique per obligation).
        due_at: When the timer becomes due.
        signal: Signal name to send on firing, or None.
        wakeup: Whether to enqueue a wake-up message on firing.
        details: Structured context forwarded into the signal payload.
    """

    flow_name: str
    run_id: UUID
    name: str
    due_at: Timestamp
    signal: str | None = None
    wakeup: bool = False
    details: dict[str, Any] | None = None


@define(slots=True, kw_only=True)
class TimerRepository:
    """Repository for durable timer documents.

    Attributes:
        store: CairnDB blob store the timers live in.
    """

    store: BlobStorage

    @staticmethod
    def _key(flow_name: str, run_id: UUID, name: str) -> str:
        return f"timers/{flow_name}/{run_id}/{name}.json"

    def set(
        self,
        flow_name: str,
        run_id: UUID,
        name: str,
        *,
        due_at: Timestamp,
        signal: str | None = None,
        wakeup: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Create or reschedule a timer (last write wins — timers are modes).

        Args:
            flow_name: Flow the obligation belongs to.
            run_id: The obligation the timer concerns.
            name: Timer name, unique per obligation (re-set to reschedule).
            due_at: When the timer becomes due.
            signal: Optional signal name sent to the obligation on firing.
            wakeup: Enqueue a wake-up message on firing.
            details: Optional structured context (kept small).
        """
        doc: dict[str, Any] = {
            "due_at": due_at.to_iso(),
            "signal": signal,
            "wakeup": wakeup,
        }
        if details:
            doc["details"] = details
        self.store.put_object_sync(
            self._key(flow_name, run_id, name), json.dumps(doc).encode()
        )

    def clear(self, flow_name: str, run_id: UUID, name: str) -> None:
        """Remove a timer (fired, or its waiting condition resolved early)."""
        try:
            self.store.delete_object_sync(self._key(flow_name, run_id, name))
        except Exception:
            logger.exception(
                "timer_delete_error",
                extra={"run_id": str(run_id), "timer": name},
            )

    def clear_all(self, flow_name: str, run_id: UUID) -> None:
        """Remove all of an obligation's timers (archive-time cleanup)."""
        for key in self.store.list_objects_sync(f"timers/{flow_name}/{run_id}/"):
            try:
                self.store.delete_object_sync(key)
            except Exception:
                logger.exception("timer_delete_error", extra={"key": key})

    def due(self, now: Timestamp | None = None) -> list[TimerDoc]:
        """List every timer whose due_at has passed.

        Args:
            now: Reference time (defaults to Timestamp.now()).
        """
        reference = now if now is not None else Timestamp.now()
        found: list[TimerDoc] = []
        for key in self.store.list_objects_sync("timers/"):
            parts = key.split("/")
            if len(parts) != 4 or not parts[3].endswith(".json"):
                continue
            obj = self.store.get_object_sync(key)
            if obj is None:
                continue
            try:
                raw = json.loads(obj.data)
                due_at = Timestamp.from_datetime(
                    datetime.fromisoformat(raw["due_at"])
                )
                if due_at.value > reference.value:
                    continue
                found.append(
                    TimerDoc(
                        flow_name=parts[1],
                        run_id=UUID(parts[2]),
                        name=parts[3].removesuffix(".json"),
                        due_at=due_at,
                        signal=raw.get("signal"),
                        wakeup=raw.get("wakeup", False),
                        details=raw.get("details"),
                    )
                )
            except Exception:
                logger.exception("timer_parse_error", extra={"key": key})
        return found

# ---
# endregion

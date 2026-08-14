"""Run-scoped signal objects — durable, idempotent, out-of-band messages.

A signal is a small immutable object at
``signals/<flow_name>/<run_id>/<name>.json`` set through a cairndb claim
(put-if-absent): duplicate senders converge on the first payload, and the
receiver observes it with a plain read.  Signals never touch the run's
lease document, so a holder's renewals can never clobber one and a signal
write can never fence a holder.

``cancel`` is the first signal; gates and other adjudication messages will
use the same channel.
"""
import json
import logging
from typing import Any
from uuid import UUID

from attrs import define
from cairndb.engine.coordination import claim_sync
from cairndb.storage.base import BlobStorage

from ..types import Timestamp

logger = logging.getLogger(__name__)

CANCEL = "cancel"


# region @signals_repository
# ---
# role: storage
# intent: set and observe run-scoped signals at signals/<flow>/<run_id>/<name>
# description: >
#   SignalRepository.send() claims the signal object put-if-absent (first
#   sender wins, duplicates converge — sending is idempotent); get() reads
#   it; clear() removes a run's signal prefix when the run is archived.
#   The cancel signal replaces the old cancel_requested CAS-conflict
#   channel: the API sends it, the worker's heartbeat observes it, and the
#   lease document stays single-writer.
# rules:
#   - Signals MUST be immutable once set — send never overwrites.
#   - send MUST be idempotent: a duplicate send is a success, not an error.
#   - Signals MUST NOT be written under the run's state/ key — the lease
#     document is single-writer by design.
# dependencies:
#   - types.time
# aliases:
#   - signals
#   - cancel-signal
# triggers:
#   - how does a cancel reach a running flow
#   - how are signals delivered
# ---


@define(slots=True, kw_only=True)
class SignalRepository:
    """Repository for immutable run-scoped signal objects.

    Attributes:
        store: CairnDB blob store the signals live in.
    """

    store: BlobStorage

    @staticmethod
    def _prefix(flow_name: str, run_id: UUID) -> str:
        return f"signals/{flow_name}/{run_id}/"

    @classmethod
    def _key(cls, flow_name: str, run_id: UUID, name: str) -> str:
        return f"{cls._prefix(flow_name, run_id)}{name}.json"

    def send(
        self,
        flow_name: str,
        run_id: UUID,
        name: str,
        *,
        actor: str,
        details: dict[str, Any] | None = None,
    ) -> bool:
        """Set a signal for a run, idempotently.

        Args:
            flow_name: Flow the run belongs to.
            run_id: The run's UUID.
            name: Signal name (e.g. ``cancel``).
            actor: Who sent it — recorded in the signal payload.
            details: Optional structured context (kept small).

        Returns:
            True when this call set the signal, False when it was already
            set (either way the signal is now present).
        """
        payload: dict[str, Any] = {
            "sent_at": Timestamp.now().to_iso(),
            "actor": actor,
        }
        if details:
            payload["details"] = details
        result = claim_sync(self.store, self._key(flow_name, run_id, name), payload)
        return result.won

    def get(self, flow_name: str, run_id: UUID, name: str) -> dict[str, Any] | None:
        """Read a signal's payload, or None when it was never sent.

        Args:
            flow_name: Flow the run belongs to.
            run_id: The run's UUID.
            name: Signal name.
        """
        obj = self.store.get_object_sync(self._key(flow_name, run_id, name))
        if obj is None:
            return None
        try:
            return json.loads(obj.data)
        except Exception:
            logger.exception(
                "signal_parse_error",
                extra={"run_id": str(run_id), "signal": name},
            )
            return None

    def clear(self, flow_name: str, run_id: UUID) -> None:
        """Remove all of a run's signals (archive-time cleanup).

        Args:
            flow_name: Flow the run belongs to.
            run_id: The run's UUID.
        """
        for key in self.store.list_objects_sync(self._prefix(flow_name, run_id)):
            try:
                self.store.delete_object_sync(key)
            except Exception:
                logger.exception("signal_delete_error", extra={"key": key})

# ---
# endregion

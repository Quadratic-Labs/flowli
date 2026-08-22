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

from flowlet.types import Timestamp

logger = logging.getLogger(__name__)

CANCEL = "cancel"
INTERRUPT = "interrupt"
PAUSE = "pause"

# Scoped control signals: unlike run-scoped signals (immutable facts),
# scope signals are *modes* — revocable by design (RESUME revokes a pause).
SCOPE_GLOBAL = "signals/_scopes/global/"
SCOPE_FLOW = "signals/_scopes/flow/"
SCOPE_RUN = "signals/_scopes/run/"


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

    def list(self, flow_name: str, run_id: UUID) -> dict[str, dict[str, Any]]:
        """Read all pending signals for a run, name → payload.

        This is the heartbeat's observation: one LIST plus one GET per
        pending signal (normally zero).

        Args:
            flow_name: Flow the run belongs to.
            run_id: The run's UUID.
        """
        prefix = self._prefix(flow_name, run_id)
        pending: dict[str, dict[str, Any]] = {}
        for key in self.store.list_objects_sync(prefix):
            name = key.removeprefix(prefix).removesuffix(".json")
            obj = self.store.get_object_sync(key)
            if obj is None:
                continue
            try:
                pending[name] = json.loads(obj.data)
            except Exception:
                logger.exception("signal_parse_error", extra={"key": key})
        return pending

    # -- scoped control signals (revocable modes) ------------------------

    @staticmethod
    def _scope_key(scope: str, name: str) -> str:
        """Key for a scoped control signal.

        Scopes: ``"global"``, ``"flow:<flow_name>"``, ``"run:<run_id>"``.
        Run scopes are keyed by obligation id alone (flow-agnostic) so an
        ancestor's scope can be checked from a child's parent/root refs
        without knowing the ancestor's flow.
        """
        if scope == "global":
            return f"{SCOPE_GLOBAL}{name}.json"
        kind, _, ident = scope.partition(":")
        if kind == "flow" and ident:
            return f"{SCOPE_FLOW}{ident}/{name}.json"
        if kind == "run" and ident:
            return f"{SCOPE_RUN}{ident}/{name}.json"
        raise ValueError(f"invalid scope {scope!r}")

    def send_scoped(
        self,
        scope: str,
        name: str,
        *,
        actor: str,
        details: dict[str, Any] | None = None,
    ) -> bool:
        """Set a scoped control signal (idempotent put-if-absent).

        Args:
            scope: ``"global"``, ``"flow:<flow_name>"``, or ``"run:<id>"``.
            name: Signal name (e.g. ``pause``).
            actor: Who set the mode — recorded in the payload.
            details: Optional structured context.
        """
        payload: dict[str, Any] = {
            "sent_at": Timestamp.now().to_iso(),
            "actor": actor,
            "scope": scope,
        }
        if details:
            payload["details"] = details
        return claim_sync(self.store, self._scope_key(scope, name), payload).won

    def get_scoped(self, scope: str, name: str) -> dict[str, Any] | None:
        """Read a scoped control signal, or None when the mode is not set."""
        obj = self.store.get_object_sync(self._scope_key(scope, name))
        if obj is None:
            return None
        try:
            return json.loads(obj.data)
        except Exception:
            logger.exception(
                "signal_parse_error", extra={"scope": scope, "signal": name}
            )
            return None

    def revoke_scoped(self, scope: str, name: str) -> None:
        """Clear a scoped control signal (e.g. RESUME revoking a pause)."""
        try:
            self.store.delete_object_sync(self._scope_key(scope, name))
        except Exception:
            logger.exception(
                "signal_revoke_error", extra={"scope": scope, "signal": name}
            )

    def paused_scopes(
        self,
        *,
        flow_name: str,
        run_id: UUID,
        parent_id: UUID | None = None,
        root_id: UUID | None = None,
    ) -> list[str]:
        """The scopes holding a pause over this obligation, if any.

        Admission checks the effective mode over: global, the flow, the
        obligation itself, its parent, and its root.  Intermediate
        ancestors beyond parent/root are a controller concern (fan-out) —
        sufficient for trees of depth ≤ 3, which covers CodeFlow's
        milestone/feature/task.
        """
        scopes = ["global", f"flow:{flow_name}", f"run:{run_id}"]
        if parent_id is not None:
            scopes.append(f"run:{parent_id}")
        if root_id is not None and root_id != parent_id:
            scopes.append(f"run:{root_id}")
        return [s for s in scopes if self.get_scoped(s, PAUSE) is not None]

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
        self.revoke_scoped(f"run:{run_id}", PAUSE)

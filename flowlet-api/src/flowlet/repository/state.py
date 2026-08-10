"""Run-state repository on the CairnDB conditional object store.

Per-run state objects live at ``state/<flow_name>/<run_id>.json``.  Every
write is conditional — put-if-absent for creation, etag compare-and-swap
for updates — so ownership transfer is arbitrated by the storage itself,
identically on the local filesystem and on blob storage.
"""
import logging
from uuid import UUID

from attrs import define
from cairndb.storage.base import BlobStorage

from ..models import RunState
from ..serdes import from_json, to_json
from ..storage import run_prefix

logger = logging.getLogger(__name__)


# region @state_repository
# ---
# role: storage
# intent: read and write state/<flow_name>/<run_id>.json via conditional writes
# description: >
#   StateRepository owns per-run state objects on the cairndb store.  A
#   read returns the state with its etag; a write passes that etag back as
#   the if_match precondition (or if_absent for first creation), so only
#   one writer can move a state at a time — the CAS discipline every actor
#   (worker claim/finalize, sweeper recovery, cancel endpoint) shares.
#   There is no local/remote split and no lock file: cairndb's filesystem
#   backend provides the same conditional semantics as Azure/S3/GCS.
# rules:
#   - write MUST return False when ownership is lost (stale etag).
#   - write with etag=None MUST be a put-if-absent (first creation only).
#   - MUST NOT raise on missing state objects; return None instead.
#   - archive MUST copy the JSON byte-for-byte into the run folder before
#     deleting the active object.
# dependencies:
#   - storage.keys
#   - models.run
#   - serdes.json
# ---

@define(slots=True, kw_only=True)
class StateRepository:
    """Repository for conditional per-run state I/O on the blob store.

    Attributes:
        store: CairnDB blob store all state objects live in.
    """

    store: BlobStorage

    @staticmethod
    def _key(flow_name: str, run_id: UUID) -> str:
        return f"state/{flow_name}/{run_id}.json"

    def read(self, flow_name: str, run_id: UUID) -> tuple[RunState, str] | None:
        """Read the current state for a run, returning the state and its etag.

        The etag must be passed back to ``write()`` to perform a conditional
        write that fails if another actor has written in the meantime.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.

        Returns:
            A ``(RunState, etag)`` pair, or None if the state does not exist.
        """
        obj = self.store.get_object_sync(self._key(flow_name, run_id))
        if obj is None:
            return None
        try:
            return from_json(RunState)(obj.data.decode("utf-8")), obj.etag
        except Exception:
            logger.exception(
                "state_read_parse_error",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )
            return None

    def write(self, state: RunState, etag: str | None) -> tuple[bool, str | None]:
        """Write a RunState conditionally, using an etag for ownership tracking.

        The ``etag`` must be the value returned by the previous ``read()`` or
        ``write()`` call for this run.  Pass ``None`` only when creating a
        new state for the first time (put-if-absent).

        Args:
            state: The new state to persist.
            etag: Etag from the caller's last successful read or write, or
                ``None`` to assert the object does not yet exist.

        Returns:
            ``(True, new_etag)`` on success; ``(False, None)`` if ownership
            was lost (stale etag or concurrent creation).
        """
        key = self._key(state.flow_name, state.run_id)
        payload = to_json(state).encode()
        if etag is not None:
            new_etag = self.store.put_object_sync(key, payload, if_match=etag)
        else:
            new_etag = self.store.put_object_sync(key, payload, if_absent=True)
        if new_etag is None:
            return False, None
        return True, new_etag

    def delete(self, flow_name: str, run_id: UUID) -> None:
        """Remove the state object for a completed run.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        try:
            self.store.delete_object_sync(self._key(flow_name, run_id))
        except Exception:
            logger.exception(
                "state_delete_error",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )

    def archive(self, flow_name: str, run_id: UUID) -> None:
        """Move a closed run's state into its run folder.

        The JSON content is copied byte-for-byte to
        ``runs/<flow_name>/<date>/<run_id>/state.json`` — colocated with the
        run's span files so the run folder is the complete, self-contained
        durable record — and the active object is removed.  Keeps the active
        ``state/`` listing O(active runs) so sweep cost never grows with run
        history.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        obj = self.store.get_object_sync(self._key(flow_name, run_id))
        if obj is None:
            return
        target = f"{run_prefix(flow_name, run_id)}/state.json"
        try:
            self.store.put_object_sync(target, obj.data)
        except Exception:
            logger.exception("state_archive_write_error", extra={"key": target})
            return
        self.delete(flow_name, run_id)

    def list_states(self, flow_name: str | None = None) -> list[RunState]:
        """List all known states, optionally filtered by flow name.

        Args:
            flow_name: When given, only states for this flow are returned.

        Returns:
            List of RunState objects parsed from the ``state/`` prefix.
        """
        prefix = f"state/{flow_name}/" if flow_name is not None else "state/"
        results: list[RunState] = []
        for key in self.store.list_objects_sync(prefix):
            parts = key.split("/")
            if len(parts) != 3 or not parts[2].endswith(".json"):
                continue
            try:
                run_id = UUID(parts[2].removesuffix(".json"))
            except ValueError:
                continue
            read = self.read(parts[1], run_id)
            if read is not None:
                results.append(read[0])
        return results

# ---
# endregion

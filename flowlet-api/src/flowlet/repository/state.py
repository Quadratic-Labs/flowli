"""Atomic state-file repository for worker-owned run state.

Implements per-run state files under ``state/<flow_name>/<run_id>.json`` with
exclusive-lock semantics for local storage and ETag-based conditional writes
for Azure blob storage.
"""
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import UUID

from attrs import define

from ..models import RunState, RunStatus
from ..serdes import to_json, from_json
from ..storage.types import StatePath, StoragePath
from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @state_repository
# ---
# role: storage
# intent: read and write /state/<flow_name>/<run_id>.json atomically
# description: >
#   StateRepository owns per-run state files.  Writes are guarded by an
#   exclusive lock file (local) or ETag conditional PUT (Azure) so that only
#   one worker can modify a state at a time.
# rules:
#   - write MUST return False when ownership is lost due to a concurrent write.
#   - Lock files MUST contain PID + expiry; stale locks are reclaimed automatically.
#   - MUST NOT raise on missing state files; return None instead.
# dependencies:
#   - storage.types.statepath
#   - models.run
#   - serdes.json
# ---


class ConditionalWriteError(Exception):
    """Raised when a conditional (ETag) PUT is rejected by Azure (HTTP 412)."""


def _dict_to_state(data: dict) -> RunState:
    return RunState(
        run_id=UUID(data["run_id"]),
        flow_name=data["flow_name"],
        status=RunStatus(data["status"]),
        worker_id=data["worker_id"],
        started_at=Timestamp.from_iso(data["started_at"]),
        heartbeat_at=Timestamp.from_iso(data["heartbeat_at"]),
        ended_at=Timestamp.from_iso(data["ended_at"]) if data.get("ended_at") else None,
        attempt=data.get("attempt", 1),
        max_retries=data.get("max_retries", 3),
    )


def _is_pid_alive(pid: int) -> bool:
    """Check whether a process is still running (POSIX only)."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


@define(slots=True, kw_only=True)
class StateRepository:
    """Repository for atomic per-run state-file I/O.

    State files live at ``root/state/<flow_name>/<run_id>.json``.  Concurrent
    writes are serialised via a sibling ``.lock`` file (local filesystem) or
    ETag conditional PUTs (Azure blob storage).

    Attributes:
        root: Storage root under which ``state/`` is written.
        lock_ttl: Seconds after which an unrenewed lock is considered stale.
            Default 60 s — slightly longer than the heartbeat interval.
    """

    root: StoragePath
    lock_ttl: int = 60

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def _state_path(self, flow_name: str, run_id: UUID) -> StatePath:
        return StatePath.build(self.root, flow_name, run_id)

    def _lock_path(self, sp: StatePath) -> Path:
        """Return the sibling .lock file path (local filesystem only)."""
        if not isinstance(sp.path, Path):
            raise TypeError("Lock files are only supported on local storage paths")
        return sp.path.with_suffix(".lock")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(self, flow_name: str, run_id: UUID) -> RunState | None:
        """Read the current state for a run.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.

        Returns:
            The parsed RunState, or None if the state file does not exist.
        """
        sp = self._state_path(flow_name, run_id)
        try:
            raw = sp.path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None
        try:
            return from_json(RunState)(raw)
        except Exception:
            logger.exception("state_read_parse_error", extra={"path": str(sp.path)})
            return None

    def write(self, state: RunState) -> bool:
        """Write a RunState atomically.

        Uses an exclusive ``.lock`` file on local paths and ETag conditional
        PUTs on Azure blob paths.

        Args:
            state: The new state to persist.

        Returns:
            True if the write succeeded; False if ownership was lost (another
            worker holds the lock or the ETag no longer matches).
        """
        sp = self._state_path(state.flow_name, state.run_id)
        if isinstance(sp.path, Path):
            return self._write_local(sp, state)
        return self._write_azure(sp, state)

    def delete(self, flow_name: str, run_id: UUID) -> None:
        """Remove the state file for a completed run.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        sp = self._state_path(flow_name, run_id)
        try:
            sp.path.unlink(missing_ok=True)
        except OSError:
            logger.exception("state_delete_error", extra={"path": str(sp.path)})

    def list_states(self, flow_name: str | None = None) -> list[RunState]:
        """List all known states, optionally filtered by flow name.

        Args:
            flow_name: When given, only states for this flow are returned.

        Returns:
            List of RunState objects parsed from the state directory.
        """
        state_root = self.root / "state"
        if not state_root.exists():
            return []

        dirs: list[StoragePath] = []
        if flow_name is not None:
            candidate = state_root / flow_name
            if candidate.exists():
                dirs = [candidate]
        else:
            dirs = [p for p in state_root.iterdir() if p.is_dir()]

        results: list[RunState] = []
        for d in dirs:
            for p in d.iterdir():
                try:
                    sp = StatePath.from_path(p)
                except ValueError:
                    continue
                s = self.read(sp.flow_name, sp.run_id)
                if s is not None:
                    results.append(s)
        return results

    # ------------------------------------------------------------------
    # Local filesystem — lock-file strategy
    # ------------------------------------------------------------------

    def _write_local(self, sp: StatePath, state: RunState) -> bool:
        lock_path = self._lock_path(sp)
        sp.path.parent.mkdir(parents=True, exist_ok=True)

        acquired = self._acquire_lock(lock_path)
        if not acquired:
            return False

        try:
            sp.path.write_text(to_json(state))
            return True
        finally:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _acquire_lock(self, lock_path: Path) -> bool:
        """Try to create the lock file exclusively.

        Reclaims stale locks (dead PID or expired TTL) before returning False.

        Args:
            lock_path: Path to the ``.lock`` file.

        Returns:
            True if the lock was acquired; False if another live owner holds it.
        """
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=self.lock_ttl)
        ).isoformat()
        payload = json.dumps({"pid": os.getpid(), "expires_at": expires_at})

        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, payload.encode())
            os.close(fd)
            return True
        except FileExistsError:
            pass

        # Lock already exists — check staleness.
        if self._reclaim_stale_lock(lock_path):
            # Try once more after reclamation.
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, payload.encode())
                os.close(fd)
                return True
            except FileExistsError:
                pass

        return False

    def _reclaim_stale_lock(self, lock_path: Path) -> bool:
        """Remove a lock file if its holder is dead or its TTL has elapsed.

        Args:
            lock_path: Path to the ``.lock`` file to inspect.

        Returns:
            True if the lock was reclaimed (deleted); False if still live.
        """
        try:
            raw = lock_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            pid: int = data["pid"]
            expires_at = datetime.fromisoformat(data["expires_at"])
        except Exception:
            # Malformed or already deleted — attempt removal.
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass
            return True

        now = datetime.now(timezone.utc)
        if _is_pid_alive(pid) and now < expires_at:
            return False

        try:
            lock_path.unlink(missing_ok=True)
            logger.info(
                "stale_lock_reclaimed",
                extra={"lock": str(lock_path), "pid": pid},
            )
            return True
        except OSError:
            return False

    # ------------------------------------------------------------------
    # Azure blob storage — ETag strategy
    # ------------------------------------------------------------------

    def _write_azure(self, sp: StatePath, state: RunState) -> bool:
        """Write state using ETag conditional PUT (Azure blob storage).

        Args:
            sp: Resolved StatePath (remote).
            state: State to persist.

        Returns:
            True on success; False if the ETag no longer matches (412).
        """
        try:
            from .storage.azure import AzureBlobPath  # type: ignore[import]
        except ImportError:
            raise RuntimeError(
                "azure-storage-blob is required for Azure state writes"
            )

        payload = to_json(state).encode()
        path: AzureBlobPath = sp.path  # type: ignore[assignment]

        try:
            etag = path.etag() if path.exists() else None
            if etag is not None:
                path.write_bytes_if_match(payload, etag=etag)
            else:
                path.write_bytes_if_none_match(payload)
            return True
        except ConditionalWriteError:
            return False

# ---
# endregion

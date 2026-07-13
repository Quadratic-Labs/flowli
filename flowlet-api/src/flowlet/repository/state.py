"""Atomic state-file repository for worker-owned run state.

Implements per-run state files under ``state/<flow_name>/<run_id>.json`` with
exclusive-lock semantics for local storage and ETag-based conditional writes
for Azure blob storage.
"""
import fcntl
import json
import logging
import os
import time
from pathlib import Path
from uuid import UUID

from attrs import define

from ..models import RunState
from ..serdes import to_json, from_json
from ..storage.types import StatePath, StoragePath

logger = logging.getLogger(__name__)


# region @state_repository
# ---
# role: storage
# intent: read and write /state/<flow_name>/<run_id>.json atomically
# description: >
#   StateRepository owns per-run state files.  Writes are guarded by an
#   exclusive flock on a persistent sibling .lock file (local) or ETag
#   conditional PUT (Azure) so that only one worker can modify a state at a
#   time.  flock is released by the kernel when the holder dies, so there is
#   no stale-lock state and no reclamation logic to race on.  State files are
#   replaced atomically (temp file + os.replace) so lock-free readers never
#   observe a partial write.
# rules:
#   - write MUST return False when ownership is lost due to a concurrent write.
#   - The .lock file is persistent and content-less; it MUST NOT be unlinked
#     while any writer may hold or wait on its flock.
#   - State-file replacement MUST be atomic (temp + os.replace).
#   - MUST NOT raise on missing state files; return None instead.
# dependencies:
#   - storage.types.statepath
#   - models.run
#   - serdes.json
# ---

class ConditionalWriteError(Exception):
    """Raised when a conditional (ETag) PUT is rejected by Azure (HTTP 412)."""


@define(slots=True, kw_only=True)
class StateRepository:
    """Repository for atomic per-run state-file I/O.

    State files live at ``root/state/<flow_name>/<run_id>.json``.  Concurrent
    writes are serialised via an exclusive ``flock`` on a sibling ``.lock``
    file (local filesystem) or ETag conditional PUTs (Azure blob storage).

    The kernel releases a flock automatically when its holder exits or
    crashes, so stale locks cannot occur and no reclamation is needed.
    The ``.lock`` file itself is a persistent, content-less rendezvous point
    and is only removed together with the state file in :meth:`delete`.

    Attributes:
        root: Storage root under which ``state/`` is written.
        lock_timeout: Seconds to wait for the write lock before reporting the
            write as lost.  Writes are millisecond-fast, so contention beyond
            this indicates a wedged writer.
    """

    root: StoragePath
    lock_timeout: float = 5.0

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

    def read(self, flow_name: str, run_id: UUID) -> tuple[RunState, str] | None:
        """Read the current state for a run, returning the state and its ETag.

        The ETag must be passed back to ``write()`` to perform a conditional
        write that fails if another worker has written in the meantime.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.

        Returns:
            A ``(RunState, etag)`` pair, or None if the state file does not exist.
        """
        sp = self._state_path(flow_name, run_id)
        if isinstance(sp.path, Path):
            return self._read_local(sp)
        return self._read_azure(sp)

    def write(self, state: RunState, etag: str | None) -> tuple[bool, str | None]:
        """Write a RunState conditionally, using an ETag for ownership tracking.

        The ``etag`` must be the value returned by the previous ``read()`` or
        ``write()`` call for this run.  Pass ``None`` only when creating a
        new state file for the first time (expects no pre-existing file).

        Uses an exclusive ``.lock`` file on local paths (mtime-verified inside
        the lock) and ETag conditional PUTs on Azure blob paths.

        Args:
            state: The new state to persist.
            etag: ETag from the caller's last successful read or write.
                ``None`` asserts that the file does not yet exist.

        Returns:
            ``(True, new_etag)`` on success; ``(False, None)`` if ownership
            was lost (stale ETag, concurrent write, or lock contention).
        """
        sp = self._state_path(state.flow_name, state.run_id)
        if isinstance(sp.path, Path):
            return self._write_local(sp, state, etag)
        return self._write_azure(sp, state, etag)

    def delete(self, flow_name: str, run_id: UUID) -> None:
        """Remove the state file for a completed run.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        sp = self._state_path(flow_name, run_id)
        try:
            sp.path.unlink(missing_ok=True)
            if isinstance(sp.path, Path):
                self._lock_path(sp).unlink(missing_ok=True)
        except OSError:
            logger.exception("state_delete_error", extra={"path": str(sp.path)})

    def archive(self, flow_name: str, run_id: UUID) -> None:
        """Move a closed run's state file into its run folder.

        The JSON content is copied byte-for-byte to
        ``root/runs/<flow_name>/<date>/<run_id>/state.json`` — colocated
        with the run's span files so the run folder is the complete,
        self-contained durable record — and the active file plus its lock
        are removed.  Keeps the active ``state/`` listing O(active runs) so
        sweep cost never grows with run history.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        from .log import run_folder

        sp = self._state_path(flow_name, run_id)
        try:
            raw = sp.path.read_bytes()
        except (FileNotFoundError, OSError):
            return
        target = run_folder(self.root, flow_name, run_id) / "state.json"
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        except OSError:
            logger.exception("state_archive_write_error", extra={"path": str(target)})
            return
        self.delete(flow_name, run_id)

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
                result = self.read(sp.flow_name, sp.run_id)
                if result is not None:
                    results.append(result[0])
        return results

    # ------------------------------------------------------------------
    # Local filesystem — lock-file strategy
    # ------------------------------------------------------------------

    def _read_local(self, sp: StatePath) -> tuple[RunState, str] | None:
        """Read state and version-based ETag from a local file.

        The ETag is the ``_v`` counter embedded in the JSON — a monotonically
        increasing integer that changes on every write, immune to filesystem
        mtime resolution issues.

        Args:
            sp: Resolved local StatePath.

        Returns:
            A ``(RunState, etag)`` pair, or None if the file does not exist.
        """
        assert isinstance(sp.path, Path)
        try:
            raw = sp.path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None
        try:
            data = json.loads(raw)
            version = data.get("_v", 0)
            state = from_json(RunState)(raw)  # _v is an unknown field — silently ignored
            return state, str(version)
        except Exception:
            logger.exception("state_read_parse_error", extra={"path": str(sp.path)})
            return None

    def _write_local(self, sp: StatePath, state: RunState, etag: str | None) -> tuple[bool, str | None]:
        """Write state atomically under an exclusive flock, with a version ETag.

        The ``_v`` counter inside the JSON acts as the ETag.  All version
        checks happen while holding an exclusive ``flock`` on a persistent
        sibling ``.lock`` file, so no concurrent writer can slip between the
        check and the write.  The kernel drops the flock if this process dies,
        so a crashed writer never blocks others.  The state file itself is
        replaced with ``os.replace`` so lock-free readers always see a
        complete document.

        Args:
            sp: Resolved local StatePath.
            state: State to persist.
            etag: Expected current version as a string, or ``None`` to assert
                the file does not yet exist.

        Returns:
            ``(True, new_etag)`` on success; ``(False, None)`` on conflict or
            lock timeout.
        """
        assert isinstance(sp.path, Path)
        lock_path = self._lock_path(sp)
        sp.path.parent.mkdir(parents=True, exist_ok=True)

        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
        try:
            if not self._flock_acquire(fd):
                logger.warning("state_write_lock_timeout", extra={"path": str(sp.path)})
                return False, None
            try:
                if sp.path.exists():
                    if etag is None:
                        return False, None  # expected new file, but one exists
                    try:
                        current_data = json.loads(sp.path.read_text(encoding="utf-8"))
                        current_version = current_data.get("_v", 0)
                    except Exception:
                        return False, None
                    if str(current_version) != etag:
                        return False, None  # stale ETag — another worker wrote
                    new_version = current_version + 1
                else:
                    if etag is not None:
                        return False, None  # expected existing file, but it's gone
                    new_version = 1

                state_data = json.loads(to_json(state))
                state_data["_v"] = new_version
                tmp_path = sp.path.with_suffix(".json.tmp")
                tmp_path.write_text(json.dumps(state_data), encoding="utf-8")
                os.replace(tmp_path, sp.path)
                return True, str(new_version)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _flock_acquire(self, fd: int, poll_interval: float = 0.02) -> bool:
        """Acquire an exclusive flock on *fd*, polling up to ``lock_timeout``.

        Non-blocking attempts with a short sleep bound the total wait, so a
        wedged writer holding the lock cannot stall callers indefinitely.

        Args:
            fd: Open file descriptor of the ``.lock`` file.
            poll_interval: Seconds between acquisition attempts.

        Returns:
            True when the lock was acquired; False on timeout.
        """
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except OSError:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Azure blob storage — ETag strategy
    # ------------------------------------------------------------------

    def _read_azure(self, sp: StatePath) -> tuple[RunState, str] | None:
        """Read state and ETag from Azure blob storage.

        Args:
            sp: Resolved StatePath (remote).

        Returns:
            A ``(RunState, etag)`` pair, or None if the blob does not exist.
        """
        try:
            from .storage.azure import AzureBlobPath  # type: ignore[import]
        except ImportError:
            raise RuntimeError(
                "azure-storage-blob is required for Azure state reads"
            )

        path: AzureBlobPath = sp.path  # type: ignore[assignment]
        try:
            raw, azure_etag = path.read_text_with_etag(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            return from_json(RunState)(raw), azure_etag
        except Exception:
            logger.exception("state_read_parse_error", extra={"path": str(sp.path)})
            return None

    def _write_azure(self, sp: StatePath, state: RunState, etag: str | None) -> tuple[bool, str | None]:
        """Write state using ETag conditional PUT (Azure blob storage).

        Uses the caller's ETag — which was captured at the last successful
        read or write — so that concurrent writes by another worker cause a
        412 and return ``(False, None)``.

        Args:
            sp: Resolved StatePath (remote).
            state: State to persist.
            etag: ETag from the caller's last read/write. ``None`` asserts
                the blob does not yet exist (``If-None-Match: *``).

        Returns:
            ``(True, new_etag)`` on success; ``(False, None)`` on 412.
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
            if etag is not None:
                new_etag = path.write_bytes_if_match(payload, etag=etag)
            else:
                new_etag = path.write_bytes_if_none_match(payload)
            return True, new_etag
        except ConditionalWriteError:
            return False, None

# ---
# endregion

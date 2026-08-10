"""Idempotent dispatch-key mapping — collapse duplicate submissions.

A caller submitting a flow may supply a ``dispatch_key`` (webhook delivery
id, ``"<flow>:<date>"`` for a cron tick, ...).  The first submission wins a
put-if-absent write of ``dispatch/<flow_name>/<sha256(key)>.json`` recording
its run_id; every later submission with the same key resolves to that same
run_id.  Duplicate wake-up messages remain harmless — the worker state
machine drops them for busy/closed runs — so dedup only has to pin the
run_id, never to suppress messages.

run_id stays a genuine uuid7 (the mapping is indirection, not a hash-derived
id), preserving the invariant that every run_id embeds a timestamp for the
runs/ date partition.
"""
import hashlib
import json
import logging
import os
from pathlib import Path
from uuid import UUID

from attrs import define
from chroniql.storage import BlobStorage

from ..storage.types import StoragePath
from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @dispatch_repository
# ---
# role: storage
# intent: map caller dispatch keys to run ids with put-if-absent semantics
# description: >
#   DispatchKeyRepository.resolve_or_create() atomically claims
#   dispatch/<flow_name>/<sha256(key)>.json for a candidate run_id.  Local
#   storage uses O_CREAT|O_EXCL (the filesystem is the arbiter); remote
#   storage uses the ChroniQL object store's if_absent put — the same CAS
#   discipline as the state repository.  Losing the race reads the mapping
#   back and returns the existing run_id, so concurrent duplicate submits
#   all converge on one run.
# rules:
#   - resolve_or_create MUST be atomic: exactly one caller per key creates.
#   - Mapping files MUST be immutable once written — never overwritten.
#   - The key on disk MUST be the sha256 of the dispatch key (safe path
#     charset, bounded length); the raw key is stored inside for audit.
#   - Remote backends MUST go through the chroniql object store.
# dependencies:
#   - storage.types
#   - types.time
# aliases:
#   - dispatch-keys
#   - idempotent-submit
# triggers:
#   - how are duplicate submissions deduplicated
#   - what is a dispatch key
# ---


@define(slots=True, kw_only=True)
class DispatchKeyRepository:
    """Repository for immutable dispatch-key → run_id mappings.

    Attributes:
        root: Storage root under which ``dispatch/`` is written.
        object_store: ChroniQL object store rooted at the same location;
            required for remote (non-``Path``) roots, unused locally.
    """

    root: StoragePath
    object_store: BlobStorage | None = None

    @staticmethod
    def digest(dispatch_key: str) -> str:
        """Return the on-disk name for a dispatch key (sha256 hex)."""
        return hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()

    def _object_key(self, flow_name: str, digest: str) -> str:
        return f"dispatch/{flow_name}/{digest}.json"

    def resolve_or_create(
        self, flow_name: str, dispatch_key: str, run_id: UUID
    ) -> tuple[UUID, bool]:
        """Claim the key for *run_id*, or resolve the previously claimed run.

        Args:
            flow_name: Flow being submitted.
            dispatch_key: Caller-supplied idempotency key.
            run_id: Candidate run_id used if this call creates the mapping.

        Returns:
            ``(run_id, True)`` when this call created the mapping, or
            ``(existing_run_id, False)`` when the key was already claimed.
        """
        digest = self.digest(dispatch_key)
        payload = json.dumps(
            {
                "run_id": str(run_id),
                "flow_name": flow_name,
                "dispatch_key": dispatch_key,
                "created_at": Timestamp.now().to_iso(),
            }
        ).encode("utf-8")

        if isinstance(self.root, Path):
            return self._resolve_local(flow_name, digest, payload, run_id)
        return self._resolve_remote(flow_name, digest, payload, run_id)

    # ------------------------------------------------------------------
    # Local filesystem — O_CREAT|O_EXCL is the arbiter
    # ------------------------------------------------------------------

    def _resolve_local(
        self, flow_name: str, digest: str, payload: bytes, run_id: UUID
    ) -> tuple[UUID, bool]:
        assert isinstance(self.root, Path)
        path = self.root / "dispatch" / flow_name / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return self._read_mapping_local(path), False
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
        return run_id, True

    @staticmethod
    def _read_mapping_local(path: Path) -> UUID:
        data = json.loads(path.read_text(encoding="utf-8"))
        return UUID(data["run_id"])

    # ------------------------------------------------------------------
    # Remote blob storage — if_absent put via the ChroniQL object store
    # ------------------------------------------------------------------

    def _require_object_store(self) -> BlobStorage:
        if self.object_store is None:
            raise RuntimeError(
                "DispatchKeyRepository needs a chroniql object store for "
                "remote storage backends; pass object_store= when configuring."
            )
        return self.object_store

    def _resolve_remote(
        self, flow_name: str, digest: str, payload: bytes, run_id: UUID
    ) -> tuple[UUID, bool]:
        store = self._require_object_store()
        key = self._object_key(flow_name, digest)
        etag = store.put_object_sync(key, payload, if_absent=True)
        if etag is not None:
            return run_id, True
        obj = store.get_object_sync(key)
        if obj is None:
            # Lost the put race yet the object is unreadable — extremely
            # unlikely; claim once more rather than failing the submit.
            etag = store.put_object_sync(key, payload, if_absent=True)
            if etag is not None:
                return run_id, True
            raise RuntimeError(f"dispatch mapping unreadable for key {key}")
        data = json.loads(obj.data.decode("utf-8"))
        return UUID(data["run_id"]), False

# ---
# endregion

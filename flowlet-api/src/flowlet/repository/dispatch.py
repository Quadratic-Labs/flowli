"""Idempotent dispatch-key mapping — collapse duplicate submissions.

A caller submitting a flow may supply a ``dispatch_key`` (webhook delivery
id, ``"<flow>:<date>"`` for a cron tick, ...).  The mapping is a CairnDB
claim: exactly one submission wins the put-if-absent of
``dispatch/<flow_name>/<sha256(key)>.json`` recording its run_id, and every
later submission with the same key converges on that same run_id.
Duplicate wake-up messages remain harmless — the worker state machine drops
them for busy/closed runs — so dedup only has to pin the run_id, never to
suppress messages.

run_id stays a genuine uuid7 (the mapping is indirection, not a hash-derived
id), preserving the invariant that every run_id embeds a timestamp for the
runs/ date partition.
"""
import hashlib
import logging
from uuid import UUID

from attrs import define
from cairndb.engine.coordination import claim_sync
from cairndb.storage.base import BlobStorage

from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @dispatch_repository
# ---
# role: storage
# intent: map caller dispatch keys to run ids with claim (put-if-absent) semantics
# description: >
#   DispatchKeyRepository.resolve_or_create() claims
#   dispatch/<flow_name>/<sha256(key)>.json for a candidate run_id via
#   cairndb's claim primitive: one winner, losers read back the winner's
#   value — so concurrent duplicate submits all converge on one run.  The
#   same code path serves the local filesystem and every blob backend.
# rules:
#   - resolve_or_create MUST be atomic: exactly one caller per key creates.
#   - Mapping objects MUST be immutable once written — never overwritten.
#   - The key on disk MUST be the sha256 of the dispatch key (safe path
#     charset, bounded length); the raw key is stored inside for audit.
# dependencies:
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
    """Repository for immutable dispatch-key → run_id claims.

    Attributes:
        store: CairnDB blob store the claims live in.
    """

    store: BlobStorage

    @staticmethod
    def digest(dispatch_key: str) -> str:
        """Return the on-disk name for a dispatch key (sha256 hex)."""
        return hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()

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
        key = f"dispatch/{flow_name}/{self.digest(dispatch_key)}.json"
        result = claim_sync(
            self.store,
            key,
            {
                "run_id": str(run_id),
                "flow_name": flow_name,
                "dispatch_key": dispatch_key,
                "created_at": Timestamp.now().to_iso(),
            },
        )
        return UUID(result.value["run_id"]), result.won

# ---
# endregion

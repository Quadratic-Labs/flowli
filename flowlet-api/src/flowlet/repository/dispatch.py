"""Idempotent dispatch-key mapping — collapse duplicate submissions.

A caller submitting a flow may supply a ``dispatch_key`` (webhook delivery
id, ``"<flow>:<date>"`` for a cron tick, ...).  The mapping is a CairnDB
claim: exactly one submission wins the put-if-absent of
``dispatch/<flow_name>/<sha256(key)>.json`` recording its obligation_id, and every
later submission with the same key converges on that same obligation_id.
Duplicate wake-up messages remain harmless — the worker state machine drops
them for busy/closed obligations — so dedup only has to pin the obligation_id, never to
suppress messages.

obligation_id stays a genuine uuid7 (the mapping is indirection, not a hash-derived
id), preserving the invariant that every obligation_id embeds a timestamp for the
obligations/ date partition.
"""
import hashlib
import logging
from uuid import UUID

from attrs import define
from cairndb.engine.coordination import claim_sync
from cairndb.storage.base import BlobStorage

from flowlet.types import Timestamp

logger = logging.getLogger(__name__)


@define(slots=True, kw_only=True)
class DispatchKeyRepository:
    """Repository for immutable dispatch-key → obligation_id claims.

    Attributes:
        store: CairnDB blob store the claims live in.
    """

    store: BlobStorage

    @staticmethod
    def digest(dispatch_key: str) -> str:
        """Return the on-disk name for a dispatch key (sha256 hex)."""
        return hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()

    def resolve_or_create(
        self, flow_name: str, dispatch_key: str, obligation_id: UUID
    ) -> tuple[UUID, bool]:
        """Claim the key for *obligation_id*, or resolve the previously claimed obligation.

        Args:
            flow_name: Flow being submitted.
            dispatch_key: Caller-supplied idempotency key.
            obligation_id: Candidate obligation_id used if this call creates the mapping.

        Returns:
            ``(obligation_id, True)`` when this call created the mapping, or
            ``(existing_obligation_id, False)`` when the key was already claimed.
        """
        key = f"dispatch/{flow_name}/{self.digest(dispatch_key)}.json"
        result = claim_sync(
            self.store,
            key,
            {
                "obligation_id": str(obligation_id),
                "flow_name": flow_name,
                "dispatch_key": dispatch_key,
                "created_at": Timestamp.now().to_iso(),
            },
        )
        return UUID(result.value["obligation_id"]), result.won

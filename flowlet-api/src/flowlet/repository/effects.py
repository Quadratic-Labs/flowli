"""Effect claims — exactly-once side-effect recording by occurrence key.

An effect's identity is derived from the obligation and the effect's name
plus occurrence — **never** from the attempt that executes it — so a retry
attempt converges on the recorded effect instead of re-firing the side
effect.  The claim (put-if-absent) at
``effects/<flow_name>/<obligation_id>/<sha256(name:occurrence)>.json`` is
the exactly-once guarantee; the account keeps a reference entry.

Semantics are the DBOS step-checkpoint contract: at-least-once execution,
exactly-once recording.  A duplicate or recovered executor loses the claim,
reads back the winner's result, and continues on the identical path.  The
standard caveat applies: the side effect itself may fire more than once in
a crash window, so effect bodies should be idempotent where possible.  A
deliberate repetition (an authorized manual re-send) mints a new
*occurrence*, which is exactly how execute-once and human override coexist.
"""
import hashlib
import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

from attrs import define
from cairndb.engine.coordination import claim_sync
from cairndb.storage.base import BlobStorage

from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @effects_repository
# ---
# role: storage
# intent: claim effect occurrences so side-effects record exactly once
# description: >
#   EffectRepository.memoize() computes the occurrence key from
#   (obligation, effect name, occurrence), returns the recorded result when
#   the key was already claimed (skipping the body), and otherwise runs the
#   body and claims the result — losers of the claim race converge on the
#   winner's result.  Deliberate repetitions pass a new occurrence.
# rules:
#   - The occurrence key MUST NOT include the attempt number — retries
#     converge on the recorded effect.
#   - Claims are immutable: an effect result is never overwritten.
#   - Results MUST be JSON-serializable (or content-addressed refs).
# dependencies:
#   - types.time
# aliases:
#   - effects
#   - effect-claims
#   - idempotency-keys
# triggers:
#   - how are side effects made idempotent
#   - how does exactly-once work
# ---


@define(slots=True, kw_only=True)
class EffectRepository:
    """Repository for immutable effect-occurrence claims.

    Attributes:
        store: CairnDB blob store the claims live in.
    """

    store: BlobStorage

    @staticmethod
    def _key(flow_name: str, obligation_id: UUID, name: str, occurrence: str) -> str:
        digest = hashlib.sha256(f"{name}:{occurrence}".encode()).hexdigest()
        return f"effects/{flow_name}/{obligation_id}/{digest}.json"

    def memoize(
        self,
        flow_name: str,
        obligation_id: UUID,
        name: str,
        body: Callable[[], Any],
        *,
        occurrence: str = "1",
        executor: str | None = None,
    ) -> tuple[Any, bool]:
        """Run *body* at most once per occurrence, converging on the record.

        Args:
            flow_name: Flow the obligation belongs to.
            obligation_id: The obligation the effect discharges.
            name: Effect name (stable across retries).
            body: Zero-argument callable producing a JSON-safe result.
            occurrence: Occurrence discriminator; a new value deliberately
                repeats the effect.
            executor: Recorded for audit (worker id / actor).

        Returns:
            ``(result, produced)`` — ``produced`` is True when this call ran
            the body, False when a previous execution's result was returned.
        """
        key = self._key(flow_name, obligation_id, name, occurrence)
        existing = self.store.get_object_sync(key)
        if existing is not None:
            import json

            recorded = json.loads(existing.data)
            logger.debug(
                "effect_replayed", extra={"key": key, "name": name}
            )
            return recorded.get("result"), False

        result = body()
        claim = claim_sync(
            self.store,
            key,
            {
                "name": name,
                "occurrence": occurrence,
                "result": result,
                "executor": executor,
                "produced_at": Timestamp.now().to_iso(),
            },
        )
        if not claim.won:
            # A concurrent executor recorded first — converge on its result.
            return claim.value.get("result"), False
        return result, True

# ---
# endregion

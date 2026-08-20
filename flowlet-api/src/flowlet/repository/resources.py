"""Generic resource leases — fenced ownership over arbitrary named resources.

The same cairndb lease primitive that owns obligations, applied to names:
write scopes, a merge-queue lock, a shared fixture — anything where "one
holder at a time, crash-safe" is the contract.  Keys live under
``resources/<sha256(name)>.json``; the epoch fence makes even a paused
holder safe (its writes fail after a steal).

Controllers own the semantics (what the name means, acquisition ordering to
avoid deadlock); the kernel provides only the fenced ownership.
"""
import hashlib
import logging

from attrs import define, field
from cairndb.engine.coordination import Lease, acquire_sync
from cairndb.storage.base import BlobStorage

logger = logging.getLogger(__name__)


# region @resources_repository
# ---
# role: storage
# intent: fenced leases over named resources (write scopes, merge lock)
# description: >
#   ResourceLeaseRepository.acquire() takes the lease on a resource name
#   (put-if-absent or fenced steal of an expired holder) and returns a
#   handle whose renew/release are epoch-fenced.  Callers acquiring several
#   resources MUST do so in a total order (sorted names) to avoid deadlock,
#   and SHOULD release in reverse.  The payload records the holder's
#   purpose for operators inspecting a stuck resource.
# rules:
#   - Multi-resource acquisition MUST follow a total order on names.
#   - A lost renewal (LeaseLost) means the resource was stolen after
#     expiry; the holder MUST stop relying on it immediately.
#   - Resource semantics (globs, exclusivity classes) are controller logic.
# dependencies:
# aliases:
#   - resource-leases
#   - write-scopes
# triggers:
#   - how are write scopes locked
#   - how does the merge queue serialize
# ---


@define(kw_only=True)
class ResourceLease:
    """An owned lease on a named resource, epoch-fenced.

    Attributes:
        name: The resource's logical name.
    """

    name: str
    _lease: Lease = field(alias="lease")

    @property
    def epoch(self) -> int:
        """The lease's fence token."""
        return self._lease.epoch

    @property
    def holder(self) -> str | None:
        """The holder this lease was acquired with."""
        return self._lease.holder

    def renew(self) -> None:
        """Extend the deadline by the ttl. Raises LeaseLost if fenced."""
        self._lease.renew_sync()

    def release(self) -> None:
        """Release the resource for the next acquirer."""
        self._lease.release_sync()


@define(slots=True, kw_only=True)
class ResourceLeaseRepository:
    """Repository for fenced leases over named resources.

    Attributes:
        store: CairnDB blob store the resource documents live in.
    """

    store: BlobStorage

    @staticmethod
    def _key(name: str) -> str:
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
        return f"resources/{digest}.json"

    def acquire(
        self, name: str, *, holder: str, ttl: float, purpose: str | None = None
    ) -> ResourceLease | None:
        """Take the lease on *name*, or None when it is actively held.

        Args:
            name: Logical resource name (e.g. ``scope:src/billing/**`` or
                ``merge-queue``).
            holder: Acquiring actor (worker id, controller id).
            ttl: Lease duration in seconds; renew within it.
            purpose: Optional human-readable note recorded in the payload.
        """
        lease = acquire_sync(
            self.store,
            self._key(name),
            ttl=ttl,
            holder=holder,
            steal_if_expired=True,
            state_fn=lambda _: {"name": name, "purpose": purpose},
        )
        if lease is None:
            return None
        logger.info(
            "resource_acquired",
            extra={"resource": name, "holder": holder, "epoch": lease.epoch},
        )
        return ResourceLease(name=name, lease=lease)

# ---
# endregion

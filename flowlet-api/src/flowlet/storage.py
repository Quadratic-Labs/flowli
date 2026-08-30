"""Storage primitives — thin helpers over the CairnDB blob store.

Flowlet owns no storage machinery of its own: every read, conditional
write, and listing goes through a cairndb
:class:`~cairndb.storage.base.BlobStorage` (filesystem, Azure, S3, or GCS —
one code path).  This module adds only the two pieces of glue the framework
needs on top of that store: the obligation-folder key convention and an
append-a-line compare-and-swap loop for the observability streams.
"""
import logging
from uuid import UUID

from cairndb.storage.base import BlobStorage

from flowlet.types import Timestamp

logger = logging.getLogger(__name__)


def obligation_prefix(flow_name: str, obligation_id: UUID) -> str:
    """Key prefix of an obligation's record folder, computed from its identity.

    The date partition comes from the obligation_id's embedded uuid7 timestamp,
    so no listing is needed to locate a known obligation.

    Args:
        flow_name: The flow the obligation belongs to.
        obligation_id: The obligation's UUID (uuid7).

    Returns:
        Key prefix ``obligations/<flow_name>/<yyyy-mm-dd>/<obligation_id>`` (no trailing
        slash).
    """
    date = Timestamp.from_uuid7(obligation_id).value.strftime("%Y-%m-%d")
    return f"obligations/{flow_name}/{date}/{obligation_id}"


def append_lines(store: BlobStorage, key: str, lines: str) -> bool:
    """Append *lines* (newline-terminated) to object *key*, atomically.

    Delegates to :meth:`~cairndb.storage.base.BlobStorage.append_object_sync`
    — a true O(len) append on backends with a native primitive (filesystem,
    Azure Append Blobs), a compare-and-swap rewrite loop elsewhere.

    Args:
        store: The blob store.
        key: Object key of the .jsonl stream.
        lines: One or more ``\\n``-terminated lines to append.

    Returns:
        True when the append landed; False after exhausting retries or on
        a storage error (logged, never raised — callers are observability
        writers that must not disturb execution).
    """
    try:
        if store.append_object_sync(key, lines.encode("utf-8")):
            return True
    except Exception:
        logger.warning("append_lines_failed", extra={"key": key}, exc_info=True)
        return False
    logger.warning("append_lines_contended", extra={"key": key})
    return False


def read_lines(store: BlobStorage, key: str) -> list[str]:
    """Read a .jsonl object's non-empty lines, or [] when absent.

    Args:
        store: The blob store.
        key: Object key of the .jsonl stream.
    """
    obj = store.get_object_sync(key)
    if obj is None:
        return []
    return [line for line in obj.data.decode("utf-8").splitlines() if line.strip()]

"""Storage primitives — thin helpers over the CairnDB blob store.

Flowlet owns no storage machinery of its own: every read, conditional
write, and listing goes through a cairndb
:class:`~cairndb.storage.base.BlobStorage` (filesystem, Azure, S3, or GCS —
one code path).  This module adds only the two pieces of glue the framework
needs on top of that store: the run-folder key convention and an
append-a-line compare-and-swap loop for the observability streams.
"""
import logging
from uuid import UUID

from cairndb.storage.base import BlobStorage

from ..types import Timestamp

logger = logging.getLogger(__name__)

_APPEND_ATTEMPTS = 8


# region @storage.keys
# ---
# role: storage
# intent: key conventions for the run-record layout on the blob store
# description: >
#   All framework data lives under four reserved key prefixes of one store:
#   state/<flow>/<run_id>.json (active control plane, CAS-written),
#   dispatch/<flow>/<digest>.json (idempotency claims, put-if-absent),
#   runs/<flow>/<yyyy-mm-dd>/<run_id>/ (the immutable run record: spans,
#   events.jsonl, archived state.json), and logs/history/ (the cairndb
#   commit log of archived runs).  The date partition is derived from the
#   run_id's uuid7 timestamp, so keys are computable without listing.
# rules:
#   - run_prefix MUST be derivable from (flow_name, run_id) alone.
#   - Key layouts MUST NOT collide with cairndb's own reserved prefixes
#     (log/, snapshots/, logs/, txapplied/).
# dependencies:
#   - types.time
# aliases:
#   - run-folder
#   - key-layout
# triggers:
#   - where are run records stored
#   - how are storage keys laid out
# ---


def run_prefix(flow_name: str, run_id: UUID) -> str:
    """Key prefix of a run's record folder, computed from its identity.

    The date partition comes from the run_id's embedded uuid7 timestamp,
    so no listing is needed to locate a known run.

    Args:
        flow_name: The flow the run belongs to.
        run_id: The run's UUID (uuid7).

    Returns:
        Key prefix ``runs/<flow_name>/<yyyy-mm-dd>/<run_id>`` (no trailing
        slash).
    """
    date = Timestamp.from_uuid7(run_id).value.strftime("%Y-%m-%d")
    return f"runs/{flow_name}/{date}/{run_id}"

# ---
# endregion


# region @storage.append
# ---
# role: storage
# intent: append lines to a blob-store object via etag compare-and-swap
# description: >
#   Blob objects have no append primitive, so append_lines() loops:
#   read the object with its etag, concatenate the new lines, and CAS-write
#   (put-if-absent when the object does not exist yet).  A lost race is
#   retried on the fresh content.  Volume is a handful of lifecycle events
#   and span batches per run, so contention is negligible; the loop is
#   bounded and reports failure instead of raising.
# rules:
#   - append_lines MUST NOT raise; it returns False on exhaustion/errors.
#   - Lines MUST only ever be appended — existing content is never altered.
# dependencies:
#   - storage.keys
# aliases:
#   - cas-append
# triggers:
#   - how are jsonl files appended on blob storage
# ---


def append_lines(store: BlobStorage, key: str, lines: str) -> bool:
    """Append *lines* (newline-terminated) to object *key*, atomically.

    Args:
        store: The blob store.
        key: Object key of the .jsonl stream.
        lines: One or more ``\\n``-terminated lines to append.

    Returns:
        True when the append landed; False after exhausting retries or on
        a storage error (logged, never raised — callers are observability
        writers that must not disturb execution).
    """
    data = lines.encode("utf-8")
    try:
        for _ in range(_APPEND_ATTEMPTS):
            obj = store.get_object_sync(key)
            if obj is None:
                if store.put_object_sync(key, data, if_absent=True) is not None:
                    return True
            else:
                if store.put_object_sync(key, obj.data + data, if_match=obj.etag) is not None:
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

# ---
# endregion

"""
Log repository for reading per-span log files.

Provides LogRepository, which reads from the runs/<name>/<uuid>.jsonl
storage layout produced by the instrumentation layer.
"""
import logging
from uuid import UUID

from .models import RunLog
from .serdes import from_json
from .storage import StoragePath
from .types import Period, PeriodUUID


logger = logging.getLogger(__name__)


# region @log_repository
# ---
# role: storage
# intent: read per-span log files from the runs/<name>/<uuid>.jsonl layout
# description: >
#   LogRepository is the read side of the log storage layer.  Logs are stored
#   under runs/<span_name>/<span_id>.jsonl — one file per instrumented span.
#   Entries in a parent span's file may reference children via
#   extra.child_span_id / extra.child_span_name, allowing recursive traversal
#   of the full call tree without an index file.
# rules:
#   - MUST be synchronous (mirrors StoragePath I/O contract).
#   - list_run_ids MUST return ids in ascending UUIDv7 (chronological) order.
#   - get_logs_recursive MUST guard against cycles via a visited set.
#   - MUST skip malformed log lines rather than raising.
# dependencies:
#   - storage.types
#   - models.run
#   - serdes.json
# aliases:
#   - run-reader
# triggers:
#   - how to read logs for a run
#   - list runs for a flow
#   - get logs recursively with children
# ---


class LogRepository:
    """Read-only repository for per-span log files.

    Reads from the ``runs/<span_name>/<span_id>.jsonl`` layout written by
    the instrumentation layer.

    Attributes:
        base_path: Root directory that contains the ``runs/`` tree.
    """

    def __init__(self, base_path: StoragePath, **_):
        self.base_path = base_path

    def list_run_ids(
        self,
        flow_name: str | list[str] | None = None,
        period: Period | PeriodUUID | None = None,
    ) -> list[tuple[str, UUID]]:
        """List recorded (flow_name, span_id) pairs.

        When ``flow_name`` is ``None`` all subdirectories under
        ``<base_path>/runs/`` are scanned.  Results are sorted in ascending
        UUIDv7 order (equals chronological order) across all requested names.

        Args:
            flow_name: Flow / task name, list of names, or None for all.
            period: Optional time or UUID period to restrict results.

        Returns:
            Sorted list of ``(flow_name, span_id)`` tuples.
        """
        runs_dir = self.base_path / "runs"
        if not runs_dir.exists():
            return []

        if flow_name is None:
            names = [e.name for e in runs_dir.iterdir() if e.is_dir()]
        elif isinstance(flow_name, str):
            names = [flow_name]
        else:
            names = flow_name

        if isinstance(period, Period):
            period = period.to_uuid7()

        results: list[tuple[str, UUID]] = []
        for name in names:
            span_dir = runs_dir / name
            if not span_dir.exists():
                continue
            for entry in span_dir.iterdir():
                if not entry.name.endswith(".jsonl"):
                    continue
                try:
                    uid = UUID(entry.name[:-6])
                except ValueError:
                    continue
                if isinstance(period, PeriodUUID) and not period.covers(uid):
                    continue
                results.append((name, uid))

        results.sort(key=lambda t: str(t[1]))
        return results

    def get_logs(self, flow_name: str, span_id: UUID) -> list[RunLog]:
        """Read all log entries for a single span.

        Args:
            flow_name: The span_name directory under ``runs/``.
            span_id: The span UUID whose ``.jsonl`` file to read.

        Returns:
            Log entries in file order.  Empty list when the file is absent or
            every line is malformed.
        """
        file = self.base_path / "runs" / flow_name / f"{span_id}.jsonl"
        if not file.exists():
            return []
        return _read_file(file)

    def get_logs_recursive(
        self,
        flow_name: str,
        span_id: UUID,
        _visited: frozenset[UUID] = frozenset(),
    ) -> list[RunLog]:
        """Read logs for a span and all its descendants.

        Follows ``child_span_id`` / ``child_span_name`` references embedded in
        log entries' ``extra`` field (written by the instrumentation layer) to
        recursively collect every child span's logs.  A visited set prevents
        infinite traversal should a cycle appear in the recorded log graph.

        Args:
            flow_name: The span_name directory for the root span.
            span_id: The root span UUID to start from.
            _visited: Internal cycle-guard; callers should not pass this.

        Returns:
            Flat list of all RunLog entries from this span and all descendants,
            in depth-first traversal order (parent logs first, then each child
            subtree in the order the child-call entries appear in the parent).
        """
        if span_id in _visited:
            return []
        logs = self.get_logs(flow_name, span_id)
        result: list[RunLog] = list(logs)
        visited = _visited | {span_id}
        for log in logs:
            child_id_str = log.extra.get("child_span_id")
            child_name = log.extra.get("child_span_name")
            if not child_id_str or not child_name:
                continue
            try:
                child_id = UUID(child_id_str)
            except ValueError:
                logger.warning(
                    "skipping_invalid_child_span_id",
                    extra={"parent_span_id": str(span_id), "raw": child_id_str},
                )
                continue
            result.extend(self.get_logs_recursive(child_name, child_id, visited))
        return result


# ---
# endregion


# region
@def _read_file(file: StoragePath) -> list[RunLog]:
    """Parse all RunLog entries from a single .jsonl log file.

    Each non-blank line is expected to be a JSON object whose fields map to
    RunLog attributes.  Malformed lines are skipped with a warning.

    Args:
        file: Path to a .jsonl log file (local or blob storage).

    Returns:
        List of RunLog instances in file order.
    """
    try:
        content = file.read_text(encoding="utf-8")
    except Exception:
        logger.exception("read_run_logs_failed", extra={"path": str(file)})
        return []

    logs: list[RunLog] = []
    for lineno, raw in enumerate(content.splitlines(), 1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            logs.append(from_json(RunLog, raw))
        except Exception:
            logger.warning(
                "skipping_malformed_log_line",
                extra={"path": str(file), "line": lineno},
            )
    return logs
# endregion

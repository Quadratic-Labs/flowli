"""
Log repository for reading per-run span files.

Provides LogRepository, which reads from the
``runs/<flow_name>/<yyyy-mm-dd>/<run_id>/spans-<attempt>.jsonl`` layout
produced by the tracing layer (flowlet.tracing.BlobSpanExporter).
"""
import logging
from uuid import UUID

from ..models import SpanRecord
from ..serdes import from_json
from ..storage.types import StoragePath
from ..types import Period, PeriodUUID, Timestamp


logger = logging.getLogger(__name__)


# region @log_repository
# ---
# role: storage
# intent: read per-run span files from the runs/<flow>/<date>/<run_id>/ layout
# description: >
#   LogRepository is the read side of the run-record storage.  Each run owns
#   one folder containing spans-<attempt>.jsonl files (all spans of one
#   execution attempt) and, once closed and archived, a state.json.  All of a
#   run's spans live in its own folder, so reading a run is a single folder
#   scan — no recursive reference-following.  The date partition is derived
#   from the run_id's uuid7 timestamp, so paths are computable without
#   listing.
# rules:
#   - MUST be synchronous (mirrors StoragePath I/O contract).
#   - list_run_ids MUST return ids in ascending UUIDv7 (chronological) order.
#   - MUST skip malformed span lines rather than raising.
# dependencies:
#   - storage.types
#   - models.run
#   - serdes.json
#   - tracing
# aliases:
#   - run-reader
# triggers:
#   - how to read spans for a run
#   - list runs for a flow
# ---


def run_folder(base_path: StoragePath, flow_name: str, run_id: UUID) -> StoragePath:
    """Compute a run's folder path from its identity alone.

    The date partition comes from the run_id's embedded uuid7 timestamp,
    so no directory listing is needed to locate a known run.

    Args:
        base_path: Root directory that contains the ``runs/`` tree.
        flow_name: The flow the run belongs to.
        run_id: The run's UUID (uuid7).

    Returns:
        Path of the run's folder.
    """
    date = Timestamp.from_uuid7(run_id).value.strftime("%Y-%m-%d")
    return base_path / "runs" / flow_name / date / str(run_id)


class LogRepository:
    """Read-only repository for per-run span files.

    Reads from the ``runs/<flow_name>/<date>/<run_id>/`` layout written by
    the tracing layer.

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
        """List recorded (flow_name, run_id) pairs.

        When ``flow_name`` is ``None`` all flow directories under
        ``<base_path>/runs/`` are scanned.  When a period is given, only the
        date partitions it covers are walked.

        Args:
            flow_name: Flow name, list of names, or None for all.
            period: Optional time or UUID period to restrict results.

        Returns:
            Sorted (oldest-first, ascending UUIDv7) list of
            ``(flow_name, run_id)`` tuples.
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
            period = period.to_uuid()

        results: list[tuple[str, UUID]] = []
        for name in names:
            flow_dir = runs_dir / name
            if not flow_dir.exists():
                continue
            for date_dir in flow_dir.iterdir():
                if not date_dir.is_dir():
                    continue
                if isinstance(period, PeriodUUID) and not _date_in_period(
                    date_dir.name, period
                ):
                    continue
                for entry in date_dir.iterdir():
                    try:
                        uid = UUID(entry.name)
                    except ValueError:
                        continue
                    if isinstance(period, PeriodUUID) and not period.covers(uid):
                        continue
                    results.append((name, uid))

        results.sort(key=lambda t: str(t[1]))
        return results

    def get_spans(self, flow_name: str, run_id: UUID) -> list[SpanRecord]:
        """Read all recorded spans for a run, across all attempts.

        Args:
            flow_name: The flow the run belongs to.
            run_id: The run's UUID.

        Returns:
            SpanRecords from every ``spans-<attempt>.jsonl`` in the run
            folder, in file order.  Empty list when the folder is absent.
        """
        folder = run_folder(self.base_path, flow_name, run_id)
        if not folder.exists():
            return []
        spans: list[SpanRecord] = []
        for entry in sorted(folder.iterdir(), key=lambda p: p.name):
            if entry.name.startswith("spans-") and entry.name.endswith(".jsonl"):
                spans.extend(_read_file(entry))
        return spans

# ---
# endregion


# region

def _date_in_period(date_name: str, period: PeriodUUID) -> bool:
    """Cheap partition filter: keep date dirs that could contain the period.

    Compares the directory's date against the period bounds at day
    granularity; malformed directory names are kept (defensive — the
    per-run covers() check still applies).
    """
    bounds = period.to_timestamp()
    try:
        if bounds.start is not None and date_name < bounds.start.value.strftime("%Y-%m-%d"):
            return False
        if bounds.end is not None and date_name > bounds.end.value.strftime("%Y-%m-%d"):
            return False
    except Exception:
        return True
    return True


def _read_file(file: StoragePath) -> list[SpanRecord]:
    """Parse all SpanRecords from a single .jsonl span file.

    Malformed lines are skipped with a warning.

    Args:
        file: Path to a spans-<attempt>.jsonl file (local or blob storage).

    Returns:
        List of SpanRecord instances in file order.
    """
    try:
        content = file.read_text(encoding="utf-8")
    except Exception:
        logger.exception("read_run_spans_failed", extra={"path": str(file)})
        return []

    spans: list[SpanRecord] = []
    for lineno, raw in enumerate(content.splitlines(), 1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            spans.append(from_json(SpanRecord)(raw))
        except Exception:
            logger.warning(
                "skipping_malformed_span_line",
                extra={"path": str(file), "line": lineno},
            )
    return spans

# endregion

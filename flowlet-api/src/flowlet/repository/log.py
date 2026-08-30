"""
Log repository for reading per-run span files.

Provides LogRepository, which reads the
``runs/<flow_name>/<yyyy-mm-dd>/<obligation_id>/spans-<attempt>.jsonl`` objects
produced by the tracing layer (flowlet.tracing.BlobSpanExporter) from the
CairnDB blob store.
"""
import logging
from uuid import UUID

from cairndb.storage.base import BlobStorage

from flowlet.models import SpanRecord
from flowlet.serdes import from_json
from flowlet.storage import read_lines, run_prefix
from flowlet.types import Period, PeriodUUID

logger = logging.getLogger(__name__)


class LogRepository:
    """Read-only repository for per-run span objects.

    Reads the ``runs/<flow_name>/<date>/<obligation_id>/`` layout written by the
    tracing layer.

    Attributes:
        store: CairnDB blob store containing the ``runs/`` tree.
    """

    def __init__(self, store: BlobStorage, **_):
        self.store = store

    def list_run_ids(
        self,
        flow_name: str | list[str] | None = None,
        period: Period | PeriodUUID | None = None,
    ) -> list[tuple[str, UUID]]:
        """List recorded (flow_name, obligation_id) pairs.

        When ``flow_name`` is ``None`` the whole ``runs/`` prefix is listed.
        When a period is given, keys outside the date partitions it covers
        are skipped.

        Args:
            flow_name: Flow name, list of names, or None for all.
            period: Optional time or UUID period to restrict results.

        Returns:
            Sorted (oldest-first, ascending UUIDv7) list of
            ``(flow_name, obligation_id)`` tuples.
        """
        if flow_name is None:
            prefixes = ["runs/"]
        elif isinstance(flow_name, str):
            prefixes = [f"runs/{flow_name}/"]
        else:
            prefixes = [f"runs/{name}/" for name in flow_name]

        if isinstance(period, Period):
            period = period.to_uuid()

        found: set[tuple[str, UUID]] = set()
        for prefix in prefixes:
            for key in self.store.list_objects_sync(prefix):
                # runs/<flow>/<date>/<obligation_id>/<object>
                parts = key.split("/")
                if len(parts) < 5:
                    continue
                name, date, raw_id = parts[1], parts[2], parts[3]
                if isinstance(period, PeriodUUID) and not _date_in_period(date, period):
                    continue
                try:
                    uid = UUID(raw_id)
                except ValueError:
                    continue
                if isinstance(period, PeriodUUID) and not period.covers(uid):
                    continue
                found.add((name, uid))

        return sorted(found, key=lambda t: str(t[1]))

    def get_spans(self, flow_name: str, obligation_id: UUID) -> list[SpanRecord]:
        """Read all recorded spans for a run, across all attempts.

        Args:
            flow_name: The flow the run belongs to.
            obligation_id: The run's UUID.

        Returns:
            SpanRecords from every ``spans-<attempt>.jsonl`` under the run
            prefix, in key order.  Empty list when nothing was recorded.
        """
        prefix = f"{run_prefix(flow_name, obligation_id)}/"
        spans: list[SpanRecord] = []
        for key in sorted(self.store.list_objects_sync(prefix)):
            leaf = key.rsplit("/", 1)[-1]
            if leaf.startswith("spans-") and leaf.endswith(".jsonl"):
                spans.extend(self._read_span_object(key))
        return spans

    def _read_span_object(self, key: str) -> list[SpanRecord]:
        """Parse all SpanRecords from a single .jsonl span object.

        Malformed lines are skipped with a warning.
        """
        spans: list[SpanRecord] = []
        for lineno, raw in enumerate(read_lines(self.store, key), 1):
            try:
                spans.append(from_json(SpanRecord)(raw))
            except Exception:
                logger.warning(
                    "skipping_malformed_span_line",
                    extra={"key": key, "line": lineno},
                )
        return spans


# region

def _date_in_period(date_name: str, period: PeriodUUID) -> bool:
    """Cheap partition filter: keep date segments that could contain the period.

    Compares the key's date segment against the period bounds at day
    granularity; malformed segments are kept (defensive — the per-run
    covers() check still applies).
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


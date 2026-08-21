"""
Query layer for run history: RunState from SQLite, full Run from logs.

Three query patterns are served:

1. ``list_recent_states`` — the last *N* RunState rows per registered flow,
   covering every flow present in the cache (including archived ones that have
   never been run, which simply produce no rows).
2. ``list_states``        — paginated, filterable list of RunState rows for
   list / table views.
3. ``get_run``            — full run detail built by loading log files
   recursively (all subflows / subtasks) and deriving a ``RunSummary`` tree via
   :func:`~flowlet.analysis.summarise`.
"""
import logging
from uuid import UUID

from sqlalchemy import Select, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .. import analysis
from ..repository.log import LogRepository
from ..serdes import destructure
from .cache import CacheRepository
from .database import Run as RunRow

logger = logging.getLogger(__name__)


# region @query
# ---
# role: api
# intent: query RunState rows from SQLite and reconstruct full Run details from log files
# description: >
#   RunQuery is the read-side of the Flowlet API query layer.  Three query
#   patterns are served:
#   1. list_recent_states — last N RunState rows per registered flow; the
#      the cache's distinct flow names are used when flow_names is None
#      is covered.  Flows never run produce no rows.
#   2. list_states        — paginated, filterable RunState query for list views.
#   3. get_run            — full run detail: logs loaded recursively from
#      storage (subflows / subtasks included), summarised into a RunSummary
#      tree, returned as a dict compatible with RunDTO.model_validate.
#   SQLite queries use CacheRepository's engine; every async query method
#   awaits cache.refresh() first (TTL-throttled scan of state files).
#   Log loading is synchronous (the store's *_sync methods are primitive).
# rules:
#   - MUST NOT write to the database; this is a read-only component.
#   - list_recent_states MUST cover every flow in the cache when
#     flow_names is None (the kernel knows no registry).
#   - get_run MUST read spans via LogRepository.get_spans (one folder per run).
#   - Timestamps MUST be serialised as plain datetime (UTC) in dicts returned by get_run.
# dependencies:
#   - analysis
#   - models.run
#   - log_repository
#   - cache.repository
#   - database.models
# aliases:
#   - run-query
# triggers:
#   - how to query recent runs
#   - how to get run details with logs
#   - paginate run states
# ---


class RunQuery:
    """Read-only query object for run history.

    Combines the run cache (RunState rows, pull-refreshed from state files)
    with span-file data to serve all read operations required by the API.

    Attributes:
        cache: CacheRepository providing the local SQLite engine, refreshed
            on demand from storage.
        cache: Also enumerates known flow names when no explicit filter is
            given to ``list_recent_states``.
        log_repo: Loads span files from run folders.
    """
    SQL_RECENT_STATES = (
        select(RunRow)
        .where(RunRow.flow_name == bindparam("flow_name"))
        .order_by(RunRow.run_id.desc())  # UUIDv7: descending = newest first
        .limit(bindparam("last_n"))
    )
    SQL_STATES = (
        select(RunRow)
        .where(RunRow.flow_name == bindparam("flow_name"))
    )

    def __init__(
        self,
        *,
        cache_repo: CacheRepository,
        log_repo: LogRepository,
        **_,
    ):
        """Initialise the query object.

        Args:
            cache_repo: CacheRepository owning the local SQLite engine.
            log_repo: LogRepository for reading span files from storage.
            **_: Unused keyword arguments accepted for dependency-injection
                compatibility.
        """
        self.cache = cache_repo
        self.log_repo = log_repo

    def _session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self.cache.session_factory()

    @classmethod
    def _sql_id_range(cls, stmt: Select) -> Select:
        # Mirrors PeriodUUID.covers(): strictly inside (start_id, end_id).
        return (
            stmt
            .where(RunRow.run_id > bindparam("start_id"))
            .where(RunRow.run_id < bindparam("end_id"))
        )

    @classmethod
    def _sql_limit_offset(cls, stmt: Select) -> Select:
        return (
            stmt
            .order_by(RunRow.run_id.desc())  # UUIDv7: descending = newest first
            .limit(bindparam("limit"))
            .offset(bindparam("offset"))
        )

    async def _known_flow_names(self) -> list[str]:
        """Distinct flow names present in the refreshed cache."""
        from sqlalchemy import text

        async with self._session_factory()() as session:
            result = await session.execute(
                text("SELECT DISTINCT flow_name FROM runs ORDER BY flow_name")
            )
            return [row[0] for row in result.fetchall()]

    async def list_recent_states(
        self,
        flow_names: list[str] | None = None,
        *,
        last_n: int = 5,
    ) -> list[RunRow]:
        """Fetch the most recent *last_n* RunState rows for each flow.

        When *flow_names* is ``None`` the cache's distinct flow names are
        used, so every flow that ever produced a run is represented.

        Args:
            flow_names: Flows to include, or ``None`` for all known flows.
            last_n: Maximum number of rows to return per flow.

        Returns:
            ``Run`` ORM rows, ordered newest-first within each flow.  Rows
            support attribute access and are compatible with
            :class:`~flowlet.api.models.RunStateDTO` via
            ``RunStateDTO.model_validate(row)``.
        """
        await self.cache.refresh()
        names = (
            flow_names
            if flow_names is not None
            else await self._known_flow_names()
        )
        if not names:
            return []

        stmt = self.SQL_RECENT_STATES
        rows: list[RunRow] = []
        async with self._session_factory()() as session:
            for name in names:
                result = await session.execute(stmt, {"flow_name": name, "last_n": last_n})
                rows.extend(result.scalars().all())

        logger.debug(
            "list_recent_states",
            extra={"flows": len(names), "rows": len(rows), "last_n": last_n},
        )
        return rows

    async def list_states(
        self,
        flow_names: list[str] | None = None,
        *,
        offset_limit: tuple[int, int] | None = None,
        id_range: tuple[UUID, UUID] | None = None,
    ) -> list[RunRow]:
        """List RunState rows with flexible filters, ordered newest first.

        Intended for paginated list or table views where runs from all flows are
        shown together without per-flow grouping.

        Args:
            flow_names: Optional flow name allow-list; ``None`` means all flows.
            status: Optional status allow-list; ``None`` means no filter.
            limit: Maximum number of rows to return.
            offset: Number of rows to skip before returning results (for
                cursor-style pagination use ``run_id`` ordering directly).

        Returns:
            ``Run`` ORM rows, newest first.  Compatible with
            :class:`~flowlet.api.models.RunStateDTO` via ``model_validate``.
        """
        names = flow_names if flow_names is not None else self.registry.list_flows()
        stmt = self.SQL_STATES
        params = {}
        if id_range:
            stmt = self._sql_id_range(stmt)
            params["start_id"] = id_range[0]
            params["end_id"] = id_range[1]
        if offset_limit:
            stmt = self._sql_limit_offset(stmt)
            params["offset"] = offset_limit[0]
            params["limit"] = offset_limit[1]

        await self.cache.refresh()
        rows: list[RunRow] = []
        async with self._session_factory()() as session:
            for name in names:
                result = await session.execute(stmt, {"flow_name": name, **params})
                rows.extend(result.scalars().all())

        logger.debug("list_states", extra={"flows": len(names), "rows": len(rows)})
        return rows

    async def find_flow_name(self, run_id: UUID) -> str | None:
        """Resolve the flow that owns *run_id*, or None when unknown.

        Args:
            run_id: UUID of the run to resolve.

        Returns:
            The owning flow's name, or None if the run is not in the cache.
        """
        await self.cache.refresh()
        async with self._session_factory()() as session:
            result = await session.execute(
                select(RunRow.flow_name).where(RunRow.run_id == run_id)
            )
            return result.scalar_one_or_none()

    async def get_run_by_run_id(self, run_id: UUID, with_logs: bool = True) -> dict:
        """Load a run by run_id alone, looking up flow_name from the database.

        Convenience wrapper around ``get_run`` that first queries the SQLite
        snapshot to discover which flow owns the run.

        Args:
            run_id: UUID of the run to fetch.
            with_logs: should or not include logs in the return value

        Returns:
            Dict with ``RunSummaryDTO``-compatible keys plus a ``"logs"`` list
            if requested.  Suitable for ``RunDTO.model_validate(result)``.

        Raises:
            ValueError: When no run with the given ``run_id`` exists.
        """
        await self.cache.refresh()
        async with self._session_factory()() as session:
            result = await session.execute(
                select(RunRow).where(RunRow.run_id == run_id)
            )
            row = result.scalar_one_or_none()
            if row is None:
                raise ValueError(f"Run {run_id} not found")
            return self.get_run(row.flow_name, run_id, with_logs)

    def get_run(self, flow_name: str, run_id: UUID, with_logs: bool = True) -> dict:
        """Load all logs for a run and compute its hierarchical summary.

        Reads log entries recursively from storage starting at the root span
        identified by (*flow_name*, *run_id*), following every
        the run folder — every span of the run lives there, so subflow and
        all nested subflows and subtasks are included.

        Derives a :class:`~flowlet.models.RunSummary` tree from those logs via
        :func:`~flowlet.analysis.summarise`, then serialises the result to a
        dict compatible with :class:`~flowlet.api.models.RunDTO`.

        Args:
            flow_name: Name of the root flow (selects the log subdirectory).
            run_id: UUID of the root span to start loading from.
            with_logs: should or not include logs in the return value

        Returns:
            Dict with ``RunSummaryDTO``-compatible keys (``span_id``,
            ``span_name``, ``span_type``, ``status``, ``start_ts``,
            ``end_ts``, ``children``) plus a ``"logs"`` list of
            ``RunLogDTO``-compatible dicts.  Suitable for
            ``RunDTO.model_validate(result)``.

        Raises:
            ValueError: When the loaded logs contain no identifiable root span
                (propagated from :func:`~flowlet.analysis.summarise`).
        """
        logs = self.log_repo.get_spans(flow_name, run_id)
        logger.debug(
            "get_run",
            extra={"flow_name": flow_name, "run_id": str(run_id), "logs": len(logs)},
        )
        summary = analysis.summarise(logs)
        result = destructure(summary)
        if with_logs:
            result["logs"] = destructure(logs)
        return result

# ---
# endregion

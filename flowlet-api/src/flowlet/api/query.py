"""
Query layer for run history: ObligationSummary from SQLite, full Run from logs.

Three query patterns are served:

1. ``list_recent_states`` — the last *N* ObligationSummary rows per registered flow,
   covering every flow present in the cache (including archived ones that have
   never been run, which simply produce no rows).
2. ``list_states``        — paginated, filterable list of ObligationSummary rows for
   list / table views.
3. ``get_run``            — full run detail built by loading log files
   recursively (all subflows / subtasks) and deriving a ``TraceSummary`` tree via
   :func:`~flowlet.analysis.summarise`.
"""
import logging
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import Select, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from flowlet import analysis
from flowlet.models import ObligationSummary
from flowlet.repository.log import LogRepository
from flowlet.serdes import destructure
from flowlet.api.cache import CacheRepository, ObligationRow

if TYPE_CHECKING:
    from flowlet.history import RunHistory

logger = logging.getLogger(__name__)


class RunQuery:
    """Read-only query object for run history.

    Combines the run cache (ObligationSummary rows, pull-refreshed from state files)
    with span-file data to serve all read operations required by the API.
    When a RunHistory is configured, its durable SQLite projection is an
    additive long-horizon source, merged into ``list_recent_states``.

    Attributes:
        cache: CacheRepository providing the local SQLite engine, refreshed
            on demand from storage.
        cache: Also enumerates known flow names when no explicit filter is
            given to ``list_recent_states``.
        log_repo: Loads span files from run folders.
        history: Optional RunHistory; None when ``configs.history`` is off.
    """
    SQL_RECENT_STATES = (
        select(ObligationRow)
        .where(ObligationRow.flow_name == bindparam("flow_name"))
        .order_by(ObligationRow.obligation_id.desc())  # UUIDv7: descending = newest first
        .limit(bindparam("last_n"))
    )
    SQL_STATES = (
        select(ObligationRow)
        .where(ObligationRow.flow_name == bindparam("flow_name"))
    )

    def __init__(
        self,
        *,
        cache_repo: CacheRepository,
        log_repo: LogRepository,
        history: "RunHistory | None" = None,
        **_,
    ):
        """Initialise the query object.

        Args:
            cache_repo: CacheRepository owning the local SQLite engine.
            log_repo: LogRepository for reading span files from storage.
            history: Optional RunHistory providing the durable long-horizon
                projection; None when ``configs.history`` is off.
            **_: Unused keyword arguments accepted for dependency-injection
                compatibility.
        """
        self.cache = cache_repo
        self.history = history
        self.log_repo = log_repo

    def _session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self.cache.session_factory()

    @classmethod
    def _sql_id_range(cls, stmt: Select) -> Select:
        # Mirrors PeriodUUID.covers(): strictly inside (start_id, end_id).
        return (
            stmt
            .where(ObligationRow.obligation_id > bindparam("start_id"))
            .where(ObligationRow.obligation_id < bindparam("end_id"))
        )

    @classmethod
    def _sql_limit_offset(cls, stmt: Select) -> Select:
        return (
            stmt
            .order_by(ObligationRow.obligation_id.desc())  # UUIDv7: descending = newest first
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
    ) -> list[ObligationRow | ObligationSummary]:
        """Fetch the most recent *last_n* ObligationSummary rows for each flow.

        When *flow_names* is ``None`` the cache's and (if configured) the
        history projection's distinct flow names are used, so every flow
        that ever produced a run is represented — including one whose only
        runs have aged out of the ephemeral cache since the last cold start.

        When a RunHistory is configured, its rows are merged in per flow,
        additively: a obligation_id already present from the cache is never
        duplicated or overwritten (the cache is live and always wins), and
        the merged list per flow is still capped at *last_n*.

        Args:
            flow_names: Flows to include, or ``None`` for all known flows.
            last_n: Maximum number of rows to return per flow.

        Returns:
            ``ObligationRow`` rows and/or ``ObligationSummary`` objects, ordered
            newest-first within each flow.  Both support attribute access
            and are compatible with :class:`~flowlet.api.models.ObligationSummaryDTO`
            via ``ObligationSummaryDTO.model_validate(row)``.
        """
        await self.cache.refresh()
        cache_names = await self._known_flow_names()
        if flow_names is not None:
            names = flow_names
        elif self.history is not None:
            names = sorted(set(cache_names) | set(await self.history.known_flow_names()))
        else:
            names = cache_names
        if not names:
            return []

        stmt = self.SQL_RECENT_STATES
        rows: list[ObligationRow | ObligationSummary] = []
        cache_counts: dict[str, int] = {}
        async with self._session_factory()() as session:
            for name in names:
                result = await session.execute(stmt, {"flow_name": name, "last_n": last_n})
                flow_rows = result.scalars().all()
                cache_counts[name] = len(flow_rows)
                rows.extend(flow_rows)

        if self.history is not None:
            seen = {row.obligation_id for row in rows}
            for name in names:
                remaining = last_n - cache_counts.get(name, 0)
                if remaining <= 0:
                    continue
                for state in await self.history.list_states([name], last_n=remaining):
                    if state.obligation_id not in seen:
                        rows.append(state)
                        seen.add(state.obligation_id)

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
    ) -> list[ObligationRow]:
        """List ObligationSummary rows with flexible filters, ordered newest first.

        Intended for paginated list or table views where runs from all flows are
        shown together without per-flow grouping.

        Args:
            flow_names: Optional flow name allow-list; ``None`` means all flows.
            status: Optional status allow-list; ``None`` means no filter.
            limit: Maximum number of rows to return.
            offset: Number of rows to skip before returning results (for
                cursor-style pagination use ``obligation_id`` ordering directly).

        Returns:
            ``ObligationRow`` rows, newest first.  Compatible with
            :class:`~flowlet.api.models.ObligationSummaryDTO` via ``model_validate``.
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
        rows: list[ObligationRow] = []
        async with self._session_factory()() as session:
            for name in names:
                result = await session.execute(stmt, {"flow_name": name, **params})
                rows.extend(result.scalars().all())

        logger.debug("list_states", extra={"flows": len(names), "rows": len(rows)})
        return rows

    async def find_flow_name(self, obligation_id: UUID) -> str | None:
        """Resolve the flow that owns *obligation_id*, or None when unknown.

        Args:
            obligation_id: UUID of the run to resolve.

        Returns:
            The owning flow's name, or None if the run is not in the cache.
        """
        await self.cache.refresh()
        async with self._session_factory()() as session:
            result = await session.execute(
                select(ObligationRow.flow_name).where(ObligationRow.obligation_id == obligation_id)
            )
            return result.scalar_one_or_none()

    async def get_run_by_run_id(self, obligation_id: UUID, with_logs: bool = True) -> dict:
        """Load a run by obligation_id alone, looking up flow_name from the database.

        Convenience wrapper around ``get_run`` that first queries the SQLite
        snapshot to discover which flow owns the run.

        Args:
            obligation_id: UUID of the run to fetch.
            with_logs: should or not include logs in the return value

        Returns:
            Dict with ``TraceSummaryDTO``-compatible keys plus a ``"logs"`` list
            if requested.  Suitable for ``TraceDTO.model_validate(result)``.

        Raises:
            ValueError: When no run with the given ``obligation_id`` exists.
        """
        await self.cache.refresh()
        async with self._session_factory()() as session:
            result = await session.execute(
                select(ObligationRow).where(ObligationRow.obligation_id == obligation_id)
            )
            row = result.scalar_one_or_none()
            if row is None:
                raise ValueError(f"Run {obligation_id} not found")
            return self.get_run(row.flow_name, obligation_id, with_logs)

    def get_run(self, flow_name: str, obligation_id: UUID, with_logs: bool = True) -> dict:
        """Load all logs for a run and compute its hierarchical summary.

        Reads log entries recursively from storage starting at the root span
        identified by (*flow_name*, *obligation_id*), following every
        the run folder — every span of the run lives there, so subflow and
        all nested subflows and subtasks are included.

        Derives a :class:`~flowlet.models.TraceSummary` tree from those logs via
        :func:`~flowlet.analysis.summarise`, then serialises the result to a
        dict compatible with :class:`~flowlet.api.models.TraceDTO`.

        Args:
            flow_name: Name of the root flow (selects the log subdirectory).
            obligation_id: UUID of the root span to start loading from.
            with_logs: should or not include logs in the return value

        Returns:
            Dict with ``TraceSummaryDTO``-compatible keys (``span_id``,
            ``span_name``, ``span_type``, ``status``, ``start_ts``,
            ``end_ts``, ``children``) plus a ``"logs"`` list of
            ``RunLogDTO``-compatible dicts.  Suitable for
            ``TraceDTO.model_validate(result)``.

        Raises:
            ValueError: When the loaded logs contain no identifiable root span
                (propagated from :func:`~flowlet.analysis.summarise`).
        """
        logs = self.log_repo.get_spans(flow_name, obligation_id)
        logger.debug(
            "get_run",
            extra={"flow_name": flow_name, "obligation_id": str(obligation_id), "logs": len(logs)},
        )
        summary = analysis.summarise(logs)
        result = destructure(summary)
        if with_logs:
            result["logs"] = destructure(logs)
        return result

"""Account-transition feed — an ordered, cursor-addressable wake-up channel.

Delta 3 of the account model (abstractions v0.3): reconcilers that derive
obligations from completion events — dependency controllers, reviewer
dispatch — need lifecycle transitions cheaply and *orderedly* observable.
Polling ``list_views`` is O(active set) per tick with neither ordering nor
a cursor; the feed is the CairnDB named log ``transitions`` (keys under
``logs/transitions/``), fed by the same emission sites as the per-run
event log — every :meth:`flowlet.events.EventLog.append` also lands one
``account.transition`` event here when the feed is configured.

The feed is a wake-up channel, never authority:

- Appends happen *after* the account CAS and are strictly non-throwing.
  A crash between the CAS and the append drops that event; a re-driven
  transition can duplicate one.
- Consumers therefore confirm what they read against the account and keep
  a reconciliation poll as the slow-path fallback — the feed makes the
  common case fast, it does not replace the poll.
- The cursor is the log's commit number.  Pages end on commit boundaries,
  so resuming from a returned cursor never skips an event and never
  re-yields one, beyond commit granularity.

One sequencer orders the whole feed (a CairnDB named log is totally
ordered), which caps its append rate — acceptable under the engine's
stated scale profile of low-frequency, high-value obligations.
"""
import asyncio
import logging
from typing import Any

from attrs import Factory, define
from cairndb import Event, EventType, SchemaVersion
from cairndb import Timestamp as CairnTimestamp
from cairndb.engine.logs import Log
from cairndb.storage.base import BlobStorage

logger = logging.getLogger(__name__)

TRANSITIONS_LOG_NAME = "transitions"
TRANSITION_SCHEMA_VERSION = "1.0.0"
ACCOUNT_TRANSITION = "account.transition"


@define(slots=True, kw_only=True)
class TransitionPage:
    """One page of the feed.

    Attributes:
        entries: Transition records in log order — the per-run event record
            (ts, run_id, flow_name, event, actor, attempt, from/to, cause)
            plus ``seq``, the entry's ``<commit>.<index>`` position.
        cursor: Commit number of the last fully-consumed commit; pass it
            back as ``after`` to continue.  Unchanged when no entries.
    """
    entries: list[dict[str, Any]] = Factory(list)
    cursor: int = 0


@define(slots=True, kw_only=True)
class TransitionFeed:
    """Writer/reader for the account-transition named log.

    Attributes:
        store: The framework's blob store; events land on the named log
            ``transitions`` (keys under ``logs/transitions/``).
    """

    store: BlobStorage

    def record(self, record: dict[str, Any]) -> None:
        """Append one transition; strictly non-throwing.

        The feed is derived, so its liveness must never gate a transition:
        any failure is logged and swallowed — the account already holds the
        truth, and reconcilers' fallback poll covers the gap.

        Args:
            record: The per-run event record (see EventLog.append).
        """
        try:
            asyncio.run(self._append(record))
        except Exception:
            logger.warning(
                "transition_append_failed",
                extra={
                    "run_id": record.get("run_id"),
                    "event": record.get("event"),
                },
                exc_info=True,
            )

    async def _append(self, record: dict[str, Any]) -> None:
        log = Log(self.store, TRANSITIONS_LOG_NAME)
        try:
            await log.append(
                Event(
                    event_type=EventType(ACCOUNT_TRANSITION),
                    timestamp=CairnTimestamp.now(),
                    payload=record,
                    schema_version=SchemaVersion(TRANSITION_SCHEMA_VERSION),
                )
            )
        finally:
            await log.close()

    def read(self, after: int = 0, limit: int = 256) -> TransitionPage:
        """Read transitions after *after*, in order, with a resume cursor.

        Args:
            after: Cursor from a previous page (0 reads from the start).
            limit: Soft page cap — the page completes the commit it is in
                when the cap is hit, so the cursor stays on a commit
                boundary.

        Returns:
            The next page; ``entries`` is empty at the tail.
        """
        return asyncio.run(self._read(after, limit))

    async def _read(self, after: int, limit: int) -> TransitionPage:
        log = Log(self.store, TRANSITIONS_LOG_NAME)
        entries: list[dict[str, Any]] = []
        cursor = after
        async for commit in log.read_commits(after=after):
            for i, event in enumerate(commit.events):
                entry = dict(event.payload)
                entry["seq"] = f"{commit.number}.{i}"
                entries.append(entry)
            cursor = commit.number
            if len(entries) >= limit:
                break
        return TransitionPage(entries=entries, cursor=cursor)

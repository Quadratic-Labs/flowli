"""In-memory PubSub implementation for tests and single-process usage.

Uses a ``threading.Condition`` so that ``subscribe`` blocks correctly until
the next event arrives.  Multiple subscribers on the same channel each
receive their own independent copy of the event stream from the moment
they subscribed — i.e. late-join semantics.
"""
import logging
import threading
from collections import defaultdict
from collections.abc import Iterator

from attrs import define, field

from ..types import JsonData

logger = logging.getLogger(__name__)


# region @pubsub.memory
# ---
# role: adapter
# intent: thread-safe in-memory PubSub for tests and single-process usage
# description: >
#   InMemoryPubSub stores published events in per-channel queues protected by
#   a shared Condition variable.  Each subscriber holds its own read cursor so
#   that multiple concurrent subscribers on the same channel are independent.
# rules:
#   - MUST be thread-safe.
#   - publish MUST NOT raise.
#   - subscribe MUST yield events in publish order.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class InMemoryPubSub:
    """Thread-safe in-memory publish/subscribe bus.

    Suitable for unit tests and single-process integration scenarios.

    Attributes:
        _store: Per-channel list of accumulated event payloads.
        _cond: Condition variable used to wake sleeping subscribers.
        _closed: When True, all subscribe generators will stop iteration.
    """

    _store: dict[str, list[JsonData]] = field(factory=lambda: defaultdict(list), alias="_store")
    _cond: threading.Condition = field(factory=threading.Condition, alias="_cond")
    _closed: bool = field(default=False, alias="_closed")

    def publish(self, channel: str, event: JsonData) -> None:
        """Publish an event to all subscribers of *channel*.

        Args:
            channel: Logical channel name.
            event: JSON-serialisable event payload.
        """
        with self._cond:
            self._store[channel].append(event)
            self._cond.notify_all()

    def subscribe(self, channel: str) -> Iterator[JsonData]:
        """Yield events on *channel* in publish order, blocking between events.

        The generator starts from the next event published *after* the call to
        ``subscribe`` (late-join semantics).  It stops when ``close()`` is called.

        Args:
            channel: Logical channel name.

        Yields:
            Event payloads in arrival order.
        """
        with self._cond:
            cursor = len(self._store.get(channel, []))

        while True:
            with self._cond:
                while (
                    not self._closed
                    and len(self._store.get(channel, [])) <= cursor
                ):
                    self._cond.wait()

                if self._closed:
                    return

                events = self._store[channel]
                while cursor < len(events):
                    yield events[cursor]
                    cursor += 1

    def close(self) -> None:
        """Signal all subscribers to stop iteration."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def events(self, channel: str) -> list[JsonData]:
        """Return a snapshot of all events published to *channel*.

        Intended for test assertions.

        Args:
            channel: Logical channel name.

        Returns:
            Immutable copy of the event list.
        """
        with self._cond:
            return list(self._store.get(channel, []))

# ---
# endregion

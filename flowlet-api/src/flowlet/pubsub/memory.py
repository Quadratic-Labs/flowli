"""In-memory PubSub implementation for tests and single-process usage.

Uses a ``threading.Condition`` so that ``subscribe`` blocks correctly until
the next event arrives.  Multiple subscribers on the same channel each
receive their own independent copy of the event stream from the moment
they subscribed — i.e. late-join semantics.
"""
import logging
import threading
from collections import defaultdict
from collections.abc import Callable, Iterator

from attrs import define, field

logger = logging.getLogger(__name__)


# region @pubsub.memory
# ---
# role: adapter
# intent: thread-safe in-memory PubSub for tests and single-process usage
# description: >
#   InMemoryPubSub[T] stores published events as JSON strings in per-channel
#   queues protected by a shared Condition variable.  Serialization and
#   deserialization are handled internally via the injected serializer /
#   deserializer pair; callers work with domain objects of type T.
#   Each subscriber holds its own read cursor so that multiple concurrent
#   subscribers on the same channel are independent.
# rules:
#   - MUST be thread-safe.
#   - publish MUST NOT raise.
#   - subscribe MUST yield events in publish order.
#   - serializer / deserializer MUST be provided at construction.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class InMemoryPubSub[T]:
    """Thread-safe in-memory publish/subscribe bus.

    Suitable for unit tests and single-process integration scenarios.
    Callers publish and receive domain objects of type ``T``; JSON
    serialisation is handled internally.

    Attributes:
        serializer: Converts a domain object to a JSON string.
        deserializer: Reconstructs a domain object from a JSON string.
        _store: Per-channel list of accumulated wire payloads (JSON strings).
        _cond: Condition variable used to wake sleeping subscribers.
        _closed: When True, all subscribe generators will stop iteration.
    """

    serializer: Callable[[T], str]
    deserializer: Callable[[str], T]
    _store: dict[str, list[str]] = field(factory=lambda: defaultdict(list), alias="_store")
    _cond: threading.Condition = field(factory=threading.Condition, alias="_cond")
    _closed: bool = field(default=False, alias="_closed")

    def publish(self, channel: str, event: T) -> None:
        """Publish a domain event to all subscribers of *channel*.

        Args:
            channel: Logical channel name.
            event: Domain object to publish.
        """
        with self._cond:
            self._store[channel].append(self.serializer(event))
            self._cond.notify_all()
        logger.debug("pubsub_publish", extra={"channel": channel})

    def subscribe(self, channel: str) -> Iterator[T]:
        """Yield events on *channel* in publish order, blocking between events.

        The generator starts from the next event published *after* the call to
        ``subscribe`` (late-join semantics).  It stops when ``close()`` is called.

        Args:
            channel: Logical channel name.

        Yields:
            Domain objects in arrival order.
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

                raw_events = self._store[channel]
                while cursor < len(raw_events):
                    yield self.deserializer(raw_events[cursor])
                    cursor += 1

    def close(self) -> None:
        """Signal all subscribers to stop iteration."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        logger.info("pubsub_closed")

    def events(self, channel: str) -> list[T]:
        """Return a snapshot of all events published to *channel*.

        Intended for test assertions.

        Args:
            channel: Logical channel name.

        Returns:
            Immutable copy of the deserialised event list.
        """
        with self._cond:
            return [self.deserializer(raw) for raw in self._store.get(channel, [])]

# ---
# endregion

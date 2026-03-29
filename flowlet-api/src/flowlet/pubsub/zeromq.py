"""ZeroMQ PubSub implementation for local multi-process usage.

Uses ZMQ PUB/SUB sockets.  The publisher binds to a well-known endpoint;
subscribers connect to it.  Channel names are used as ZMQ topic prefixes.

Install ``pyzmq`` to use this backend::

    pip install pyzmq
"""
import logging
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING

from attrs import define, field

from .config import ZeroMQPubSubConfig

if TYPE_CHECKING:
    import zmq  # type: ignore[import-untyped]

try:
    import zmq  # type: ignore[import-untyped]
except ImportError:
    pass

logger = logging.getLogger(__name__)


# region @pubsub.zeromq
# ---
# role: adapter
# intent: multi-process PubSub over a local ZeroMQ PUB/SUB socket pair
# description: >
#   ZeroMQPubSub[T] wraps a zmq.PUB socket for publishing and creates a fresh
#   zmq.SUB socket per subscribe() call.  Channel names become ZMQ topic
#   prefixes, so a subscriber on "state/flow" receives all events published
#   to channels that start with "state/flow".  Callers work with domain
#   objects of type T; the injected serializer/deserializer pair handles the
#   wire format (JSON bytes) transparently.
# rules:
#   - publish MUST NOT raise; log errors and continue.
#   - subscribe yields deserialised domain objects from ZMQ multipart frames.
#   - The publisher socket MUST be bound before any subscriber connects.
#   - serializer / deserializer MUST be provided at construction.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class ZeroMQPubSub[T]:
    """ZeroMQ-backed publish/subscribe bus for multi-process use.

    The publisher socket is created lazily on the first call to ``publish``.
    Each call to ``subscribe`` creates its own independent SUB socket.
    Callers publish and receive domain objects of type ``T``; JSON
    serialisation is handled internally.

    Attributes:
        serializer: Converts a domain object to a JSON string.
        deserializer: Reconstructs a domain object from a JSON string.
        endpoint: ZMQ endpoint the publisher binds to.
            Subscribers connect to the same endpoint.
        _ctx: Shared ZMQ context (created on first use).
        _pub: Bound PUB socket (created on first use).
    """

    config: ZeroMQPubSubConfig
    serializer: Callable[[T], str]
    deserializer: Callable[[str], T]
    _ctx: object = field(default=None, alias="_ctx")
    _pub: object = field(default=None, alias="_pub")

    def _ensure_publisher(self) -> None:
        if self._pub is not None:
            return
        self._ctx = zmq.Context.instance()
        self._pub = self._ctx.socket(zmq.PUB)  # type: ignore[attr-defined]
        self._pub.bind(self.config.endpoint)

    def publish(self, channel: str, event: T) -> None:
        """Publish a domain event to *channel*.

        Args:
            channel: Logical channel name used as the ZMQ topic prefix.
            event: Domain object to publish.
        """
        try:
            self._ensure_publisher()
            self._pub.send_multipart([channel.encode(), self.serializer(event).encode()])  # type: ignore[union-attr]
        except Exception:
            logger.exception("zmq_publish_error", extra={"channel": channel})

    def subscribe(self, channel: str) -> Iterator[T]:
        """Subscribe to *channel* and yield arriving domain events.

        Creates a new SUB socket for this subscription.  Blocks between events.
        The generator runs until the socket is explicitly closed or the process
        exits.

        Args:
            channel: Logical channel name (ZMQ topic prefix filter).

        Yields:
            Domain objects in arrival order.
        """
        ctx = zmq.Context.instance()
        sub = ctx.socket(zmq.SUB)  # type: ignore[attr-defined]
        sub.connect(self.config.endpoint)
        sub.setsockopt_string(zmq.SUBSCRIBE, channel)  # type: ignore[attr-defined]
        try:
            while True:
                parts = sub.recv_multipart()
                if len(parts) < 2:
                    continue
                yield self.deserializer(parts[1].decode())
        finally:
            sub.close()

    def close(self) -> None:
        """Close the publisher socket and release the ZMQ context."""
        if self._pub is not None:
            self._pub.close()
            self._pub = None

# ---
# endregion

"""ZeroMQ PubSub implementation for local multi-process usage.

Uses ZMQ PUB/SUB sockets.  The publisher binds to a well-known endpoint;
subscribers connect to it.  Channel names are used as ZMQ topic prefixes.

Install ``pyzmq`` to use this backend::

    pip install pyzmq
"""
import json
import logging
from collections.abc import Iterator

from attrs import define, field

from ..types import JsonData

logger = logging.getLogger(__name__)

_DEFAULT_ENDPOINT = "tcp://127.0.0.1:5555"


# region @pubsub.zeromq
# ---
# role: adapter
# intent: multi-process PubSub over a local ZeroMQ PUB/SUB socket pair
# description: >
#   ZeroMQPubSub wraps a zmq.PUB socket for publishing and creates a fresh
#   zmq.SUB socket per subscribe() call.  Channel names become ZMQ topic
#   prefixes, so a subscriber on "state/flow" receives all events published
#   to channels that start with "state/flow".
# rules:
#   - publish MUST NOT raise; log errors and continue.
#   - subscribe yields JSON-decoded dicts from ZMQ multipart frames.
#   - The publisher socket MUST be bound before any subscriber connects.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class ZeroMQPubSub:
    """ZeroMQ-backed publish/subscribe bus for multi-process use.

    The publisher socket is created lazily on the first call to ``publish``.
    Each call to ``subscribe`` creates its own independent SUB socket.

    Attributes:
        endpoint: ZMQ endpoint the publisher binds to.
            Subscribers connect to the same endpoint.
        _ctx: Shared ZMQ context (created on first use).
        _pub: Bound PUB socket (created on first use).
    """

    endpoint: str = field(default=_DEFAULT_ENDPOINT)
    _ctx: object = field(default=None, alias="_ctx")
    _pub: object = field(default=None, alias="_pub")

    def _ensure_publisher(self) -> None:
        if self._pub is not None:
            return
        try:
            import zmq  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("Install 'pyzmq' to use ZeroMQPubSub") from exc

        self._ctx = zmq.Context.instance()
        self._pub = self._ctx.socket(zmq.PUB)  # type: ignore[attr-defined]
        self._pub.bind(self.endpoint)

    def publish(self, channel: str, event: JsonData) -> None:
        """Publish *event* to *channel*.

        Args:
            channel: Logical channel name used as the ZMQ topic prefix.
            event: JSON-serialisable event payload.
        """
        try:
            self._ensure_publisher()
            payload = json.dumps(event).encode()
            self._pub.send_multipart([channel.encode(), payload])  # type: ignore[union-attr]
        except Exception:
            logger.exception("zmq_publish_error", extra={"channel": channel})

    def subscribe(self, channel: str) -> Iterator[JsonData]:
        """Subscribe to *channel* and yield arriving events.

        Creates a new SUB socket for this subscription.  Blocks between events.
        The generator runs until the socket is explicitly closed or the process
        exits.

        Args:
            channel: Logical channel name (ZMQ topic prefix filter).

        Yields:
            Decoded event payloads.
        """
        try:
            import zmq  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("Install 'pyzmq' to use ZeroMQPubSub") from exc

        ctx = zmq.Context.instance()
        sub = ctx.socket(zmq.SUB)  # type: ignore[attr-defined]
        sub.connect(self.endpoint)
        sub.setsockopt_string(zmq.SUBSCRIBE, channel)  # type: ignore[attr-defined]
        try:
            while True:
                parts = sub.recv_multipart()
                if len(parts) < 2:
                    continue
                try:
                    yield json.loads(parts[1])
                except json.JSONDecodeError:
                    logger.warning("zmq_invalid_json", extra={"channel": channel})
        finally:
            sub.close()

    def close(self) -> None:
        """Close the publisher socket and release the ZMQ context."""
        if self._pub is not None:
            self._pub.close()
            self._pub = None

# ---
# endregion

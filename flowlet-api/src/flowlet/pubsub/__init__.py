"""PubSub protocol and implementations for run-state change notifications.

Architecture
------------
- PubSubProtocol: minimal QoS-0 interface (publish + subscribe).
- InMemoryPubSub: thread-safe in-process bus; suitable for tests.
- ZeroMQPubSub: local multi-process bus via ZMQ PUB/SUB sockets.
- AzureWebPubSub: cloud-scale bus via Azure Web PubSub service.

Callers should depend only on PubSubProtocol so that the backend can be
swapped without touching business logic.
"""
from collections.abc import Iterator
from typing import Protocol

from .config import AzureWebPubSubConfig, InMemoryPubSubConfig, ZeroMQPubSubConfig, PubSubConfig


# region @pubsub
# ---
# role: adapter
# intent: fire-and-forget publish + blocking subscribe interface for run-state events
# description: >
#   PubSubProtocol[T] is the minimal contract that every backend must satisfy.
#   publish is QoS-0 (fire-and-forget); no delivery guarantee is required on
#   the publish side.  subscribe yields events in arrival order and blocks
#   until the next event (or raises StopIteration when the channel is closed).
#   Implementations are generic: a serializer/deserializer pair is injected at
#   construction so that callers work entirely with domain objects.  The wire
#   format (JSON string) is an implementation detail hidden inside each backend.
# rules:
#   - publish MUST NOT raise on delivery failure; log and swallow.
#   - subscribe MUST be a blocking generator; StopIteration signals end-of-stream.
#   - Implementations MUST be thread-safe.
#   - serializer/deserializer MUST be injected at construction; never hard-coded.
# dependencies:
#   - types.std
# ---


class PubSubProtocol[T](Protocol):
    """Minimal publish/subscribe contract for run-state change events.

    All implementations must be thread-safe.  The publish side is QoS-0
    (best-effort); the subscribe side blocks until the next event arrives.

    Callers work with domain objects of type ``T``.  Each backend accepts a
    ``serializer`` / ``deserializer`` pair at construction and handles the
    wire format (JSON string) internally.
    """

    def publish(self, channel: str, event: T) -> None:
        """Publish a domain event to a channel (fire-and-forget).

        Args:
            channel: Logical channel name (e.g. ``state/my_flow``).
            event: Domain object to publish.
        """
        ...

    def subscribe(self, channel: str) -> Iterator[T]:
        """Subscribe to a channel and yield arriving domain events.

        Args:
            channel: Logical channel name to listen on.

        Yields:
            Successive domain objects in arrival order.
        """
        ...

# ---
# endregion


__all__ = [
    "PubSubProtocol",
    "AzureWebPubSubConfig",
    "InMemoryPubSubConfig",
    "ZeroMQPubSubConfig",
    "PubSubConfig",
]

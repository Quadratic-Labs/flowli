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

from ..types import JsonData


# region @pubsub
# ---
# role: adapter
# intent: fire-and-forget publish + blocking subscribe interface for run-state events
# description: >
#   PubSubProtocol is the minimal contract that every backend must satisfy.
#   publish is QoS-0 (fire-and-forget); no delivery guarantee is required on
#   the publish side.  subscribe yields events in arrival order and blocks
#   until the next event (or raises StopIteration when the channel is closed).
# rules:
#   - publish MUST NOT raise on delivery failure; log and swallow.
#   - subscribe MUST be a blocking generator; StopIteration signals end-of-stream.
#   - Implementations MUST be thread-safe.
# dependencies:
#   - types.std
# ---


class PubSubProtocol(Protocol):
    """Minimal publish/subscribe contract for run-state change events.

    All implementations must be thread-safe.  The publish side is QoS-0
    (best-effort); the subscribe side blocks until the next event arrives.
    """

    def publish(self, channel: str, event: JsonData) -> None:
        """Publish an event to a channel (fire-and-forget).

        Args:
            channel: Logical channel name (e.g. ``state/my_flow``).
            event: JSON-serialisable payload dict.
        """
        ...

    def subscribe(self, channel: str) -> Iterator[JsonData]:
        """Subscribe to a channel and yield arriving events.

        Args:
            channel: Logical channel name to listen on.

        Yields:
            Successive event payloads in arrival order.
        """
        ...

# ---
# endregion


__all__ = ["PubSubProtocol", "JsonData"]

"""Azure Web PubSub implementation.

Uses the ``azure-messaging-webpubsubservice`` SDK on the publish side and a
WebSocket client for the subscribe side.

Install the Azure SDK to use this backend::

    pip install azure-messaging-webpubsubservice
"""
import logging
import queue as _queue
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING

from attrs import define

from .config import AzureWebPubSubConfig

if TYPE_CHECKING:
    import websocket  # type: ignore[import-untyped]
    from azure.messaging.webpubsubservice import WebPubSubServiceClient  # type: ignore[import-untyped]

try:
    from azure.messaging.webpubsubservice import WebPubSubServiceClient  # type: ignore[import-untyped]
    import websocket  # type: ignore[import-untyped]
except ImportError:
    pass

logger = logging.getLogger(__name__)


# region @pubsub.azure
# ---
# role: adapter
# intent: cloud-scale PubSub via Azure Web PubSub service
# description: >
#   AzureWebPubSub[T] delegates publish to the Azure Web PubSub REST SDK
#   (send_to_group) and subscribe to a WebSocket connection established via
#   the service client.  Hub name maps to the application name; channel
#   names become group names within the hub.  Callers work with domain
#   objects of type T; the injected serializer/deserializer pair handles
#   the wire format (JSON string) transparently.
# rules:
#   - publish MUST NOT raise; log and swallow on SDK errors.
#   - subscribe MUST yield deserialised domain objects from the WebSocket stream.
#   - connection_string MUST be provided at construction time.
#   - serializer / deserializer MUST be provided at construction.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class AzureWebPubSub[T]:
    """Azure Web PubSub backed publish/subscribe bus.

    Callers publish and receive domain objects of type ``T``; JSON
    serialisation is handled internally.

    Attributes:
        serializer: Converts a domain object to a JSON string.
        deserializer: Reconstructs a domain object from a JSON string.
        connection_string: Azure Web PubSub service connection string.
        hub: Hub name (maps to the application / service group).
    """

    config: AzureWebPubSubConfig
    serializer: Callable[[T], str]
    deserializer: Callable[[str], T]

    def _client(self) -> WebPubSubServiceClient:
        return WebPubSubServiceClient.from_connection_string(
            self.config.connection_string, hub=self.config.hub
        )

    def publish(self, channel: str, event: T) -> None:
        """Publish a domain event to all subscribers of *channel* via Azure Web PubSub.

        Args:
            channel: Logical channel name; mapped to a Web PubSub group.
            event: Domain object to publish.
        """
        try:
            client = self._client()
            client.send_to_group(
                group=channel,
                message=self.serializer(event),
                content_type="application/json",
            )
        except Exception:
            logger.exception("azure_pubsub_publish_error", extra={"channel": channel})

    def subscribe(self, channel: str) -> Iterator[T]:
        """Subscribe to *channel* via an Azure Web PubSub WebSocket connection.

        Opens a WebSocket to the service, joins the *channel* group, and yields
        deserialised domain objects as they arrive.

        Args:
            channel: Logical channel name (Web PubSub group).

        Yields:
            Domain objects from the service.
        """
        client = self._client()
        token = client.get_client_access_token(groups=[channel])
        ws_url: str = token["url"]

        q: _queue.Queue[T | None] = _queue.Queue()

        def _on_message(ws, message: str) -> None:
            q.put(self.deserializer(message))

        def _on_error(ws, err) -> None:
            logger.error("azure_pubsub_ws_error", extra={"error": str(err)})
            q.put(None)  # sentinel

        def _on_close(ws, *_) -> None:
            q.put(None)  # sentinel

        ws = websocket.WebSocketApp(
            ws_url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )
        t = threading.Thread(target=ws.run_forever, daemon=True)
        t.start()

        try:
            while True:
                item = q.get()
                if item is None:
                    return
                yield item  # type: ignore[misc]
        finally:
            ws.close()

# ---
# endregion

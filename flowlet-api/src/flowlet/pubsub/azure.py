"""Azure Web PubSub implementation.

Uses the ``azure-messaging-webpubsubservice`` SDK on the publish side and a
WebSocket client for the subscribe side.

Install the Azure SDK to use this backend::

    pip install azure-messaging-webpubsubservice
"""
import json
import logging
from collections.abc import Iterator

from attrs import define, field

from ..types import JsonData

logger = logging.getLogger(__name__)


# region @pubsub.azure
# ---
# role: adapter
# intent: cloud-scale PubSub via Azure Web PubSub service
# description: >
#   AzureWebPubSub delegates publish to the Azure Web PubSub REST SDK
#   (send_to_all) and subscribe to a WebSocket connection established via
#   the service client.  Hub name maps to the application name; channel
#   names become group names within the hub.
# rules:
#   - publish MUST NOT raise; log and swallow on SDK errors.
#   - subscribe MUST yield JSON-decoded dicts from the WebSocket message stream.
#   - connection_string MUST be provided at construction time.
# dependencies:
#   - pubsub
# ---


@define(slots=False, kw_only=True)
class AzureWebPubSub:
    """Azure Web PubSub backed publish/subscribe bus.

    Attributes:
        connection_string: Azure Web PubSub service connection string.
        hub: Hub name (maps to the application / service group).
    """

    connection_string: str
    hub: str = field(default="flowlet")

    def _client(self):
        try:
            from azure.messaging.webpubsubservice import WebPubSubServiceClient  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "Install 'azure-messaging-webpubsubservice' to use AzureWebPubSub"
            ) from exc
        return WebPubSubServiceClient.from_connection_string(
            self.connection_string, hub=self.hub
        )

    def publish(self, channel: str, event: JsonData) -> None:
        """Publish *event* to all subscribers of *channel* via Azure Web PubSub.

        Args:
            channel: Logical channel name; mapped to a Web PubSub group.
            event: JSON-serialisable event payload.
        """
        try:
            client = self._client()
            client.send_to_group(
                group=channel,
                message=json.dumps(event),
                content_type="application/json",
            )
        except Exception:
            logger.exception("azure_pubsub_publish_error", extra={"channel": channel})

    def subscribe(self, channel: str) -> Iterator[JsonData]:
        """Subscribe to *channel* via an Azure Web PubSub WebSocket connection.

        Opens a WebSocket to the service, joins the *channel* group, and yields
        decoded event payloads as they arrive.

        Args:
            channel: Logical channel name (Web PubSub group).

        Yields:
            Decoded JSON payloads from the service.
        """
        try:
            from azure.messaging.webpubsubservice import WebPubSubServiceClient  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "Install 'azure-messaging-webpubsubservice' to use AzureWebPubSub"
            ) from exc

        client = self._client()
        token = client.get_client_access_token(groups=[channel])
        ws_url: str = token["url"]

        try:
            import websocket  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "Install 'websocket-client' to use AzureWebPubSub.subscribe"
            ) from exc

        import queue as _queue
        q: _queue.Queue[JsonData | None] = _queue.Queue()

        def _on_message(ws, message: str) -> None:
            try:
                q.put(json.loads(message))
            except json.JSONDecodeError:
                logger.warning("azure_pubsub_invalid_json", extra={"channel": channel})

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
        import threading
        t = threading.Thread(target=ws.run_forever, daemon=True)
        t.start()

        try:
            while True:
                item = q.get()
                if item is None:
                    return
                yield item
        finally:
            ws.close()

# ---
# endregion

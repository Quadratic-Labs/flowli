"""WebSocket connection manager for real-time RunState broadcasting."""
import asyncio
import logging
import queue
from typing import TYPE_CHECKING

from fastapi import WebSocket

from ..serdes import to_json

if TYPE_CHECKING:
    from ..models import RunState

logger = logging.getLogger(__name__)


# region @ws.manager
# ---
# role: api
# intent: manage WebSocket connections and broadcast RunState events to connected clients
# description: >
#   ConnectionManager bridges subscriber daemon threads and WebSocket clients.
#   enqueue() is called by SnapshotSubscriber after each state update (thread-safe
#   via queue.SimpleQueue).  run() is an async loop that blocks on the queue via
#   run_in_executor so it never busy-waits, then broadcasts each RunState as JSON
#   to all active connections.  Dead connections are silently removed on send failure.
#   A None sentinel in the queue signals shutdown.
# rules:
#   - enqueue() MUST be thread-safe; it is called from subscriber daemon threads.
#   - run() MUST be started as an asyncio Task in the FastAPI lifespan.
#   - _broadcast() MUST silently remove dead connections; MUST NOT raise.
#   - stop() MUST send the None sentinel to unblock run().
# dependencies:
#   - models.run
# aliases:
#   - connection-manager
#   - websocket-broadcast
# triggers:
#   - how does the frontend receive realtime updates
#   - websocket endpoint
#   - live run state streaming
# ---


class ConnectionManager:
    """Manages WebSocket connections and broadcasts RunState events.

    Thread-safe on the enqueue side (called from subscriber daemon threads);
    all WebSocket operations are performed in the FastAPI event loop.

    Attributes:
        _connections: Active WebSocket connections (event-loop only).
        _queue: Thread-safe bridge between subscriber threads and the event loop.
    """

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []
        self._queue: queue.SimpleQueue = queue.SimpleQueue()

    def enqueue(self, state: RunState) -> None:
        """Enqueue a RunState for broadcast.  Thread-safe.

        Args:
            state: RunState to broadcast to all connected WebSocket clients.
        """
        self._queue.put(state)

    async def connect(self, ws: WebSocket) -> None:
        """Accept and register a WebSocket connection.

        Args:
            ws: Incoming WebSocket connection.
        """
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Deregister a WebSocket connection.

        Args:
            ws: WebSocket connection to remove.
        """
        try:
            self._connections.remove(ws)
        except ValueError:
            pass

    async def _broadcast(self, state: RunState) -> None:
        message = to_json(state)
        dead: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def run(self) -> None:
        """Async broadcast loop.  Must be started as an asyncio Task in the lifespan.

        Blocks on the internal queue via ``run_in_executor`` (non-blocking for
        the event loop) and broadcasts each ``RunState`` to all connected clients.
        Returns when a ``None`` sentinel is received via ``stop()``.
        """
        loop = asyncio.get_running_loop()
        while True:
            state = await loop.run_in_executor(None, self._queue.get)
            if state is None:
                return
            try:
                await self._broadcast(state)
            except Exception:
                logger.exception("ws_broadcast_error")

    def stop(self) -> None:
        """Send stop sentinel to unblock run().  Idempotent."""
        self._queue.put(None)

# ---
# endregion

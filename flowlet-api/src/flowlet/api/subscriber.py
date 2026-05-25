"""Background subscriber that feeds pubsub RunState events into SnapshotRepository."""
import asyncio
import logging
import threading
from typing import TYPE_CHECKING

from attrs import define, field

if TYPE_CHECKING:
    from ..models import RunState
    from ..pubsub import PubSubProtocol
    from ..registry import Registry
    from .repository import SnapshotRepository
    from .ws import ConnectionManager

logger = logging.getLogger(__name__)


# region @snapshot.subscriber
# ---
# role: adapter
# intent: background threads that feed pubsub RunState events into SnapshotRepository
# description: >
#   SnapshotSubscriber bridges the publish side (workers) and the read side
#   (SnapshotRepository).  start() reads registry.list_flows() once and spawns
#   one daemon thread per flow; each thread calls pubsub.subscribe("state/<flow_name>")
#   and feeds every received RunState into snapshot_repo.update() via asyncio.run().
#   The registry is treated as static (all flows registered before start() is called).
#   Daemon threads die naturally with the process; call stop() for clean shutdown.
# rules:
#   - MUST use daemon threads so it never blocks process exit.
#   - MUST NOT raise on individual event failures; log and continue.
#   - start() and stop() MUST be idempotent.
#   - Each flow MUST have at most one active subscription thread.
#   - start() MUST be called after all flows are registered.
# dependencies:
#   - pubsub
#   - snapshot.repository
#   - registry.registry
#   - models.run
# aliases:
#   - snapshot-subscriber
# triggers:
#   - how does the api get state updates
#   - background pubsub subscription
#   - snapshot subscriber
# ---


@define(slots=False, kw_only=True)
class SnapshotSubscriber:
    """Background subscriber that feeds pubsub RunState events into SnapshotRepository.

    Subscribes to every registered flow's ``state/<flow_name>`` channel on a
    dedicated daemon thread.  Call ``start()`` once after all flows have been
    registered; call ``stop()`` for a clean shutdown.

    Attributes:
        pubsub: PubSub bus to subscribe to.
        snapshot_repo: Repository that persists incoming RunState events.
        registry: Flow registry consulted when start() is called.
        connection_manager: Optional WebSocket connection manager.
    """

    pubsub: "PubSubProtocol[RunState]"
    snapshot_repo: "SnapshotRepository"
    registry: "Registry"
    connection_manager: "ConnectionManager | None" = field(default=None)
    _stop_event: threading.Event = field(factory=threading.Event, alias="_stop_event")
    _started: bool = field(default=False, alias="_started")

    def __init__(self, *, pubsub, snapshot_repo, registry, connection_manager=None, **_):
        """Initialize from dependency injection dict.

        Args:
            pubsub: PubSub bus to subscribe to.
            snapshot_repo: Repository that persists incoming RunState events.
            registry: Flow registry consulted when start() is called.
            connection_manager: Optional WebSocket connection manager.
            **_: Absorbs any unused keys from full dependency dict.
        """
        self.pubsub = pubsub
        self.snapshot_repo = snapshot_repo
        self.registry = registry
        self.connection_manager = connection_manager
        self._stop_event = threading.Event()
        self._started = False

    def start(self) -> None:
        """Spawn one subscription thread per registered flow.  Idempotent."""
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        flows = self.registry.list_flows()
        for flow_name in flows:
            t = threading.Thread(
                target=self._subscribe,
                args=(flow_name,),
                name=f"flowlet-subscriber-{flow_name}",
                daemon=True,
            )
            t.start()
        logger.info("snapshot_subscriber_threads_started", extra={"flows": len(flows)})

    def stop(self) -> None:
        """Signal all subscription threads to stop.  Idempotent."""
        self._stop_event.set()
        logger.info("snapshot_subscriber_stopped")

    def _subscribe(self, flow_name: str) -> None:
        """Subscribe to a single flow's state channel and persist each event.

        Args:
            flow_name: Flow name, used to build the channel ``state/<flow_name>``.
        """
        logger.info("snapshot_subscriber_started", extra={"flow_name": flow_name})
        try:
            for state in self.pubsub.subscribe(f"state/{flow_name}"):
                if self._stop_event.is_set():
                    return
                try:
                    asyncio.run(self.snapshot_repo.update([state]))
                except Exception:
                    logger.exception(
                        "snapshot_subscriber_update_failed",
                        extra={"run_id": str(state.run_id), "flow_name": flow_name},
                    )
                if self.connection_manager is not None:
                    self.connection_manager.enqueue(state)
        except Exception:
            logger.exception(
                "snapshot_subscriber_error", extra={"flow_name": flow_name}
            )

# ---
# endregion

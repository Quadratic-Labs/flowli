"""Run-state repository — cairndb lease documents on the object store.

Per-run state lives at ``state/<flow_name>/<run_id>.json`` as a cairndb
lease document ``{epoch, holder, deadline_at, state}`` whose state payload
is the :class:`~flowlet.models.RunState` wire dict.  Ownership is the lease:
acquiring (fresh, after a release, or by stealing an expired lease) bumps
the epoch fence, and every write through the lease is etag-guarded, so a
fenced holder can never publish an outcome.  The engine arbitrates
identically on the local filesystem and on blob storage.
"""
import json
import logging
from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from attrs import define, field
from cairndb.engine.coordination import Lease, acquire_sync
from cairndb.storage.base import BlobStorage

from ..models import RunState
from ..serdes import from_payload, to_payload
from ..storage import run_prefix
from ..types import Timestamp

logger = logging.getLogger(__name__)


# region @state_repository
# ---
# role: storage
# intent: own run lease documents at state/<flow_name>/<run_id>.json
# description: >
#   StateRepository wraps the cairndb lease primitive with RunState-typed
#   payloads.  acquire() takes ownership and applies a state transition
#   atomically with the acquisition itself (engine state_fn), returning a
#   StateLease whose renew/write/release are epoch-fenced; a fenced holder
#   gets LeaseLost and must discard its outcome.  read()/list_views() parse
#   lease documents into StateView (payload + envelope) without touching
#   ownership.  Terminal states are released (holder None) and archived by
#   the sweeper into the run folder.
# rules:
#   - Ownership MUST only be taken via acquire(); never by direct writes.
#   - acquire()'s state_fn MUST be pure — a lost CAS race re-runs it.
#   - Expiry and ownership MUST be judged from the envelope (holder,
#     deadline_at), never from RunState fields.
#   - MUST NOT raise on missing state objects; return None instead.
#   - archive MUST write the RunState payload in the to_json wire format to
#     runs/<flow>/<date>/<run_id>/state.json before deleting the document.
# dependencies:
#   - storage.keys
#   - models.run
#   - serdes.json
# aliases:
#   - state-repo
#   - run-lease-documents
# triggers:
#   - where does run ownership live
#   - how is a run claimed
# ---

class AlreadyClosed(Exception):
    """Raised by an acquire transition to refuse a lease on a closed run.

    Carries the closed RunState so callers can report the terminal status
    without another read.
    """

    def __init__(self, state: RunState):
        super().__init__(f"run {state.run_id} already closed ({state.status})")
        self.state = state


@define(slots=True, kw_only=True)
class StateView:
    """A parsed, read-only view of one run's lease document.

    Attributes:
        state: The RunState payload (the run's durable record).
        holder: Current lease holder, or None when released.
        deadline_at: Lease expiry (holder-written); for a released lease
            this is the release time — useful as a staleness reference.
        epoch: The lease's monotonic fence token.
    """

    state: RunState
    holder: str | None
    deadline_at: Timestamp | None
    epoch: int

    def held(self, now: Timestamp | None = None) -> bool:
        """Whether the lease is actively held (has a live, unexpired holder)."""
        if self.holder is None:
            return False
        if self.deadline_at is None:
            return False
        reference = now if now is not None else Timestamp.now()
        return reference.value <= self.deadline_at.value


@define(kw_only=True)
class StateLease:
    """An owned run lease with RunState-typed payload access.

    Wraps the engine lease: ``renew``/``write``/``release`` are fenced by
    the epoch — :class:`cairndb.LeaseLost` propagates when ownership was
    stolen, and the holder must discard its outcome.

    Attributes:
        state: The RunState payload as of the last read or write through
            this lease.
    """

    state: RunState
    _lease: Lease = field(alias="lease")

    @property
    def epoch(self) -> int:
        """The lease's fence token (monotonic across acquisitions)."""
        return self._lease.epoch

    @property
    def holder(self) -> str | None:
        """The holder this lease was acquired with."""
        return self._lease.holder

    def renew(self) -> None:
        """Extend the lease deadline by its ttl. Raises LeaseLost if fenced."""
        self._lease.renew_sync()

    def write(self, state: RunState) -> None:
        """Replace the payload under the ownership guard."""
        self._lease.write_sync(to_payload(state))
        self.state = state

    def release(self, state: RunState | None = None) -> None:
        """Release ownership, optionally recording a final payload.

        The document remains (holder None) so the epoch stays monotonic;
        terminal payloads stay readable until the sweeper archives them.
        """
        if state is not None:
            self._lease.release_sync(to_payload(state))
            self.state = state
        else:
            self._lease.release_sync()


@define(slots=True, kw_only=True)
class StateRepository:
    """Repository for run lease documents on the blob store.

    Attributes:
        store: CairnDB blob store all state documents live in.
    """

    store: BlobStorage

    @staticmethod
    def _key(flow_name: str, run_id: UUID) -> str:
        return f"state/{flow_name}/{run_id}.json"

    @staticmethod
    def _parse_view(data: bytes) -> StateView | None:
        doc = json.loads(data)
        payload = doc.get("state")
        if payload is None:
            return None
        deadline_raw = doc.get("deadline_at")
        return StateView(
            state=from_payload(RunState)(payload),
            holder=doc.get("holder"),
            deadline_at=(
                Timestamp.from_datetime(datetime.fromisoformat(deadline_raw))
                if deadline_raw
                else None
            ),
            epoch=doc.get("epoch", 0),
        )

    def read(self, flow_name: str, run_id: UUID) -> StateView | None:
        """Read one run's lease document without touching ownership.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.

        Returns:
            The parsed StateView, or None when the document does not exist
            or carries no payload yet.
        """
        obj = self.store.get_object_sync(self._key(flow_name, run_id))
        if obj is None:
            return None
        try:
            return self._parse_view(obj.data)
        except Exception:
            logger.exception(
                "state_read_parse_error",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )
            return None

    def acquire(
        self,
        flow_name: str,
        run_id: UUID,
        *,
        ttl: float,
        holder: str,
        state_fn: Callable[[RunState | None], RunState],
    ) -> StateLease | None:
        """Take ownership of a run, applying a transition atomically.

        ``state_fn`` receives the current RunState (None when the document
        does not exist yet) and returns the payload to write with the
        acquisition itself — there is never a window where the lease is
        held but its payload is stale.  It must be pure (a lost CAS race
        re-runs it) and may raise — typically :class:`AlreadyClosed` — to
        refuse the acquisition with nothing written.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
            ttl: Lease duration in seconds; heartbeats renew it.
            holder: Identifier of the acquiring actor (worker id, "api",
                "sweeper").
            state_fn: Pure transition from current payload to new payload.

        Returns:
            The owned StateLease, or None when the lease is actively held
            by someone else.
        """

        def _payload_fn(payload):
            current = from_payload(RunState)(payload) if payload is not None else None
            return to_payload(state_fn(current))

        lease = acquire_sync(
            self.store,
            self._key(flow_name, run_id),
            ttl=ttl,
            holder=holder,
            steal_if_expired=True,
            state_fn=_payload_fn,
        )
        if lease is None:
            return None
        return StateLease(
            state=from_payload(RunState)(lease.state), lease=lease
        )

    def delete(self, flow_name: str, run_id: UUID) -> None:
        """Remove the lease document for a run.

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        try:
            self.store.delete_object_sync(self._key(flow_name, run_id))
        except Exception:
            logger.exception(
                "state_delete_error",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )

    def archive(self, flow_name: str, run_id: UUID) -> None:
        """Move a closed run's record into its run folder.

        The RunState payload is written in the ``to_json`` wire format to
        ``runs/<flow_name>/<date>/<run_id>/state.json`` — colocated with the
        run's span files so the run folder is the complete, self-contained
        durable record — and the lease document is removed.  Keeps the
        active ``state/`` listing O(active runs).

        Args:
            flow_name: Name of the flow.
            run_id: UUID identifying the run.
        """
        obj = self.store.get_object_sync(self._key(flow_name, run_id))
        if obj is None:
            return
        try:
            payload = json.loads(obj.data).get("state")
        except Exception:
            logger.exception("state_archive_parse_error", extra={"run_id": str(run_id)})
            return
        if payload is None:
            self.delete(flow_name, run_id)
            return
        target = f"{run_prefix(flow_name, run_id)}/state.json"
        try:
            self.store.put_object_sync(target, json.dumps(payload).encode())
        except Exception:
            logger.exception("state_archive_write_error", extra={"key": target})
            return
        self.delete(flow_name, run_id)

    def list_views(self, flow_name: str | None = None) -> list[StateView]:
        """List all active lease documents, optionally filtered by flow.

        Args:
            flow_name: When given, only states for this flow are returned.

        Returns:
            List of StateView objects parsed from the ``state/`` prefix.
        """
        prefix = f"state/{flow_name}/" if flow_name is not None else "state/"
        results: list[StateView] = []
        for key in self.store.list_objects_sync(prefix):
            parts = key.split("/")
            if len(parts) != 3 or not parts[2].endswith(".json"):
                continue
            try:
                run_id = UUID(parts[2].removesuffix(".json"))
            except ValueError:
                continue
            view = self.read(parts[1], run_id)
            if view is not None:
                results.append(view)
        return results

    def list_states(self, flow_name: str | None = None) -> list[RunState]:
        """List active run records (payloads only) — see :meth:`list_views`."""
        return [view.state for view in self.list_views(flow_name)]

# ---
# endregion

"""Obligation-scoped message channels — ordered, durable, consumed exactly once.

Where a signal is a single-shot latch (one name, first payload wins), a
message channel is a stream: senders append immutable objects under
``messages/<flow_name>/<obligation_id>/<topic>/<uuid7>.json`` and the obligation's
executor consumes them in order through ``flowlet.recv``.  The uuid7 hex id
is the ordering — the blob store's lexicographic prefix listing returns the
topic in send order with no index.

Consumption is **not** recorded here: the consumption cursor lives in the
obligation's account (:class:`~flowlet.models.Consumption` entries, written
under the lease fence), so only the current attempt can consume, and a
retried attempt replays recorded consumptions deterministically before
taking fresh messages — the effect discipline applied to the channel.

Sends are at-least-once by default; a ``dedup_key`` makes them idempotent:
duplicate sends with the same key converge on one message object (the DBOS
``send`` guarantee, achieved by a claim instead of a checkpoint).
"""
import hashlib
import json
import logging
import re
from typing import Any
from uuid import UUID, uuid7

from attrs import define
from cairndb.engine.coordination import claim_sync
from cairndb.storage.base import BlobStorage

from flowlet.types import Timestamp

logger = logging.getLogger(__name__)

# Topic names become path segments: one safe token, no leading underscore
# (the "_dedup/" segment is reserved for the idempotency claims).
_TOPIC_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def validate_topic(topic: str) -> str:
    """Return *topic* unchanged, or raise ValueError when it is unsafe."""
    if not _TOPIC_RE.match(topic):
        raise ValueError(
            f"invalid topic {topic!r}: one token of [A-Za-z0-9_.-], "
            "not starting with an underscore"
        )
    return topic


@define(slots=True, kw_only=True)
class MessageDoc:
    """A parsed message object.

    Attributes:
        id: The message's uuid7 hex id — the topic's ordering key.
        topic: Channel the message was sent on.
        sent_at: ISO timestamp recorded at send.
        actor: Who sent it.
        body: The JSON-safe message payload.
    """

    id: str
    topic: str
    sent_at: str
    actor: str
    body: Any = None


@define(slots=True, kw_only=True)
class MessageRepository:
    """Repository for immutable, ordered obligation-scoped messages.

    Attributes:
        store: CairnDB blob store the messages live in.
    """

    store: BlobStorage

    @staticmethod
    def _prefix(flow_name: str, obligation_id: UUID, topic: str | None = None) -> str:
        base = f"messages/{flow_name}/{obligation_id}/"
        return f"{base}{topic}/" if topic is not None else base

    @classmethod
    def _key(cls, flow_name: str, obligation_id: UUID, topic: str, message_id: str) -> str:
        return f"{cls._prefix(flow_name, obligation_id, topic)}{message_id}.json"

    @classmethod
    def _dedup_key(cls, flow_name: str, obligation_id: UUID, topic: str, dedup: str) -> str:
        digest = hashlib.sha256(dedup.encode()).hexdigest()
        return f"{cls._prefix(flow_name, obligation_id, topic)}_dedup/{digest}.json"

    def send(
        self,
        flow_name: str,
        obligation_id: UUID,
        topic: str,
        body: Any = None,
        *,
        actor: str,
        dedup_key: str | None = None,
    ) -> tuple[str, bool]:
        """Append a message to an obligation's topic, optionally idempotently.

        With a ``dedup_key``, the key's claim decides the message id once;
        the message object itself is then written put-if-absent under that
        id, so a crash between the two writes heals on the retry and every
        duplicate sender converges on one message.

        Args:
            flow_name: Flow the obligation belongs to.
            obligation_id: The obligation's UUID.
            topic: Channel name (see :func:`validate_topic`).
            body: JSON-safe message payload.
            actor: Who sent it — recorded in the message.
            dedup_key: Optional idempotency key; duplicate sends with the
                same key converge on the first message.

        Returns:
            ``(message_id, created)`` — ``created`` is False when a
            ``dedup_key`` resolved to a previously sent message.

        Raises:
            ValueError: The topic name is unsafe.
        """
        validate_topic(topic)
        message_id = uuid7().hex
        created = True
        if dedup_key is not None:
            claim = claim_sync(
                self.store,
                self._dedup_key(flow_name, obligation_id, topic, dedup_key),
                {"message_id": message_id},
            )
            if not claim.won:
                message_id = claim.value["message_id"]
                created = False
        payload = {
            "sent_at": Timestamp.now().to_iso(),
            "actor": actor,
            "body": body,
        }
        # Put-if-absent: on the dedup retry path the object may already
        # exist (the winner wrote it) or be missing (the winner crashed in
        # between) — either way the channel converges on one object.
        claim_sync(
            self.store, self._key(flow_name, obligation_id, topic, message_id), payload
        )
        return message_id, created

    def read(
        self, flow_name: str, obligation_id: UUID, topic: str, message_id: str
    ) -> MessageDoc | None:
        """Read one message by id, or None when it does not exist."""
        obj = self.store.get_object_sync(
            self._key(flow_name, obligation_id, topic, message_id)
        )
        if obj is None:
            return None
        try:
            raw = json.loads(obj.data)
        except Exception:
            logger.exception(
                "message_parse_error",
                extra={"obligation_id": str(obligation_id), "topic": topic, "id": message_id},
            )
            return None
        return MessageDoc(
            id=message_id,
            topic=topic,
            sent_at=raw.get("sent_at", ""),
            actor=raw.get("actor", ""),
            body=raw.get("body"),
        )

    def list_topic(
        self,
        flow_name: str,
        obligation_id: UUID,
        topic: str,
        *,
        after: str | None = None,
    ) -> list[MessageDoc]:
        """List a topic's messages in send order, optionally past a cursor.

        Args:
            flow_name: Flow the obligation belongs to.
            obligation_id: The obligation's UUID.
            topic: Channel name.
            after: Message id to resume past (exclusive); None from the start.
        """
        prefix = self._prefix(flow_name, obligation_id, topic)
        ids = sorted(
            key.removeprefix(prefix).removesuffix(".json")
            for key in self.store.list_objects_sync(prefix)
            if "/" not in key.removeprefix(prefix)  # skip _dedup/ claims
        )
        found: list[MessageDoc] = []
        for message_id in ids:
            if after is not None and message_id <= after:
                continue
            doc = self.read(flow_name, obligation_id, topic, message_id)
            if doc is not None:
                found.append(doc)
        return found

    def counts(self, flow_name: str, obligation_id: UUID) -> dict[str, int]:
        """Total messages per topic for an obligation (dedup claims excluded)."""
        prefix = self._prefix(flow_name, obligation_id)
        totals: dict[str, int] = {}
        for key in self.store.list_objects_sync(prefix):
            parts = key.removeprefix(prefix).split("/")
            if len(parts) != 2 or not parts[1].endswith(".json"):
                continue  # _dedup/ entries have three segments
            totals[parts[0]] = totals.get(parts[0], 0) + 1
        return totals

    def clear(self, flow_name: str, obligation_id: UUID) -> None:
        """Remove all of an obligation's messages (archive-time cleanup)."""
        for key in self.store.list_objects_sync(self._prefix(flow_name, obligation_id)):
            try:
                self.store.delete_object_sync(key)
            except Exception:
                logger.exception("message_delete_error", extra={"key": key})

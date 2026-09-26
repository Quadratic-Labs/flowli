"""Identifier rules and generators. See specs/01-domain-model.md."""

from __future__ import annotations

import hashlib
import re
import uuid

from .errors import InvalidName

Eid = uuid.UUID
"""An execution id: UUIDv7 for a started execution, UUIDv5 for a derived child."""

FLOWLI_NAMESPACE = uuid.UUID("6f1c2e3a-5b7d-4f90-8a1b-2c3d4e5f6071")

FRAME_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
LOG_SEGMENT_RE = re.compile(r"^[a-z0-9._-]+$")  # channel and queue names, CairnDB log names


def _check(pattern: re.Pattern[str], value: str, what: str) -> str:
    if not pattern.match(value):
        raise InvalidName(f"invalid {what} {value!r}: must match {pattern.pattern}")
    return value


def parse_eid(value: Eid | str) -> Eid:
    """Accept a UUID or its canonical string. Raise InvalidName otherwise."""
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except ValueError, AttributeError, TypeError:
        raise InvalidName(f"invalid eid {value!r}: expected a UUID") from None


def check_frame_name(name: str) -> str:
    return _check(FRAME_NAME_RE, name, "frame name")


def check_channel(channel: str) -> str:
    return _check(LOG_SEGMENT_RE, channel, "channel")


def check_queue(queue: str) -> str:
    return _check(LOG_SEGMENT_RE, queue, "queue")


def digest_text(text: str) -> str:
    """sha256 hex of a string. Used to derive names from frame ids, which hold '/' and '#'."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()  # pragma: no mutate


def new_eid() -> Eid:
    """A UUIDv7: time-ordered, carries its creation instant (cairndb Timestamp.from_uuid7)."""
    return uuid.uuid7()

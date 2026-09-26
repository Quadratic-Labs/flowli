"""Channels and messages. See specs/01-domain-model.md section 7."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .names import Eid, check_channel
from .provenance import Provenance


def execution_channel(eid: Eid, name: str) -> str:
    """A channel scoped to one execution: '{eid}.{name}'."""
    return check_channel(f"{eid}.{name}")


@dataclass(frozen=True, slots=True)
class Message:
    channel: str
    seq: int
    payload: Any
    sent_by: Provenance
    correlation: str | None = None

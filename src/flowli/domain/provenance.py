"""Who, what, where, when. See specs/01-domain-model.md section 1."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, replace
from typing import Literal

from cairndb import Timestamp

ActorKind = Literal["worker", "human", "system", "schedule"]


@dataclass(frozen=True, slots=True)
class Actor:
    kind: ActorKind
    id: str
    on_behalf_of: str | None = None

    @classmethod
    def worker(cls, worker_id: str) -> Actor:
        return cls("worker", worker_id)

    @classmethod
    def human(cls, email: str, on_behalf_of: str | None = None) -> Actor:
        return cls("human", email, on_behalf_of)

    @classmethod
    def system(cls, name: str, on_behalf_of: str | None = None) -> Actor:
        return cls("system", name, on_behalf_of)

    @classmethod
    def schedule(cls, name: str) -> Actor:
        return cls("schedule", name)


@dataclass(frozen=True, slots=True)
class Site:
    host: str
    pid: int
    worker_id: str
    instance: str | None = None
    region: str | None = None
    epoch: int | None = None

    @classmethod
    def local(cls, worker_id: str, instance: str | None = None, region: str | None = None) -> Site:
        return cls(
            host=socket.gethostname(),
            pid=os.getpid(),
            worker_id=worker_id,
            instance=instance,
            region=region,
        )

    def with_epoch(self, epoch: int | None) -> Site:
        return replace(self, epoch=epoch)


@dataclass(frozen=True, slots=True)
class Code:
    workflow: str
    version: str
    frame_kind: str
    frame_name: str
    code_ref: str | None = None


@dataclass(frozen=True, slots=True)
class Provenance:
    actor: Actor
    site: Site
    code: Code
    attempt: int
    at: Timestamp

    def __post_init__(self) -> None:
        if self.attempt < 1:
            raise ValueError("attempt starts at 1")

    @classmethod
    def now(cls, actor: Actor, site: Site, code: Code, attempt: int = 1) -> Provenance:
        return cls(actor=actor, site=site, code=code, attempt=attempt, at=Timestamp.now())

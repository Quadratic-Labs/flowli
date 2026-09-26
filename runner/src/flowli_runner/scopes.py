"""Write-scope leases. See specs/10-agent-runner.md section 7.

The envelope names the path globs an attempt may change. The runner takes one
CairnDB lease per glob, in sorted order, before it starts the agent: sorted
order gives no deadlock, and a conflict releases what it took.

The runner holds them for the length of the attempt and renews them with the
task lease, so a dead runner's scopes free themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from flowli.log import get_logger

log = get_logger("flowli_runner.scopes")

PREFIX = "wf/scopes"


def key(glob: str) -> str:
    return f"{PREFIX}/{glob.replace('/', '~')}"


@dataclass
class ScopeSet:
    """The scopes one attempt holds."""

    holder: str
    leases: list[Any] = field(default_factory=list)
    globs: list[str] = field(default_factory=list)

    async def renew(self) -> None:
        for lease in self.leases:
            await lease.renew()

    async def release(self) -> None:
        for lease in reversed(self.leases):
            try:
                await lease.release()
            except Exception:
                log.warning("scope_not_released", holder=self.holder)
        self.leases.clear()
        self.globs.clear()


async def acquire(db: Any, globs: list[str], *, holder: str, ttl: float) -> ScopeSet | None:
    """Take every glob, or nothing. None when one of them is held elsewhere."""
    held = ScopeSet(holder)
    for glob in sorted(set(globs)):
        lease = await db.lease(key(glob), ttl=ttl, holder=holder, steal_if_expired=True)
        if lease is None:
            log.info("scope_conflict", glob=glob, holder=holder)
            await held.release()
            return None
        held.leases.append(lease)
        held.globs.append(glob)
    return held


__all__ = ["ScopeSet", "acquire", "key"]

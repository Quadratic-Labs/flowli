"""The gotchas registry. See flowlet/docs/specs/11-codeflow.md section 9.

Append-only, content-addressed, each entry scoped to the paths it is about.
The envelope of a task carries the ones its write scope meets. This is what
makes attempt 50 cheaper than attempt 5, and the cap is what stops every
envelope from carrying the whole history.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Any

from flowlet.domain.names import digest_text


@dataclass(frozen=True, slots=True)
class Gotcha:
    scope: list[str]  # path globs
    text: str
    at: str = ""

    @property
    def id(self) -> str:
        return digest_text(self.text)[:16]

    def covers(self, write_scope: list[str]) -> bool:
        return any(_meets(scope, glob) for scope in self.scope for glob in write_scope)

    def specificity(self) -> int:
        """A narrower scope wins a place in the envelope over a broader one."""
        return max(len(g.rstrip("*").rstrip("/")) for g in self.scope) if self.scope else 0


def _meets(one: str, other: str) -> bool:
    return fnmatch(one, other) or fnmatch(other, one) or _root(one).startswith(_root(other)) or (
        _root(other).startswith(_root(one))
    )


def _root(glob: str) -> str:
    return glob.split("*", 1)[0].rstrip("/")


@dataclass
class Registry:
    """Append-only: an entry is never changed, only added or retired."""

    entries: list[Gotcha] = field(default_factory=list)
    retired: set[str] = field(default_factory=set)

    def add(self, gotcha: Gotcha) -> str:
        if all(existing.id != gotcha.id for existing in self.entries):
            self.entries.append(gotcha)
        return gotcha.id

    def retire(self, gotcha_id: str) -> None:
        self.retired.add(gotcha_id)

    def for_scope(self, write_scope: list[str], *, limit: int = 10) -> list[dict[str, Any]]:
        """The gotchas to inject, most specific and most recent first."""
        live = [g for g in self.entries if g.id not in self.retired and g.covers(write_scope)]
        ordered = sorted(
            enumerate(live), key=lambda pair: (-pair[1].specificity(), -pair[0])
        )
        return [{"id": g.id, "text": g.text, "scope": g.scope} for _, g in ordered[:limit]]


__all__ = ["Gotcha", "Registry"]

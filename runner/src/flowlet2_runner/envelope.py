"""The two contracts of an agent task. See docs/specs/10-agent-runner.md section 8.

The delegate payload is the envelope, the reply message is the report. Both
are data of the workflow, not of the engine: the runner carries them and never
judges them. The shapes are Flowlet v1's, which this reuses verbatim
(`CodeFlow/codeflow/docs/specs/functional.md`, sections 9.1 to 9.4).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from flowlet.codec import structure, unstructure


@dataclass(frozen=True, slots=True)
class Worktree:
    """Where the effects of one attempt land."""

    path: str
    branch: str
    base_commit: str


@dataclass(frozen=True, slots=True)
class Envelope:
    """What the agent is asked to do, and where it may write."""

    intent: str
    spec_ref: str | None = None
    acceptance_criteria: list[str] = field(default_factory=list)
    write_scope: list[str] = field(default_factory=list)
    verification: list[str] = field(default_factory=list)
    gotchas: list[str] = field(default_factory=list)
    base_ref: str | None = None
    agent: dict[str, Any] = field(default_factory=dict)
    worktree: Worktree | None = None  # the runner fills this in

    @classmethod
    def from_payload(cls, payload: Any) -> Envelope:
        if isinstance(payload, Envelope):
            return payload
        return structure(payload or {}, cls)

    def to_payload(self) -> dict[str, Any]:
        return dict(unstructure(self))


@dataclass(frozen=True, slots=True)
class Report:
    """What came of one attempt.

    `outcome` is the agent's own word for how it ended. The workflow decides
    what that means: the runner delivers the report and judges nothing
    (section 8).
    """

    outcome: str  # completed | partial | blocked | interrupted | failed
    summary: str = ""
    commits: list[str] = field(default_factory=list)
    branch: str | None = None
    base_commit: str | None = None
    verification: list[dict[str, Any]] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)  # references, never bytes
    error: str | None = None
    resume: dict[str, Any] | None = None  # the agent's compaction, on an interrupt
    cost: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return dict(unstructure(self))


__all__ = ["Envelope", "Report", "Worktree"]

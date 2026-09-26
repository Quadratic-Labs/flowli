"""Building the envelope of one attempt. See spec 11 sections 3.1 and 9.

The envelope is what the agent is asked to do. This is where the scoped
knowledge of the registry is injected, and where the prior attempt is carried
forward: a rework gets a fresh agent, seeded with what the last one produced.
"""

from __future__ import annotations

from typing import Any

from .gotchas import Registry


def build_envelope(
    task: dict[str, Any],
    attempt: int,
    prior: dict[str, Any] | None = None,
    gotchas: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The payload of the delegate to the agent queue."""
    envelope = {
        "intent": task["intent"],
        "spec_ref": task.get("spec_ref"),
        "acceptance_criteria": list(task.get("acceptance_criteria") or []),
        "write_scope": list(task.get("write_scope") or []),
        "verification": list(task.get("verification") or []),
        "gotchas": [g["text"] for g in gotchas or []],
        "agent": dict(task.get("agent") or {}),
        "attempt": attempt,
    }
    if prior is not None:
        # A fresh context, seeded with what failed. A context that already
        # failed tends to fail the same way (spec 11, section 3.1).
        envelope["prior"] = prior
    return envelope


def summarize(report: dict[str, Any], gates: dict[str, Any], reason: str) -> dict[str, Any]:
    """What the next attempt is told about this one."""
    return {
        "reason": reason,
        "outcome": report.get("outcome"),
        "summary": report.get("summary"),
        "gate_failures": list(gates.get("failures") or []),
        "commits": list(report.get("commits") or []),
        "branch": report.get("branch"),
        "evidence": list(report.get("evidence") or []),
    }


def injected(registry: Registry, task: dict[str, Any], *, limit: int = 10) -> list[dict[str, Any]]:
    return registry.for_scope(list(task.get("write_scope") or []), limit=limit)


__all__ = ["build_envelope", "injected", "summarize"]

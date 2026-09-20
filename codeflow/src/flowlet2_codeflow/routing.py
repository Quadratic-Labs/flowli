"""Routing after an attempt. See flowlet/docs/specs/11-codeflow.md section 4.

A fixed function, not a model's output. The gates come first, the judgment
second, and a reviewer can never overrule a failed gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

VERDICTS = frozenset({"accept", "rework", "reject"})


class Route(StrEnum):
    APPROVED = "approved"
    REWORK = "rework"
    ESCALATED = "escalated"


@dataclass(frozen=True, slots=True)
class Decision:
    route: Route
    reason: str

    def __str__(self) -> str:  # pragma: no cover - for a log line
        return f"{self.route}: {self.reason}"


def decide(
    gates: dict[str, Any],
    judgment: dict[str, Any] | None,
    attempt: int,
    max_attempts: int,
) -> Decision:
    """The table of section 4."""
    if not gates.get("passed"):
        failures = ", ".join(gates.get("failures", [])) or "a gate failed"
        return Decision(Route.REWORK, failures)

    if judgment is None:
        # The review expired. Nobody decided, so nobody can approve.
        return Decision(Route.ESCALATED, "no review before the deadline")

    verdict = str(judgment.get("verdict", ""))
    if verdict not in VERDICTS:
        return Decision(Route.ESCALATED, f"the reviewer said {verdict!r}, which is not a verdict")

    findings = judgment.get("findings") or []
    blocking = [f for f in findings if str(f.get("severity")) == "blocking"]

    if verdict == "accept":
        if blocking:
            # A contradiction. It is not resolved here, and it says something
            # about the reviewer.
            return Decision(Route.ESCALATED, f"accepted with {len(blocking)} blocking findings")
        return Decision(Route.APPROVED, "the gates passed and the reviewer accepted")

    if verdict == "reject":
        return Decision(Route.ESCALATED, "the reviewer rejected the task as ill-posed")

    if attempt >= max_attempts:
        return Decision(Route.ESCALATED, f"no attempt left after {attempt}")
    return Decision(Route.REWORK, _findings_summary(findings))


def _findings_summary(findings: list[dict[str, Any]]) -> str:
    if not findings:
        return "the reviewer asked for a rework"
    first = findings[0]
    more = f" and {len(findings) - 1} more" if len(findings) > 1 else ""
    return f"{first.get('severity', 'major')}: {first.get('rationale', '')}{more}"


__all__ = ["VERDICTS", "Decision", "Route", "decide"]

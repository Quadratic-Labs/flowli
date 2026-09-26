"""The deterministic gates on a report. See spec 10 section 8 and spec 11 section 4.

These run in a step of the task workflow, after the reply. They are pure
functions of the report and the envelope, which is why the runner reports the
paths it changed: a worker has no checkout to look at.
"""

from __future__ import annotations

from fnmatch import fnmatch
from typing import Any


def run_gates(report: dict[str, Any], envelope: dict[str, Any]) -> dict[str, Any]:
    """`{"passed": bool, "failures": [...]}`. A failure is a rework, always."""
    failures: list[str] = []

    outcome = str(report.get("outcome", ""))
    if outcome in ("failed", "blocked"):
        failures.append(f"the attempt ended {outcome}: {report.get('error') or 'no reason given'}")

    failures.extend(_verification(report, envelope))
    failures.extend(_scope(report, envelope))
    failures.extend(_criteria(report, envelope, outcome))

    return {"passed": not failures, "failures": failures}


def _verification(report: dict[str, Any], envelope: dict[str, Any]) -> list[str]:
    """Every command of the envelope ran, and every one of them passed."""
    ran = {str(v.get("command")): v for v in report.get("verification") or []}
    out = []
    for command in envelope.get("verification") or []:
        result = ran.get(str(command))
        if result is None:
            out.append(f"the verification {command!r} did not run")
        elif int(result.get("exit_code", 1)) != 0:
            out.append(f"the verification {command!r} exited {result.get('exit_code')}")
    return out


def _scope(report: dict[str, Any], envelope: dict[str, Any]) -> list[str]:
    """Nothing outside the write scope changed."""
    scope = envelope.get("write_scope") or []
    if not scope:
        return []
    outside = [
        path
        for path in report.get("changed_paths") or []
        if not any(fnmatch(path, glob) for glob in scope)
    ]
    if not outside:
        return []
    shown = ", ".join(sorted(outside)[:5])
    return [f"{len(outside)} path(s) outside the write scope: {shown}"]


def _criteria(report: dict[str, Any], envelope: dict[str, Any], outcome: str) -> list[str]:
    """A `completed` outcome names every acceptance criterion."""
    if outcome != "completed":
        return []
    wanted = set(envelope.get("acceptance_criteria") or [])
    if not wanted:
        return []
    claimed = {
        str(c) for claim in report.get("claims") or [] for c in claim.get("criteria_satisfied", [])
    }
    missing = sorted(wanted - claimed)
    return [f"completed, but {', '.join(missing)} is not claimed"] if missing else []


__all__ = ["run_gates"]

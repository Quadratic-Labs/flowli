"""The plan and what it holds. See flowlet/docs/specs/11-codeflow.md section 5.

A plan is data of the milestone workflow. It is not a board, and nothing
outside that execution writes it: one execution has one writer, so there is no
version to reconcile against.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from flowlet.codec import structure, unstructure


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """One unit of work: one intent, one write scope, one branch."""

    id: str
    intent: str
    feature: str | None = None
    spec_ref: str | None = None
    acceptance_criteria: list[str] = field(default_factory=list)
    write_scope: list[str] = field(default_factory=list)
    verification: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    agent: dict[str, Any] = field(default_factory=dict)
    exclusive: bool = False  # a lock file, a migration, generated code

    def to_payload(self) -> dict[str, Any]:
        return dict(unstructure(self))


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    id: str
    title: str
    spec_ref: str | None = None
    tasks: list[TaskSpec] = field(default_factory=list)

    def stages(self) -> list[list[str]]:
        """The dependency graph, flattened into what may run together.

        Order is the shape of the feature workflow, so the planner is what
        turns edges into stages; the engine reads no dependency field.
        """
        return stages_of(self.tasks)

    def task(self, task_id: str) -> TaskSpec:
        for task in self.tasks:
            if task.id == task_id:
                return task
        raise KeyError(task_id)

    def to_payload(self) -> dict[str, Any]:
        return dict(unstructure(self))

    @classmethod
    def from_payload(cls, payload: Any) -> FeatureSpec:
        return structure(payload, cls)


@dataclass(frozen=True, slots=True)
class Milestone:
    id: str
    title: str
    features: list[FeatureSpec] = field(default_factory=list)
    gotchas: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return dict(unstructure(self))

    @classmethod
    def from_payload(cls, payload: Any) -> Milestone:
        return structure(payload, cls)


class PlanError(ValueError):
    """A delta that the milestone refuses, with a machine-readable reason."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def stages_of(tasks: list[TaskSpec]) -> list[list[str]]:
    """Group tasks into stages: everything in one stage may run together.

    Raises `PlanError` when the dependencies hold a cycle, because a cycle is
    a plan that can never run and it must be refused before anything starts.
    """
    remaining = {t.id: set(t.depends_on) for t in tasks}
    if any(dep not in remaining for deps in remaining.values() for dep in deps):
        unknown = sorted(
            {dep for deps in remaining.values() for dep in deps if dep not in remaining}
        )
        raise PlanError("unknown_dependency", f"no such task: {', '.join(unknown)}")

    out: list[list[str]] = []
    while remaining:
        ready = sorted(tid for tid, deps in remaining.items() if not deps)
        if not ready:
            raise PlanError("cyclic_plan", f"a cycle holds {', '.join(sorted(remaining))}")
        out.append(ready)
        for tid in ready:
            del remaining[tid]
        for deps in remaining.values():
            deps.difference_update(ready)
    return out


def scopes_overlap(one: TaskSpec, other: TaskSpec) -> bool:
    """Two tasks that may write the same paths must not run together.

    The runner's leases make an overlap safe rather than corrupting
    (`10-agent-runner.md`, section 7); this refuses it in the plan, where a
    person can still read the reason.
    """
    if one.exclusive or other.exclusive:
        return True
    # `src/a/**` covers `src/a/deep/**`, so one root containing the other is
    # an overlap, not only two equal roots.
    return any(
        first == second or first.startswith(f"{second}/") or second.startswith(f"{first}/")
        for first in (_root(g) for g in one.write_scope)
        for second in (_root(g) for g in other.write_scope)
    )


def _root(glob: str) -> str:
    """The part of a glob before its first wildcard, which is what two globs
    must share to be able to collide."""
    head = glob.split("*", 1)[0]
    return head.rstrip("/")


def check_feature(feature: FeatureSpec, *, max_open_tasks: int = 50) -> list[list[str]]:
    """Validate a feature and return its stages. See spec 11, section 5."""
    ids = [t.id for t in feature.tasks]
    if len(ids) != len(set(ids)):
        raise PlanError("duplicate_task_id", "two tasks share one id")
    if len(ids) > max_open_tasks:
        raise PlanError("too_many_tasks", f"{len(ids)} tasks, limit {max_open_tasks}")
    stages = stages_of(feature.tasks)
    for stage in stages:
        for i, first in enumerate(stage):
            for second in stage[i + 1 :]:
                if scopes_overlap(feature.task(first), feature.task(second)):
                    raise PlanError(
                        "overlapping_scopes", f"{first} and {second} may write the same paths"
                    )
    return stages


# --- deltas ----------------------------------------------------------------


GATED_BY_DEFAULT = frozenset(
    {"create_feature", "request_spec_amendment", "declare_milestone_complete", "cancel_task"}
)
OPERATIONS = GATED_BY_DEFAULT | {"create_task", "split_task", "reprioritize", "escalate"}


@dataclass(frozen=True, slots=True)
class Applied:
    """What a delta did. A refused operation carries its reason back."""

    milestone: Milestone
    applied: list[str] = field(default_factory=list)
    refused: list[dict[str, str]] = field(default_factory=list)
    gated: list[dict[str, Any]] = field(default_factory=list)
    complete: bool = False


def apply_delta(
    milestone: Milestone,
    delta: dict[str, Any],
    *,
    gated_ops: frozenset[str] = GATED_BY_DEFAULT,
    max_open_tasks: int = 50,
) -> Applied:
    """Apply the operations of one delta, in order.

    One refused operation does not refuse the delta: the reason goes back, and
    the next delta sees it. An operation a gate covers is not applied here; it
    is returned for the review of spec 11, section 8.
    """
    result = Applied(milestone)
    for operation in delta.get("operations", []):
        op = str(operation.get("op", ""))
        if op not in OPERATIONS:
            result.refused.append({"op": op, "reason": "unknown_operation"})
            continue
        if op in gated_ops:
            result.gated.append(operation)
            continue
        try:
            result = replace(result, milestone=_apply(result.milestone, op, operation,
                                                     max_open_tasks=max_open_tasks))
        except PlanError as refusal:
            result.refused.append({"op": op, "reason": refusal.code, "detail": refusal.detail})
        else:
            result.applied.append(op)
    return result


def _apply(
    milestone: Milestone, op: str, operation: dict[str, Any], *, max_open_tasks: int
) -> Milestone:
    match op:
        case "create_task":
            task = structure(operation["task"], TaskSpec)
            features = [
                replace(f, tasks=[*f.tasks, task]) if f.id == task.feature else f
                for f in milestone.features
            ]
            if all(f.id != task.feature for f in milestone.features):
                raise PlanError("unknown_feature", f"no feature {task.feature}")
            updated = replace(milestone, features=features)
            check_feature(next(f for f in features if f.id == task.feature),
                          max_open_tasks=max_open_tasks)
            return updated
        case "split_task":
            parts = [structure(t, TaskSpec) for t in operation["into"]]
            target = str(operation["task"])
            features = []
            for feature in milestone.features:
                if all(t.id != target for t in feature.tasks):
                    features.append(feature)
                    continue
                kept = [t for t in feature.tasks if t.id != target]
                features.append(replace(feature, tasks=[*kept, *parts]))
            return replace(milestone, features=features)
        case "reprioritize":
            order = list(operation["order"])
            by_id = {f.id: f for f in milestone.features}
            if set(order) != set(by_id):
                raise PlanError("partial_order", "the order must name every feature")
            return replace(milestone, features=[by_id[fid] for fid in order])
        case "escalate":
            return milestone
        case _:  # pragma: no cover - the gate list covers the rest
            raise PlanError("not_applicable", op)

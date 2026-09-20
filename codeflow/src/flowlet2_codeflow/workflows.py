"""The three workflows. See flowlet/docs/specs/11-codeflow.md section 3.

The controller is these functions. There is no reconciler: a milestone is an
execution, a feature is a child of it, and a task is a child of a feature.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any

from flowlet.patterns import delegate, review
from flowlet.runtime import Context
from flowlet.runtime import Registry as WorkflowRegistry

from .envelope import build_envelope, injected, summarize
from .gates import run_gates
from .gotchas import Registry as Gotchas
from .model import FeatureSpec, Milestone, PlanError, apply_delta, check_feature
from .routing import Route, decide

AGENT_QUEUE = "agents"
MERGE_QUEUE = "merge"
REVIEW_QUEUE = "code-review"
ESCALATION_QUEUE = "escalation"
PLAN_QUEUE = "plan"


@dataclass(frozen=True, slots=True)
class Policy:
    """See spec 11, section 13."""

    max_attempts: int = 3
    max_open_tasks: int = 50
    max_turns: int = 100
    agent_timeout: timedelta = timedelta(hours=6)
    review_timeout: timedelta | None = timedelta(days=2)
    merge_timeout: timedelta = timedelta(hours=1)
    queues: dict[str, str] = field(default_factory=dict)

    def queue(self, name: str) -> str:
        return self.queues.get(name, name)


def register(
    registry: WorkflowRegistry,
    *,
    policy: Policy | None = None,
    gotchas: Gotchas | None = None,
    version: str = "1",
) -> dict[str, Any]:
    """Register the three workflows on a registry, and return them by name.

    A deployment calls this once. The policy and the registry of gotchas are
    the two things it configures.
    """
    policy = policy or Policy()
    gotchas = gotchas or Gotchas()

    @registry.workflow("codeflow.task", version)
    async def task(ctx: Context, spec: dict[str, Any]) -> dict[str, Any]:
        """One unit of work: attempts, gates, review, then a merge."""
        prior: dict[str, Any] | None = None
        for attempt in range(1, policy.max_attempts + 1):
            key = str(attempt)
            envelope = await ctx.step(
                build_envelope, spec, attempt, prior, injected(gotchas, spec),
                name="envelope", key=key,
            )
            reply = await delegate(
                ctx, policy.queue(AGENT_QUEUE), envelope,
                timeout=policy.agent_timeout, key=key,
            )
            if reply is None:
                return await _escalate(ctx, spec, "no agent answered", key, policy)

            report = dict(reply.payload)
            gates = await ctx.step(run_gates, report, envelope, name="gates", key=key)

            judgment: dict[str, Any] | None = None
            if gates["passed"]:
                decision = await review(
                    ctx, policy.queue(REVIEW_QUEUE),
                    {"task": spec["id"], "report": report, "gates": gates},
                    timeout=policy.review_timeout, key=key,
                )
                judgment = None if decision is None else {
                    "verdict": decision.verdict,
                    "by": decision.by.id,
                    **(decision.data or {}),
                }

            route = decide(gates, judgment, attempt, policy.max_attempts)
            await ctx.announce(
                "codeflow.routed",
                {"task": spec["id"], "attempt": attempt, "route": str(route.route),
                 "reason": route.reason},
                name="routed", key=key,
            )

            if route.route is Route.APPROVED:
                merged = await delegate(
                    ctx, policy.queue(MERGE_QUEUE),
                    {"task": spec["id"], "branch": report.get("branch"),
                     "base_commit": report.get("base_commit")},
                    timeout=policy.merge_timeout, name="merge", key=key,
                )
                if merged is not None and merged.payload.get("merged"):
                    return {"outcome": "completed", "attempts": attempt,
                            "commit": merged.payload.get("commit")}
                # A conflict or a failed gate of the merge queue is a rework
                # with that context, and the queue is not blocked by it.
                reason = (merged.payload.get("reason") if merged else "the merge queue timed out")
                prior = summarize(report, gates, str(reason))
                continue

            if route.route is Route.ESCALATED:
                return await _escalate(ctx, spec, route.reason, key, policy)

            prior = summarize(report, gates, route.reason)

        return await _escalate(ctx, spec, "no attempt left", str(policy.max_attempts), policy)

    async def _escalate(
        ctx: Context, spec: dict[str, Any], reason: str, key: str, policy: Policy
    ) -> dict[str, Any]:
        await ctx.announce(
            "codeflow.escalated", {"task": spec["id"], "reason": reason},
            name="escalated", key=key,
        )
        decision = await review(
            ctx, policy.queue(ESCALATION_QUEUE),
            {"task": spec["id"], "intent": spec.get("intent"), "reason": reason},
            # Its own name: the code review of this attempt already holds
            # `review-*:{key}` (spec 11, section 3.1).
            name="escalation", key=key,
        )
        return {
            "outcome": "escalated",
            "reason": reason,
            "verdict": None if decision is None else decision.verdict,
        }

    @registry.workflow("codeflow.feature", version)
    async def feature(ctx: Context, spec: dict[str, Any]) -> dict[str, Any]:
        """The tasks of one feature, a stage at a time."""
        parsed = FeatureSpec.from_payload(spec)
        stages = await ctx.step(check_feature, parsed, name="plan")
        done: dict[str, Any] = {}
        for index, stage in enumerate(stages):
            results = await ctx.gather(
                *[
                    ctx.child(task, parsed.task(tid).to_payload(), key=tid)
                    for tid in stage
                ]
            )
            done.update(dict(zip(stage, results, strict=True)))
            if any(r["outcome"] == "escalated" for r in results):
                return {"outcome": "escalated", "stage": index, "tasks": done}
        return {"outcome": "completed", "tasks": done}

    @registry.workflow("codeflow.milestone", version)
    async def milestone(ctx: Context, spec: dict[str, Any]) -> dict[str, Any]:
        """The plan, the mode, and the features it runs.

        The plan is local state: a replay must see the arguments exactly as
        the first run did (`04-api.md`, section 1).
        """
        plan = Milestone.from_payload(spec["milestone"])
        awaits_plan = bool(spec.get("await_plan", True))
        done: list[dict[str, str]] = []
        seen = 0
        turn = 0

        while turn < policy.max_turns:
            turn += 1
            if seen < len(plan.features):
                ready = plan.features[seen]
                seen += 1
                (result,) = await ctx.gather(
                    ctx.child(feature, ready.to_payload(), key=ready.id)
                )
                done.append({ready.id: result["outcome"]})
                continue

            if not awaits_plan:
                break
            message = await ctx.receive("plan", key=str(turn))
            if message is None:
                break

            # A step returns JSON, so the plan comes back as data and is
            # structured again here (`04-api.md`, section 2.1).
            applied = await ctx.step(
                _apply, plan.to_payload(), message.payload, policy.max_open_tasks,
                name="delta", key=str(turn),
            )
            plan = Milestone.from_payload(applied["plan"])
            if applied["refused"]:
                await ctx.announce(
                    "codeflow.delta_refused", {"refused": applied["refused"]},
                    name="refused", key=str(turn),
                )
            for index, operation in enumerate(applied["gated"]):
                # An operation a gate covers is not applied by the delta: it is
                # a review (spec 11, section 8).
                key = f"{turn}-{index}"
                decision = await review(
                    ctx, policy.queue(PLAN_QUEUE), {"operation": operation}, key=key,
                )
                if decision is not None and decision.verdict == "accept":
                    plan = Milestone.from_payload(
                        await ctx.step(
                            _apply_one, plan.to_payload(), operation, policy.max_open_tasks,
                            name="apply", key=key,
                        )
                    )
            if message.payload.get("final"):
                awaits_plan = False

        return {"outcome": "completed", "features": done, "turns": turn}

    return {"task": task, "feature": feature, "milestone": milestone}


def _apply(plan: dict[str, Any], delta: dict[str, Any], max_open_tasks: int) -> dict[str, Any]:
    """The step that applies a delta. It is memoized, so a replay never
    applies it twice and the plan cannot drift from the journal."""
    milestone = Milestone.from_payload(plan)
    try:
        result = apply_delta(milestone, delta, max_open_tasks=max_open_tasks)
    except PlanError as refusal:
        return {
            "plan": plan,
            "refused": [{"op": "delta", "reason": refusal.code, "detail": refusal.detail}],
            "gated": [],
        }
    return {
        "plan": result.milestone.to_payload(),
        "refused": result.refused,
        "gated": result.gated,
    }


def _apply_one(
    plan: dict[str, Any], operation: dict[str, Any], max_open_tasks: int
) -> dict[str, Any]:
    """Apply one operation that a person approved."""
    milestone = Milestone.from_payload(plan)
    if operation.get("op") == "create_feature":
        feature = FeatureSpec.from_payload(operation["feature"])
        check_feature(feature, max_open_tasks=max_open_tasks)
        return replace(milestone, features=[*milestone.features, feature]).to_payload()
    if operation.get("op") == "declare_milestone_complete":
        return milestone.to_payload()
    result = apply_delta(
        milestone, {"operations": [operation]}, gated_ops=frozenset(),
        max_open_tasks=max_open_tasks,
    )
    return result.milestone.to_payload()


__all__ = [
    "AGENT_QUEUE",
    "ESCALATION_QUEUE",
    "MERGE_QUEUE",
    "PLAN_QUEUE",
    "REVIEW_QUEUE",
    "Policy",
    "register",
]

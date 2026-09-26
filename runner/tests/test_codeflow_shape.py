"""The controller of spec 11, in miniature, running for real.

Spec 11 claims that CodeFlow needs no reconciler: a milestone is an execution,
a feature is a child, a task is a child of a feature, and routing is a pure
function. These tests are that claim, executed — the workflows below are the
ones in sections 3 and 4, with fakes in place of agents and of git.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.domain import Actor, ExecutionStatus, Site
from flowli.patterns import delegate, review
from flowli.runtime import Engine

from .conftest import HUMAN, T0

MAX_ATTEMPTS = 3
CFO = Actor.human("lead@example.com")


def build_envelope(intent: str, attempt: int) -> dict:
    return {"intent": intent, "attempt": attempt}


# --- section 4: routing is a pure function ---------------------------------


def decide(gates: dict, judgment: dict | None, attempt: int, max_attempts: int) -> str:
    """The table of spec 11 section 4. A reviewer cannot overrule a gate."""
    if not gates["passed"]:
        return "rework"
    assert judgment is not None
    verdict = judgment["verdict"]
    blocking = any(f.get("severity") == "blocking" for f in judgment.get("findings", []))
    if verdict == "accept":
        return "escalated" if blocking else "approved"
    if verdict == "reject":
        return "escalated"
    return "rework" if attempt < max_attempts else "escalated"


def test_a_failed_gate_is_a_rework_whatever_the_reviewer_said():
    assert decide({"passed": False}, {"verdict": "accept"}, 1, 3) == "rework"


def test_an_accept_with_a_blocking_finding_is_a_contradiction():
    judgment = {"verdict": "accept", "findings": [{"severity": "blocking"}]}
    assert decide({"passed": True}, judgment, 1, 3) == "escalated"


def test_the_last_rework_escalates():
    assert decide({"passed": True}, {"verdict": "rework"}, 3, 3) == "escalated"


# --- the workflows ---------------------------------------------------------


@pytest.fixture
def world():
    """An engine with the three workflows of spec 11 section 3."""
    clock = ManualClock(T0)
    backend = MemoryBackend(clock=clock)
    engine = Engine(backend.ports, Site.local("w-1"), clock=clock)
    merged: list[str] = []
    gate_results: list[bool] = []

    @engine.workflow("codeflow.task", "1")
    async def task(ctx, spec):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            key = str(attempt)
            envelope = await ctx.step(
                build_envelope, spec["intent"], attempt, name="envelope", key=key
            )
            reply = await delegate(ctx, "agents", envelope, timeout=timedelta(hours=1), key=key)
            if reply is None:
                return {"outcome": "escalated", "reason": "no agent answered"}

            report = reply.payload
            gates = await ctx.step(
                lambda: {"passed": gate_results.pop(0) if gate_results else True},
                name="gates",
                key=key,
            )
            judgment = None
            if gates["passed"]:
                decision = await review(ctx, "code-review", {"report": report}, key=key)
                judgment = None if decision is None else {"verdict": decision.verdict}

            route = decide(gates, judgment, attempt, MAX_ATTEMPTS)
            if route == "approved":
                # A second delegate in one frame needs its own name: the frame
                # id is `{name}-enqueue:{key}`, and two of them would collide.
                await delegate(ctx, "merge", {"branch": report["branch"]}, name="merge", key=key)
                merged.append(report["branch"])
                return {"outcome": "completed", "attempts": attempt}
            if route == "escalated":
                return {"outcome": "escalated", "attempts": attempt}
        return {"outcome": "escalated", "attempts": MAX_ATTEMPTS}

    @engine.workflow("codeflow.feature", "1")
    async def feature(ctx, spec):
        done: dict[str, Any] = {}
        for stage in spec["stages"]:
            results = await ctx.gather(
                *[ctx.child(task, {"intent": f"do {tid}"}, key=tid) for tid in stage]
            )
            done.update(dict(zip(stage, results, strict=True)))
            if any(r["outcome"] == "escalated" for r in results):
                return {"outcome": "escalated", "tasks": done}
        return {"outcome": "completed", "tasks": done}

    @engine.workflow("codeflow.milestone", "1")
    async def milestone(ctx, spec):
        # The plan is local state, never a mutation of the arguments: a replay
        # sees the arguments exactly as the first run did.
        plan = list(spec["features"])
        done: list[dict] = []
        seen = 0
        waited = False
        while seen < len(plan):
            ready = plan[seen]
            seen += 1
            (result,) = await ctx.gather(ctx.child(feature, ready, key=ready["id"]))
            done.append({ready["id"]: result["outcome"]})
            if seen == len(plan) and spec.get("await_plan") and not waited:
                # The plan may still grow: a delta is a message, not a turn.
                waited = True
                message = await ctx.receive("plan", key=str(seen))
                if message is not None:
                    plan.extend(message.payload["features"])
        return {"outcome": "completed", "done": done}

    return type(
        "World",
        (),
        {
            "engine": engine,
            "backend": backend,
            "clock": clock,
            "task": task,
            "feature": feature,
            "milestone": milestone,
            "merged": merged,
            "gates": gate_results,
        },
    )


async def run(world, *, agent_says=None, verdict="accept", limit=400):
    """Drive every process: the worker, the agent queue, the merge queue and
    the reviewers. Each is a consumer of its own queue, and none of them plans."""
    engine, backend = world.engine, world.backend
    worker = engine.worker(queues=["default"])
    for _ in range(limit):
        if await worker.run_once():
            continue
        if await _answer_agents(engine, backend, agent_says):
            continue
        if await _answer_merges(engine, backend):
            continue
        if await _decide_reviews(engine, backend, verdict):
            continue
        return
    raise AssertionError("the controller did not settle")


async def _answer_agents(engine, backend, agent_says) -> bool:
    claimed = await backend.queue.dequeue("agents", "agent-runner", 60)
    if claimed is None:
        return False
    from flowli.domain import DelegateTask

    target = DelegateTask.from_task_payload(claimed.task.payload)
    payload = agent_says or {"outcome": "completed", "branch": f"agent/{target.eid}"}
    await engine.deliver(target.eid, target.reply_channel, payload, by=Actor.worker("agent"))
    await backend.queue.ack(claimed)
    return True


async def _answer_merges(engine, backend) -> bool:
    """The merge queue: one consumer, so one writer to the integration branch."""
    claimed = await backend.queue.dequeue("merge", "merge-queue", 60)
    if claimed is None:
        return False
    from flowli.domain import DelegateTask

    target = DelegateTask.from_task_payload(claimed.task.payload)
    await engine.deliver(target.eid, target.reply_channel, {"merged": True}, by=Actor.worker("mq"))
    await backend.queue.ack(claimed)
    return True


async def _decide_reviews(engine, backend, verdict) -> bool:
    for queue in ("code-review", "escalation"):
        tasks = await backend.queue.pending(queue)
        if not tasks:
            continue
        rid = tasks[0].payload["payload"]["rid"]
        from flowli.domain import DelegateTask

        target = DelegateTask.from_task_payload(tasks[0].payload)
        await engine.reviews.decide(rid, eid=target.eid, queue=queue, verdict=verdict, by=CFO)
        return True
    return False


# --- the claim -------------------------------------------------------------


async def test_a_milestone_runs_its_features_and_their_tasks(world):
    """No reconciler anywhere: the plan is the shape of the workflows."""
    eid = await world.engine.start(
        world.milestone,
        {"features": [{"id": "F-1", "stages": [["T-1", "T-2"], ["T-3"]]}]},
        by=HUMAN,
    )
    await run(world)

    assert await world.engine.status(eid) is ExecutionStatus.COMPLETED
    result = (await world.engine.journal(eid))[-1].item.payload["value"]
    assert result["done"] == [{"F-1": "completed"}]
    assert len(world.merged) == 3  # every task reached the merge queue


async def test_a_stage_runs_together_and_the_next_one_waits(world):
    eid = await world.engine.start(
        world.milestone,
        {"features": [{"id": "F-1", "stages": [["T-1", "T-2"], ["T-3"]]}]},
        by=HUMAN,
    )
    await run(world)

    # The children of the feature are one execution per task, keyed by task id.
    (feature_row,) = [e for e in await _children(world, eid)]
    tasks = await _children(world, feature_row)
    assert len(tasks) == 3


async def _children(world, eid) -> list[str]:
    seen = []
    for entry in await world.backend.control.read():
        payload = entry.item.payload
        parent = (payload.get("parent") or {}).get("eid") if payload.get("parent") else None
        if entry.item.type == "execution.created" and parent == str(eid):
            seen.append(payload["eid"])
    return seen


async def test_a_failed_gate_reworks_with_a_fresh_attempt(world):
    """The first attempt fails its gates, so the second one runs: one task,
    two agent sessions, one merge."""
    world.gates.extend([False, True])
    eid = await world.engine.start(
        world.task,
        {"intent": "fix the thing"},
        by=HUMAN,
    )
    await run(world)

    result = (await world.engine.journal(eid))[-1].item.payload["value"]
    assert result == {"outcome": "completed", "attempts": 2}
    assert len(world.merged) == 1


async def test_a_rejected_review_escalates_the_feature(world):
    eid = await world.engine.start(
        world.feature,
        {"stages": [["T-1"]]},
        by=HUMAN,
    )
    await run(world, verdict="reject")

    result = (await world.engine.journal(eid))[-1].item.payload["value"]
    assert result["outcome"] == "escalated"
    assert world.merged == []  # nothing reached the integration branch


async def test_a_plan_delta_is_a_message_not_a_turn(world):
    """Spec 11 section 3.3: whoever plans sends a message; there is no
    reconciler to run, and no board version to reconcile against."""
    eid = await world.engine.start(
        world.milestone,
        {"features": [{"id": "F-1", "stages": [["T-1"]]}], "await_plan": True},
        by=HUMAN,
    )
    await run(world)  # its plan is done, so it waits on the `plan` channel

    assert await world.engine.status(eid) is ExecutionStatus.SUSPENDED
    delta = {"features": [{"id": "F-2", "stages": [["T-9"]]}]}
    await world.engine.signal(eid, "plan", delta, by=HUMAN)
    await run(world)

    result = (await world.engine.journal(eid))[-1].item.payload["value"]
    assert result["done"] == [{"F-1": "completed"}, {"F-2": "completed"}]

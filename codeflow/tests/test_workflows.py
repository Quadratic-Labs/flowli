"""The controller, running. Spec 11 section 3.

Every consumer here is a stand-in for a process: the agent runner, the merge
queue and the reviewers. None of them plans anything — the plan is the shape
of the workflows.
"""

from __future__ import annotations

from flowlet.domain import ExecutionStatus

from flowlet_codeflow import FeatureSpec, Milestone, TaskSpec
from flowlet_codeflow.gotchas import Gotcha

from .conftest import HUMAN, Agents, drive


def task(tid="T-1", **kwargs) -> TaskSpec:
    return TaskSpec(
        id=tid, intent=f"do {tid}", feature="F-1",
        write_scope=kwargs.pop("scope", ["src/a/**"]),
        verification=kwargs.pop("verification", ["just test"]),
        acceptance_criteria=kwargs.pop("criteria", ["AC-1"]),
        **kwargs,
    )


async def result_of(engine, eid) -> dict:
    entries = await engine.journal(eid)
    assert entries, "no journal"
    return dict(entries[-1].item.payload["value"])


# --- one task --------------------------------------------------------------


async def test_a_clean_attempt_is_reviewed_and_merged(engine, backend):
    agents = Agents()
    eid = await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(engine, backend, agents=agents)

    assert await engine.status(eid) is ExecutionStatus.COMPLETED
    assert await result_of(engine, eid) == {
        "outcome": "completed", "attempts": 1, "commit": "deadbee"
    }


async def test_a_failed_gate_reworks_with_the_prior_attempt_in_the_envelope(engine, backend):
    """A rework gets a fresh agent, seeded with what failed."""
    agents = Agents([
        {"outcome": "completed", "branch": "b1", "changed_paths": ["src/a/x.py", "docs/y.md"],
         "verification": [{"command": "just test", "exit_code": 0}],
         "claims": [{"criteria_satisfied": ["AC-1"]}]},
    ])
    eid = await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(engine, backend, agents=agents)

    assert (await result_of(engine, eid))["attempts"] == 2
    second = agents.envelopes[1]
    assert second["attempt"] == 2
    assert "outside the write scope" in second["prior"]["gate_failures"][0]
    assert second["prior"]["branch"] == "b1"


async def test_a_rejected_review_escalates_and_asks_a_person(engine, backend):
    eid = await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(engine, backend, agents=Agents(), verdicts={"code-review": "reject"})

    result = await result_of(engine, eid)
    assert result["outcome"] == "escalated"
    assert "ill-posed" in result["reason"]
    # The escalation is itself a review, so a person closed it.
    assert result["verdict"] == "accept"


async def test_a_conflict_from_the_merge_queue_is_a_rework(engine, backend):
    """The queue is not blocked by one bad merge: the task takes it back."""
    agents = Agents()
    eid = await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(
        engine, backend, agents=agents,
        merges=[{"merged": False, "reason": "conflict", "conflicts": ["src/a/x.py"]}],
    )

    assert (await result_of(engine, eid))["attempts"] == 2
    assert agents.envelopes[1]["prior"]["reason"] == "conflict"


async def test_the_attempts_run_out(engine, backend):
    bad = {"outcome": "failed", "error": "the agent gave up", "branch": "b"}
    eid = await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(engine, backend, agents=Agents([bad, bad, bad]))

    result = await result_of(engine, eid)
    assert result["outcome"] == "escalated"
    assert result["reason"] == "no attempt left"


async def test_the_gotchas_of_a_scope_reach_the_envelope(engine, backend, gotchas):
    gotchas.add(Gotcha(scope=["src/a/**"], text="the fixture resets between tests"))
    gotchas.add(Gotcha(scope=["docs/**"], text="not about this task"))
    agents = Agents()

    await engine.start(engine.workflows["task"], task().to_payload(), by=HUMAN)
    await drive(engine, backend, agents=agents)

    assert agents.envelopes[0]["gotchas"] == ["the fixture resets between tests"]


# --- a feature -------------------------------------------------------------


def feature_of(*tasks) -> dict:
    return FeatureSpec(id="F-1", title="the feature", tasks=list(tasks)).to_payload()


async def test_a_feature_runs_a_stage_together_and_the_next_one_after(engine, backend):
    spec = feature_of(
        task("T-1", scope=["src/a/**"]),
        task("T-2", scope=["src/b/**"]),
        TaskSpec(id="T-3", intent="last", feature="F-1", depends_on=["T-1", "T-2"],
                 write_scope=["src/c/**"]),
    )
    eid = await engine.start(engine.workflows["feature"], spec, by=HUMAN)
    await drive(engine, backend, agents=Agents())

    result = await result_of(engine, eid)
    assert result["outcome"] == "completed"
    assert set(result["tasks"]) == {"T-1", "T-2", "T-3"}


async def test_one_escalated_task_stops_the_feature_after_its_stage(engine, backend):
    spec = feature_of(
        task("T-1"),
        TaskSpec(id="T-2", intent="next", feature="F-1", depends_on=["T-1"],
                 write_scope=["src/b/**"]),
    )
    eid = await engine.start(engine.workflows["feature"], spec, by=HUMAN)
    await drive(engine, backend, agents=Agents(), verdicts={"code-review": "reject"})

    result = await result_of(engine, eid)
    assert result["outcome"] == "escalated"
    assert result["stage"] == 0
    assert set(result["tasks"]) == {"T-1"}  # the second stage never started


# --- a milestone -----------------------------------------------------------


def milestone_of(*features, await_plan=False) -> dict:
    plan = Milestone(id="M-1", title="the milestone", features=list(features))
    return {"milestone": plan.to_payload(), "await_plan": await_plan}


async def test_a_milestone_runs_its_features_in_order(engine, backend):
    spec = milestone_of(
        FeatureSpec(id="F-1", title="one", tasks=[task("T-1")]),
        FeatureSpec(id="F-2", title="two", tasks=[TaskSpec(id="T-2", intent="x", feature="F-2")]),
    )
    eid = await engine.start(engine.workflows["milestone"], spec, by=HUMAN)
    await drive(engine, backend, agents=Agents())

    result = await result_of(engine, eid)
    assert result["features"] == [{"F-1": "completed"}, {"F-2": "completed"}]


async def test_a_plan_delta_is_a_message_and_a_gated_operation_is_a_review(engine, backend):
    """Spec 11 sections 3.3 and 8: whoever plans sends a message, and an
    operation a gate covers waits for a person."""
    spec = milestone_of(
        FeatureSpec(id="F-1", title="one", tasks=[task("T-1")]), await_plan=True
    )
    eid = await engine.start(engine.workflows["milestone"], spec, by=HUMAN)
    await drive(engine, backend, agents=Agents())
    assert await engine.status(eid) is ExecutionStatus.SUSPENDED

    added = FeatureSpec(id="F-2", title="added", tasks=[
        TaskSpec(id="T-9", intent="added task", feature="F-2", write_scope=["src/z/**"]),
    ])
    await engine.signal(eid, "plan", {
        "final": True,
        "operations": [{"op": "create_feature", "feature": added.to_payload()}],
    }, by=HUMAN)
    await drive(engine, backend, agents=Agents())

    result = await result_of(engine, eid)
    assert result["features"] == [{"F-1": "completed"}, {"F-2": "completed"}]


async def test_a_refused_operation_comes_back_with_its_reason(engine, backend):
    spec = milestone_of(FeatureSpec(id="F-1", title="one", tasks=[]), await_plan=True)
    eid = await engine.start(engine.workflows["milestone"], spec, by=HUMAN)
    await drive(engine, backend, agents=Agents())

    stray = TaskSpec(id="T-9", intent="x", feature="F-404")
    await engine.signal(eid, "plan", {
        "final": True, "operations": [{"op": "create_task", "task": stray.to_payload()}],
    }, by=HUMAN)
    await drive(engine, backend, agents=Agents())

    announced = [
        s.item.payload for s in await backend.control.read()
        if s.item.type == "announce.codeflow.delta_refused"
    ]
    assert announced and announced[0]["refused"][0]["reason"] == "unknown_feature"

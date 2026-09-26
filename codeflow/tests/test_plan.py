"""The plan and its deltas. Spec 11 section 5."""

import pytest

from flowli_codeflow import (
    FeatureSpec,
    Milestone,
    PlanError,
    TaskSpec,
    apply_delta,
    check_feature,
)
from flowli_codeflow.model import stages_of


def task(tid, *, depends=(), scope=("src/a/**",), exclusive=False) -> TaskSpec:
    return TaskSpec(
        id=tid, intent=f"do {tid}", feature="F-1",
        depends_on=list(depends), write_scope=list(scope), exclusive=exclusive,
    )


def test_independent_tasks_share_one_stage():
    assert stages_of([task("T-1", scope=("src/a/**",)), task("T-2", scope=("src/b/**",))]) == [
        ["T-1", "T-2"]
    ]


def test_a_dependency_makes_a_second_stage():
    stages = stages_of([task("T-1"), task("T-2", depends=["T-1"])])
    assert stages == [["T-1"], ["T-2"]]


def test_a_cycle_is_refused_before_anything_runs():
    with pytest.raises(PlanError) as caught:
        stages_of([task("T-1", depends=["T-2"]), task("T-2", depends=["T-1"])])
    assert caught.value.code == "cyclic_plan"


def test_a_dependency_on_nothing_is_refused():
    with pytest.raises(PlanError) as caught:
        stages_of([task("T-1", depends=["T-9"])])
    assert caught.value.code == "unknown_dependency"


def test_two_tasks_of_one_stage_may_not_write_the_same_paths():
    feature = FeatureSpec(id="F-1", title="f", tasks=[
        task("T-1", scope=("src/a/**",)), task("T-2", scope=("src/a/deep/**",)),
    ])
    with pytest.raises(PlanError) as caught:
        check_feature(feature)
    assert caught.value.code == "overlapping_scopes"


def test_an_exclusive_task_runs_with_nothing_else():
    feature = FeatureSpec(id="F-1", title="f", tasks=[
        task("T-1", scope=("src/a/**",)), task("T-2", scope=("docs/**",), exclusive=True),
    ])
    with pytest.raises(PlanError):
        check_feature(feature)


def test_a_dependency_lets_them_share_a_scope():
    feature = FeatureSpec(id="F-1", title="f", tasks=[
        task("T-1"), task("T-2", depends=["T-1"]),
    ])
    assert check_feature(feature) == [["T-1"], ["T-2"]]


# --- deltas ----------------------------------------------------------------


def milestone() -> Milestone:
    return Milestone(id="M-1", title="m", features=[FeatureSpec(id="F-1", title="f", tasks=[])])


def test_create_task_applies_and_is_validated():
    result = apply_delta(milestone(), {"operations": [
        {"op": "create_task", "task": task("T-1").to_payload()},
    ]})
    assert result.applied == ["create_task"]
    assert result.milestone.features[0].tasks[0].id == "T-1"


def test_a_task_for_no_feature_is_refused_with_a_reason():
    stray = TaskSpec(id="T-1", intent="x", feature="F-9")
    result = apply_delta(milestone(), {"operations": [
        {"op": "create_task", "task": stray.to_payload()},
    ]})
    assert result.applied == []
    assert result.refused == [
        {"op": "create_task", "reason": "unknown_feature", "detail": "no feature F-9"}
    ]


def test_one_refusal_does_not_refuse_the_delta():
    stray = TaskSpec(id="T-9", intent="x", feature="F-9")
    result = apply_delta(milestone(), {"operations": [
        {"op": "create_task", "task": stray.to_payload()},
        {"op": "create_task", "task": task("T-1").to_payload()},
    ]})
    assert result.applied == ["create_task"] and len(result.refused) == 1
    assert [t.id for t in result.milestone.features[0].tasks] == ["T-1"]


def test_a_gated_operation_is_not_applied_here():
    """It goes to a review instead (spec 11, section 8)."""
    result = apply_delta(milestone(), {"operations": [
        {"op": "create_feature", "feature": FeatureSpec(id="F-2", title="g").to_payload()},
    ]})
    assert result.applied == []
    assert result.gated[0]["op"] == "create_feature"
    assert [f.id for f in result.milestone.features] == ["F-1"]


def test_an_unknown_operation_is_refused():
    result = apply_delta(milestone(), {"operations": [{"op": "delete_everything"}]})
    assert result.refused == [{"op": "delete_everything", "reason": "unknown_operation"}]


def test_too_many_tasks_is_refused():
    many = [{"op": "create_task", "task": task(f"T-{n}", scope=(f"src/{n}/**",)).to_payload()}
            for n in range(4)]
    result = apply_delta(milestone(), {"operations": many}, max_open_tasks=2)
    assert any(r["reason"] == "too_many_tasks" for r in result.refused)


def test_reprioritize_must_name_every_feature():
    start = Milestone(id="M-1", title="m", features=[
        FeatureSpec(id="F-1", title="a"), FeatureSpec(id="F-2", title="b"),
    ])
    good = apply_delta(start, {"operations": [{"op": "reprioritize", "order": ["F-2", "F-1"]}]})
    assert [f.id for f in good.milestone.features] == ["F-2", "F-1"]

    bad = apply_delta(start, {"operations": [{"op": "reprioritize", "order": ["F-2"]}]})
    assert bad.refused[0]["reason"] == "partial_order"

"""Routing and the deterministic gates. Spec 11 section 4."""

from flowlet_codeflow import Route, decide, run_gates

PASSED = {"passed": True, "failures": []}
FAILED = {"passed": False, "failures": ["the verification 'just test' exited 1"]}


# --- routing ---------------------------------------------------------------


def test_a_failed_gate_is_a_rework_whatever_the_reviewer_said():
    assert decide(FAILED, {"verdict": "accept"}, 1, 3).route is Route.REWORK


def test_gates_pass_and_the_reviewer_accepts():
    assert decide(PASSED, {"verdict": "accept"}, 1, 3).route is Route.APPROVED


def test_an_accept_with_a_blocking_finding_is_a_contradiction():
    judgment = {"verdict": "accept", "findings": [{"severity": "blocking"}]}
    decision = decide(PASSED, judgment, 1, 3)
    assert decision.route is Route.ESCALATED
    assert "blocking" in decision.reason


def test_a_rework_has_another_attempt_until_it_does_not():
    assert decide(PASSED, {"verdict": "rework"}, 1, 3).route is Route.REWORK
    assert decide(PASSED, {"verdict": "rework"}, 3, 3).route is Route.ESCALATED


def test_a_reject_says_the_task_is_ill_posed():
    assert decide(PASSED, {"verdict": "reject"}, 1, 3).route is Route.ESCALATED


def test_a_verdict_outside_the_three_words_escalates():
    assert decide(PASSED, {"verdict": "approve"}, 1, 3).route is Route.ESCALATED


def test_no_review_before_the_deadline_escalates():
    assert decide(PASSED, None, 1, 3).route is Route.ESCALATED


# --- gates -----------------------------------------------------------------


ENVELOPE = {
    "verification": ["just test"],
    "write_scope": ["src/billing/**"],
    "acceptance_criteria": ["AC-1", "AC-2"],
}


def report(**overrides):
    base = {
        "outcome": "completed",
        "verification": [{"command": "just test", "exit_code": 0}],
        "changed_paths": ["src/billing/hooks.py"],
        "claims": [{"criteria_satisfied": ["AC-1", "AC-2"]}],
    }
    return {**base, **overrides}


def test_a_clean_report_passes():
    assert run_gates(report(), ENVELOPE) == {"passed": True, "failures": []}


def test_a_verification_that_did_not_run_fails():
    result = run_gates(report(verification=[]), ENVELOPE)
    assert not result["passed"]
    assert "did not run" in result["failures"][0]


def test_a_verification_that_failed_fails():
    result = run_gates(
        report(verification=[{"command": "just test", "exit_code": 1}]), ENVELOPE
    )
    assert "exited 1" in result["failures"][0]


def test_a_path_outside_the_write_scope_fails():
    result = run_gates(report(changed_paths=["src/billing/a.py", "src/auth/b.py"]), ENVELOPE)
    assert "outside the write scope" in result["failures"][0]
    assert "src/auth/b.py" in result["failures"][0]


def test_completed_must_claim_every_criterion():
    result = run_gates(report(claims=[{"criteria_satisfied": ["AC-1"]}]), ENVELOPE)
    assert "AC-2 is not claimed" in result["failures"][0]


def test_a_partial_outcome_is_not_held_to_the_criteria():
    result = run_gates(report(outcome="partial", claims=[]), ENVELOPE)
    assert result["passed"]  # it never said it was done


def test_a_failed_attempt_fails_the_gates():
    result = run_gates(report(outcome="failed", error="the host went away"), ENVELOPE)
    assert "the host went away" in result["failures"][0]

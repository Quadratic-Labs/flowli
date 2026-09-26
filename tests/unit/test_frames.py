from datetime import timedelta

import pytest

from flowli.api.frames import build
from flowli.codec import structure, unstructure
from flowli.domain import (
    ROOT_FID,
    DuplicateFrameError,
    Entry,
    EntryType,
    Failed,
    FrameIdAllocator,
    FrameRef,
    InvalidName,
    RetryPolicy,
    Sequenced,
    child_fid,
    digest_text,
)
from tests.ids import E_ABC


def seqd(entries):
    return [Sequenced(i + 1, e) for i, e in enumerate(entries)]


def test_child_fid_ordinal_and_key_forms():
    assert child_fid(ROOT_FID, "fetch", ordinal=0) == "root/fetch#0"
    assert child_fid("root/a#0", "b", key="k1") == "root/a#0/b:k1"


def test_child_fid_requires_exactly_one_of_ordinal_or_key():
    with pytest.raises(ValueError, match="^give exactly one of ordinal or key$"):
        child_fid(ROOT_FID, "x")
    with pytest.raises(ValueError, match="^give exactly one of ordinal or key$"):
        child_fid(ROOT_FID, "x", ordinal=0, key="k")


def test_child_fid_validates_names():
    with pytest.raises(InvalidName):
        child_fid(ROOT_FID, "a/b", ordinal=0)
    with pytest.raises(InvalidName):
        child_fid(ROOT_FID, "a", key="k#1")


def test_allocator_counts_same_named_frames_in_call_order():
    alloc = FrameIdAllocator(ROOT_FID)
    assert alloc.allocate("step") == "root/step#0"
    assert alloc.allocate("other") == "root/other#0"
    assert alloc.allocate("step") == "root/step#1"


def test_allocator_rejects_duplicate_key():
    alloc = FrameIdAllocator(ROOT_FID)
    alloc.allocate("child", key="0")
    with pytest.raises(DuplicateFrameError) as exc_info:
        alloc.allocate("child", key="0")
    assert exc_info.value.fid == "root/child:0"
    assert str(exc_info.value) == "duplicate frame id root/child:0"


def test_allocator_is_replay_stable():
    a, b = FrameIdAllocator(ROOT_FID), FrameIdAllocator(ROOT_FID)
    calls = [("s", None), ("s", None), ("c", "k"), ("s", None)]
    assert [a.allocate(n, k) for n, k in calls] == [b.allocate(n, k) for n, k in calls]


def test_frame_ref_is_plain_data_and_codec_validates_eid():
    from flowli.codec import structure, unstructure

    ref = FrameRef(E_ABC, "root")
    assert unstructure(ref) == {"eid": str(E_ABC), "fid": "root"}
    assert structure({"eid": str(E_ABC), "fid": "root"}, FrameRef) == ref
    with pytest.raises(InvalidName):
        structure({"eid": "BAD", "fid": "root"}, FrameRef)


def test_retry_policy_defaults_to_no_retry():
    p = RetryPolicy()
    assert not p.allows_retry(1, retryable=True)


def test_retry_policy_rejects_invalid_max_attempts():
    with pytest.raises(ValueError, match="^max_attempts must be >= 1$"):
        RetryPolicy(max_attempts=0)


def test_retry_policy_rejects_invalid_backoff_factor():
    with pytest.raises(ValueError, match="^backoff_factor must be >= 1.0$"):
        RetryPolicy(backoff_factor=0.5)


def test_retry_policy_allows_until_max_and_respects_retryable():
    p = RetryPolicy(max_attempts=3)
    assert p.allows_retry(1, True)
    assert p.allows_retry(2, True)
    assert not p.allows_retry(3, True)
    assert not p.allows_retry(1, False)


def test_retry_policy_backoff_and_cap():
    p = RetryPolicy(
        max_attempts=5,
        backoff=timedelta(seconds=10),
        backoff_factor=2.0,
        max_backoff=timedelta(seconds=30),
    )
    assert p.delay_after(1) == timedelta(seconds=10)
    assert p.delay_after(2) == timedelta(seconds=20)
    assert p.delay_after(3) == timedelta(seconds=30)
    assert p.delay_after(4) == timedelta(seconds=30)


def test_retry_policy_roundtrip():
    p = RetryPolicy(max_attempts=2, backoff=timedelta(seconds=1.5))
    assert structure(unstructure(p), RetryPolicy) == p


def test_failed_from_exception():
    f = Failed.from_exception(KeyError("x"), retryable=False)
    assert f.error_type == "KeyError"
    assert not f.retryable
    assert structure(unstructure(f), Failed) == f


def test_failed_from_exception_defaults_to_retryable():
    f = Failed.from_exception(KeyError("x"))
    assert f.retryable is True


def test_reply_channel_matches_the_documented_formula():
    """01-domain-model.md section 3: reply_channel is
    "{eid}.reply.{digest_text(fid + "/" + frame)[:16]}" -- the separator and
    the 16-char slice are both part of the contract, not incidental."""
    ref = FrameRef(E_ABC, "root/x#0")
    expected = f"{E_ABC}.reply.{digest_text(ref.fid + '/' + 'review:r1')[:16]}"
    assert ref.reply_channel("review:r1") == expected


# region ----- flowli.api.frames.build -----
# `build()` folds a journal into the tree of `specs/09-http-api.md`
# section 8.3. Each `execution.*`/`frame.*` branch below is exercised as the
# very *first* entry for its fid at least once, so the node it creates is
# fresh rather than a lookup of an already-cached node -- only then do the
# literal kind/name/status values the branch passes actually get observed.


def test_root_created_fresh_by_each_execution_event_has_the_right_kind_and_name(prov):
    """Every execution.* branch creates the root node itself when nothing
    has touched `root` yet -- kind must stay "root" and name must be the
    workflow, never `node()`'s own step/"" defaults or a mixed-up literal."""
    cases = [
        (Entry.execution_started(prov, {}), "running"),
        (Entry.execution_suspended(prov, ["channel:a"]), "suspended"),
        (Entry.execution_resumed(prov, 2, "signal"), "running"),
        (Entry.execution_completed(prov, "the-value"), "completed"),
        (Entry.execution_failed(prov, Failed("IOError", "boom")), "failed"),
        (Entry.execution_cancelled(prov, {"kind": "human", "id": "t"}), "cancelled"),
    ]
    for entry, status in cases:
        root = build(seqd([entry]))
        assert root.fid == ROOT_FID
        assert root.kind == "root"
        assert root.name == prov.code.workflow
        assert root.status == status


def test_execution_started_sets_started_at_once(prov):
    root = build(seqd([Entry.execution_started(prov, {})]))
    assert root.started_at == prov.at.to_iso()


def test_execution_completed_records_the_value(prov):
    root = build(seqd([Entry.execution_completed(prov, "the-value")]))
    assert root.status == "completed"
    assert root.ended_at == prov.at.to_iso()
    assert root.value == "the-value"
    assert root.suspended_on is None


def test_execution_failed_records_the_full_error(prov):
    root = build(seqd([Entry.execution_failed(prov, Failed("IOError", "boom"))]))
    assert root.status == "failed"
    assert root.ended_at == prov.at.to_iso()
    assert root.error == {"type": "IOError", "message": "boom"}
    assert root.suspended_on is None


def test_execution_cancelled_sets_status_and_ended_at(prov):
    root = build(seqd([Entry.execution_cancelled(prov, {"kind": "human", "id": "t"})]))
    assert root.status == "cancelled"
    assert root.ended_at == prov.at.to_iso()
    assert root.suspended_on is None


def test_execution_suspended_joins_reasons_and_clears_when_the_list_is_empty(prov):
    joined = build(seqd([Entry.execution_suspended(prov, ["channel:a", "timer:t1"])]))
    assert joined.status == "suspended"
    assert joined.suspended_on == "channel:a, timer:t1"

    cleared = build(seqd([Entry.execution_suspended(prov, [])]))
    assert cleared.suspended_on is None


def test_frame_started_with_a_full_payload_uses_its_own_kind_name_and_attempt(prov):
    fid = "root/approve#0"
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, fid, "gate", "approval", "d1", 3),
        ]
    )
    n = build(entries).children[0]
    assert n.kind == "gate"
    assert n.name == "approval"
    assert n.attempts == 3
    assert n.status == "running"
    assert n.started_at == prov.at.to_iso()


def test_frame_started_without_kind_name_or_attempt_falls_back_to_defaults(prov):
    fid = "root/approve#0"
    raw = Entry(EntryType.FRAME_STARTED, fid, {"args_digest": "d1"}, prov)
    entries = seqd([Entry.execution_started(prov, {}), raw])
    n = build(entries).children[0]
    assert n.kind == "step"
    assert n.name == "approve#0"  # falls back to the fid's own tail
    assert n.attempts == 1


def test_a_frame_hangs_from_its_actual_parent_when_the_parent_exists(prov):
    parent_fid = "root/parent#0"
    child_fid_ = "root/parent#0/child#0"
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, parent_fid, "step", "parent", "d", 1),
            Entry.frame_started(prov, child_fid_, "step", "child", "d", 1),
        ]
    )
    root = build(entries)
    assert [c.fid for c in root.children] == [parent_fid]
    assert [c.fid for c in root.children[0].children] == [child_fid_]


def test_frame_completed_without_a_prior_start_hangs_from_root_with_node_defaults(prov):
    """No `frame.started` for this fid, and its own parent never appeared
    either -- `node()`'s own kind/name defaults apply, and the comment on
    the tree-building loop ("a frame under a gone parent hangs from the
    root rather than vanishing") is exactly what must happen here."""
    fid = "root/ghost#0/child#0"
    entries = seqd(
        [Entry.execution_started(prov, {}), Entry.frame_completed(prov, fid, 1, "the-value")]
    )
    root = build(entries)
    assert [c.fid for c in root.children] == [fid]
    child = root.children[0]
    assert child.kind == "step"
    assert child.name == "child#0"
    assert child.status == "completed"
    assert child.ended_at == prov.at.to_iso()
    assert child.value == "the-value"
    assert child.suspended_on is None


def test_frame_failed_records_the_full_error_and_the_optional_retry_at(prov):
    fid = "root/flaky#0"
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, fid, "step", "flaky", "d", 1),
            Entry.frame_failed(
                prov,
                fid,
                1,
                Failed("IOError", "boom", retryable=False),
                retry_at="2026-09-07T10:00:00.000000Z",
            ),
        ]
    )
    n = build(entries).children[0]
    assert n.status == "failed"
    assert n.ended_at == prov.at.to_iso()
    assert n.error == {
        "type": "IOError",
        "message": "boom",
        "retryable": False,
        "retry_at": "2026-09-07T10:00:00.000000Z",
    }


def test_frame_failed_defaults_retryable_true_and_retry_at_none_when_absent(prov):
    fid = "root/flaky#0"
    raw = Entry(EntryType.FRAME_FAILED, fid, {"error_type": "X", "message": "y"}, prov)
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, fid, "step", "flaky", "d", 1),
            raw,
        ]
    )
    n = build(entries).children[0]
    assert n.error["retryable"] is True
    assert n.error["retry_at"] is None


def test_frame_suspended_records_the_condition_and_deadline(prov):
    fid = "root/receive#0"
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, fid, "receive", "receive", "d", 1),
            Entry.frame_suspended(
                prov, fid, 1, "channel:payments", deadline="2026-09-08T00:00:00Z"
            ),
        ]
    )
    n = build(entries).children[0]
    assert n.status == "suspended"
    assert n.suspended_on == "channel:payments"
    assert n.deadline == "2026-09-08T00:00:00Z"


def test_frame_fulfilled_returns_to_running_and_clears_suspend_state(prov):
    fid = "root/receive#0"
    entries = seqd(
        [
            Entry.execution_started(prov, {}),
            Entry.frame_started(prov, fid, "receive", "receive", "d", 1),
            Entry.frame_suspended(
                prov, fid, 1, "channel:payments", deadline="2026-09-08T00:00:00Z"
            ),
            Entry.frame_fulfilled(prov, fid, 1, "channel:payments", 3),
        ]
    )
    n = build(entries).children[0]
    assert n.status == "running"
    assert n.suspended_on is None
    assert n.deadline is None


# endregion

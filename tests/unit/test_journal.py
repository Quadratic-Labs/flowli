import pytest

from flowli.codec import structure, unstructure
from flowli.domain import (
    ROOT_FID,
    Completed,
    Condition,
    Entry,
    EntryType,
    Failed,
    FrameRef,
    MemoTable,
    Sequenced,
    Task,
    TaskKind,
)
from tests.ids import E_ABC


def seqd(entries):
    return [Sequenced(i + 1, e) for i, e in enumerate(entries)]


def test_entry_codec_roundtrip(prov):
    e = Entry.frame_completed(prov, "root/fetch#0", 1, {"id": 1})
    d = unstructure(e)
    assert d["type"] == "frame.completed" and d["fid"] == "root/fetch#0"
    assert d["provenance"]["at"] == "2026-09-07T09:00:00.123000Z"
    assert d["provenance"]["actor"] == {"kind": "worker", "id": "w-1", "on_behalf_of": None}
    assert structure(d, Entry) == e


def test_entry_rejects_unknown_type_but_accepts_announce(prov):
    with pytest.raises(ValueError, match=r"unknown entry type 'frame\.exploded'"):
        Entry("frame.exploded", "root", {}, prov)
    Entry.announce(prov, "root", "review.requested", {"rid": "r1"})


def test_condition_parse():
    assert Condition.parse(Condition.channel("a.b")) == ("channel", "a.b")
    assert Condition.parse(Condition.operator()) == ("operator", None)


def test_entry_execution_started_payload(prov):
    e = Entry.execution_started(prov, {"a": 1})
    assert e.type == EntryType.EXECUTION_STARTED
    assert e.fid == ROOT_FID
    assert e.payload == {"args": {"a": 1}}
    assert e.provenance is prov


def test_entry_frame_completed_payload(prov):
    fid = "root/a#0"
    e = Entry.frame_completed(prov, fid, 2, {"v": 1})
    assert e.type == EntryType.FRAME_COMPLETED
    assert e.fid == fid
    assert e.payload == {"attempt": 2, "value": {"v": 1}}
    assert e.provenance is prov


def test_entry_frame_failed_payload(prov):
    fid = "root/a#0"
    e = Entry.frame_failed(prov, fid, 2, Failed("IOError", "boom", retryable=False))
    assert e.type == EntryType.FRAME_FAILED
    assert e.payload == {
        "attempt": 2,
        "error_type": "IOError",
        "message": "boom",
        "retryable": False,
    }
    assert "retry_at" not in e.payload
    assert e.provenance is prov

    e2 = Entry.frame_failed(
        prov, fid, 3, Failed("IOError", "boom"), retry_at="2026-09-08T00:00:00Z"
    )
    assert e2.payload["retry_at"] == "2026-09-08T00:00:00Z"


def test_entry_frame_suspended_payload(prov):
    fid = "root/a#0"
    e = Entry.frame_suspended(prov, fid, 1, Condition.timer("t1"), deadline="2026-09-08T00:00:00Z")
    assert e.type == EntryType.FRAME_SUSPENDED
    assert e.payload == {
        "attempt": 1,
        "on": Condition.timer("t1"),
        "deadline": "2026-09-08T00:00:00Z",
    }

    e2 = Entry.frame_suspended(prov, fid, 1, Condition.timer("t1"))
    assert e2.payload == {"attempt": 1, "on": Condition.timer("t1")}


def test_entry_frame_fulfilled_payload(prov):
    fid = "root/a#0"
    e = Entry.frame_fulfilled(prov, fid, 1, Condition.timer("t1"), 5)
    assert e.type == EntryType.FRAME_FULFILLED
    assert e.payload == {"attempt": 1, "on": Condition.timer("t1"), "message_seq": 5}


def test_entry_task_enqueued_payload(prov):
    task = Task(
        queue="default",
        kind=TaskKind.START,
        target=FrameRef(E_ABC, "root/x#0"),
        reason="start",
        enqueued_by=prov,
    )
    e = Entry.task_enqueued(task)
    assert e.type == EntryType.TASK_ENQUEUED
    assert e.fid == "root/x#0"
    assert e.payload == {
        "task_id": task.task_id,
        "queue": "default",
        "kind": "start",
        "target": {"eid": str(E_ABC), "fid": "root/x#0"},
        "reason": "start",
    }
    assert e.provenance is prov


def test_entry_execution_lifecycle_uses_root_fid(prov):
    assert Entry.execution_suspended(prov, ["timer:t1"]).fid == ROOT_FID
    assert Entry.execution_resumed(prov, 2, "lease").fid == ROOT_FID
    assert Entry.execution_failed(prov, Failed("X", "y")).fid == ROOT_FID
    assert Entry.execution_cancelled(prov, {"kind": "human", "id": "t"}).fid == ROOT_FID
    assert Entry.execution_migrated(prov, "1.0.0", "1.1.0").fid == ROOT_FID


def test_memo_first_completed_wins(prov):
    fid = "root/score#0"
    entries = seqd(
        [
            Entry.frame_started(prov, fid, "step", "score", "d1", 1),
            Entry.frame_started(prov, fid, "step", "score", "d1", 1),  # second worker
            Entry.frame_completed(prov, fid, 1, 0.93),
            Entry.frame_completed(prov, fid, 1, 0.50),  # loser
        ]
    )
    t = MemoTable.build(entries)
    assert t.memos[fid] == Completed(0.93)
    assert t.digests[fid] == "d1"
    assert t.tail == 4


def test_memo_failures_and_next_attempt(prov):
    fid = "root/flaky#0"
    entries = seqd(
        [
            Entry.frame_started(prov, fid, "step", "flaky", "d", 1),
            Entry.frame_failed(prov, fid, 1, Failed("IOError", "boom", retryable=False)),
            Entry.frame_started(prov, fid, "step", "flaky", "d", 2),
            Entry.frame_failed(
                prov,
                fid,
                2,
                Failed("IOError", "boom again"),
                retry_at="2026-09-07T10:00:00.000000Z",
            ),
        ]
    )
    t = MemoTable.build(entries)
    assert t.next_attempt(fid) == 3
    assert fid not in t.memos
    # exact content of every recorded failure, including the per-attempt retryable flag
    assert t.failures[fid] == [
        Failed("IOError", "boom", retryable=False),
        Failed("IOError", "boom again", retryable=True),
    ]
    assert t.retry_at[fid] == "2026-09-07T10:00:00.000000Z"

    # a later failure without a retry_at clears the pending retry instant for that fid
    t.apply(
        Sequenced(
            5, Entry.frame_failed(prov, fid, 3, Failed("IOError", "boom again", retryable=False))
        )
    )
    assert fid not in t.retry_at


def test_memo_frame_failed_retryable_defaults_true_when_absent(prov):
    """A frame.failed entry that predates the `retryable` field defaults it to True."""
    fid = "root/legacy#0"
    payload = {"attempt": 1, "error_type": "X", "message": "y"}
    raw = Entry(EntryType.FRAME_FAILED, fid, payload, prov)
    t = MemoTable.build(seqd([raw]))
    assert t.failures[fid] == [Failed("X", "y", retryable=True)]


def test_memo_suspended_fulfilled_and_consumed(prov):
    fid = "root/receive#0"
    ch = f"{E_ABC}.payments"
    entries = seqd(
        [
            Entry.frame_started(prov, fid, "receive", "receive", "d", 1),
            Entry.frame_suspended(
                prov, fid, 1, Condition.channel(ch), deadline="2026-09-08T00:00:00.000000Z"
            ),
            Entry.execution_suspended(prov, [Condition.channel(ch)]),
        ]
    )
    t = MemoTable.build(entries)
    assert t.suspended[fid] == Condition.channel(ch)
    assert t.deadlines[fid] == "2026-09-08T00:00:00.000000Z"
    assert t.last_consumed(ch) == 0
    assert not t.is_terminal

    t.apply(Sequenced(4, Entry.execution_resumed(prov, 2, "message:payments")))
    t.apply(Sequenced(5, Entry.frame_fulfilled(prov, fid, 1, Condition.channel(ch), 3)))
    assert fid not in t.suspended
    assert fid not in t.deadlines
    assert t.fulfilled[fid] == 3
    assert t.last_consumed(ch) == 3

    # a duplicate fulfilled entry for an already-fulfilled fid is a no-op, not a KeyError,
    # and a lower message_seq never lowers the channel's high-water mark
    t.apply(Sequenced(6, Entry.frame_fulfilled(prov, fid, 1, Condition.channel(ch), 1)))
    assert fid not in t.suspended
    assert t.last_consumed(ch) == 3


def test_memo_consumed_records_seq_zero(prov):
    """The channel high-water mark must reflect the actual seq, not a nonzero default."""
    fid = "root/receive#0"
    ch = f"{E_ABC}.orders"
    entries = seqd(
        [
            Entry.frame_suspended(prov, fid, 1, Condition.channel(ch)),
            Entry.frame_fulfilled(prov, fid, 1, Condition.channel(ch), 0),
        ]
    )
    t = MemoTable.build(entries)
    assert t.last_consumed(ch) == 0


def test_memo_terminal_and_cancelled(prov):
    t = MemoTable.build(seqd([Entry.execution_completed(prov, "approved")]))
    assert t.terminal == Completed("approved")
    assert t.is_terminal
    t2 = MemoTable.build(seqd([Entry.execution_failed(prov, Failed("X", "y"))]))
    assert t2.terminal == Failed("X", "y", retryable=False)
    t3 = MemoTable.build(seqd([Entry.execution_cancelled(prov, {"kind": "human", "id": "t"})]))
    assert t3.cancelled and t3.is_terminal


def test_check_digest_detects_nondeterminism(prov):
    fid = "root/a#0"
    t = MemoTable.build(seqd([Entry.frame_started(prov, fid, "step", "a", "d1", 1)]))
    assert t.check_digest(fid, "d1") is None
    assert t.check_digest(fid, "d2") == "d1"
    assert t.check_digest("root/unknown#0", "zz") is None


def test_entry_types_cover_spec():
    names = {e.value for e in EntryType}
    assert {
        "frame.started",
        "frame.completed",
        "frame.failed",
        "frame.suspended",
        "frame.fulfilled",
        "execution.started",
        "execution.suspended",
        "execution.resumed",
        "execution.completed",
        "execution.failed",
        "execution.cancelled",
        "execution.created",
        "task.enqueued",
    } <= names

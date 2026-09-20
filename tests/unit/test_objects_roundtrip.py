import os
import socket
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from cairndb import Timestamp

from flowlet.codec import structure, unstructure
from flowlet.domain import (
    Actor,
    Execution,
    ExecutionStatus,
    FrameRef,
    Message,
    Provenance,
    Site,
    Task,
    TaskKind,
    Timer,
    can_transition,
    execution_channel,
)
from tests.ids import E_ABC, E_PAR


def test_provenance_roundtrip(prov):
    assert structure(unstructure(prov), Provenance) == prov


def test_site_with_epoch(site):
    assert site.with_epoch(7).epoch == 7
    assert site.epoch == 1


def test_actor_constructors():
    assert Actor.human("t@x.io").kind == "human"
    assert Actor.human("t@x.io", on_behalf_of="cfo@x.io").on_behalf_of == "cfo@x.io"
    assert Actor.system("ui", on_behalf_of="t@x.io").on_behalf_of == "t@x.io"
    assert Actor.schedule("nightly").kind == "schedule"


def test_site_local():
    s = Site.local("w-1", instance="c1", region="eu")
    assert s.host == socket.gethostname()
    assert s.pid == os.getpid()
    assert s.worker_id == "w-1"
    assert s.instance == "c1"
    assert s.region == "eu"


def test_provenance_attempt_must_be_at_least_one(prov):
    with pytest.raises(ValueError, match="^attempt starts at 1$"):
        replace(prov, attempt=0)


def test_execution_roundtrip(prov):
    ex = Execution(
        eid=E_ABC,
        workflow="w",
        version="1",
        args=["inv-42"],
        created_by=prov,
        parent=FrameRef(E_PAR, "root/child:0"),
        dispatch_key="invoice:42",
    )
    assert structure(unstructure(ex), Execution) == ex


def test_status_machine():
    S = ExecutionStatus
    assert can_transition(S.PENDING, S.RUNNING)
    assert can_transition(S.RUNNING, S.SUSPENDED)
    assert can_transition(S.SUSPENDED, S.RUNNING)
    assert can_transition(S.SUSPENDED, S.CANCELLED)
    assert not can_transition(S.COMPLETED, S.RUNNING)
    assert not can_transition(S.SUSPENDED, S.COMPLETED)
    assert S.FAILED.is_terminal and not S.SUSPENDED.is_terminal


def test_task_roundtrip_and_visibility(prov):
    nb = Timestamp(datetime(2026, 9, 8, tzinfo=UTC))
    t = Task(
        queue="default",
        kind=TaskKind.RESUME,
        target=FrameRef(E_ABC, "root"),
        reason="message:payments",
        enqueued_by=prov,
        key="message:payments:1",
        not_before=nb,
    )
    assert t.task_id == f"resume:{E_ABC}:message:payments:1"
    assert Task.id_for(TaskKind.START, E_ABC) == f"start:{E_ABC}"
    assert structure(unstructure(t), Task) == t
    assert t.visible_at(Timestamp(datetime(2026, 1, 1, tzinfo=UTC))) == nb


def test_frame_ref_derives_its_names():
    ref = FrameRef(E_ABC, "root/x#0")
    assert execution_channel(E_ABC, "payments") == f"{E_ABC}.payments"
    assert ref.child_channel == f"{E_ABC}.child.{ref.child_eid}"
    assert ref.step_channel.startswith(f"{E_ABC}.step.") and "#" not in ref.step_channel
    assert ref.reply_channel("review:r1") == ref.reply_channel("review:r1")
    assert ref.reply_channel("a") != ref.reply_channel("b")
    assert FrameRef(E_ABC, "root").child_channel != ref.child_channel


def test_message_roundtrip(prov):
    m = Message(f"{E_ABC}.payments", 1, {"amount": 10}, prov, correlation="c1")
    assert structure(unstructure(m), Message) == m


def test_timer_id_sorts_by_due_time(prov):
    ref = FrameRef(E_ABC, "root/sleep#0")
    t1 = Timer(Timestamp(datetime(2026, 9, 7, 10, 0, tzinfo=UTC)), ref)
    t2 = Timer(Timestamp(datetime(2026, 9, 7, 10, 1, tzinfo=UTC)), ref)
    assert t1.timer_id < t2.timer_id
    assert t1.is_due(Timestamp(datetime(2026, 9, 7, 10, 0, tzinfo=UTC)))
    assert not t2.is_due(Timestamp(datetime(2026, 9, 7, 10, 0, tzinfo=UTC)))
    assert structure(unstructure(t1), Timer) == t1

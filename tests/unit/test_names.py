import uuid

import pytest

from flowlet.domain import (
    FLOWLET2_NAMESPACE,
    FrameRef,
    InvalidName,
    check_channel,
    check_frame_name,
    check_queue,
    new_eid,
    parse_eid,
)


def test_new_eid_is_uuid7():
    eid = new_eid()
    assert isinstance(eid, uuid.UUID) and eid.version == 7
    assert len(str(eid)) == 36


def test_new_eids_are_time_ordered_and_unique():
    ids = [new_eid() for _ in range(50)]
    assert len(set(ids)) == 50
    assert ids == sorted(ids)  # v7: creation order is sort order


def test_new_eid_fits_a_cairndb_log_name():
    check_channel(f"{new_eid()}.payments")


def test_child_eid_is_deterministic_uuid5():
    parent = new_eid()
    a = FrameRef(parent, "root/child:0").child_eid
    assert a == FrameRef(parent, "root/child:0").child_eid
    assert a != FrameRef(parent, "root/child:1").child_eid
    assert a.version == 5
    assert a == uuid.uuid5(FLOWLET2_NAMESPACE, f"{parent}/root/child:0")


def test_parse_eid_accepts_uuid_and_canonical_string():
    eid = new_eid()
    assert parse_eid(eid) is eid
    assert parse_eid(str(eid)) == eid
    assert parse_eid(eid.hex) == eid
    for bad in ["", "01abc", "not-a-uuid", None, 42]:
        with pytest.raises(InvalidName):
            parse_eid(bad)  # type: ignore[arg-type]


def test_parse_eid_error_names_the_bad_value_and_says_what_was_expected():
    with pytest.raises(InvalidName) as exc_info:
        parse_eid("not-a-uuid")  # type: ignore[arg-type]
    message = str(exc_info.value)
    assert "not-a-uuid" in message
    assert "expected a UUID" in message


def test_check_frame_name():
    check_frame_name("fetch_invoice-2")
    for bad in ["a/b", "a#b", "a:b", "", "a b"]:
        with pytest.raises(InvalidName):
            check_frame_name(bad)


def test_check_frame_name_error_says_frame_name_and_the_bad_value():
    with pytest.raises(InvalidName) as exc_info:
        check_frame_name("a/b")
    message = str(exc_info.value)
    assert "invalid frame name 'a/b'" in message
    assert "must match" in message


def test_check_channel_matches_cairndb_log_name_rule():
    check_channel("01abc.reply.deadbeef")
    for bad in ["Payments", "a/b", "a b", ""]:
        with pytest.raises(InvalidName):
            check_channel(bad)


def test_check_channel_error_says_channel_and_the_bad_value():
    with pytest.raises(InvalidName) as exc_info:
        check_channel("Payments")
    message = str(exc_info.value)
    assert "invalid channel 'Payments'" in message
    assert "must match" in message


def test_check_queue_matches_cairndb_log_name_rule():
    check_queue("01abc.payments")
    for bad in ["Payments", "a/b", "a b", ""]:
        with pytest.raises(InvalidName):
            check_queue(bad)


def test_check_queue_error_says_queue_and_the_bad_value():
    with pytest.raises(InvalidName) as exc_info:
        check_queue("Payments")
    message = str(exc_info.value)
    assert "invalid queue 'Payments'" in message
    assert "must match" in message

"""Attempt logs: the buffer, the processor and the port. Spec 03 section 12."""

import asyncio
import json

import pytest
import structlog

from flowli.adapters import evidence as keys
from flowli.adapters.memory import MemoryBackend, MemoryEvidence
from flowli.domain import EvidenceRef, InvalidName, digest_text
from flowli.evidence import TAIL_LINES, AttemptBuffer, EvidenceWriter, NullEvidence, capture
from flowli.log import configure_logging
from tests.ids import E_ABC

REF = EvidenceRef(E_ABC, "root/fetch#0", 1)


@pytest.fixture
def port() -> MemoryEvidence:
    return MemoryEvidence()


def lines(data: bytes) -> list[dict]:
    return [json.loads(line) for line in data.splitlines() if line]


# --- the buffer ------------------------------------------------------------


def test_a_buffer_keeps_lines_until_it_is_drained():
    buffer = AttemptBuffer(REF)
    buffer.add({"event": "one"})
    buffer.add({"event": "two"})
    assert lines(buffer.drain()) == [{"event": "one"}, {"event": "two"}]
    assert buffer.drain() is None  # nothing new


def test_a_buffer_serializes_lines_with_sorted_keys():
    """Deterministic key order, regardless of the caller's insertion order."""
    buffer = AttemptBuffer(REF)
    buffer.add({"b": 1, "a": 2})
    assert buffer.drain() == b'{"a": 2, "b": 1}\n'


def test_a_value_that_json_cannot_render_does_not_break_the_step():
    buffer = AttemptBuffer(REF)
    buffer.add({"event": "odd", "value": object()})
    line = lines(buffer.drain())[0]
    assert line["event"] == "odd"
    assert "value" in line  # default=str stringifies it; the fallback never fires
    assert "unrenderable" not in line


def test_the_fallback_fires_when_json_still_cannot_render_the_event():
    """default=str only handles values; a circular reference still raises."""
    circular: dict = {}
    circular["self"] = circular
    buffer = AttemptBuffer(REF)
    buffer.add({"event": "loopy", "bad": circular})
    assert lines(buffer.drain()) == [{"event": "loopy", "unrenderable": True}]


def test_add_uses_the_full_prospective_size_to_decide_dropping():
    """The check adds the new line's length to the running total, not subtracts it."""
    buffer = AttemptBuffer(REF, limit=0)
    buffer.add({"event": "x"})
    assert buffer.dropped == 1
    assert buffer.pending == []


def test_add_keeps_the_line_when_the_total_exactly_equals_the_limit():
    """Only strictly over the limit drops a line -- equal to it still fits."""
    probe = AttemptBuffer(REF)
    probe.add({"event": "x"})
    line = probe.pending[0]

    buffer = AttemptBuffer(REF, limit=len(line))
    buffer.add({"event": "x"})
    assert buffer.dropped == 0
    assert buffer.pending == [line]


def test_drain_accumulates_written_across_multiple_drains():
    buffer = AttemptBuffer(REF)
    buffer.add({"event": "one"})
    first = buffer.drain()
    assert buffer.written == len(first)

    buffer.add({"event": "two"})
    second = buffer.drain()
    assert buffer.written == len(first) + len(second)


def test_the_tail_is_bounded_to_tail_lines():
    buffer = AttemptBuffer(REF, limit=0)  # every line is dropped straight to the tail
    for n in range(TAIL_LINES + 50):
        buffer.add({"event": "line", "n": n})
    assert len(buffer.tail) == TAIL_LINES

    kept = [x["n"] for x in lines(buffer.final()) if x.get("event") == "line"]
    assert kept[0] == 50  # the oldest survivor once the tail wrapped
    assert kept[-1] == TAIL_LINES + 50 - 1


def test_final_does_not_inject_placeholder_bytes_when_nothing_is_pending():
    buffer = AttemptBuffer(REF, limit=0)
    buffer.add({"event": "x"})  # dropped straight to the tail, nothing pending
    assert buffer.final().startswith(b"{")


def test_past_the_limit_the_head_and_the_tail_survive_with_a_marker():
    buffer = AttemptBuffer(REF, limit=200)
    for n in range(200):
        buffer.add({"event": "line", "n": n})
    written = lines(buffer.final())

    assert written[0] == {"event": "line", "n": 0}  # the head
    marker = next(x for x in written if x["event"] == "evidence_truncated")
    assert marker["dropped"] > 100
    assert written[-1] == {"event": "line", "n": 199}  # the tail


# --- the writer ------------------------------------------------------------


async def test_collecting_writes_what_the_step_logged(port):
    writer = EvidenceWriter(port, flush_interval=0)
    async with writer.collecting(REF):
        capture(None, "info", {"event": "fetched", "n": 12})

    assert lines(await port.read_log(REF)) == [{"event": "fetched", "n": 12}]


async def test_a_step_that_logs_nothing_writes_nothing(port):
    writer = EvidenceWriter(port, flush_interval=0)
    async with writer.collecting(REF):
        pass
    assert await port.read_log(REF) == b""
    assert await port.list(REF.eid) == []


async def test_a_long_attempt_flushes_periodically(port):
    """The ticker writes what is pending so far, before the attempt ends."""
    writer = EvidenceWriter(port, flush_interval=0.02)
    async with writer.collecting(REF):
        capture(None, "info", {"event": "first"})
        await asyncio.sleep(0.2)  # give the ticker time to fire at least once
        assert await port.read_log(REF) != b""
        capture(None, "info", {"event": "second"})

    assert lines(await port.read_log(REF)) == [{"event": "first"}, {"event": "second"}]


async def test_the_processor_does_nothing_outside_an_attempt():
    event = {"event": "loose"}
    assert capture(None, "info", event) is event  # and no error


async def test_the_processor_never_raises_even_when_the_buffer_add_fails(monkeypatch, port):
    def boom(event):
        raise ValueError("boom")

    writer = EvidenceWriter(port, flush_interval=0)
    async with writer.collecting(REF) as buffer:
        monkeypatch.setattr(buffer, "add", boom)
        event = {"event": "fetched"}
        assert capture(None, "info", event) is event  # must not raise


async def test_an_evidence_failure_never_reaches_the_frame(port):
    class Broken:
        async def append_log(self, ref, part):
            raise OSError("bucket is gone")

    seen = []
    writer = EvidenceWriter(Broken(), flush_interval=0, on_error=seen.append)
    async with writer.collecting(REF):
        capture(None, "info", {"event": "fetched"})
    assert isinstance(seen[0], OSError)


async def test_null_evidence_accepts_and_stores_nothing():
    null = NullEvidence()
    await null.append_log(REF, b"x")
    assert await null.read_log(REF) == b""
    assert await null.list(REF.eid) == []
    assert await null.delete_for(REF.eid) == 0


# --- the port --------------------------------------------------------------


async def test_the_key_of_evidence_is_the_attempt_not_the_frame(port):
    await port.append_log(REF, b'{"event": "first"}\n')
    retry = EvidenceRef(REF.eid, REF.fid, 2)
    await port.append_log(retry, b'{"event": "second"}\n')

    assert lines(await port.read_log(REF)) == [{"event": "first"}]
    assert lines(await port.read_log(retry)) == [{"event": "second"}]
    items = await port.list(REF.eid)
    assert {(i.ref.attempt, i.name) for i in items} == {(1, "log"), (2, "log")}
    (first,) = [i for i in items if i.ref.attempt == 1]
    assert (first.media_type, first.size) == (keys.NDJSON, len(b'{"event": "first"}\n'))


async def test_parts_join_in_order(port):
    for n in range(12):
        await port.append_log(REF, f'{{"n": {n}}}\n'.encode())
    assert [x["n"] for x in lines(await port.read_log(REF))] == list(range(12))


async def test_get_of_the_reserved_log_name_returns_the_joined_log(port):
    await port.append_log(REF, b'{"event": "first"}\n')
    await port.append_log(REF, b'{"event": "second"}\n')
    assert await port.get(REF, "log") == await port.read_log(REF)
    assert lines(await port.get(REF, "log")) == [{"event": "first"}, {"event": "second"}]


async def test_get_of_the_reserved_log_name_is_none_when_empty(port):
    assert await port.get(REF, "log") is None


async def test_attachments_and_deletion(port):
    key = await port.put(REF, "transcript.txt", b"hello", "text/plain")
    assert key.endswith("/a/transcript.txt")
    assert await port.get(REF, "transcript.txt") == b"hello"
    assert await port.get(REF, "missing.txt") is None

    (item,) = [i for i in await port.list(REF.eid) if i.name == "transcript.txt"]
    assert (item.media_type, item.size, item.ref.fid) == ("text/plain", 5, REF.fid)

    assert await port.delete_for(REF.eid) > 0
    assert await port.list(REF.eid) == []


async def test_an_attachment_name_is_one_segment(port):
    """`log` is reserved, and a name is never a path."""
    for bad in ("../escape", "a/b", "log", "", ".hidden"):
        with pytest.raises(InvalidName):
            await port.put(REF, bad, b"x", "text/plain")


# --- the key layout ---------------------------------------------------------


def test_check_attachment_name_names_the_bad_value():
    """The message is surfaced verbatim as the API error detail (see problems.py)."""
    with pytest.raises(InvalidName, match=r"bad/name"):
        keys.check_attachment_name("bad/name")


def test_frame_segment_is_a_16_char_digest_prefix_and_the_attempt():
    """Spec 03 section 3: `{digest_text(fid)[:16]}.{attempt}`."""
    fid = "root/fetch#0"
    segment = keys.frame_segment(fid, 3)
    digest_part, _, attempt_part = segment.rpartition(".")
    assert digest_part == digest_text(fid)[:16]
    assert len(digest_part) == 16
    assert attempt_part == "3"


def test_meta_bytes_is_deterministic_key_order():
    ref = EvidenceRef(E_ABC, "root/fetch#0", 2)
    assert keys.meta_bytes(ref) == b'{"attempt": 2, "fid": "root/fetch#0"}'


# --- end to end through a workflow -----------------------------------------


async def test_a_step_leaves_its_log_as_evidence(prov):
    configure_logging("INFO", "json")
    backend = MemoryBackend()
    from flowli.domain import Actor, Site
    from flowli.runtime import Engine

    engine = Engine(backend.ports, Site.local("w-1"), registry=None)
    log = structlog.get_logger("app")

    @engine.workflow("noisy", "1")
    async def noisy(ctx):
        def work():
            log.info("looked_it_up", rows=3)
            return "ok"

        return await ctx.step(work, name="work")

    eid = await engine.start(noisy, by=Actor.human("t@example.com"))
    worker = engine.worker()
    while await worker.run_once():
        pass

    (item,) = await backend.evidence.list(eid)
    assert item.ref.fid == "root/work#0" and item.ref.attempt == 1
    events = [x["event"] for x in lines(await backend.evidence.read_log(item.ref))]
    assert "looked_it_up" in events

"""The control plane over executions. Spec 09 section 8."""

import hashlib

import pytest
from fastapi import Request

from flowli.api.deps import ApiConfig
from flowli.api.reads import (
    STALE_HEADER,
    decode_cursor,
    encode_cursor,
    etag,
    freshness,
    json_response,
)
from flowli.api.routes.executions import _written

from .conftest import READER, auth, drain

START = {"workflow": "invoice_approval", "version": "3", "args": {"invoice_id": "i-1"}}


async def test_start_returns_201_and_an_eid(client, engine):
    r = await client.post("/executions", json=START, headers=auth())
    assert r.status_code == 201
    body = r.json()
    assert body["deduplicated"] is False
    execution = await engine.execution(body["eid"])
    assert execution.workflow == "invoice_approval"
    # The actor comes from the token, never from the body.
    assert execution.created_by.actor.id == "ops@example.com"


async def test_a_dispatch_key_converges(client):
    first = await client.post(
        "/executions", json={**START, "dispatch_key": "inv-1"}, headers=auth()
    )
    second = await client.post(
        "/executions", json={**START, "dispatch_key": "inv-1"}, headers=auth()
    )
    assert first.json()["eid"] == second.json()["eid"]
    assert first.json()["deduplicated"] is False
    assert second.json()["deduplicated"] is True


async def test_bad_arguments_are_422_with_the_errors(client):
    r = await client.post(
        "/executions",
        json={"workflow": "invoice_approval", "version": "3", "args": {"amount": "not a number"}},
        headers=auth(),
    )
    assert r.status_code == 422
    body = r.json()
    assert body["code"] == "invalid_arguments"
    assert {e["loc"][0] for e in body["errors"]} == {"invoice_id", "amount"}


async def test_unknown_workflow_is_404(client):
    r = await client.post("/executions", json={**START, "version": "9"}, headers=auth())
    assert r.status_code == 404
    assert r.json()["code"] == "unknown_workflow"


async def test_read_before_and_after_the_projection_sees_it(client, projection):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]

    fresh = await client.get(f"/executions/{eid}", headers=auth(READER))
    assert fresh.status_code == 200
    assert fresh.json()["status"] == "pending"

    await projection.refresh()
    seen = await client.get(f"/executions/{eid}", headers=auth(READER))
    assert seen.json()["workflow"] == "invoice_approval"
    assert seen.json()["created_by"] == {"kind": "human", "id": "ops@example.com"}


async def test_execution_row_reports_every_projection_field(client, engine, projection):
    """08-projection.md section 3: the row holds status, workflow, version,
    queue, parent, times, epoch, worker, `suspended_on`, result or error."""
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    await drain(engine)
    await projection.refresh()

    row = (await client.get(f"/executions/{eid}", headers=auth())).json()
    assert row["status"] == "completed"
    assert row["version"] == "3"
    assert row["queue"] == "default"
    assert row["parent_eid"] is None
    assert row["parent_fid"] is None
    assert row["dispatch_key"] is None
    assert row["created_at"] and row["updated_at"]
    assert row["last_type"] == "execution.completed"
    assert row["epoch"] == 1
    assert row["suspended_on"] is None
    assert row["error"] is None
    assert row["worker_id"] == "api-test"
    assert row["host"]
    assert row["archived_at"] is None


async def test_execution_row_reports_the_error_of_a_failed_execution(
    client, engine, projection, registry
):
    """A raise in the workflow reaches the row as `error_type`/`error_message`
    (08-projection.md section 3), shaped as `{"type", "message"}` on the row."""

    @registry.workflow("blows_up", "1")
    async def blows_up(ctx):
        raise ValueError("boom")

    eid = (
        await client.post(
            "/executions", json={"workflow": "blows_up", "version": "1"}, headers=auth()
        )
    ).json()["eid"]
    await drain(engine)
    await projection.refresh()

    row = (await client.get(f"/executions/{eid}", headers=auth())).json()
    assert row["status"] == "failed"
    assert row["result"] is None
    assert row["error"] == {"type": "ValueError", "message": "boom"}


async def test_unknown_execution_is_404(client):
    r = await client.get("/executions/0199aaaa-0000-7000-8000-000000000000", headers=auth())
    assert r.status_code == 404
    body = r.json()
    assert body["code"] == "unknown_execution"
    assert body["title"] == "Unknown execution"
    assert body["detail"] == "no execution 0199aaaa-0000-7000-8000-000000000000"


async def test_a_bad_eid_is_400(client):
    r = await client.get("/executions/not-a-uuid", headers=auth())
    assert r.status_code == 400
    body = r.json()
    assert body["code"] == "invalid_name"
    assert body["title"] == "Invalid name"
    assert "not-a-uuid" in body["detail"]


async def test_list_filters_and_pages(client, engine, projection):
    for n in range(3):
        await client.post(
            "/executions",
            json={**START, "args": {"invoice_id": f"i-{n}"}},
            headers=auth(),
        )
    await projection.refresh()

    page = await client.get("/executions?limit=2", headers=auth(READER))
    body = page.json()
    assert len(body["items"]) == 2
    assert body["next_cursor"]

    rest = await client.get(
        f"/executions?limit=2&cursor={body['next_cursor']}", headers=auth(READER)
    )
    seen = {i["eid"] for i in body["items"]} | {i["eid"] for i in rest.json()["items"]}
    assert len(seen) == 3  # the pages do not overlap

    empty = await client.get("/executions?workflow=nothing", headers=auth(READER))
    assert empty.json()["items"] == []


async def test_invalid_cursor_is_400(client):
    r = await client.get("/executions?cursor=not-a-cursor", headers=auth(READER))
    assert r.status_code == 400
    body = r.json()
    assert body["code"] == "invalid_cursor"
    assert body["title"] == "Invalid cursor"


async def test_etag_gives_304_on_an_execution(client, projection):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    await projection.refresh()
    first = await client.get(f"/executions/{eid}", headers=auth())
    again = await client.get(
        f"/executions/{eid}", headers={**auth(), "If-None-Match": first.headers["etag"]}
    )
    assert again.status_code == 304
    assert again.headers["etag"] == first.headers["etag"]


async def test_journal_and_frames(client, engine, projection):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    await drain(engine)

    journal = (await client.get(f"/executions/{eid}/journal", headers=auth())).json()
    types = [e["type"] for e in journal["items"]]
    assert types[0] == "execution.started"
    assert "frame.completed" in types
    # The worker appends `execution.started`, so the actor there is the
    # worker. The human who asked for the run is on the record and on the
    # control log's `execution.created`.
    assert journal["items"][0]["actor"] == {
        "kind": "worker",
        "id": "api-test",
        "on_behalf_of": None,
    }
    # Pin the journal entry DTO's exact key names (top-level and nested),
    # so a renamed key is caught even though every value is environment-
    # dependent and can't be asserted verbatim.
    entry = journal["items"][0]
    assert set(entry.keys()) == {
        "seq",
        "type",
        "fid",
        "payload",
        "at",
        "attempt",
        "actor",
        "site",
        "code",
    }
    assert set(entry["site"].keys()) == {"host", "worker_id", "epoch"}
    assert set(entry["code"].keys()) == {
        "workflow",
        "version",
        "frame_kind",
        "frame_name",
        "code_ref",
    }

    after = await client.get(f"/executions/{eid}/journal?after={journal['tail']}", headers=auth())
    assert after.json()["items"] == []

    frames = (await client.get(f"/executions/{eid}/frames", headers=auth())).json()["root"]
    assert frames["fid"] == "root" and frames["kind"] == "root"
    assert frames["status"] == "completed"
    # Pin FrameNode.as_dict()'s exact key names, same reasoning as the
    # journal entry DTO pin above -- a renamed key must fail this.
    assert set(frames.keys()) == {
        "fid",
        "kind",
        "name",
        "status",
        "attempts",
        "started_at",
        "ended_at",
        "value",
        "error",
        "suspended_on",
        "deadline",
        "evidence",
        "children",
    }
    # The tree comes from the frame ids, which are paths.
    assert [c["name"] for c in frames["children"]] == ["settle"]
    assert frames["children"][0]["status"] == "completed"
    assert frames["children"][0]["attempts"] == 1


async def test_signal_resumes_a_waiting_execution(client, engine, projection):
    eid = (
        await client.post("/executions", json={"workflow": "waits", "version": "1"}, headers=auth())
    ).json()["eid"]
    await drain(engine)
    await projection.refresh()
    assert (await client.get(f"/executions/{eid}", headers=auth())).json()["status"] == "suspended"

    sent = await client.post(
        f"/executions/{eid}/signal", json={"channel": "go", "payload": "now"}, headers=auth()
    )
    assert sent.status_code == 200 and sent.json()["seq"] >= 1
    await drain(engine)
    await projection.refresh()

    row = (await client.get(f"/executions/{eid}", headers=auth())).json()
    assert row["status"] == "completed" and row["result"] == "now"


async def test_cancel_then_cancel_again_is_409(client, engine, projection):
    eid = (
        await client.post("/executions", json={"workflow": "waits", "version": "1"}, headers=auth())
    ).json()["eid"]
    await drain(engine)

    assert (await client.post(f"/executions/{eid}/cancel", headers=auth())).status_code == 200
    await drain(engine)
    await projection.refresh()
    assert (await client.get(f"/executions/{eid}", headers=auth())).json()["status"] == "cancelled"

    again = await client.post(f"/executions/{eid}/cancel", headers=auth())
    assert again.status_code == 409
    assert again.json()["code"] == "terminal_execution"


async def test_migrate_moves_the_version(client, engine, projection):
    eid = (
        await client.post("/executions", json={"workflow": "waits", "version": "1"}, headers=auth())
    ).json()["eid"]
    await drain(engine)

    r = await client.post(f"/executions/{eid}/migrate", json={"version": "1"}, headers=auth())
    assert r.status_code == 200 and r.json()["version"] == "1"


async def test_migrate_of_a_finished_execution_is_409(client, engine):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    await drain(engine)
    r = await client.post(f"/executions/{eid}/migrate", json={"version": "4"}, headers=auth())
    assert r.status_code == 409
    assert r.json()["code"] == "terminal_execution"


@pytest.mark.parametrize("path", ["/executions/{eid}/children", "/executions/{eid}/archive"])
async def test_children_and_archive(client, engine, path):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    await drain(engine)
    r = await client.get(path.format(eid=eid), headers=auth())
    if path.endswith("children"):
        assert r.status_code == 200 and r.json()["items"] == []
    else:
        assert r.status_code == 404  # nothing archived it yet


async def test_min_seq_waits_for_the_projection(client, projection):
    """A write says where the projection is; the next read waits for it."""
    r = await client.post("/executions", json=START, headers=auth())
    seq = int(r.headers["x-control-seq"])

    listed = await client.get(f"/executions?min_seq={seq}", headers=auth(READER))
    assert listed.status_code == 200
    assert "x-projection-stale" not in listed.headers
    assert r.json()["eid"] in {i["eid"] for i in listed.json()["items"]}


async def test_a_read_that_waits_too_long_still_answers(client, projection):
    """Stale data with a header beats a failure: the client asked to wait,
    not to fail."""
    await client.post("/executions", json=START, headers=auth())
    unreachable = 10**12
    r = await client.get(f"/executions?min_seq={unreachable}", headers=auth(READER))
    assert r.status_code == 200
    assert r.headers["x-projection-stale"] == "true"


# --- flowli.api.reads, exercised directly ---------------------------------------------


def test_cursor_round_trips_even_when_the_base64_needs_padding():
    # Chosen so the base64 (before the trailing "=" is stripped) needs
    # padding: one "=" for the first pair, two for the second.
    for value in [
        ("a", "bb"),
        ("10", "0199aaaa-0000-7000-8000-00000000000a"),
    ]:
        cursor = encode_cursor(value)
        assert "=" not in cursor
        assert decode_cursor(cursor) == value


def test_etag_treats_none_as_a_blank_distinct_from_real_content():
    assert etag("a") != etag("b")
    assert etag(None) == etag("")  # None is a designated blank, not the word "None"


def test_etag_matches_its_documented_digest():
    parts = ("executions", None, "invoice_approval", 3, "some-cursor")
    joined = "\x1f".join("" if p is None else str(p) for p in parts)
    expected = hashlib.sha256(joined.encode()).hexdigest()[:32]
    assert etag(*parts) == f'"{expected}"'


def _bare_request(if_none_match: str | None = None) -> Request:
    headers = {} if if_none_match is None else {"if-none-match": if_none_match}
    raw_headers = [(k.encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "headers": raw_headers, "method": "GET"})


def test_json_response_defaults_to_200_with_no_stale_header():
    r = json_response(_bare_request(), {"a": 1}, '"tag"')
    assert r.status_code == 200
    assert STALE_HEADER not in r.headers


def test_json_response_honors_an_explicit_status():
    r = json_response(_bare_request(), {"a": 1}, '"tag"', status=201)
    assert r.status_code == 201


async def test_freshness_skips_the_wait_when_no_min_seq_is_given():
    class ProjectionThatMustNotBeAsked:
        async def wait_for(self, seq, timeout=30.0):
            raise AssertionError("freshness must not wait when min_seq is None")

    assert await freshness(ProjectionThatMustNotBeAsked(), None, 2000) is False


async def test_freshness_converts_wait_ms_to_seconds_for_the_timeout():
    calls = []

    class FakeProjection:
        async def wait_for(self, seq, timeout=30.0):
            calls.append((seq, timeout))
            return True

    stale = await freshness(FakeProjection(), 42, 2000)
    assert stale is False
    assert calls == [(42, 2.0)]


async def test_written_reports_nothing_when_the_projection_has_no_sequence_yet():
    """section 4.4: "no sequence yet" means no header at all -- not a header
    holding the literal string "None"."""

    class ProjectionWithNoSequenceYet:
        async def refresh(self):
            return None

    assert await _written(ProjectionWithNoSequenceYet(), ApiConfig()) == {}

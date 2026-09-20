"""Evidence over HTTP. Spec 09 section 10."""

from urllib.parse import quote

from .conftest import OPERATOR, READER, RUNNER, auth, drain

DELEGATING = {"workflow": "delegates", "version": "1", "args": {"amount": 100}}
# The frame that waits for the agent (patterns.delegate, default name).
FID = "root/delegate-receive#0"


async def put_transcript(client, eid, name="transcript.txt", body=b"the agent said things"):
    return await client.put(
        f"/evidence/{eid}/attempts/1/{name}?fid={quote(FID)}&queue=agents",
        content=body,
        headers={**auth(RUNNER), "Content-Type": "text/plain"},
    )


async def test_a_consumer_puts_what_is_too_large_for_a_reply(client, engine):
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    await drain(engine)

    put = await put_transcript(client, eid)
    assert put.status_code == 201
    assert put.json()["ref"].endswith("/a/transcript.txt")

    got = await client.get(
        f"/evidence/{eid}/attempts/1/transcript.txt?fid={quote(FID)}", headers=auth(RUNNER)
    )
    assert got.status_code == 200
    assert got.content == b"the agent said things"
    # The media type put_transcript() sent, not a hardcoded guess.
    assert got.headers["content-type"].startswith("text/plain")


async def test_listing_an_execution_and_one_attempt(client, engine):
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    await put_transcript(client, eid)
    await put_transcript(client, eid, "diff.patch", b"--- a\n+++ b\n")

    listed = (await client.get(f"/evidence/{eid}", headers=auth(OPERATOR))).json()["items"]
    assert {i["name"] for i in listed} == {"transcript.txt", "diff.patch"}
    assert {i["fid"] for i in listed} == {FID}  # the meta object maps the digest back

    transcript = next(i for i in listed if i["name"] == "transcript.txt")
    assert transcript == {
        "eid": eid,
        "fid": FID,
        "attempt": 1,
        "name": "transcript.txt",
        "media_type": "text/plain",  # the Content-Type put_transcript() sent
        "size": 21,  # len(b"the agent said things")
    }

    one = await client.get(
        f"/evidence/{eid}/attempts/1?fid={quote(FID)}", headers=auth(OPERATOR)
    )
    assert {i["name"] for i in one.json()["items"]} == {"transcript.txt", "diff.patch"}


async def test_missing_evidence_is_404(client, engine):
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    r = await client.get(
        f"/evidence/{eid}/attempts/1/nothing.txt?fid={quote(FID)}", headers=auth(OPERATOR)
    )
    assert r.status_code == 404
    assert r.json()["code"] == "unknown_evidence"


async def test_reading_evidence_needs_the_capability(client, engine):
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    assert (await client.get(f"/evidence/{eid}", headers=auth(READER))).status_code == 403


async def test_putting_evidence_needs_the_queue_capability(client, engine):
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    r = await client.put(
        f"/evidence/{eid}/attempts/1/x.txt?fid={quote(FID)}&queue=finance",
        content=b"x",
        headers=auth(RUNNER),
    )
    assert r.status_code == 403  # the runner consumes `agents`, not `finance`


async def test_the_frame_tree_says_which_frames_have_evidence(client, engine):
    """The interface offers a drill-down and reads nothing until asked."""
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    await drain(engine)

    before = (await client.get(f"/executions/{eid}/frames", headers=auth(OPERATOR))).json()
    assert all(not node["evidence"] for node in _walk(before["root"]))

    await put_transcript(client, eid)
    after = (await client.get(f"/executions/{eid}/frames", headers=auth(OPERATOR))).json()
    marked = [node["fid"] for node in _walk(after["root"]) if node["evidence"]]
    assert marked == [FID]


def _walk(node):
    yield node
    for child in node["children"]:
        yield from _walk(child)

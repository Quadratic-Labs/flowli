"""The review inbox and the decision. Spec 09 section 8.5."""

from .conftest import FINANCE, OPERATOR, READER, auth, drain


async def _pending_review(client, engine, projection, amount=100):
    r = await client.post(
        "/executions",
        json={"workflow": "needs_review", "version": "1", "args": {"amount": amount}},
        headers=auth(OPERATOR),
    )
    await drain(engine)
    await projection.refresh()
    return r.json()["eid"]


async def test_the_inbox_shows_a_pending_review(client, engine, projection):
    eid = await _pending_review(client, engine, projection)
    body = (await client.get("/reviews", headers=auth(FINANCE))).json()
    (row,) = body["items"]
    assert row["queue"] == "finance"
    assert row["status"] == "pending"
    assert row["eid"] == eid
    assert row["payload"] == {"amount": 100}
    assert row["deadline"] is None
    assert row["requested_at"]
    assert row["decided_at"] is None
    assert row["verdict"] is None
    assert row["decided_by"] is None


async def test_decide_delivers_the_verdict_to_the_workflow(client, engine, projection):
    eid = await _pending_review(client, engine, projection)
    rid = (await client.get("/reviews", headers=auth(FINANCE))).json()["items"][0]["rid"]

    r = await client.post(
        f"/reviews/{rid}/decide", json={"verdict": "approve"}, headers=auth(FINANCE)
    )
    assert r.status_code == 200
    # The actor of the decision is the token holder, never a body field.
    assert r.json()["decided_by"] == {"kind": "human", "id": "cfo@example.com"}

    await drain(engine)
    await projection.refresh()
    row = (await client.get(f"/executions/{eid}", headers=auth(OPERATOR))).json()
    assert row["status"] == "completed" and row["result"] == "approve"


async def test_a_second_decision_returns_the_first(client, engine, projection):
    await _pending_review(client, engine, projection)
    rid = (await client.get("/reviews", headers=auth(FINANCE))).json()["items"][0]["rid"]

    first = await client.post(
        f"/reviews/{rid}/decide", json={"verdict": "approve"}, headers=auth(FINANCE)
    )
    second = await client.post(
        f"/reviews/{rid}/decide", json={"verdict": "reject"}, headers=auth(FINANCE)
    )
    assert second.status_code == 200
    assert second.json()["verdict"] == first.json()["verdict"] == "approve"


async def test_deciding_needs_the_queue_of_the_row(client, engine, projection):
    """The queue comes from the projection row, so a caller cannot name one
    it is allowed to decide."""
    await _pending_review(client, engine, projection)
    rid = (await client.get("/reviews", headers=auth(FINANCE))).json()["items"][0]["rid"]

    r = await client.post(
        f"/reviews/{rid}/decide", json={"verdict": "approve"}, headers=auth(READER)
    )
    assert r.status_code == 403
    assert r.json()["detail"] == "missing capability reviews:decide:finance"


async def test_unknown_review_is_404(client):
    r = await client.post(
        "/reviews/nope/decide", json={"verdict": "approve"}, headers=auth(FINANCE)
    )
    assert r.status_code == 404
    assert r.json()["code"] == "unknown_review"

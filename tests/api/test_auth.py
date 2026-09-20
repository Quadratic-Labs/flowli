"""Who may do what. Spec 09 sections 5 and 6."""

from flowlet.api.auth import Principal, _bearer
from flowlet.api.problems import Problem
from flowlet.domain import Actor

from .conftest import FINANCE, OPERATOR, READER, auth


async def test_no_token_is_401(client):
    r = await client.get("/workflows")
    assert r.status_code == 401
    body = r.json()
    assert body["code"] == "unauthenticated"
    assert body["title"] == "Unauthenticated"


async def test_unknown_token_is_401(client):
    r = await client.get("/workflows", headers=auth("nonsense"))
    assert r.status_code == 401


async def test_missing_capability_is_403(client):
    r = await client.post(
        "/executions",
        json={"workflow": "invoice_approval", "version": "3", "args": {"invoice_id": "i-1"}},
        headers=auth(READER),
    )
    assert r.status_code == 403
    body = r.json()
    assert body["code"] == "forbidden"
    assert body["title"] == "Forbidden"
    # An unscoped capability names itself alone in the message, not "cap:None".
    assert body["detail"] == "missing capability executions:start"


async def test_the_actor_in_a_body_is_refused(client):
    """Flowlet v1 read the actor from the body, so a review was self-asserted."""
    r = await client.post(
        "/executions",
        json={
            "workflow": "invoice_approval",
            "version": "3",
            "args": {"invoice_id": "i-1"},
            "by": "someone@example.com",
        },
        headers=auth(OPERATOR),
    )
    assert r.status_code == 422  # extra="forbid": the field does not exist


async def test_health_needs_no_token(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_bearer_extracts_the_token():
    assert _bearer("Bearer abc123") == "abc123"
    assert _bearer("bearer abc123") == "abc123"  # the scheme name is case-insensitive


def test_bearer_rejects_other_schemes():
    assert _bearer("Basic abc123") is None


def test_bearer_rejects_missing_or_empty_header():
    assert _bearer(None) is None
    assert _bearer("") is None


def test_bearer_rejects_bearer_with_no_token():
    assert _bearer("Bearer") is None
    assert _bearer("Bearer ") is None


def test_bearer_splits_on_the_first_space_only():
    """A token that happens to contain a space is not chopped from the end."""
    assert _bearer("Bearer a b") == "a b"


def test_scoped_capability():
    who = Principal(Actor.human("cfo@example.com"), frozenset({"reviews:decide:finance"}))
    assert who.allows("reviews:decide", "finance")
    assert not who.allows("reviews:decide", "legal")

    star = Principal(Actor.human("root@example.com"), frozenset({"reviews:decide:*"}))
    assert star.allows("reviews:decide", "legal")


async def test_on_behalf_of_reaches_provenance(client, engine):
    r = await client.post(
        "/executions",
        json={"workflow": "invoice_approval", "version": "3", "args": {"invoice_id": "i-9"}},
        headers={**auth(FINANCE if False else OPERATOR), "X-On-Behalf-Of": "cfo@example.com"},
    )
    eid = r.json()["eid"]
    execution = await engine.execution(eid)
    assert execution.created_by.actor.on_behalf_of == "cfo@example.com"
    assert execution.created_by.actor.id == "ops@example.com"


def test_problem_message_prefers_detail_over_title():
    """Problem is raised as a plain exception too, so its own message (as
    seen by str()/repr(), e.g. in a traceback) should favor the specific
    detail and only fall back to the generic title when there is no detail."""
    with_detail = Problem(400, "invalid_name", "Invalid name", "eid 'x' is bad")
    assert str(with_detail) == "eid 'x' is bad"

    without_detail = Problem(401, "unauthenticated", "Unauthenticated")
    assert str(without_detail) == "Unauthenticated"

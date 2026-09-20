"""The catalog: what a client needs to submit a workflow. Spec 09 section 7."""

import inspect

import pytest

from flowlet.api import ApiConfig, Catalog, StaticAuthenticator, create_app
from flowlet.domain import WorkflowNotRegistered
from flowlet.runtime import Registry, WorkflowRef

from .conftest import OPERATOR, READER, auth


async def test_list_workflows(client):
    r = await client.get("/workflows", headers=auth(READER))
    assert r.status_code == 200
    items = {(i["name"], i["version"]): i for i in r.json()["items"]}
    assert set(items) == {
        ("delegates", "1"),
        ("invoice_approval", "3"),
        ("needs_review", "1"),
        ("untyped", "1"),
        ("waits", "1"),
    }
    assert items[("invoice_approval", "3")]["summary"] == "Approve one invoice."
    assert items[("invoice_approval", "3")]["has_schema"] is True
    # An argument with no annotation leaves the whole workflow unchecked
    # rather than half-checked.
    assert items[("untyped", "1")]["has_schema"] is False
    # Spec 09 section 7: an item holds `queue` under that exact key.
    assert items[("invoice_approval", "3")]["queue"] == "default"


async def test_workflow_schema_skips_the_context(client):
    r = await client.get("/workflows/invoice_approval/versions/3", headers=auth(READER))
    schema = r.json()["schema"]
    assert set(schema["properties"]) == {"invoice_id", "amount"}
    assert schema["required"] == ["invoice_id"]  # `amount` has a default
    # The model name is built from the workflow's name and version, so two
    # workflows never collide even though pydantic keys schemas by name.
    assert schema["title"] == "invoice_approval_3_Args"
    assert "description" in r.json()


async def test_unknown_workflow_is_404(client):
    r = await client.get("/workflows/nope/versions/1", headers=auth(READER))
    assert r.status_code == 404
    body = r.json()
    assert body["code"] == "unknown_workflow"
    # 09-http-api.md 4.2: the problem body carries type/title/status too.
    assert body["type"] == "about:blank#unknown_workflow"
    assert body["title"] == "Unknown workflow"
    assert body["status"] == 404
    assert "nope" in body["detail"]
    assert r.headers["content-type"].startswith("application/problem+json")


def test_workflow_not_registered_carries_the_requested_name_and_version():
    """The exception exposes the (name, version) a caller asked for as
    attributes, not only inside the rendered message text."""
    with pytest.raises(WorkflowNotRegistered) as exc_info:
        Registry().get("nope", "9")
    assert exc_info.value.workflow == "nope"
    assert exc_info.value.version == "9"


def test_of_unregistered_function_is_named_by_its_own_name():
    """`Registry.of` on a plain function that was never registered raises
    `WorkflowNotRegistered` naming the function itself, with version `"?"`
    since the registry has no version to report."""

    def never_registered(ctx):
        return None

    with pytest.raises(WorkflowNotRegistered) as exc_info:
        Registry().of(never_registered)
    assert exc_info.value.workflow == "never_registered"
    assert exc_info.value.version == "?"


def test_of_unregistered_callable_without_a_name_falls_back_to_repr():
    """A callable with no `__name__` (anything other than a plain function or
    method) is identified by its `repr` instead."""

    class NoNameCallable:
        def __call__(self, ctx):
            return None

    fn = NoNameCallable()
    with pytest.raises(WorkflowNotRegistered) as exc_info:
        Registry().of(fn)
    assert exc_info.value.workflow == repr(fn)
    assert exc_info.value.version == "?"


def test_registering_the_same_name_and_version_twice_is_rejected():
    """Re-registering an already-taken (name, version) key raises `ValueError`
    naming the exact workflow and version that collided."""
    reg = Registry()

    @reg.workflow("dup", "1")
    async def first(ctx):
        return 1

    async def second(ctx):
        return 2

    with pytest.raises(ValueError, match=r"workflow 'dup' version '1' already registered"):
        reg.register(WorkflowRef("dup", "1", second))


async def test_catalog_etag_gives_304(client):
    first = await client.get("/workflows", headers=auth(OPERATOR))
    again = await client.get(
        "/workflows", headers={**auth(OPERATOR), "If-None-Match": first.headers["etag"]}
    )
    assert again.status_code == 304


async def test_catalog_uses_the_configured_default_queue(engine, projection, principals):
    """When `create_app` builds its own catalog (no catalog was passed in),
    it must build it with the configured default queue, not the `Catalog`
    class's own hardcoded one."""
    app = create_app(
        engine,
        authenticator=StaticAuthenticator(principals),
        projection=projection,
        config=ApiConfig(default_queue="agents"),
    )
    entries = app.state.flowlet.catalog.entries()
    assert entries
    assert all(e.queue == "agents" for e in entries)


def test_catalog_own_default_queue_is_default():
    """`Catalog`'s own hardcoded default (used when nothing configures it)
    is `"default"` -- distinct from, but coincidentally equal to,
    `ApiConfig`'s own default of the same value."""
    reg = Registry()

    @reg.workflow("solo", "1")
    async def solo(ctx) -> str:
        return "ok"

    catalog = Catalog(reg)
    assert catalog.default_queue == "default"
    assert catalog.entry("solo", "1").queue == "default"


def test_description_is_the_full_docstring_only_when_there_is_more_than_a_summary():
    """Spec 09 section 7: `summary` is the first line of the docstring;
    `description` is the full docstring, but only when there is more to it
    than the summary line."""
    reg = Registry()

    @reg.workflow("multiline", "1")
    async def multiline(ctx) -> str:
        """Summary line.

        Extra detail that only shows up in the description.
        """
        return "ok"

    @reg.workflow("oneline", "1")
    async def oneline(ctx) -> str:
        """Just one line."""
        return "ok"

    catalog = Catalog(reg)
    with_extra = catalog.entry("multiline", "1")
    assert with_extra.summary == "Summary line."
    assert with_extra.description == inspect.getdoc(multiline)

    summary_only = catalog.entry("oneline", "1")
    assert summary_only.summary == "Just one line."
    assert summary_only.description is None

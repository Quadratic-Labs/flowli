"""`/executions`. See specs/09-http-api.md section 8."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from flowli.domain import Eid, ExecutionStatus, TaskKind, parse_eid
from flowli.runtime.engine import UnknownExecution

from .. import frames as frametree
from ..auth import (
    EXECUTIONS_CANCEL,
    EXECUTIONS_MIGRATE,
    EXECUTIONS_READ,
    EXECUTIONS_SIGNAL,
    EXECUTIONS_START,
    Principal,
)
from ..deps import CatalogDep, Config, Engine, Limit, Projection, require
from ..dto import (
    MigrateRequest,
    SignalRequest,
    StartRequest,
    execution_record,
    execution_row,
    journal_entry,
)
from ..problems import Problem, unknown_execution
from ..reads import SEQ_HEADER, decode_cursor, encode_cursor, etag, freshness, json_response

router = APIRouter(tags=["executions"])

Reader = Annotated[Principal, Depends(require(EXECUTIONS_READ))]
MinSeq = Annotated[int | None, Query(ge=0)]
TERMINAL = (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED)


def _eid(value: str) -> Eid:
    return parse_eid(value)  # InvalidName -> 400


async def _written(projection: Any, config: Config) -> dict[str, str]:
    """The header that says where the projection is (section 4.4).

    The engine's write methods return the result of the write, not the
    sequence of the announcement they made. The service catches the
    projection up instead and reports where it got to, so a client that sends
    the value back as `min_seq` waits for nothing.
    """
    if not config.refresh_after_write:
        return {}
    seq = await projection.refresh()
    return {} if seq is None else {SEQ_HEADER: str(seq)}


# --- writes ---------------------------------------------------------------------------


@router.post("/executions", status_code=201)
async def start_execution(
    body: StartRequest,
    engine: Engine,
    projection: Projection,
    catalog: CatalogDep,
    config: Config,
    who: Annotated[Principal, Depends(require(EXECUTIONS_START))],
) -> Response:
    ref = engine.registry.get(body.workflow, body.version)  # WorkflowNotRegistered -> 404
    try:
        args = catalog.validate(body.workflow, body.version, body.args)
    except ValidationError as exc:
        raise Problem(
            422,
            "invalid_arguments",
            "Invalid arguments",
            f"{body.workflow} version {body.version}",
            errors=exc.errors(include_url=False),
        ) from None

    result = await engine.start_result(
        ref.fn, key=body.dispatch_key, queue=body.queue, by=who.actor, **args
    )
    return JSONResponse(
        {"eid": str(result.eid), "deduplicated": result.deduplicated},
        status_code=201,
        headers=await _written(projection, config),
    )


@router.post("/executions/{eid}/signal")
async def signal_execution(
    eid: str,
    body: SignalRequest,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Annotated[Principal, Depends(require(EXECUTIONS_SIGNAL))],
) -> Response:
    parsed = _eid(eid)
    await engine.execution(parsed)  # a signal to an unknown execution is a 404
    seq = await engine.signal(
        parsed, body.channel, body.payload, by=who.actor, correlation=body.correlation
    )
    return JSONResponse({"seq": seq}, headers=await _written(projection, config))


@router.post("/executions/{eid}/cancel")
async def cancel_execution(
    eid: str,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Annotated[Principal, Depends(require(EXECUTIONS_CANCEL))],
) -> Response:
    parsed = _eid(eid)
    status = await engine.status(parsed)  # UnknownExecution -> 404
    if status in TERMINAL:
        raise Problem(409, "terminal_execution", "Execution is terminal", f"status {status.value}")
    await engine.cancel(parsed, by=who.actor)
    headers = await _written(projection, config)
    asked = await _cancel_delegates(engine, projection, parsed, who)
    return JSONResponse(
        {"eid": eid, "cancel_requested": True, "tasks_cancelled": asked}, headers=headers
    )


async def _cancel_delegates(engine: Any, projection: Any, eid: Eid, who: Principal) -> list[str]:
    """Ask the consumer of every delegate task of this execution to stop.

    A consumer outside the engine does not watch the execution lease, so a
    cancel must reach its task too (`10-agent-runner.md`, section 5). The
    engine cannot do this itself: the journal memo of an enqueue is the task
    id and not its queue. The `tasks` table of the projection holds the pair
    (`05-protocols.md`, section 8).
    """
    prov = engine.provenance(who.actor, frame_name="cancel")
    asked = []
    for row in projection.tasks(
        eid=eid, kind=TaskKind.DELEGATE.value, limit=100
    ):  # pragma: no mutate
        # A task that was answered and acked is gone already; the adapters
        # treat a request for it as a no-op.
        await engine.ports.queue.request_cancel(row.queue, row.task_id, prov)
        asked.append(row.task_id)
    return asked


@router.post("/executions/{eid}/migrate")
async def migrate_execution(
    eid: str,
    body: MigrateRequest,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Annotated[Principal, Depends(require(EXECUTIONS_MIGRATE))],
) -> Response:
    parsed = _eid(eid)
    try:
        execution = await engine.migrate(parsed, body.version, by=who.actor)
    except UnknownExecution:
        raise
    except ValueError as exc:
        raise Problem(409, "terminal_execution", "Execution is terminal", str(exc)) from None
    return JSONResponse(
        {"eid": eid, "version": execution.version}, headers=await _written(projection, config)
    )


# --- reads ----------------------------------------------------------------------------


@router.get("/executions")
async def list_executions(
    request: Request,
    projection: Projection,
    config: Config,
    limit: Limit,
    who: Reader,
    status: Annotated[str | None, Query()] = None,
    workflow: Annotated[str | None, Query()] = None,
    parent_eid: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    min_seq: MinSeq = None,
) -> Response:
    stale = await freshness(projection, min_seq, config.wait_ms)
    rows = projection.executions(
        status=None if status is None else ExecutionStatus(status),
        workflow=workflow,
        parent_eid=parent_eid,
        limit=limit,
        after=decode_cursor(cursor),
    )
    items = [execution_row(r) for r in rows]
    next_cursor = (
        encode_cursor((rows[-1].updated_at, str(rows[-1].eid))) if len(rows) == limit else None
    )
    tag = etag(
        "executions",
        status,
        workflow,
        parent_eid,
        cursor,
        limit,
        len(items),
        *(f"{i['eid']}@{i['updated_at']}" for i in items),
    )
    return json_response(request, {"items": items, "next_cursor": next_cursor}, tag, stale=stale)


@router.get("/executions/{eid}")
async def get_execution(
    request: Request,
    eid: str,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Reader,
    min_seq: MinSeq = None,
) -> Response:
    parsed = _eid(eid)
    stale = await freshness(projection, min_seq, config.wait_ms)
    row = projection.execution(parsed)
    if row is not None:
        tag = etag("execution", eid, row.updated_at, row.last_type)
        return json_response(request, execution_row(row), tag, stale=stale)
    # The window between a write and the next refresh: fold the log instead.
    execution = await engine.execution(parsed)  # UnknownExecution -> 404
    status = await engine.status(parsed)
    tag = etag("execution", eid, "live", status.value)
    return json_response(request, execution_record(execution, status), tag, stale=stale)


@router.get("/executions/{eid}/journal")
async def get_journal(
    request: Request,
    eid: str,
    engine: Engine,
    who: Reader,
    after: Annotated[int, Query(ge=0)] = 0,
) -> Response:
    parsed = _eid(eid)
    entries = await engine.journal(parsed)
    if not entries:
        await engine.execution(parsed)  # an empty journal is not an unknown execution
    tail = entries[-1].seq if entries else 0
    items = [journal_entry(s) for s in entries if s.seq > after]
    return json_response(request, {"items": items, "tail": tail}, etag("journal", eid, tail, after))


@router.get("/executions/{eid}/frames")
async def get_frames(request: Request, eid: str, engine: Engine, who: Reader) -> Response:
    parsed = _eid(eid)
    entries = await engine.journal(parsed)
    if not entries:
        await engine.execution(parsed)
    evidence = {i.ref.fid for i in await engine.ports.evidence.list(parsed)}
    root = frametree.build(entries, evidence)
    tail = entries[-1].seq if entries else 0
    body = {"root": None if root is None else root.as_dict(), "tail": tail}
    return json_response(request, body, etag("frames", eid, tail, len(evidence)))


@router.get("/executions/{eid}/children")
async def get_children(request: Request, eid: str, projection: Projection, who: Reader) -> Response:
    rows = projection.children(_eid(eid))
    items = [execution_row(r) for r in rows]
    tag = etag("children", eid, len(items), *(f"{i['eid']}@{i['updated_at']}" for i in items))
    return json_response(request, {"items": items, "next_cursor": None}, tag)


@router.get("/executions/{eid}/archive")
async def get_archive(request: Request, eid: str, engine: Engine, who: Reader) -> Response:
    data = await engine.archive(_eid(eid))
    if data is None:
        raise unknown_execution(eid)
    return json_response(request, data, etag("archive", eid, data.get("archived_at")))

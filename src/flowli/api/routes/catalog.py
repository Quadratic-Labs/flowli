"""`/workflows`. See specs/09-http-api.md section 7."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from ..auth import WORKFLOWS_READ, Principal
from ..deps import CatalogDep, require
from ..reads import etag, json_response

router = APIRouter(tags=["catalog"])

Reader = Annotated[Principal, Depends(require(WORKFLOWS_READ))]


@router.get("/workflows")
def list_workflows(request: Request, catalog: CatalogDep, who: Reader) -> Response:
    items = [entry.summary_dict() for entry in catalog.entries()]
    tag = etag("workflows", len(items), *(f"{i['name']}@{i['version']}" for i in items))
    return json_response(request, {"items": items, "next_cursor": None}, tag)


@router.get("/workflows/{name}/versions/{version}")
def get_workflow(
    request: Request, name: str, version: str, catalog: CatalogDep, who: Reader
) -> Response:
    entry = catalog.entry(name, version)  # WorkflowNotRegistered -> 404
    body = entry.detail_dict()
    return json_response(request, body, etag("workflow", name, version, entry.has_schema))

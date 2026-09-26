"""The workflow catalog. See specs/09-http-api.md section 7.

This is what is left of Flowli v1's `taskflow` package: a JSON Schema
derived from the signature of a workflow function, plus the defaults a start
uses. It holds no state and reads the `Registry` of this process.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, get_type_hints

from pydantic import ValidationError, create_model

from flowli.runtime.registry import Registry


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    name: str
    version: str
    summary: str | None
    description: str | None
    queue: str
    schema: dict[str, Any] | None

    @property
    def has_schema(self) -> bool:
        return self.schema is not None

    def summary_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "summary": self.summary,
            "queue": self.queue,
            "has_schema": self.has_schema,
        }

    def detail_dict(self) -> dict[str, Any]:
        return {**self.summary_dict(), "description": self.description, "schema": self.schema}


def _docstring(fn: Any) -> tuple[str | None, str | None]:
    doc = inspect.getdoc(fn)
    if not doc:
        return None, None
    summary, _, rest = doc.partition("\n")
    return summary.strip() or None, doc if rest.strip() else None


def _model(name: str, fn: Any) -> Any | None:
    """A pydantic model of the arguments, or None when they cannot be typed.

    The first parameter is the `Context` and is skipped. A parameter with no
    usable annotation makes the whole schema unavailable: a half-checked
    argument list is worse than an unchecked one, because it looks checked.
    """
    try:
        hints = get_type_hints(fn)
    except Exception:
        return None
    parameters = list(inspect.signature(fn).parameters.values())[1:]
    fields: dict[str, Any] = {}
    for p in parameters:
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            return None
        annotation = hints.get(p.name)
        if annotation is None:
            return None
        default = ... if p.default is inspect.Parameter.empty else p.default
        fields[p.name] = (annotation, default)
    try:
        return create_model(f"{name}_Args", **fields)
    except Exception:
        return None


class Catalog:
    """Every registered workflow, with its argument schema.

    Built once, at startup: a schema never changes while the process runs.
    """

    def __init__(self, registry: Registry, *, default_queue: str = "default") -> None:
        self.registry = registry
        self.default_queue = default_queue
        self._entries: dict[tuple[str, str], CatalogEntry] = {}
        self._models: dict[tuple[str, str], Any] = {}
        self._build()

    def _build(self) -> None:
        for ref in self.registry.entries():
            name, version = ref.name, ref.version
            summary, description = _docstring(ref.fn)
            model = _model(f"{name}_{version}", ref.fn)
            if model is not None:
                self._models[(name, version)] = model
            self._entries[(name, version)] = CatalogEntry(
                name=name,
                version=version,
                summary=summary,
                description=description,
                queue=self.default_queue,
                schema=None if model is None else model.model_json_schema(),
            )

    def entries(self) -> list[CatalogEntry]:
        return list(self._entries.values())

    def entry(self, name: str, version: str) -> CatalogEntry:
        ref = self.registry.get(name, version)  # raises WorkflowNotRegistered
        return self._entries[(ref.name, ref.version)]

    def validate(self, name: str, version: str, args: dict[str, Any]) -> dict[str, Any]:
        """The arguments, checked against the schema. Unchecked when there is none."""
        model = self._models.get((name, version))
        if model is None:
            return args
        return dict(model(**args).model_dump())


__all__ = ["Catalog", "CatalogEntry", "ValidationError"]

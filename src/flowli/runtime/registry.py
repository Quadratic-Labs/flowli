"""Workflow registry: (name, version) -> WorkflowRef, and fn -> WorkflowRef."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from flowli.domain import WorkflowNotRegistered

from .context import WorkflowRef


class Registry:
    def __init__(self) -> None:
        self._by_key: dict[tuple[str, str], WorkflowRef] = {}
        self._by_fn: dict[Callable[..., Any], WorkflowRef] = {}

    def workflow(
        self, name: str, version: str
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.register(WorkflowRef(name, version, fn))
            return fn

        return decorate

    def register(self, ref: WorkflowRef) -> None:
        key = (ref.name, ref.version)
        if key in self._by_key:
            raise ValueError(f"workflow {ref.name!r} version {ref.version!r} already registered")
        self._by_key[key] = ref
        self._by_fn[ref.fn] = ref

    def entries(self) -> list[WorkflowRef]:
        """Every registered workflow, ordered by name and version."""
        return [self._by_key[key] for key in sorted(self._by_key)]

    def get(self, name: str, version: str) -> WorkflowRef:
        try:
            return self._by_key[(name, version)]
        except KeyError:
            raise WorkflowNotRegistered(name, version) from None

    def of(self, fn: Callable[..., Any]) -> WorkflowRef:
        try:
            return self._by_fn[fn]
        except KeyError:
            raise WorkflowNotRegistered(getattr(fn, "__name__", repr(fn)), "?") from None

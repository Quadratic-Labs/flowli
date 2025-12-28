from typing import Callable, Literal, Protocol

from .tracker import TrackerProtocol
from ..types import SpanType


class ExecutorProtocol(Protocol):
    tracker: TrackerProtocol

    def executable(
        self,
        fn: Callable,
        name: str,
        typ: SpanType,
    ) -> Callable:
        """Decorator that injects the flowlet execution logic around a function."""
        ...
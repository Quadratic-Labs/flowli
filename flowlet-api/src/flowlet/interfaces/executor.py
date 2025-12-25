from typing import Callable, Literal, Protocol

from .tracker import TrackerProtocol


class ExecutorProtocol(Protocol):
    tracker: TrackerProtocol

    def executable(
        self,
        fn: Callable,
        name: str,
        typ: Literal["task"] | Literal["flow"],
    ) -> Callable:
        """Decorator that injects the flowlet execution logic around a function."""
        ...
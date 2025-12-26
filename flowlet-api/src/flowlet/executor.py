import functools
from typing import Callable, Literal
from uuid import uuid7

from .interfaces.executor import ExecutorProtocol
from .interfaces.tracker import TrackerProtocol


class ExecutorInProcess(ExecutorProtocol):
    def __init__(
        self,
        tracker: TrackerProtocol,
        **_,
    ):
        self.tracker = tracker

    def executable(
        self,
        fn: Callable,
        name: str,
        typ: Literal["task"] | Literal["flow"],
    ) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            run_id = None
            # if not self.tracker.context_manager.is_active():
                # self.tracker.logger.info(f"Begin Run {run_id}")
            with self.tracker.context_manager.begin_span(name, typ):
                if run_id is None:
                    root = self.tracker.context_manager.get_root_span()
                    if root is not None:
                        run_id = root.run_id
                self.tracker.logger.info(f"Starting")
                try:
                    result = fn(*args, **kwargs)
                except Exception as err:
                    self.tracker.logger.exception("Error", err)
                    raise err
                else:
                    self.tracker.logger.success("Success")
            if not self.tracker.context_manager.is_active() and run_id:
                self.tracker.flush_run(str(run_id))
            return result
        return wrapper

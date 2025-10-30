import functools
from typing import Callable, Dict

from .context import FlowContext, TaskContext


class FlowRegister:
    def __init__(self, *, db_session_factory, **_):
        self.db_session_factory = db_session_factory
        self.flows: Dict[str, Callable] = {}
        self.tasks: Dict[str, Callable] = {}

    def list_flows(self):
        return list(self.flows.keys())

    def list_tasks(self):
        return list(self.tasks.keys())

    def flow(self, name: str | None=None):
        """
        Decorator to register a function as a flow.
        The decorated function should call task functions (or plain functions).
        """
        def _decorator(fn: Callable):
            flow_name = name or fn.__name__
            if flow_name in self.list_flows():
                raise ValueError(f"Flow {flow_name!r} already registered")
            tracker = FlowContext(flow_name, db_session_factory=self.db_session_factory)

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                with tracker:
                    result = fn(*args, **kwargs)
                return result

            wrapper.__flow_name__ = flow_name  # type: ignore
            self.flows[flow_name] = wrapper
            return wrapper

        return _decorator

    def task(self, name: str | None=None):
        """
        Decorator to wrap a task function so it logs start/finish/exceptions to DB
        tied to the current flow run (via contextvar).

        Now uses TaskThreadExecutor to separate execution logic from definition.
        """
        def _decorator(fn: Callable):
            task_name = name or fn.__name__
            if task_name in self.list_tasks():
                raise ValueError(f"Task {task_name!r} already registered")
            tracker = TaskContext(task_name, db_session_factory=self.db_session_factory)

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                with tracker:
                    return fn(*args, **kwargs)

            wrapper.__task_name__ = task_name  # type: ignore
            self.tasks[task_name] = wrapper
            return wrapper

        return _decorator
from functools import wraps
from typing import Callable, Dict

from .context import FlowContext, TaskContext
from .database import get_db_session_factory


class FlowManager():
    _FLOWS: Dict[str, Callable] = {}

    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory

    def __getitem__(self, key):
        return self._FLOWS[key]

    def __setitem__(self, key, value):
        self._FLOWS[key] = value

    def __delitem__(self, key):
        del self._FLOWS[key]

    def __contains__(self, key):
        return key in self._FLOWS

    def keys(self):
        return self._FLOWS.keys()

    def values(self):
        return self._FLOWS.values()

    def items(self):
        return self._FLOWS.items()

    def flow(self, name: str | None=None):
        """
        Decorator to register a function as a flow.
        The decorated function should call task functions (or plain functions).
        """
        def _decorator(fn: Callable):
            flow_name = name or fn.__name__
            if flow_name in self._FLOWS:
                raise RuntimeError(f"Flow {flow_name!r} already registered")
            self._FLOWS[flow_name] = fn
            tracker = FlowContext(flow_name, self.db_session_factory)

            @wraps(fn)
            def wrapper(*args, **kwargs):
                with tracker:
                    result = fn(*args, **kwargs)
                return result

            wrapper.__flow_name__ = flow_name
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
            tracker = TaskContext(task_name, self.db_session_factory)

            @wraps(fn)
            def wrapper(*args, **kwargs):
                with tracker:
                    return fn(*args, **kwargs)

            wrapper.__task_name__ = task_name
            return wrapper

        return _decorator
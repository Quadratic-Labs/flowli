"""
Flowlet - A lightweight flow orchestration framework

Flowlet provides a simple way to define and orchestrate workflows.

Defining flows:
    >>> from flowlet import configure
    >>>
    >>> flowlet = configure({"database": {"url": "postgresql://localhost/mydb"}})
    >>>
    >>> @flowlet.task()
    >>> def fetch_data():
    ...     return {"data": [1, 2, 3]}
    >>>
    >>> @flowlet.flow()
    >>> def my_workflow():
    ...     data = fetch_data()
    ...     return data

FastAPI Integration:
    >>> from flowlet import create_router
    >>> from fastapi import FastAPI
    >>>
    >>> flowlet = configure({"database": {"url": "postgresql://localhost/mydb"}})
    >>>
    >>> app = FastAPI()
    >>> app.include_router(flowlet.get_router())

Structured Logging:
    >>> from flowlet import configure
    >>> from flowlet.logging_manager import initialize_logging
    >>>
    >>> flowlet = configure({"database": {"url": "postgresql://localhost/mydb"}})
    >>> initialize_logging(logs_dir="./logs", runs_dir="./runs")
    >>>
    >>> # All logs will now have automatic context injection
"""
from .core import Flowlet, configure

__all__ = [
    "Flowlet",
    "configure",
]

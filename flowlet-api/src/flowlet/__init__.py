"""
Flowlet - A lightweight flow orchestration framework

Flowlet provides a simple way to define and orchestrate workflows.

Defining flows:
    >>> from flowlet import configure
    >>>
    >>> flowlet = configure({"storage": {"type": "filesystem", "base_path": "./storage"}})
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
    >>> flowlet = configure({"storage": {"type": "sqlite", "database_path": "./flowlet.db"}})
    >>>
    >>> app = FastAPI()
    >>> app.include_router(flowlet.get_router())

Storage Configuration:
    >>> from flowlet import configure, FilesystemStorageConfig, AzureBlobStorageConfig, SQLiteStorageConfig
    >>>
    >>> # Filesystem storage
    >>> flowlet = configure({"storage": {"type": "filesystem", "base_path": "./storage"}})
    >>>
    >>> # Azure Blob storage
    >>> flowlet = configure({"storage": {
    ...     "type": "azure_blob",
    ...     "connection_string": "...",
    ...     "container_name": "logs"
    ... }})
    >>>
    >>> # SQLite storage
    >>> flowlet = configure({"storage": {"type": "sqlite", "database_path": "./flowlet.db"}})
"""
from .core import Flowlet, configure
from .config import FlowletConfig

__all__ = [
    "Flowlet",
    "configure",
    "FlowletConfig",
]

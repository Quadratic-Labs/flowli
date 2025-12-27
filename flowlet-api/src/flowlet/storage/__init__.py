"""
Persistence layer for Flowlet.

Provides file-like interfaces and path abstractions for various storage backends.
"""

from .azure import (
    AzureBlobFile,
    AzureBlobPath,
    DEFAULT_CHUNK_SIZE,
    MAX_APPEND_BLOCK_SIZE,
    AzureBlobHandler,
    AzureBlobStreamHandler,
)
from .filesystem.logging import FilesystemHandler
from .sqlite_logging import SQLiteHandler, SQLiteRunHandler

__all__ = [
    'AzureBlobFile',
    'AzureBlobPath',
    'DEFAULT_CHUNK_SIZE',
    'MAX_APPEND_BLOCK_SIZE',
    'AzureBlobHandler',
    'AzureBlobStreamHandler',
    'FilesystemHandler',
    'SQLiteHandler',
    'SQLiteRunHandler',
]

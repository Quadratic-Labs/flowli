"""
Persistence layer for Flowlet.

Provides file-like interfaces and path abstractions for various storage backends.
"""

from .azure import AzureBlobFile, open_azure_blob, DEFAULT_CHUNK_SIZE, MAX_APPEND_BLOCK_SIZE
from .azure_path import AzureBlobPath
from .azure_logging import AzureBlobHandler, AzureBlobStreamHandler
from .filesystem_logging import FilesystemHandler, FilesystemRunHandler
from .sqlite_logging import SQLiteHandler, SQLiteRunHandler

__all__ = [
    'AzureBlobFile',
    'open_azure_blob',
    'AzureBlobPath',
    'DEFAULT_CHUNK_SIZE',
    'MAX_APPEND_BLOCK_SIZE',
    'AzureBlobHandler',
    'AzureBlobStreamHandler',
    'FilesystemHandler',
    'FilesystemRunHandler',
    'SQLiteHandler',
    'SQLiteRunHandler',
]

"""
Azure Blob Storage backend for Flowlet.

Provides file-like interfaces, path abstractions, and logging handlers
for Azure Blob Storage.
"""

from .file import AzureBlobFile, open_azure_blob, DEFAULT_CHUNK_SIZE, MAX_APPEND_BLOCK_SIZE
from .path import AzureBlobPath
from .logging import AzureBlobHandler, AzureBlobStreamHandler

__all__ = [
    'AzureBlobFile',
    'open_azure_blob',
    'AzureBlobPath',
    'DEFAULT_CHUNK_SIZE',
    'MAX_APPEND_BLOCK_SIZE',
    'AzureBlobHandler',
    'AzureBlobStreamHandler',
]

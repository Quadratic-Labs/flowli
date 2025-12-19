"""
Persistence layer for Flowlet.

Provides file-like interfaces and path abstractions for various storage backends.
"""

from .azure import AzureBlobFile, open_azure_blob, DEFAULT_CHUNK_SIZE, MAX_APPEND_BLOCK_SIZE
from .azure_path import AzureBlobPath

__all__ = [
    'AzureBlobFile',
    'open_azure_blob',
    'AzureBlobPath',
    'DEFAULT_CHUNK_SIZE',
    'MAX_APPEND_BLOCK_SIZE',
]

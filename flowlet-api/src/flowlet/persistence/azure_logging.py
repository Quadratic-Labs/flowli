"""
Azure Blob Storage logging handler.

Provides a Python logging Handler that exports log records to Azure Blob Storage
using appendable blobs (AppendBlob) for efficient log streaming.
"""

import logging
import threading
from typing import Optional, TYPE_CHECKING, Any

from .azure import AzureBlobFile

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient


class AzureBlobHandler(logging.Handler):
    """
    Logging handler that writes logs to Azure Blob Storage using AppendBlob.

    This handler efficiently appends log records to an Azure AppendBlob,
    making it ideal for centralized log collection and long-term storage.

    Features:
    - Uses AppendBlob for efficient appending without read-modify-write cycles
    - Thread-safe log writing with locks
    - Automatic blob creation if it doesn't exist
    - Supports custom formatters (JSON, text, etc.)
    - Buffered writes with configurable flush behavior

    Example:
        # Basic usage with JSON formatting
        from flowlet.persistence.azure_logging import AzureBlobHandler
        from flowlet.logging import JSONFormatter

        handler = AzureBlobHandler(
            connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
            container_name='logs',
            blob_name='app.log'
        )
        handler.setFormatter(JSONFormatter())

        logger = logging.getLogger('myapp')
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        logger.info('Application started')  # Written to Azure blob

        # Clean up
        handler.close()

    Example with path-based blob names:
        # Organize logs by date and application
        import datetime
        today = datetime.date.today().isoformat()

        handler = AzureBlobHandler(
            connection_string=conn_str,
            container_name='logs',
            blob_name=f'myapp/{today}/app.log'
        )

    Example with auto-flush:
        # Flush after every log record (less efficient but more real-time)
        handler = AzureBlobHandler(
            connection_string=conn_str,
            container_name='logs',
            blob_name='realtime.log',
            auto_flush=True
        )
    """

    def __init__(
        self,
        connection_string: str,
        container_name: str,
        blob_name: str,
        level: int = logging.NOTSET,
        blob_service_client: Optional['BlobServiceClient'] = None,
        auto_flush: bool = False,
        encoding: str = 'utf-8'
    ):
        """
        Initialize Azure Blob Storage logging handler.

        Args:
            connection_string: Azure Storage connection string
            container_name: Name of the blob container for logs
            blob_name: Name/path of the log blob (will be created as AppendBlob)
            level: Minimum log level to handle (default: NOTSET)
            blob_service_client: Optional pre-configured BlobServiceClient for shared connections
            auto_flush: If True, flush after every emit (default: False for better performance)
            encoding: Text encoding for log messages (default: 'utf-8')
        """
        super().__init__(level)

        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_name = blob_name
        self.blob_service_client = blob_service_client
        self.auto_flush = auto_flush
        self.encoding = encoding

        # Thread safety
        self._lock = threading.Lock()

        # Azure blob file handle
        self._blob_file: Optional[AzureBlobFile] = None
        self._closed = False

        # Initialize the blob file in append mode
        self._init_blob_file()

    def _init_blob_file(self) -> None:
        """Initialize the Azure blob file in append mode."""
        try:
            self._blob_file = AzureBlobFile(
                connection_string=self.connection_string,
                container_name=self.container_name,
                blob_name=self.blob_name,
                mode='a',  # Append mode - creates AppendBlob if doesn't exist
                encoding=self.encoding,
                blob_service_client=self.blob_service_client
            )
        except Exception as e:
            # Log initialization error but don't crash
            self.handleError(None)  # type: ignore
            raise RuntimeError(f"Failed to initialize Azure blob file: {e}") from e

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the Azure blob.

        Args:
            record: The log record to emit
        """
        if self._closed:
            return

        try:
            # Format the log record
            msg = self.format(record)

            # Ensure message ends with newline
            if not msg.endswith('\n'):
                msg += '\n'

            # Thread-safe write to blob
            with self._lock:
                if self._blob_file and not self._blob_file.closed:
                    self._blob_file.write(msg)

                    # Auto-flush if configured
                    if self.auto_flush:
                        self._blob_file.flush()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """
        Flush any buffered log records to Azure Blob Storage.

        This forces any pending writes to be uploaded to Azure.
        """
        if self._closed:
            return

        try:
            with self._lock:
                if self._blob_file and not self._blob_file.closed:
                    self._blob_file.flush()
        except Exception:
            self.handleError(None)  # type: ignore

    def close(self) -> None:
        """
        Close the handler and upload any pending log data.

        This should be called when the application shuts down to ensure
        all logs are written to Azure.
        """
        if self._closed:
            return

        try:
            with self._lock:
                if self._blob_file and not self._blob_file.closed:
                    self._blob_file.close()
                self._closed = True
        except Exception:
            self.handleError(None)  # type: ignore
        finally:
            super().close()

    def __enter__(self) -> 'AzureBlobHandler':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        _ = exc_type, exc_val, exc_tb
        self.close()

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed

    @property
    def is_append_blob(self) -> bool:
        """Check if using native AppendBlob operations."""
        if self._blob_file:
            return self._blob_file.is_append_blob
        return False

    @property
    def is_degraded_append_mode(self) -> bool:
        """Check if using degraded append mode (less efficient)."""
        if self._blob_file:
            return self._blob_file.is_degraded_append_mode
        return False


class AzureBlobStreamHandler(AzureBlobHandler):
    """
    Variant of AzureBlobHandler with auto-flush enabled by default.

    This is useful for real-time log streaming where you want logs
    to appear in Azure immediately, at the cost of some performance.

    Example:
        handler = AzureBlobStreamHandler(
            connection_string=conn_str,
            container_name='logs',
            blob_name='realtime.log'
        )
        # Each log record is immediately flushed to Azure
    """

    def __init__(
        self,
        connection_string: str,
        container_name: str,
        blob_name: str,
        level: int = logging.NOTSET,
        blob_service_client: Optional['BlobServiceClient'] = None,
        encoding: str = 'utf-8'
    ):
        """
        Initialize Azure Blob Storage streaming handler with auto-flush.

        Args:
            connection_string: Azure Storage connection string
            container_name: Name of the blob container for logs
            blob_name: Name/path of the log blob
            level: Minimum log level to handle
            blob_service_client: Optional pre-configured BlobServiceClient
            encoding: Text encoding for log messages
        """
        super().__init__(
            connection_string=connection_string,
            container_name=container_name,
            blob_name=blob_name,
            level=level,
            blob_service_client=blob_service_client,
            auto_flush=True,  # Always auto-flush for streaming
            encoding=encoding
        )

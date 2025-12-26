"""
SQLite logging handler for Flowlet.

Provides Python logging Handlers that export log records to a SQLite database.
"""

import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional


class SQLiteHandler(logging.Handler):
    """
    Logging handler that writes logs to a SQLite database.

    This handler stores log records in a SQLite table with proper
    indexing for efficient querying.

    Features:
    - Automatic table creation
    - Thread-safe database writes with locks
    - Indexes on run_id and timestamp for fast queries
    - Stores logs as JSON in a structured format

    Example:
        from flowlet.persistence.sqlite_logging import SQLiteHandler
        from flowlet.logging import JSONFormatter

        handler = SQLiteHandler(
            database_path='./flowlet.db',
            table_name='logs'
        )
        handler.setFormatter(JSONFormatter())

        logger = logging.getLogger('flowlet')
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    """

    def __init__(
        self,
        database_path: str,
        table_name: str = 'logs',
        level: int = logging.NOTSET
    ):
        """
        Initialize SQLite logging handler.

        Args:
            database_path: Path to the SQLite database file
            table_name: Name of the table to store logs
            level: Minimum log level to handle (default: NOTSET)
        """
        super().__init__(level)

        self.database_path = Path(database_path)
        self.table_name = table_name

        # Thread safety
        self._lock = threading.Lock()
        self._connection: Optional[sqlite3.Connection] = None
        self._closed = False

        # Initialize database and table
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the database and create the logs table if it doesn't exist."""
        # Ensure parent directory exists
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect and create table
        with self._lock:
            self._connection = sqlite3.connect(str(self.database_path), check_same_thread=False)
            cursor = self._connection.cursor()

            # Create logs table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    flow_name TEXT,
                    span_id TEXT,
                    parent_span_id TEXT,
                    span_type TEXT,
                    span_name TEXT,
                    timestamp TEXT,
                    level TEXT,
                    message TEXT,
                    log_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for efficient querying
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_run_id
                ON {self.table_name}(run_id)
            """)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp
                ON {self.table_name}(timestamp)
            """)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_span_id
                ON {self.table_name}(span_id)
            """)

            self._connection.commit()

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the SQLite database.

        Args:
            record: The log record to emit
        """
        if self._closed or not self._connection:
            return

        try:
            # Format the log record (should be JSON)
            msg = self.format(record)

            # Parse JSON to extract fields
            try:
                log_data = json.loads(msg)
            except json.JSONDecodeError:
                # If not JSON, store as-is
                log_data = {"message": msg}

            # Extract fields
            run_id = log_data.get('run_id')
            flow_name = log_data.get('flow_name')
            span_id = log_data.get('span_id')
            parent_span_id = log_data.get('parent_span_id')
            span_type = log_data.get('span_type')
            span_name = log_data.get('span_name')
            timestamp = log_data.get('ts')
            level = log_data.get('level')
            message = log_data.get('message')

            # Thread-safe write to database
            with self._lock:
                if self._connection:
                    cursor = self._connection.cursor()
                    cursor.execute(f"""
                        INSERT INTO {self.table_name}
                        (run_id, flow_name, span_id, parent_span_id, span_type, span_name,
                         timestamp, level, message, log_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        run_id, flow_name, span_id, parent_span_id, span_type, span_name,
                        timestamp, level, message, msg
                    ))
                    self._connection.commit()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush the database connection."""
        if self._closed or not self._connection:
            return

        try:
            with self._lock:
                if self._connection:
                    self._connection.commit()
        except Exception:
            self.handleError(None)  # type: ignore

    def close(self) -> None:
        """Close the database connection."""
        if self._closed:
            return

        try:
            with self._lock:
                if self._connection:
                    self._connection.close()
                    self._connection = None
                self._closed = True
        except Exception:
            self.handleError(None)  # type: ignore
        finally:
            super().close()

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed


class SQLiteRunHandler(logging.Handler):
    """
    Logging handler that writes run summaries to a SQLite database.

    This handler stores run summaries in a dedicated table with
    the complete run hierarchy as JSON.

    Example:
        handler = SQLiteRunHandler(
            database_path='./flowlet.db',
            table_name='runs'
        )
        handler.setFormatter(logging.Formatter("%(message)s"))

        run_logger = logging.getLogger('flowlet-run')
        run_logger.addHandler(handler)
    """

    def __init__(
        self,
        database_path: str,
        table_name: str = 'runs',
        level: int = logging.NOTSET
    ):
        """
        Initialize SQLite run summary handler.

        Args:
            database_path: Path to the SQLite database file
            table_name: Name of the table to store run summaries
            level: Minimum log level to handle (default: NOTSET)
        """
        super().__init__(level)

        self.database_path = Path(database_path)
        self.table_name = table_name

        # Thread safety
        self._lock = threading.Lock()
        self._connection: Optional[sqlite3.Connection] = None
        self._closed = False

        # Initialize database and table
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the database and create the runs table if it doesn't exist."""
        # Ensure parent directory exists
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect and create table
        with self._lock:
            self._connection = sqlite3.connect(str(self.database_path), check_same_thread=False)
            cursor = self._connection.cursor()

            # Create runs table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT UNIQUE,
                    span_id TEXT,
                    span_type TEXT,
                    span_name TEXT,
                    status TEXT,
                    start_ts TEXT,
                    end_ts TEXT,
                    duration TEXT,
                    summary_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_run_id
                ON {self.table_name}(run_id)
            """)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_start_ts
                ON {self.table_name}(start_ts)
            """)

            self._connection.commit()

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a run summary to the SQLite database.

        Args:
            record: The log record to emit
        """
        if self._closed or not self._connection:
            return

        try:
            # Format the log record (should be JSON)
            msg = self.format(record)

            # Parse JSON to extract fields
            try:
                summary_data = json.loads(msg)
            except json.JSONDecodeError:
                return

            # Extract fields
            span_id = summary_data.get('span_id')
            span_type = summary_data.get('span_type')
            span_name = summary_data.get('span_name')
            status = summary_data.get('status')
            start_ts = summary_data.get('start_ts')
            end_ts = summary_data.get('end_ts')
            duration = summary_data.get('duration')

            # Use span_id as run_id for the root span
            run_id = span_id

            if not run_id:
                return

            # Thread-safe write to database
            with self._lock:
                if self._connection:
                    cursor = self._connection.cursor()
                    # Use INSERT OR REPLACE to handle duplicates
                    cursor.execute(f"""
                        INSERT OR REPLACE INTO {self.table_name}
                        (run_id, span_id, span_type, span_name, status, start_ts, end_ts, duration, summary_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        run_id, span_id, span_type, span_name, status, start_ts, end_ts, duration, msg
                    ))
                    self._connection.commit()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush the database connection."""
        if self._closed or not self._connection:
            return

        try:
            with self._lock:
                if self._connection:
                    self._connection.commit()
        except Exception:
            self.handleError(None)  # type: ignore

    def close(self) -> None:
        """Close the database connection."""
        if self._closed:
            return

        try:
            with self._lock:
                if self._connection:
                    self._connection.close()
                    self._connection = None
                self._closed = True
        except Exception:
            self.handleError(None)  # type: ignore
        finally:
            super().close()

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed

"""
DuckDB connection management.

DuckDB allows one read-write connection and multiple read-only connections
within the same process. The fetcher thread uses the read-write connection,
and the API uses read-only connections.
"""

import threading
import duckdb
from pathlib import Path

from polymarket.config import DUCKDB_FILE

DB_PATH = DUCKDB_FILE

_write_conn = None
_write_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get the singleton read-write connection (thread-safe via lock)."""
    global _write_conn
    with _write_lock:
        if _write_conn is None:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            _write_conn = duckdb.connect(str(DB_PATH))
        return _write_conn


def get_readonly_connection() -> duckdb.DuckDBPyConnection:
    """Create a new read-only connection for API queries.

    Each call returns a fresh connection — callers should close it when done,
    or use it as a context manager.
    """
    return duckdb.connect(str(DB_PATH), read_only=True)


def close_connection():
    """Close the write connection (call on shutdown)."""
    global _write_conn
    with _write_lock:
        if _write_conn is not None:
            _write_conn.close()
            _write_conn = None

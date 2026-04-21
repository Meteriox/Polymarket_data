"""
DuckDB connection management.

Single database file, single write connection (singleton).
API queries use cursors from the same connection, executed via
asyncio.run_in_executor() to avoid blocking the event loop.
Fetcher writes are short transactions (< 100ms per batch), so
API reads are never blocked for long.
"""

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import duckdb

from polymarket.config import DUCKDB_FILE

logger = logging.getLogger(__name__)

DB_PATH = DUCKDB_FILE

_conn = None
_conn_lock = threading.Lock()

_query_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="db-query")


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get the singleton DuckDB connection (thread-safe)."""
    global _conn
    with _conn_lock:
        if _conn is None:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            _conn = duckdb.connect(str(DB_PATH))
            logger.info(f"DuckDB connected: {DB_PATH}")
        return _conn


def get_cursor() -> duckdb.DuckDBPyConnection:
    """Get a cursor for read queries (used by API routes)."""
    return get_connection().cursor()


def _run_query(sql: str, params=None):
    """Execute a query on a cursor in the thread pool (blocking call)."""
    cursor = get_cursor()
    try:
        if params:
            return cursor.execute(sql, params).fetchdf()
        return cursor.execute(sql).fetchdf()
    finally:
        cursor.close()


async def execute_query_async(sql: str, params=None):
    """Run a DuckDB query without blocking the uvicorn event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _query_pool,
        partial(_run_query, sql, params),
    )


def close_connection():
    """Close the connection (call on shutdown)."""
    global _conn
    with _conn_lock:
        if _conn is not None:
            _conn.close()
            _conn = None
            logger.info("DuckDB connection closed")

"""
DuckDB connection management with hybrid parquet + DB architecture.

On startup, register_parquet_views() scans the data directory for parquet
files and creates unified VIEWs that UNION ALL the parquet data with
the _new tables (which hold incremental fetcher data).

This means queries against "orderfilled", "trades", etc. transparently
cover both historical parquet data and new live data — with zero import cost.
"""

import logging
import threading
from pathlib import Path
from typing import Iterable

import duckdb

from polymarket.config import DATA_DIR, DATASET_DIR, DATA_CLEAN_DIR, DUCKDB_FILE
from polymarket.db.schema import TABLE_COLUMNS

logger = logging.getLogger(__name__)

DB_PATH = DUCKDB_FILE

_write_conn = None
_write_lock = threading.Lock()

PARQUET_SEARCH_MAP = {
    'orderfilled': ['orderfilled.parquet', 'orderfilled_part*.parquet'],
    'trades': ['trades.parquet'],
    'markets': ['markets.parquet'],
    'quant': ['quant.parquet'],
    'users': ['users.parquet'],
}


def _find_parquet_files(
    patterns: list[str],
    search_dirs: Iterable[Path],
) -> list[Path]:
    """Find parquet files matching patterns across directories."""
    resolved: list[Path] = []
    seen: set[Path] = set()
    for directory in search_dirs:
        if not directory.exists():
            continue
        for pattern in patterns:
            for path in sorted(directory.glob(pattern)):
                real = path.resolve()
                if path.is_file() and real not in seen:
                    resolved.append(path)
                    seen.add(real)
    return resolved


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _canonical_col(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _build_parquet_select(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    parquet_paths: list[Path],
    target_columns: list[str],
) -> str | None:
    """Build a SELECT that reads parquet files and aligns columns to the target schema."""
    path_list = ", ".join(f"'{_sql_quote(str(p))}'" for p in parquet_paths)
    source_expr = f"read_parquet([{path_list}], union_by_name=true)"

    try:
        source_cols = conn.execute(f"DESCRIBE SELECT * FROM {source_expr}").fetchall()
    except Exception as e:
        logger.warning(f"Cannot describe parquet for {table}: {e}")
        return None

    source_by_canonical = {_canonical_col(name): name for name, *_ in source_cols}

    select_parts: list[str] = []
    for col in target_columns:
        clean_col = col.strip('"')
        canonical = _canonical_col(clean_col)
        if canonical in source_by_canonical:
            src = source_by_canonical[canonical]
            select_parts.append(f'"{src}" AS "{clean_col}"')
        else:
            select_parts.append(f'NULL AS "{clean_col}"')

    return f"SELECT {', '.join(select_parts)} FROM {source_expr}"


def register_parquet_views(conn: duckdb.DuckDBPyConnection):
    """Create unified VIEWs combining parquet files + _new tables.

    For each table (e.g. "orderfilled"):
      - If parquet files exist: CREATE VIEW orderfilled AS (parquet SELECT) UNION ALL (SELECT FROM orderfilled_new)
      - If no parquet files:    CREATE VIEW orderfilled AS SELECT * FROM orderfilled_new
    """
    search_dirs = [DATA_DIR, DATASET_DIR, DATA_CLEAN_DIR]

    for table, patterns in PARQUET_SEARCH_MAP.items():
        target_columns = TABLE_COLUMNS.get(table, [])
        if not target_columns:
            continue

        new_table = f"{table}_new"
        col_list = ", ".join(f'"{c.strip(chr(34))}"' for c in target_columns)

        parquet_paths = _find_parquet_files(patterns, search_dirs)

        conn.execute(f"DROP VIEW IF EXISTS {table}")

        if parquet_paths:
            parquet_select = _build_parquet_select(
                conn, table, parquet_paths, target_columns
            )
            if parquet_select:
                file_names = ", ".join(p.name for p in parquet_paths[:3])
                if len(parquet_paths) > 3:
                    file_names += ", ..."
                logger.info(
                    f"  {table}: {len(parquet_paths)} parquet file(s) "
                    f"({file_names}) + {new_table}"
                )
                conn.execute(
                    f"""
                    CREATE VIEW {table} AS
                    {parquet_select}
                    UNION ALL
                    SELECT {col_list} FROM {new_table}
                    """
                )
                continue

        logger.info(f"  {table}: {new_table} only (no parquet files found)")
        conn.execute(
            f"CREATE VIEW {table} AS SELECT {col_list} FROM {new_table}"
        )


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get the singleton read-write connection (thread-safe via lock)."""
    global _write_conn
    with _write_lock:
        if _write_conn is None:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            _write_conn = duckdb.connect(str(DB_PATH))
        return _write_conn


def get_readonly_connection() -> duckdb.DuckDBPyConnection:
    """Create a cursor from the main connection for API queries.

    DuckDB does not allow mixing read-write and read-only connections
    to the same file. Instead we create cursors from the singleton
    write connection — each cursor is safe to use from its own thread.
    """
    conn = get_connection()
    return conn.cursor()


def close_connection():
    """Close the write connection (call on shutdown)."""
    global _write_conn
    with _write_lock:
        if _write_conn is not None:
            _write_conn.close()
            _write_conn = None

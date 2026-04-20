#!/usr/bin/env python3
"""
Import existing parquet files into DuckDB.

Usage:
    python -m polymarket.db.import_parquet
    python -m polymarket.db.import_parquet --data-dir /path/to/parquet/files
    python -m polymarket.db.import_parquet --skip-existing
"""

import argparse
import logging
import time
from pathlib import Path
from typing import Iterable

import duckdb

from polymarket.config import DATA_DIR, DATASET_DIR, DATA_CLEAN_DIR
from polymarket.db.engine import DB_PATH
from polymarket.db.schema import init_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

IMPORT_MAP = [
    ('orderfilled', ['orderfilled.parquet', 'orderfilled_part*.parquet']),
    ('trades', ['trades.parquet']),
    ('markets', ['markets.parquet']),
    ('quant', ['quant.parquet']),
    ('users', ['users.parquet']),
]


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _quote_ident(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _canonical_col(name: str) -> str:
    """Normalize column name for loose matching."""
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _build_read_parquet_expr(parquet_paths: list[Path]) -> str:
    path_list_sql = ", ".join(f"'{_sql_quote(str(path))}'" for path in parquet_paths)
    return f"read_parquet([{path_list_sql}], union_by_name=true)"


def _build_aligned_select(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    parquet_paths: list[Path],
) -> str:
    """Build SELECT list aligned to target table schema."""
    source_scan = _build_read_parquet_expr(parquet_paths)

    source_cols = conn.execute(
        f"DESCRIBE SELECT * FROM {source_scan}"
    ).fetchall()
    source_by_canonical = {
        _canonical_col(name): name
        for name, *_ in source_cols
    }

    target_cols = conn.execute(f"PRAGMA table_info('{table}')").fetchall()

    select_exprs: list[str] = []
    for _, col_name, col_type, *_ in target_cols:
        canonical = _canonical_col(col_name)
        if canonical in source_by_canonical:
            source_name = source_by_canonical[canonical]
            select_exprs.append(
                f"{_quote_ident(source_name)} AS {_quote_ident(col_name)}"
            )
        else:
            logger.warning(
                f"    Missing source column for {table}.{col_name}, filling NULL"
            )
            select_exprs.append(
                f"CAST(NULL AS {col_type}) AS {_quote_ident(col_name)}"
            )

    return f"SELECT {', '.join(select_exprs)} FROM {source_scan}"


def _resolve_parquet_sources(
    table: str,
    filename_patterns: list[str],
    search_dirs: Iterable[Path],
) -> list[Path]:
    """Resolve parquet files for a table from multiple directories/patterns."""
    resolved: list[Path] = []
    seen: set[Path] = set()
    for directory in search_dirs:
        if not directory.exists():
            continue
        for pattern in filename_patterns:
            for path in sorted(directory.glob(pattern)):
                if path.is_file() and path not in seen:
                    resolved.append(path)
                    seen.add(path)
    if not resolved:
        logger.warning(
            f"  Skipped {table}: no files matched {filename_patterns} "
            f"in {[str(p) for p in search_dirs]}"
        )
    return resolved


def import_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    parquet_paths: list[Path],
    skip_existing: bool = False,
):
    """Import one or multiple parquet files into a DuckDB table."""
    if not parquet_paths:
        return

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if skip_existing and row_count > 0:
        logger.info(f"  Skipped {table}: already has {row_count:,} rows")
        return

    if row_count > 0:
        logger.info(f"  Clearing {table} ({row_count:,} existing rows)...")
        conn.execute(f"DELETE FROM {table}")

    file_count = len(parquet_paths)
    file_list = ", ".join(p.name for p in parquet_paths[:3])
    if file_count > 3:
        file_list = f"{file_list}, ..."
    logger.info(f"  Importing {table} from {file_count} file(s): {file_list}")
    t0 = time.time()

    aligned_select = _build_aligned_select(conn, table, parquet_paths)

    conn.execute(
        f"""
        INSERT INTO {table}
        {aligned_select}
        """
    )

    new_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    elapsed = time.time() - t0
    size_mb = sum(path.stat().st_size for path in parquet_paths) / 1024 / 1024
    logger.info(f"  Done: {new_count:,} rows, {size_mb:.0f} MB, {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description='Import parquet files into DuckDB')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Directory containing parquet files (default: auto-detect)')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip tables that already have data')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Importing parquet data into DuckDB")
    logger.info(f"Database: {DB_PATH}")
    logger.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    init_schema(conn)

    if args.data_dir:
        search_dirs = [Path(args.data_dir)]
    else:
        search_dirs = [DATA_DIR, DATASET_DIR, DATA_CLEAN_DIR]

    for table, patterns in IMPORT_MAP:
        parquet_paths = _resolve_parquet_sources(table, patterns, search_dirs)
        import_table(conn, table, parquet_paths, args.skip_existing)

    conn.close()

    db_size_mb = DB_PATH.stat().st_size / 1024 / 1024
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Import complete. Database size: {db_size_mb:.0f} MB")
    logger.info(f"Database file: {DB_PATH}")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()

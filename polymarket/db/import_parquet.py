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
    ('orderfilled', 'orderfilled.parquet', 'dataset'),
    ('trades', 'trades.parquet', 'dataset'),
    ('markets', 'markets.parquet', 'dataset'),
    ('quant', 'quant.parquet', 'data_clean'),
    ('users', 'users.parquet', 'data_clean'),
]


def import_table(conn: duckdb.DuckDBPyConnection, table: str,
                 parquet_path: Path, skip_existing: bool = False):
    """Import a single parquet file into a DuckDB table."""
    if not parquet_path.exists():
        logger.warning(f"  Skipped {table}: {parquet_path} not found")
        return

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if skip_existing and row_count > 0:
        logger.info(f"  Skipped {table}: already has {row_count:,} rows")
        return

    if row_count > 0:
        logger.info(f"  Clearing {table} ({row_count:,} existing rows)...")
        conn.execute(f"DELETE FROM {table}")

    logger.info(f"  Importing {table} from {parquet_path.name} ...")
    t0 = time.time()

    conn.execute(f"""
        INSERT INTO {table}
        SELECT * FROM read_parquet('{parquet_path}')
    """)

    new_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    elapsed = time.time() - t0
    size_mb = parquet_path.stat().st_size / 1024 / 1024
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

    for table, filename, subdir in IMPORT_MAP:
        if args.data_dir:
            parquet_path = Path(args.data_dir) / filename
        else:
            base = DATASET_DIR if subdir == 'dataset' else DATA_CLEAN_DIR
            parquet_path = base / filename

        import_table(conn, table, parquet_path, args.skip_existing)

    conn.close()

    db_size_mb = DB_PATH.stat().st_size / 1024 / 1024
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Import complete. Database size: {db_size_mb:.0f} MB")
    logger.info(f"Database file: {DB_PATH}")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()

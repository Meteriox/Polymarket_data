#!/usr/bin/env python3
"""
Verify parquet data availability and register DuckDB views.

With the hybrid architecture, parquet files are NOT imported into DuckDB.
Instead, they are queried directly via VIEWs at runtime.

This script:
  1. Creates _new tables (for incremental fetcher data)
  2. Registers unified VIEWs (parquet + _new tables)
  3. Prints row counts to verify data availability

Usage:
    python -m polymarket.db.import_parquet
"""

import logging
import time

import duckdb

from polymarket.db.engine import DB_PATH, register_parquet_views
from polymarket.db.schema import init_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

TABLES = ['orderfilled', 'trades', 'markets', 'quant', 'users']


def main():
    logger.info("=" * 60)
    logger.info("Polymarket Data — Verify & Register Views")
    logger.info(f"Database: {DB_PATH}")
    logger.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    init_schema(conn)

    logger.info("")
    logger.info("Registering parquet views...")
    t0 = time.time()
    register_parquet_views(conn)
    elapsed = time.time() - t0
    logger.info(f"Views registered in {elapsed:.1f}s")

    logger.info("")
    logger.info("Table/View row counts:")
    for table in TABLES:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            count = row[0] if row else 0
            logger.info(f"  {table:20s}: {count:>15,} rows")
        except Exception as e:
            logger.warning(f"  {table:20s}: error - {e}")

    try:
        row = conn.execute(
            "SELECT MAX(block_number) FROM orderfilled"
        ).fetchone()
        if row and row[0] is not None:
            logger.info(f"\n  Latest block: {int(row[0]):,}")
    except Exception:
        pass

    conn.close()

    db_size_mb = DB_PATH.stat().st_size / 1024 / 1024
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Database size: {db_size_mb:.0f} MB (indexes + _new tables only)")
    logger.info("Parquet files are queried directly, not imported.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()

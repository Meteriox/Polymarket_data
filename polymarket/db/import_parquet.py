#!/usr/bin/env python3
"""
Batch-import parquet files into DuckDB tables.

Designed for large datasets (100GB+) on resource-constrained servers.
Imports data row-group by row-group with configurable pacing to avoid
saturating CPU/memory/IO.

Features:
  - Checkpoint after every row-group → full crash recovery
  - Configurable memory limit, thread count, and sleep interval
  - Indexes are created AFTER all data is loaded (much faster)
  - Progress logging with ETA

Usage:
    # Default settings (4GB mem, 4 threads, 0.3s sleep)
    python -m polymarket.db.import_parquet

    # Conservative for busy servers
    python -m polymarket.db.import_parquet --memory 2GB --threads 2 --sleep 1.0

    # Resume after interruption (automatic)
    python -m polymarket.db.import_parquet
"""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from polymarket.config import DATA_DIR, DATASET_DIR, DATA_CLEAN_DIR, DUCKDB_FILE
from polymarket.db.schema import TABLE_COLUMNS, TABLE_DDLS, INDEX_DDL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

STATE_FILE = DATA_DIR / 'import_state.json'

PARQUET_SEARCH = {
    'markets':     (['markets.parquet'],          [DATA_DIR, DATASET_DIR]),
    'orderfilled': (['orderfilled.parquet', 'orderfilled_part*.parquet'], [DATA_DIR, DATASET_DIR]),
    'trades':      (['trades.parquet'],           [DATA_DIR, DATASET_DIR]),
    'quant':       (['quant.parquet'],            [DATA_DIR, DATA_CLEAN_DIR]),
    'users':       (['users.parquet'],            [DATA_DIR, DATA_CLEAN_DIR]),
}

IMPORT_ORDER = ['markets', 'orderfilled', 'trades', 'quant', 'users']

REQUIRED_TABLES = {'orderfilled', 'trades', 'quant', 'users'}


def _find_files(patterns: list[str], dirs: list[Path]) -> list[Path]:
    found: list[Path] = []
    seen: set[Path] = set()
    for d in dirs:
        if not d.exists():
            continue
        for pat in patterns:
            for p in sorted(d.glob(pat)):
                real = p.resolve()
                if p.is_file() and real not in seen:
                    found.append(p)
                    seen.add(real)
    return found


def _deduplicate_orderfilled(files: list[Path]) -> list[Path]:
    """If both orderfilled.parquet and orderfilled_part*.parquet exist,
    use only the part files to avoid importing duplicate data."""
    whole = [f for f in files if f.name == 'orderfilled.parquet']
    parts = [f for f in files if f.name != 'orderfilled.parquet']
    if whole and parts:
        logger.warning(
            f"  Found both orderfilled.parquet and {len(parts)} part file(s). "
            f"Using part files only to avoid duplicates."
        )
        return parts
    return files


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _format_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _canonical_col(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _build_insert_sql(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    parquet_path: str,
    rg_start: int,
    rg_end: int,
    target_columns: list[str],
) -> str:
    """Build INSERT ... SELECT that aligns parquet columns to the table schema."""
    read_expr = (
        f"read_parquet('{parquet_path}', "
        f"row_group_range=({rg_start}, {rg_end}))"
    )

    source_cols = conn.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()
    source_by_canonical = {_canonical_col(name): name for name, *_ in source_cols}

    select_parts: list[str] = []
    for col in target_columns:
        clean_col = col.strip('"')
        canonical = _canonical_col(clean_col)
        if canonical in source_by_canonical:
            src = source_by_canonical[canonical]
            select_parts.append(f'"{src}"')
        else:
            select_parts.append("NULL")

    col_list = ", ".join(f'"{c.strip(chr(34))}"' for c in target_columns)
    select_list = ", ".join(select_parts)

    return f"INSERT INTO {table} ({col_list}) SELECT {select_list} FROM {read_expr}"


def import_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    state: dict,
    sleep_sec: float,
):
    """Import all parquet files for one table, row-group by row-group."""
    patterns, search_dirs = PARQUET_SEARCH[table]
    files = _find_files(patterns, search_dirs)

    if table == 'orderfilled':
        files = _deduplicate_orderfilled(files)

    if not files:
        if table in REQUIRED_TABLES:
            raise RuntimeError(
                f"FATAL: no parquet files found for required table '{table}'. "
                f"Searched patterns {patterns} in {[str(d) for d in search_dirs]}"
            )
        logger.warning(f"  {table}: no parquet files found, skipping")
        state[table] = {"status": "done", "rows": 0}
        _save_state(state)
        return

    target_columns = TABLE_COLUMNS[table]

    tbl_state = state.get(table, {})
    saved_status = tbl_state.get("status")

    if saved_status in ("done", "importing"):
        db_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        expected_rows = tbl_state.get("rows", 0)

        if saved_status == "done" and db_rows > 0 and db_rows >= expected_rows:
            logger.info(
                f"  {table}: already imported ({db_rows:,} rows in DB), skipping"
            )
            return

        if saved_status == "importing" and db_rows > 0 and db_rows >= expected_rows:
            pass  # DB consistent with checkpoint, resume normally
        elif db_rows < expected_rows:
            logger.warning(
                f"  {table}: state says {expected_rows:,} rows imported but DB has "
                f"{db_rows:,} rows — reimporting from scratch"
            )
            conn.execute(f"DELETE FROM {table}")
            tbl_state = {}
            state[table] = {}
            _save_state(state)

    resume_file_idx = tbl_state.get("file_idx", 0)
    resume_rg_idx = tbl_state.get("rg_idx", 0)
    total_rows_imported = tbl_state.get("rows", 0)

    total_rg_all_files = 0
    file_infos = []
    for f in files:
        meta = pq.read_metadata(str(f))
        file_infos.append((f, meta.num_row_groups, meta.num_rows))
        total_rg_all_files += meta.num_row_groups

    total_rows_all = sum(info[2] for info in file_infos)

    done_rg = 0
    for i in range(resume_file_idx):
        done_rg += file_infos[i][1]
    done_rg += resume_rg_idx

    logger.info(
        f"  {table}: {len(files)} file(s), {total_rg_all_files} row groups, "
        f"{total_rows_all:,} rows total"
    )
    if done_rg > 0:
        logger.info(f"  Resuming from file {resume_file_idx}, row group {resume_rg_idx}")

    t_start = time.time()
    processed_rg = 0

    for file_idx, (filepath, num_rg, num_rows) in enumerate(file_infos):
        if file_idx < resume_file_idx:
            continue

        start_rg = resume_rg_idx if file_idx == resume_file_idx else 0
        parquet_path = str(filepath).replace("'", "''")

        for rg_idx in range(start_rg, num_rg):
            sql = _build_insert_sql(
                conn, table, parquet_path, rg_idx, rg_idx + 1, target_columns
            )
            result = conn.execute(sql).fetchone()
            batch_rows = result[0] if result else 0
            total_rows_imported += batch_rows
            processed_rg += 1
            current_rg = done_rg + processed_rg

            elapsed = time.time() - t_start
            if processed_rg > 0 and elapsed > 0:
                rg_per_sec = processed_rg / elapsed
                remaining_rg = total_rg_all_files - current_rg
                eta = remaining_rg / rg_per_sec if rg_per_sec > 0 else 0
                eta_str = _format_duration(eta)
            else:
                eta_str = "..."

            if processed_rg % 5 == 0 or rg_idx == num_rg - 1:
                pct = current_rg * 100.0 / total_rg_all_files
                logger.info(
                    f"    [{pct:5.1f}%] {filepath.name} rg {rg_idx+1}/{num_rg} | "
                    f"{total_rows_imported:,} rows | ETA {eta_str}"
                )

            state[table] = {
                "status": "importing",
                "file_idx": file_idx,
                "rg_idx": rg_idx + 1,
                "rows": total_rows_imported,
                "updated_at": datetime.now().isoformat(),
            }
            _save_state(state)

            if sleep_sec > 0:
                time.sleep(sleep_sec)

    state[table] = {
        "status": "done",
        "rows": total_rows_imported,
        "finished_at": datetime.now().isoformat(),
    }
    _save_state(state)

    elapsed = time.time() - t_start
    logger.info(
        f"  {table}: done — {total_rows_imported:,} rows in {_format_duration(elapsed)}"
    )


def main():
    parser = argparse.ArgumentParser(description='Import parquet files into DuckDB')
    parser.add_argument('--memory', type=str, default='4GB',
                        help='DuckDB memory limit (default: 4GB)')
    parser.add_argument('--threads', type=int, default=4,
                        help='DuckDB thread count (default: 4)')
    parser.add_argument('--sleep', type=float, default=0.3,
                        help='Sleep between row-groups in seconds (default: 0.3)')
    parser.add_argument('--reset', action='store_true',
                        help='Drop all tables and reimport from scratch')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Polymarket Data — Parquet Import")
    logger.info("=" * 60)
    logger.info(f"  Database:  {DUCKDB_FILE}")
    logger.info(f"  Memory:    {args.memory}")
    logger.info(f"  Threads:   {args.threads}")
    logger.info(f"  Sleep:     {args.sleep}s per row-group")
    logger.info("=" * 60)

    DUCKDB_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_FILE))

    conn.execute(f"SET memory_limit = '{args.memory}'")
    conn.execute(f"SET threads = {args.threads}")

    if args.reset:
        logger.warning("Reset mode: dropping all tables...")
        for table in IMPORT_ORDER:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        logger.info("All tables dropped, state cleared")

    for ddl in TABLE_DDLS:
        conn.execute(ddl)

    state = _load_state()

    t_total = time.time()

    for table in IMPORT_ORDER:
        logger.info(f"\n{'─' * 40}")
        logger.info(f"Importing: {table}")
        logger.info(f"{'─' * 40}")
        import_table(conn, table, state, args.sleep)

    all_done = all(state.get(t, {}).get("status") == "done" for t in IMPORT_ORDER)

    if all_done:
        logger.info(f"\n{'=' * 60}")
        logger.info("All tables imported. Creating indexes...")
        logger.info(f"{'=' * 60}")

        for idx_sql in INDEX_DDL:
            idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
            logger.info(f"  Creating {idx_name}...")
            t0 = time.time()
            conn.execute(idx_sql)
            logger.info(f"    done in {_format_duration(time.time() - t0)}")

    conn.close()

    elapsed_total = time.time() - t_total

    logger.info(f"\n{'=' * 60}")
    logger.info("  Import Summary")
    logger.info(f"{'=' * 60}")
    for table in IMPORT_ORDER:
        tbl = state.get(table, {})
        status = tbl.get("status", "unknown")
        rows = tbl.get("rows", 0)
        logger.info(f"  {table:20s}: {status:10s} {rows:>15,} rows")

    db_size_mb = DUCKDB_FILE.stat().st_size / 1024 / 1024
    logger.info(f"\n  Database size: {db_size_mb:,.0f} MB")
    logger.info(f"  Total time:    {_format_duration(elapsed_total)}")
    logger.info(f"{'=' * 60}")

    if all_done:
        logger.info("\nImport complete! You can now start the service:")
        logger.info("  docker compose up -d --build")
        logger.info("\nAfter verifying the API works correctly, you may")
        logger.info("manually delete the parquet files to free disk space.")


if __name__ == '__main__':
    main()

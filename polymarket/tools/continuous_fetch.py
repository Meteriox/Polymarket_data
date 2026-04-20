#!/usr/bin/env python3
"""
Continuous blockchain data fetcher with DuckDB storage.

Fetches new blocks from Polygon, decodes OrderFilled events, and inserts
directly into DuckDB tables. Supports batch mode (catch-up) and real-time
mode (follow chain tip every 2 seconds).

Usage:
    # As a standalone script
    python -m polymarket.tools.continuous_fetch

    # Normally started via polymarket.service (fetcher thread + API server)
"""

import sys
import time
import signal
import argparse
import logging
from pathlib import Path

import pandas as pd

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from polymarket.fetchers.rpc import LogFetcher
from polymarket.processors import (
    EventDecoder,
    extract_trades,
    load_token_mapping,
    clean_trades_df,
    clean_users_df
)
from polymarket.config import MARKETS_FILE, MISSING_MARKETS_FILE
from polymarket.db.engine import get_connection
from polymarket.db.schema import init_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DuckDBWriter:
    """Write decoded blockchain data directly into DuckDB tables."""

    def __init__(self, conn):
        self.conn = conn
        self.row_counts = {
            'orderfilled': 0,
            'trades': 0,
            'quant': 0,
            'users': 0,
        }

    def write_events(self, events: list):
        if not events:
            return
        df = pd.DataFrame(events)
        self.conn.execute("INSERT INTO orderfilled_new SELECT * FROM df")
        self.row_counts['orderfilled'] += len(df)

    def write_trades(self, trades: list):
        if not trades:
            return
        df = pd.DataFrame(trades)
        self.conn.execute("INSERT INTO trades_new SELECT * FROM df")
        self.row_counts['trades'] += len(df)

    def write_quant(self, quant_df: pd.DataFrame):
        if quant_df is None or len(quant_df) == 0:
            return
        self.conn.execute("INSERT INTO quant_new SELECT * FROM quant_df")
        self.row_counts['quant'] += len(quant_df)

    def write_users(self, users_df: pd.DataFrame):
        if users_df is None or len(users_df) == 0:
            return
        self.conn.execute("INSERT INTO users_new SELECT * FROM users_df")
        self.row_counts['users'] += len(users_df)

    def summary(self) -> str:
        parts = [f"{k}: {v:,}" for k, v in self.row_counts.items() if v > 0]
        return ", ".join(parts) if parts else "no new data"


class ContinuousFetcher:
    """Fetches new blocks and writes data into DuckDB."""

    def __init__(self, conn=None, batch_size=100):
        self.batch_size = batch_size
        self.conn = conn or get_connection()
        init_schema(self.conn)

        self.writer = DuckDBWriter(self.conn)
        self.fetcher = LogFetcher()
        self.decoder = EventDecoder()

        self.token_mapping = load_token_mapping(MARKETS_FILE)
        if MISSING_MARKETS_FILE.exists():
            self.token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
        logger.info(f"Loaded {len(self.token_mapping)} token mappings")

        self.last_processed_block = self._resolve_start_block()

        self.should_stop = False

    def _resolve_start_block(self) -> int | None:
        """Determine start block: DB max(block_number) or None."""
        try:
            result = self.conn.execute(
                "SELECT MAX(block_number) FROM orderfilled"
            ).fetchone()
            if result and result[0] is not None:
                block = int(result[0])
                logger.info(f"Resuming from DB, last block: {block:,}")
                return block
        except Exception as e:
            logger.warning(f"Could not query start block from DB: {e}")
        logger.info("No existing data found, will start from chain tip")
        return None

    def get_latest_block(self) -> int | None:
        try:
            return self.fetcher.client.get_latest_block()
        except Exception as e:
            logger.error(f"Failed to get latest block: {e}")
            return None

    def fetch_and_process_range(self, start_block: int, end_block: int) -> bool:
        try:
            logs = self.fetcher.fetch_range_in_batches(start_block, end_block)
            if logs is None or len(logs) == 0:
                logger.info(f"Blocks {start_block:,}-{end_block:,}: no events")
                return True

            logger.info(f"  Fetched {len(logs)} logs")

            decoded = [self.decoder.decode(log) for log in logs]
            events = [self.decoder.format_event(e) for e in decoded]
            if not events:
                return True

            logger.info(f"  Decoded {len(events)} events")

            trades = extract_trades(events)
            logger.info(f"  Extracted {len(trades)} trades")

            self.writer.write_events(events)
            self.writer.write_trades(trades)

            if trades:
                trades_df = pd.DataFrame(trades)
                quant_df = clean_trades_df(trades_df)
                users_df = clean_users_df(trades_df)
                self.writer.write_quant(quant_df)
                self.writer.write_users(users_df)

            logger.info(f"  Written to DB")
            return True

        except Exception as e:
            logger.error(f"Error processing blocks {start_block}-{end_block}: {e}")
            return False

    def run(self):
        """Main loop: fetch new blocks continuously."""
        logger.info("\n" + "=" * 60)
        logger.info("=== Continuous Fetcher Started ===")
        logger.info("=" * 60)
        logger.info(f"Batch size: {self.batch_size} blocks")
        logger.info("Press Ctrl+C or send SIGTERM to stop")
        logger.info("=" * 60 + "\n")

        if self.last_processed_block is None:
            latest_block = self.get_latest_block()
            if latest_block is None:
                logger.error("Cannot get latest block, exiting")
                return
            self.last_processed_block = latest_block - self.batch_size
            logger.info(f"First run, starting from block {self.last_processed_block:,}\n")
        else:
            logger.info(f"Continuing from block {self.last_processed_block:,}\n")

        consecutive_errors = 0
        max_errors = 10
        last_log_time = time.time()

        try:
            while not self.should_stop:
                try:
                    latest_block = self.get_latest_block()
                    if latest_block is None:
                        consecutive_errors += 1
                        if consecutive_errors >= max_errors:
                            logger.error(f"{max_errors} consecutive errors, exiting")
                            break
                        time.sleep(5)
                        continue

                    consecutive_errors = 0
                    next_block = self.last_processed_block + 1

                    if next_block > latest_block:
                        if time.time() - last_log_time > 30:
                            logger.info(
                                f"[REALTIME] Current: {self.last_processed_block:,}, "
                                f"Latest: {latest_block:,}, waiting..."
                            )
                            last_log_time = time.time()
                        time.sleep(2)
                        continue

                    blocks_behind = latest_block - self.last_processed_block

                    if blocks_behind >= self.batch_size:
                        end_block = next_block + self.batch_size - 1
                        logger.info(
                            f"[BATCH] Processing {next_block:,} - {end_block:,} "
                            f"({blocks_behind:,} behind)"
                        )
                        success = self.fetch_and_process_range(next_block, end_block)
                        if success:
                            self.last_processed_block = end_block
                            logger.info(f"State: {end_block:,}\n")
                        else:
                            time.sleep(5)
                            continue
                        time.sleep(0.5)
                    else:
                        end_block = next_block
                        logger.info(
                            f"[REALTIME] Processing block {next_block:,} "
                            f"(latest: {latest_block:,})"
                        )
                        success = self.fetch_and_process_range(next_block, end_block)
                        if success:
                            self.last_processed_block = end_block
                            logger.info(f"State: {end_block:,}\n")
                        else:
                            time.sleep(5)
                            continue
                        last_log_time = time.time()
                        time.sleep(2)

                except Exception as e:
                    logger.error(f"Loop error: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_errors:
                        logger.error(f"{max_errors} consecutive errors, exiting")
                        break
                    time.sleep(5)

        finally:
            logger.info("\n" + "=" * 60)
            logger.info(f"Session summary: {self.writer.summary()}")
            logger.info("=== Continuous Fetcher Stopped ===")
            logger.info("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Continuous blockchain data fetcher')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Blocks per batch in catch-up mode (default: 100)')
    args = parser.parse_args()

    fetcher = ContinuousFetcher(batch_size=args.batch_size)

    def _handle_signal(signum, frame):
        logger.info(f"\nReceived signal {signum}, shutting down gracefully...")
        fetcher.should_stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    fetcher.run()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Polymarket Data Service — unified entry point.

Runs the continuous blockchain fetcher in a background thread and serves
the FastAPI query API in the main thread (via uvicorn).

Prerequisites:
    Data must be imported first:  python -m polymarket.db.import_parquet

Usage:
    python -m polymarket.service
    python -m polymarket.service --port 8000 --host 0.0.0.0
    python -m polymarket.service --no-fetcher        # API only
    python -m polymarket.service --fetcher-only       # Fetcher only
"""

import argparse
import logging
import os
import signal
import threading
import time

import uvicorn

from polymarket.db.engine import get_connection, close_connection
from polymarket.db.schema import init_schema
from polymarket.api.app import create_app
from polymarket.tools.continuous_fetch import ContinuousFetcher
from polymarket.state import service_state

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def _run_fetcher(conn, batch_size: int):
    """Target function for the fetcher background thread."""
    try:
        fetcher = ContinuousFetcher(conn=conn, batch_size=batch_size)
        service_state.set_fetcher(threading.current_thread(), fetcher)
        fetcher.run()
    except Exception as e:
        logger.error(f"Fetcher thread crashed: {e}")
    finally:
        service_state.clear()


def _shutdown_handler(signum, frame):
    """Gracefully stop fetcher on SIGTERM/SIGINT."""
    logger.info(f"Received signal {signum}, stopping...")
    service_state.stop_fetcher()


def main():
    parser = argparse.ArgumentParser(description='Polymarket Data Service')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='API server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8000,
                        help='API server port (default: 8000)')
    parser.add_argument('--batch-size', type=int,
                        default=int(os.getenv('BATCH_SIZE', '100')),
                        help='Fetcher batch size (default: 100, env: BATCH_SIZE)')
    parser.add_argument('--no-fetcher', action='store_true',
                        help='Run API server only, no blockchain fetcher')
    parser.add_argument('--fetcher-only', action='store_true',
                        help='Run fetcher only, no API server')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Polymarket Data Service")
    logger.info("=" * 60)

    conn = get_connection()
    init_schema(conn)

    row = conn.execute("SELECT COUNT(*) FROM orderfilled").fetchone()
    row_count = row[0] if row else 0
    if row_count == 0:
        logger.warning("No data in database! Run import first:")
        logger.warning("  python -m polymarket.db.import_parquet")

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    if args.fetcher_only:
        logger.info("Mode: fetcher only")
        _run_fetcher(conn, args.batch_size)
        close_connection()
        return

    fetcher_thread = None
    if not args.no_fetcher:
        logger.info("Starting fetcher thread...")
        fetcher_thread = threading.Thread(
            target=_run_fetcher,
            args=(conn, args.batch_size),
            daemon=True,
            name="fetcher"
        )
        fetcher_thread.start()
        time.sleep(1)
        logger.info("Fetcher thread started")
    else:
        logger.info("Fetcher disabled (--no-fetcher)")

    logger.info(f"Starting API server on {args.host}:{args.port}")
    logger.info(f"API docs: http://{args.host}:{args.port}/docs")
    logger.info("=" * 60 + "\n")

    app = create_app()
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
        access_log=False,
    )

    service_state.stop_fetcher()
    if fetcher_thread is not None:
        fetcher_thread.join(timeout=30)

    close_connection()
    logger.info("Service stopped")


if __name__ == '__main__':
    main()

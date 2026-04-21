"""
DuckDB table schemas for Polymarket data.

All data (historical + incremental) lives in the same tables.
The import script loads parquet files into these tables once;
the continuous fetcher appends new data directly.
"""

import duckdb

# ── Table DDL ────────────────────────────────────────────────

ORDERFILLED_DDL = """
CREATE TABLE IF NOT EXISTS orderfilled (
    transaction_hash VARCHAR,
    block_number BIGINT,
    log_index INTEGER,
    timestamp BIGINT,
    contract VARCHAR,
    event_name VARCHAR,
    datetime VARCHAR,
    order_hash VARCHAR,
    maker VARCHAR,
    taker VARCHAR,
    maker_asset_id VARCHAR,
    taker_asset_id VARCHAR,
    maker_amount_filled BIGINT,
    taker_amount_filled BIGINT,
    maker_fee BIGINT,
    taker_fee BIGINT,
    protocol_fee BIGINT
);
"""

TRADES_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    timestamp BIGINT,
    datetime VARCHAR,
    block_number BIGINT,
    transaction_hash VARCHAR,
    contract VARCHAR,
    event_id VARCHAR,
    event_slug VARCHAR,
    event_title VARCHAR,
    market_id VARCHAR,
    condition_id VARCHAR,
    question VARCHAR,
    nonusdc_side VARCHAR,
    maker VARCHAR,
    taker VARCHAR,
    maker_asset VARCHAR,
    taker_asset VARCHAR,
    maker_direction VARCHAR,
    taker_direction VARCHAR,
    price DOUBLE,
    usd_amount DOUBLE,
    token_amount DOUBLE,
    asset_id VARCHAR,
    order_hash VARCHAR
);
"""

MARKETS_DDL = """
CREATE TABLE IF NOT EXISTS markets (
    id VARCHAR,
    question VARCHAR,
    answer1 VARCHAR,
    answer2 VARCHAR,
    token1 VARCHAR,
    token2 VARCHAR,
    condition_id VARCHAR,
    neg_risk BOOLEAN,
    slug VARCHAR,
    volume VARCHAR,
    created_at VARCHAR,
    closed BOOLEAN,
    active BOOLEAN,
    archived BOOLEAN,
    end_date VARCHAR,
    outcome_prices VARCHAR,
    event_id VARCHAR,
    event_slug VARCHAR,
    event_title VARCHAR
);
"""

QUANT_DDL = """
CREATE TABLE IF NOT EXISTS quant (
    timestamp BIGINT,
    datetime VARCHAR,
    block_number BIGINT,
    transaction_hash VARCHAR,
    contract VARCHAR,
    event_id VARCHAR,
    event_slug VARCHAR,
    event_title VARCHAR,
    market_id VARCHAR,
    condition_id VARCHAR,
    question VARCHAR,
    nonusdc_side VARCHAR,
    maker VARCHAR,
    taker VARCHAR,
    maker_asset VARCHAR,
    taker_asset VARCHAR,
    maker_direction VARCHAR,
    taker_direction VARCHAR,
    price DOUBLE,
    usd_amount DOUBLE,
    token_amount DOUBLE,
    asset_id VARCHAR,
    order_hash VARCHAR
);
"""

USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
    timestamp BIGINT,
    datetime VARCHAR,
    block_number BIGINT,
    transaction_hash VARCHAR,
    event_id VARCHAR,
    market_id VARCHAR,
    condition_id VARCHAR,
    "user" VARCHAR,
    role VARCHAR,
    price DOUBLE,
    token_amount DOUBLE,
    usd_amount DOUBLE
);
"""

TABLE_DDLS = [
    ORDERFILLED_DDL,
    TRADES_DDL,
    MARKETS_DDL,
    QUANT_DDL,
    USERS_DDL,
]

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_orderfilled_block ON orderfilled (block_number);",
    "CREATE INDEX IF NOT EXISTS idx_trades_market_block ON trades (market_id, block_number);",
    "CREATE INDEX IF NOT EXISTS idx_trades_maker ON trades (maker);",
    "CREATE INDEX IF NOT EXISTS idx_trades_taker ON trades (taker);",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_block ON quant (market_id, block_number);",
    'CREATE INDEX IF NOT EXISTS idx_users_user ON users ("user", market_id);',
    "CREATE INDEX IF NOT EXISTS idx_markets_token1 ON markets (token1);",
    "CREATE INDEX IF NOT EXISTS idx_markets_token2 ON markets (token2);",
]

# ── Column lists (used by import script for schema alignment) ────

TABLE_COLUMNS = {
    'orderfilled': [
        'transaction_hash', 'block_number', 'log_index', 'timestamp',
        'contract', 'event_name', 'datetime', 'order_hash',
        'maker', 'taker', 'maker_asset_id', 'taker_asset_id',
        'maker_amount_filled', 'taker_amount_filled',
        'maker_fee', 'taker_fee', 'protocol_fee',
    ],
    'trades': [
        'timestamp', 'datetime', 'block_number', 'transaction_hash',
        'contract', 'event_id', 'event_slug', 'event_title',
        'market_id', 'condition_id', 'question', 'nonusdc_side',
        'maker', 'taker', 'maker_asset', 'taker_asset',
        'maker_direction', 'taker_direction',
        'price', 'usd_amount', 'token_amount', 'asset_id', 'order_hash',
    ],
    'markets': [
        'id', 'question', 'answer1', 'answer2', 'token1', 'token2',
        'condition_id', 'neg_risk', 'slug', 'volume', 'created_at',
        'closed', 'active', 'archived', 'end_date', 'outcome_prices',
        'event_id', 'event_slug', 'event_title',
    ],
    'quant': [
        'timestamp', 'datetime', 'block_number', 'transaction_hash',
        'contract', 'event_id', 'event_slug', 'event_title',
        'market_id', 'condition_id', 'question', 'nonusdc_side',
        'maker', 'taker', 'maker_asset', 'taker_asset',
        'maker_direction', 'taker_direction',
        'price', 'usd_amount', 'token_amount', 'asset_id', 'order_hash',
    ],
    'users': [
        'timestamp', 'datetime', 'block_number', 'transaction_hash',
        'event_id', 'market_id', 'condition_id',
        '"user"', 'role', 'price', 'token_amount', 'usd_amount',
    ],
}


def init_schema(conn: duckdb.DuckDBPyConnection):
    """Create tables (if not exist) and indexes."""
    for ddl in TABLE_DDLS:
        conn.execute(ddl)
    for idx in INDEX_DDL:
        try:
            conn.execute(idx)
        except Exception:
            pass


def create_indexes(conn: duckdb.DuckDBPyConnection):
    """Create indexes (separate call for post-import)."""
    for idx in INDEX_DDL:
        conn.execute(idx)

"""
DuckDB table schemas for Polymarket data.

Architecture:
  - Historical data lives in parquet files, queried directly via DuckDB views.
  - New data from the continuous fetcher is written to "_new" tables.
  - Unified views (e.g. "orderfilled") combine both sources with UNION ALL,
    so API queries always see the full dataset without importing parquet into DB.
"""

import duckdb

# ── Incremental tables: only hold new data from the fetcher ──────────────

ORDERFILLED_NEW_DDL = """
CREATE TABLE IF NOT EXISTS orderfilled_new (
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

TRADES_NEW_DDL = """
CREATE TABLE IF NOT EXISTS trades_new (
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

MARKETS_NEW_DDL = """
CREATE TABLE IF NOT EXISTS markets_new (
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

QUANT_NEW_DDL = """
CREATE TABLE IF NOT EXISTS quant_new (
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

USERS_NEW_DDL = """
CREATE TABLE IF NOT EXISTS users_new (
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

NEW_TABLE_DDLS = [
    ORDERFILLED_NEW_DDL,
    TRADES_NEW_DDL,
    MARKETS_NEW_DDL,
    QUANT_NEW_DDL,
    USERS_NEW_DDL,
]

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_orderfilled_new_block ON orderfilled_new (block_number);",
    "CREATE INDEX IF NOT EXISTS idx_trades_new_market_block ON trades_new (market_id, block_number);",
    "CREATE INDEX IF NOT EXISTS idx_quant_new_market_block ON quant_new (market_id, block_number);",
    'CREATE INDEX IF NOT EXISTS idx_users_new_user ON users_new ("user", market_id);',
    "CREATE INDEX IF NOT EXISTS idx_markets_new_token1 ON markets_new (token1);",
    "CREATE INDEX IF NOT EXISTS idx_markets_new_token2 ON markets_new (token2);",
]

# ── Column lists for each unified view (used in parquet registration) ────

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
    """Create incremental tables and indexes."""
    for ddl in NEW_TABLE_DDLS:
        conn.execute(ddl)
    for idx in INDEX_DDL:
        conn.execute(idx)

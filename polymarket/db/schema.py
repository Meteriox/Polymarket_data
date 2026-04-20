"""
DuckDB table schemas for Polymarket data.
"""

import duckdb

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
    id VARCHAR PRIMARY KEY,
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

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_orderfilled_block ON orderfilled (block_number);",
    "CREATE INDEX IF NOT EXISTS idx_trades_market_block ON trades (market_id, block_number);",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_block ON quant (market_id, block_number);",
    "CREATE INDEX IF NOT EXISTS idx_users_user ON users (\"user\", market_id);",
    "CREATE INDEX IF NOT EXISTS idx_markets_token1 ON markets (token1);",
    "CREATE INDEX IF NOT EXISTS idx_markets_token2 ON markets (token2);",
]


def init_schema(conn: duckdb.DuckDBPyConnection):
    """Create all tables and indexes if they don't exist."""
    for ddl in [ORDERFILLED_DDL, TRADES_DDL, MARKETS_DDL, QUANT_DDL, USERS_DDL]:
        conn.execute(ddl)
    for idx in INDEX_DDL:
        conn.execute(idx)

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
    maker_amount_filled VARCHAR,
    taker_amount_filled VARCHAR,
    maker_fee VARCHAR,
    taker_fee VARCHAR,
    protocol_fee VARCHAR
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

# ── Column definitions (used by import script for schema alignment) ────
# Each entry maps column_name → DuckDB type string.

TABLE_COLUMN_TYPES = {
    'orderfilled': {
        'transaction_hash': 'VARCHAR', 'block_number': 'BIGINT',
        'log_index': 'INTEGER', 'timestamp': 'BIGINT',
        'contract': 'VARCHAR', 'event_name': 'VARCHAR',
        'datetime': 'VARCHAR', 'order_hash': 'VARCHAR',
        'maker': 'VARCHAR', 'taker': 'VARCHAR',
        'maker_asset_id': 'VARCHAR', 'taker_asset_id': 'VARCHAR',
        'maker_amount_filled': 'VARCHAR', 'taker_amount_filled': 'VARCHAR',
        'maker_fee': 'VARCHAR', 'taker_fee': 'VARCHAR',
        'protocol_fee': 'VARCHAR',
    },
    'trades': {
        'timestamp': 'BIGINT', 'datetime': 'VARCHAR',
        'block_number': 'BIGINT', 'transaction_hash': 'VARCHAR',
        'contract': 'VARCHAR', 'event_id': 'VARCHAR',
        'event_slug': 'VARCHAR', 'event_title': 'VARCHAR',
        'market_id': 'VARCHAR', 'condition_id': 'VARCHAR',
        'question': 'VARCHAR', 'nonusdc_side': 'VARCHAR',
        'maker': 'VARCHAR', 'taker': 'VARCHAR',
        'maker_asset': 'VARCHAR', 'taker_asset': 'VARCHAR',
        'maker_direction': 'VARCHAR', 'taker_direction': 'VARCHAR',
        'price': 'DOUBLE', 'usd_amount': 'DOUBLE',
        'token_amount': 'DOUBLE', 'asset_id': 'VARCHAR',
        'order_hash': 'VARCHAR',
    },
    'markets': {
        'id': 'VARCHAR', 'question': 'VARCHAR',
        'answer1': 'VARCHAR', 'answer2': 'VARCHAR',
        'token1': 'VARCHAR', 'token2': 'VARCHAR',
        'condition_id': 'VARCHAR', 'neg_risk': 'BOOLEAN',
        'slug': 'VARCHAR', 'volume': 'VARCHAR',
        'created_at': 'VARCHAR', 'closed': 'BOOLEAN',
        'active': 'BOOLEAN', 'archived': 'BOOLEAN',
        'end_date': 'VARCHAR', 'outcome_prices': 'VARCHAR',
        'event_id': 'VARCHAR', 'event_slug': 'VARCHAR',
        'event_title': 'VARCHAR',
    },
    'quant': {
        'timestamp': 'BIGINT', 'datetime': 'VARCHAR',
        'block_number': 'BIGINT', 'transaction_hash': 'VARCHAR',
        'contract': 'VARCHAR', 'event_id': 'VARCHAR',
        'event_slug': 'VARCHAR', 'event_title': 'VARCHAR',
        'market_id': 'VARCHAR', 'condition_id': 'VARCHAR',
        'question': 'VARCHAR', 'nonusdc_side': 'VARCHAR',
        'maker': 'VARCHAR', 'taker': 'VARCHAR',
        'maker_asset': 'VARCHAR', 'taker_asset': 'VARCHAR',
        'maker_direction': 'VARCHAR', 'taker_direction': 'VARCHAR',
        'price': 'DOUBLE', 'usd_amount': 'DOUBLE',
        'token_amount': 'DOUBLE', 'asset_id': 'VARCHAR',
        'order_hash': 'VARCHAR',
    },
    'users': {
        'timestamp': 'BIGINT', 'datetime': 'VARCHAR',
        'block_number': 'BIGINT', 'transaction_hash': 'VARCHAR',
        'event_id': 'VARCHAR', 'market_id': 'VARCHAR',
        'condition_id': 'VARCHAR', 'user': 'VARCHAR',
        'role': 'VARCHAR', 'price': 'DOUBLE',
        'token_amount': 'DOUBLE', 'usd_amount': 'DOUBLE',
    },
}

TABLE_COLUMNS = {
    table: list(cols.keys()) for table, cols in TABLE_COLUMN_TYPES.items()
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

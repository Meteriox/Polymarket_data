# Data Directory

This directory stores all Polymarket data.

## Structure

```
data/
├── polymarket.duckdb          # DuckDB database (indexes + new data only)
├── orderfilled_part1.parquet  # Historical data (queried directly, not imported)
├── orderfilled_part2.parquet
├── ...
├── trades.parquet
├── markets.parquet
├── quant.parquet
├── users.parquet
├── dataset/                   # Alternative location for parquet files
└── data_clean/                # Alternative location for parquet files
```

## How It Works

Parquet files are **NOT imported** into DuckDB. Instead, DuckDB creates VIEWs
that query them directly at runtime. This avoids the massive memory/CPU cost
of importing 100+GB of data.

- Historical data: queried from parquet files via DuckDB VIEWs (zero import)
- New data: written to `_new` tables in DuckDB by the continuous fetcher
- API queries: hit unified VIEWs that UNION ALL both sources transparently

## Setup

1. Download parquet files from HuggingFace and place them here
2. Start the service: `docker compose up -d --build`
3. API docs: http://localhost:8000/docs

## DuckDB Tables

| Table | Description |
|-------|-------------|
| `orderfilled` | VIEW: parquet + orderfilled_new |
| `trades` | VIEW: parquet + trades_new |
| `markets` | VIEW: parquet + markets_new |
| `quant` | VIEW: parquet + quant_new |
| `users` | VIEW: parquet + users_new |

## Note

All files in this directory (except `.gitkeep` and this README) are ignored by git.

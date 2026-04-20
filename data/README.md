# Data Directory

This directory stores all Polymarket data.

## Structure

```
data/
├── polymarket.duckdb          # DuckDB database (main data store)
├── dataset/                   # Parquet files (for initial import)
│   ├── orderfilled.parquet
│   ├── trades.parquet
│   └── markets.parquet
├── data_clean/                # Cleaned parquet files (for initial import)
│   ├── quant.parquet
│   └── users.parquet
└── latest_result/             # CSV preview (latest 1000 records)
```

## Setup

1. Download parquet data from HuggingFace and place them in the directories above
2. Import into DuckDB: `docker compose run --rm import`
3. Start the service: `docker compose up -d --build`
4. API docs: http://localhost:8000/docs
5. Parquet files can be deleted after import to save space

## DuckDB Tables

| Table | Description |
|-------|-------------|
| `orderfilled` | Raw blockchain OrderFilled events |
| `trades` | Processed trades with market metadata |
| `markets` | Market information from Gamma API |
| `quant` | Clean market data (unified YES perspective) |
| `users` | User behavior data (maker/taker split) |

## Note

All files in this directory (except `.gitkeep` and this README) are ignored by git.

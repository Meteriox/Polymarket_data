# Polymarket Data

### Complete Data Infrastructure for Polymarket — Fetch, Process, Analyze

A comprehensive toolkit and dataset for Polymarket prediction markets. Fetch trading data directly from Polygon blockchain and Gamma API, process into multiple analysis-ready formats, and analyze with ease.

**Zhengjie Wang**1,2, **Leiyu Chao**1,3, **Yu Bao**1,4, **Lian Cheng**1,3, **Jianhan Liao**1,5, **Yikang Li**1,†

1Shanghai Innovation Institute    2Westlake University    3Shanghai Jiao Tong University  
4Harbin Institute of Technology    5Fudan University

†Corresponding author

---

## TL;DR

We provide **107GB of trading data** from Polymarket containing **1.1 billion records** across 268K+ markets, along with a complete toolkit to fetch, process, and analyze the data. Perfect for market research, behavioral studies, and quantitative analysis.

**Get all historical data before 2026**: Download the complete dataset from [HuggingFace](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data), or use this toolkit to fetch the latest data yourself.

## Highlights

- **Complete Data**: 1.1 billion trading records from Polymarket's inception to present
- **Direct Data Access**: Fetch data directly from Polygon blockchain, no third-party dependencies
- **Multiple Formats**: 5 analysis-ready datasets for different research needs
- **Real-time Updates**: Continuous mode to sync new data every 2 seconds
- **Fault-tolerant Import**: Row-group-level checkpoint, safe to interrupt and resume anytime
- **Query API**: FastAPI + DuckDB native tables with indexes, async non-blocking queries on 100GB+ data

## vs Third-party Data Sources


| Field                                | Polymarket Data | Third-party |
| ------------------------------------ | --------------- | ----------- |
| block_number                         | Yes             | No          |
| contract name                        | Yes             | No          |
| maker_fee / taker_fee / protocol_fee | Yes             | No          |
| order_hash                           | Yes             | No          |
| market_id (auto-linked)              | Yes             | Yes         |
| Missing token auto-fill              | Yes             | Yes         |


## Dataset Overview


| File                  | Size | Records | Description                                    |
| --------------------- | ---- | ------- | ---------------------------------------------- |
| `orderfilled.parquet` | 31GB | 293.3M  | Raw blockchain events from OrderFilled logs    |
| `trades.parquet`      | 32GB | 293.3M  | Processed trades with market metadata linkage  |
| `markets.parquet`     | 68MB | 268,706 | Market information and metadata                |
| `quant.parquet`       | 21GB | 170.3M  | Clean market data with unified YES perspective |
| `users.parquet`       | 23GB | 340.6M  | User behavior data split by maker/taker roles  |


**Total**: 107GB, 1.1 billion records

**Download from HuggingFace**: [SII-WANGZJ/Polymarket_data](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data)

## Use Cases

### Market Research & Analysis

- Study prediction market dynamics and price discovery mechanisms
- Analyze market efficiency and information aggregation
- Research crowd wisdom and forecasting accuracy

### Behavioral Studies

- Track individual user trading patterns and decision-making
- Study market participant behavior under different conditions
- Analyze risk preferences and trading strategies

### Data Science & Machine Learning

- Train models for price prediction and market forecasting
- Feature engineering for time-series analysis
- Develop algorithms for market analysis

### Academic Research

- Economics and finance research on prediction markets
- Social science studies on collective intelligence
- Computer science research on blockchain data analysis

## Quick Start (Docker Compose)

### Prerequisites

- Docker and Docker Compose installed
- ~120GB disk space (for parquet files + DuckDB database)

### 1. Clone and Configure

```bash
git clone https://github.com/SII-WANGZJ/Polymarket_data.git
cd Polymarket_data

# Optional: create environment config
cp .env.example .env
# Optional: edit .env to set ALCHEMY_API_KEY for faster RPC
```

### 2. Download Data

```bash
pip install huggingface_hub

# Download all parquet files (saved under data/)
hf download SII-WANGZJ/Polymarket_data --repo-type dataset --local-dir data
```

Parquet files (split files like `orderfilled_part1.parquet` are supported) are auto-detected in `data/`, `data/dataset/`, and `data/data_clean/`.

### 3. Import Data into DuckDB

All parquet data must be imported into native DuckDB tables before the service can query it. The import script handles this with configurable resource limits:

```bash
# Default settings (4GB memory, 4 threads, 0.3s sleep between row-groups)
docker compose run --rm --profile tools import

# Conservative for busy servers (uses less CPU/memory, takes longer)
docker compose run --rm --profile tools import --memory 2GB --threads 2 --sleep 1.0

# Reset and reimport everything from scratch
docker compose run --rm --profile tools import --reset
```

The import process:

- Imports data **row-group by row-group** with configurable pacing
- **Checkpoints** progress after each row-group — safe to interrupt and resume
- Creates **indexes** after all data is loaded for optimal query performance
- Handles `orderfilled_part*.parquet` split files automatically (deduplicates with `orderfilled.parquet`)
- Validates data integrity on resume — detects DB/state mismatches and reimports if needed

For 107GB of data with default settings, expect ~2–4 hours depending on disk speed.

### 4. Start the Service

```bash
docker compose up -d --build
```

The service:

- **Queries** DuckDB native tables with indexes (fast analytical queries)
- **Fetches** new blocks from Polygon every 2 seconds (appended to the same tables)
- **Serves** the query API on `http://localhost:8000`
- **Auto-restarts** if the process crashes

### 5. Use the API

```bash
# Service status
curl http://localhost:8000/api/status

# Interactive API docs (Swagger UI)
open http://localhost:8000/docs

# Query trades by market
curl "http://localhost:8000/api/trades?market_id=YOUR_MARKET_ID&limit=10"

# Search markets
curl "http://localhost:8000/api/markets?search=Trump"

# Price history
curl "http://localhost:8000/api/market/YOUR_MARKET_ID/price"

# User trades
curl "http://localhost:8000/api/user/0xADDRESS/trades"

# Custom SQL query
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT market_id, COUNT(*) as cnt, SUM(usd_amount) as volume FROM trades GROUP BY market_id ORDER BY volume DESC LIMIT 10"}'
```

### Common Operations

```bash
# View logs
docker compose logs -f

# Stop the service
docker compose down

# Restart
docker compose restart

# Rebuild after code changes
docker compose up -d --build

# Import (or re-import) parquet data into DuckDB
docker compose run --rm --profile tools import
```

### Environment Variables

Configure via `.env` file:


| Variable          | Default                          | Description                            |
| ----------------- | -------------------------------- | -------------------------------------- |
| `ALCHEMY_API_KEY` | (empty)                          | Alchemy API key for faster Polygon RPC |
| `POLYGON_RPC_URL` | `https://polygon.llamarpc.com`   | Custom RPC endpoint                    |
| `API_PORT`        | `8000`                           | Host port for the API server           |
| `BATCH_SIZE`      | `100`                            | Blocks per batch in catch-up mode      |
| `LOG_LEVEL`       | `INFO`                           | Log level: DEBUG, INFO, WARNING, ERROR |


## API Endpoints


| Method | Endpoint                     | Description                                                   |
| ------ | ---------------------------- | ------------------------------------------------------------- |
| GET    | `/api/status`                | Service status, latest block, table row counts                |
| GET    | `/api/trades`                | Query trades (filter by market_id, maker, taker, block range) |
| GET    | `/api/markets`               | Search markets by question text                               |
| GET    | `/api/market/{id}/price`     | Price history for a specific market                           |
| GET    | `/api/user/{address}/trades` | Trading history for a wallet address                          |
| POST   | `/api/query`                 | Custom SQL query (SELECT only, powered by DuckDB)             |
| GET    | `/docs`                      | Interactive Swagger UI documentation                          |


## Architecture

```
docker compose run import                 (one-time)
└── Import parquet → DuckDB native tables + indexes

docker compose up -d                      (long-running)
└── polymarket-data container
    ├── Fetcher Thread ── Polygon RPC ──→ INSERT INTO DuckDB
    ├── FastAPI Server ── ThreadPool ──→ DuckDB SELECT ──→ JSON API
    └── data/polymarket.duckdb (persistent volume)
```

- **Single container**: fetcher thread + API server in one process
- **DuckDB native tables**: all data stored in indexed DuckDB tables (not parquet views), enabling fast analytical queries on 100GB+ data
- **Async API**: queries run in a `ThreadPoolExecutor` via `asyncio.run_in_executor()`, keeping the uvicorn event loop non-blocking
- **Single connection**: one DuckDB connection shared by fetcher (short INSERT transactions) and API (read cursors), leveraging DuckDB MVCC for concurrent access
- **Volume mount**: `./data` persists DuckDB database and checkpoint state across container restarts
- **Auto-restart**: `restart: unless-stopped` policy
- **Health check**: Docker monitors `GET /` every 30 seconds (lightweight, no DB query)

## Project Structure

```
Polymarket_data/
├── polymarket/              # Core Python package
│   ├── api/                 # FastAPI query service (routes + app factory)
│   ├── cli/                 # Command-line interface
│   ├── db/                  # DuckDB storage layer
│   │   ├── engine.py        #   Connection management + async query executor
│   │   ├── schema.py        #   Table DDL, indexes, column definitions
│   │   └── import_parquet.py#   Batch import tool (row-group checkpoint)
│   ├── fetchers/            # Data fetchers (RPC, Gamma API)
│   ├── processors/          # Data processors (decoder, cleaner)
│   ├── tools/               # Utility tools (continuous fetcher)
│   └── service.py           # Unified entry point (fetcher + API)
├── data/                    # Data storage (gitignored)
│   ├── polymarket.duckdb    #   DuckDB database
│   └── import_state.json    #   Import checkpoint (auto-managed)
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md
```

## Data Schema

### OrderFilled Events (Raw)


| Field                                     | Description                                          |
| ----------------------------------------- | ---------------------------------------------------- |
| timestamp                                 | Unix timestamp                                       |
| block_number                              | Block number                                         |
| transaction_hash                          | Transaction hash                                     |
| contract                                  | Contract name (CTF_EXCHANGE or NEGRISK_CTF_EXCHANGE) |
| maker / taker                             | Trading parties' addresses                           |
| maker_asset_id / taker_asset_id           | Asset IDs                                            |
| maker_amount_filled / taker_amount_filled | Filled amounts                                       |
| maker_fee / taker_fee / protocol_fee      | Fees (in wei)                                        |
| order_hash                                | Order hash                                           |


### Trades (Processed)


| Field                             | Description                        |
| --------------------------------- | ---------------------------------- |
| market_id                         | Market ID (auto-linked from token) |
| answer                            | Option name (YES/NO/etc.)          |
| price                             | Trade price (0-1)                  |
| usd_amount / token_amount         | USDC and token amounts             |
| maker_direction / taker_direction | Buy/sell direction                 |


### quant.parquet - Clean Market Data

Filtered and normalized trade data with unified token perspective (YES token).

**Key Features:**

- Unified perspective: All trades normalized to YES token (token1)
- Clean data: Contract trades filtered out, only real user trades
- Complete information: Maker/taker roles preserved
- Best for: Market analysis, price studies, time-series forecasting

**Schema:**

```python
{
    'transaction_hash': str,      # Blockchain transaction hash
    'block_number': int,          # Block number
    'datetime': datetime,         # Transaction timestamp
    'market_id': str,             # Market identifier
    'maker': str,                 # Maker wallet address
    'taker': str,                 # Taker wallet address
    'token_amount': float,        # Amount of tokens traded
    'usd_amount': float,          # USD value
    'price': float,               # Trade price (0-1)
}
```

### users.parquet - User Behavior Data

Split maker/taker records with unified buy direction for user analysis.

**Key Features:**

- Split records: Each trade becomes 2 records (one maker, one taker)
- Unified direction: All converted to BUY (negative amounts = selling)
- User sorted: Ordered by user for trajectory analysis
- Best for: User profiling, PnL calculation, wallet analysis

**Schema:**

```python
{
    'transaction_hash': str,      # Transaction hash
    'block_number': int,          # Block number
    'datetime': datetime,         # Timestamp
    'market_id': str,             # Market identifier
    'user': str,                  # User wallet address
    'role': str,                  # 'maker' or 'taker'
    'token_amount': float,        # Signed amount (+ buy, - sell)
    'usd_amount': float,          # USD value
    'price': float,               # Trade price
}
```

### markets.parquet - Market Metadata

Market information and outcome token details.

**Best for:** Linking trades to market context, filtering by market attributes

See [DATA_DESCRIPTION.md](polymarket_data/DATA_DESCRIPTION.md) for complete schema documentation.

## Data Processing Pipeline

```
Polygon Blockchain (RPC)    Gamma API
         ↓                      ↓
  orderfilled.parquet    markets.parquet
         ↓
  trades.parquet (+ Market linkage)
         ↓
         ├─→ quant.parquet (Unified YES perspective)
         │   └─→ Filter contracts + Normalize tokens
         │
         └─→ users.parquet (Split maker/taker)
             └─→ Split records + Unified BUY direction
```

**Key Transformations:**

1. **quant.parquet**:
  - Filter out contract trades (keep only user trades)
  - Normalize all trades to YES token perspective
  - Preserve maker/taker information
  - Result: 170.3M records (from 293.3M)
2. **users.parquet**:
  - Split each trade into 2 records (maker + taker)
  - Convert all to BUY direction (signed amounts)
  - Sort by user for easy querying
  - Result: 340.6M records (from 293.3M × 2, some filtered)

## Example Analysis

### 1. Calculate Market Statistics

```python
import pandas as pd

df = pd.read_parquet('quant.parquet')

# Market-level statistics
market_stats = df.groupby('market_id').agg({
    'usd_amount': ['sum', 'mean'],     # Total volume and average trade size
    'price': ['mean', 'std', 'min', 'max'],  # Price statistics
    'transaction_hash': 'count'         # Number of trades
}).round(4)

print(market_stats.head())
```

### 2. Track Price Evolution

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('quant.parquet')
df['datetime'] = pd.to_datetime(df['datetime'])

# Select a specific market
market_id = 'your-market-id'
market_data = df[df['market_id'] == market_id].sort_values('datetime')

# Plot price over time
plt.figure(figsize=(12, 6))
plt.plot(market_data['datetime'], market_data['price'])
plt.title(f'Price Evolution - Market {market_id}')
plt.xlabel('Date')
plt.ylabel('Price')
plt.show()
```

### 3. Analyze User Behavior

```python
import pandas as pd

df = pd.read_parquet('users.parquet')

# Calculate net position per user per market
user_positions = df.groupby(['user', 'market_id']).agg({
    'token_amount': 'sum',          # Net position (positive = long, negative = short)
    'usd_amount': 'sum',            # Total USD traded
    'transaction_hash': 'count'     # Number of trades
}).reset_index()

# Find most active users
active_users = user_positions.groupby('user').agg({
    'market_id': 'count',           # Number of markets traded
    'usd_amount': 'sum'             # Total volume
}).sort_values('usd_amount', ascending=False)

print(active_users.head(10))
```

### 4. Market Volume Analysis

```python
import pandas as pd

df = pd.read_parquet('quant.parquet')
markets = pd.read_parquet('markets.parquet')

# Join with market metadata
df = df.merge(markets[['market_id', 'question']], on='market_id', how='left')

# Top markets by volume
top_markets = df.groupby(['market_id', 'question']).agg({
    'usd_amount': 'sum'
}).sort_values('usd_amount', ascending=False).head(20)

print(top_markets)
```

## Data Quality

- **Complete History**: No missing blocks or gaps in blockchain data
- **Verified Sources**: All OrderFilled events from 2 official exchange contracts
- **Blockchain Verified**: Cross-checked against Polygon RPC nodes
- **Regular Updates**: Automated daily pipeline for fresh data
- **Open Source**: Fully reproducible collection process

**Contracts Tracked:**

- Exchange Contract 1: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
- Exchange Contract 2: `0xC5d563A36AE78145C45a50134d48A1215220f80a`

## Local Development (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .

# Import parquet data into DuckDB (required before first run)
python -m polymarket.db.import_parquet

# Import with custom resource limits
python -m polymarket.db.import_parquet --memory 2GB --threads 2 --sleep 1.0

# Reset and reimport from scratch
python -m polymarket.db.import_parquet --reset

# Start service (fetcher + API)
python -m polymarket.service --port 8000

# Or API only / fetcher only
python -m polymarket.service --no-fetcher
python -m polymarket.service --fetcher-only
```

Import parameters:

| Parameter    | Default | Description                                             |
| ------------ | ------- | ------------------------------------------------------- |
| `--memory`   | `4GB`   | DuckDB memory limit during import                       |
| `--threads`  | `4`     | DuckDB thread count during import                       |
| `--sleep`    | `0.3`   | Sleep seconds between row-groups (throttle CPU/IO)      |
| `--reset`    | off     | Drop all tables and reimport from scratch               |

## Contributing

We welcome contributions to improve the dataset and tools:

1. **Report Issues**: Found bugs or data quality issues? [Open an issue](https://github.com/SII-WANGZJ/Polymarket_data/issues)
2. **Suggest Features**: Ideas for new features? Let us know!
3. **Contribute Code**: Improve our pipeline via pull requests

## License

MIT License - Free for commercial and research use.

See [LICENSE](LICENSE) file for details.

## Contact & Support

- **Email**: [wangzhengjie@sii.edu.cn](mailto:wangzhengjie@sii.edu.cn)
- **Issues**: [GitHub Issues](https://github.com/SII-WANGZJ/Polymarket_data/issues)
- **Dataset**: [HuggingFace](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data)

## Citation

If you use this dataset or toolkit in your research, please cite:

```bibtex
@misc{polymarket_data_2026,
  title={Polymarket Data: Complete Data Infrastructure for Polymarket},
  author={Wang, Zhengjie and Chao, Leiyu and Bao, Yu and Cheng, Lian and Liao, Jianhan and Li, Yikang},
  year={2026},
  howpublished={\url{https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data}},
  note={A comprehensive dataset and toolkit for Polymarket prediction markets}
}
```

## Acknowledgments

- **Polymarket** for building the leading prediction market platform
- **Polygon** for providing reliable blockchain infrastructure
- **HuggingFace** for hosting and distributing large datasets
- The open-source community for tools and libraries

## Disclaimer

This tool is for research and educational purposes. Users are responsible for complying with Polymarket's terms of service and applicable regulations.

---

**Built for the research and data science community**

[HuggingFace](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data) • [GitHub](https://github.com/SII-WANGZJ/Polymarket_data) • [Documentation](polymarket_data/DATA_DESCRIPTION.md)
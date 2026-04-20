#!/bin/bash
# Import parquet files into DuckDB via Docker Compose
#
# Usage:
#   ./scripts/import_data.sh                   # Import all parquet files
#   ./scripts/import_data.sh --skip-existing   # Skip tables that already have data

set -e

echo "=============================================="
echo "  Polymarket Data Import"
echo "=============================================="
echo ""

# Ensure .env exists
[ -f .env ] || cp .env.example .env

# Create data directories
mkdir -p data data/dataset data/data_clean

# Check for parquet files in common locations:
# - data/
# - data/dataset/
# - data/data_clean/
PARQUET_COUNT=$(python3 - <<'PY'
from pathlib import Path

dirs = [Path("data"), Path("data/dataset"), Path("data/data_clean")]
count = 0
seen = set()
for d in dirs:
    if not d.exists():
        continue
    for p in d.glob("*.parquet"):
        if p.is_file():
            rp = p.resolve()
            if rp not in seen:
                seen.add(rp)
                count += 1
print(count)
PY
)

if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo "No parquet files found in data/, data/dataset/, or data/data_clean/"
    echo ""
    echo "Download data first:"
    echo "  pip install huggingface_hub"
    echo "  hf download SII-WANGZJ/Polymarket_data --repo-type dataset --local-dir data/dataset"
    echo ""
    echo "Supported layouts:"
    echo "  1) Put all parquet files directly under data/"
    echo "  2) Split files between data/dataset and data/data_clean"
    echo ""
    echo "Then run this script again."
    exit 1
fi

echo "Found $PARQUET_COUNT parquet file(s)"
echo ""

# Run import via the import profile
docker compose run --rm import "$@"

echo ""
echo "Import complete."
echo ""
echo "Next steps:"
echo "  Start the service: ./scripts/continuous_start.sh"

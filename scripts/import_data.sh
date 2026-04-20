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
mkdir -p data/dataset data/data_clean

# Check for parquet files
PARQUET_COUNT=0
for dir in data/dataset data/data_clean; do
    if [ -d "$dir" ]; then
        COUNT=$(find "$dir" -name "*.parquet" -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')
        PARQUET_COUNT=$((PARQUET_COUNT + COUNT))
    fi
done

if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo "No parquet files found in data/dataset/ or data/data_clean/"
    echo ""
    echo "Download data first:"
    echo "  pip install huggingface_hub"
    echo "  hf download SII-WANGZJ/Polymarket_data --repo-type dataset --local-dir data/dataset"
    echo ""
    echo "Then move quant.parquet and users.parquet to data/data_clean/"
    echo "  mkdir -p data/data_clean"
    echo "  mv data/dataset/quant.parquet data/data_clean/"
    echo "  mv data/dataset/users.parquet data/data_clean/"
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

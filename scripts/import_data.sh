#!/bin/bash
# Verify parquet data availability and register DuckDB views
#
# Usage:
#   ./scripts/import_data.sh   # Verify data and show row counts

set -e

echo "=============================================="
echo "  Polymarket Data Verification"
echo "=============================================="
echo ""

# Create data directories
mkdir -p data logs

# Check for parquet files
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
    echo "  hf download SII-WANGZJ/Polymarket_data --repo-type dataset --local-dir data"
    echo ""
    echo "Then run this script again."
    exit 1
fi

echo "Found $PARQUET_COUNT parquet file(s)"
echo ""

docker compose run --rm --profile tools verify "$@"

echo ""
echo "Next steps:"
echo "  Start the service: docker compose up -d --build"

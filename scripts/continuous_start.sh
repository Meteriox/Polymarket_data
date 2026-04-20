#!/bin/bash
# Start Polymarket Data Service via Docker Compose

set -e

echo "=============================================="
echo "  Polymarket Data Service"
echo "=============================================="

# Check .env
if [ ! -f .env ]; then
    echo "Creating .env from .env.example ..."
    cp .env.example .env
fi

# Create data directories (for volume mount)
mkdir -p data/dataset data/data_clean logs

# Build and start
echo ""
echo "Starting service ..."
docker compose up -d --build

echo ""
echo "Service started:"
echo "  API:    http://localhost:${API_PORT:-8000}"
echo "  Docs:   http://localhost:${API_PORT:-8000}/docs"
echo "  Status: http://localhost:${API_PORT:-8000}/api/status"
echo ""
echo "Commands:"
echo "  View logs:   docker compose logs -f"
echo "  Stop:        ./scripts/continuous_stop.sh"
echo "  Import data: ./scripts/import_data.sh"

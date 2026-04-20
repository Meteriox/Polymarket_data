#!/bin/bash
# Stop Polymarket Data Service

set -e

echo "Stopping Polymarket Data Service ..."
docker compose down

echo "Service stopped."

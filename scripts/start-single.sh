#!/bin/bash

set -e

echo "Starting Spark Lab single-node..."

if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed. Please install Docker."
    exit 1
fi

docker compose -f docker/docker-compose.single.yml up -d

echo "Spark single-node is starting up..."
echo "This may take a minute..."

echo ""
echo "Components:"
echo "  - Spark:      single-node mode"
echo "  - Jupyter:     http://localhost:${JUPYTER_PORT:-8888}"
echo ""
echo "Access the following:"
echo "  - Jupyter Notebook:   http://localhost:${JUPYTER_PORT:-8888}"
echo "                Token: ${JUPYTER_TOKEN:-spark-lab-token}"
echo ""
echo "To stop:"
echo "  ./scripts/stop-single.sh"
echo ""
echo "Spark single-node is ready!"

#!/bin/bash

set -e

echo "Starting Spark Lab multi-node cluster..."

if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed. Please install Docker."
    exit 1
fi

docker compose -f docker/docker-compose.yml up -d

echo "Spark cluster is starting..."
echo "This may take a minute..."
echo ""
echo "Components:"
echo "  - Spark Master:  http://localhost:${SPARK_MASTER_UI_PORT:-8080}"
echo "  - Spark Connect:  sc://spark-master:${SPARK_CONNECT_PORT:-15002}"
echo "  - Jupyter:        http://localhost:${JUPYTER_PORT:-8888}"
echo "                   Token: ${JUPYTER_TOKEN:-spark-lab-token}"
echo ""
echo "To stop:"
echo "  ./scripts/stop-cluster.sh"
echo ""
echo "Spark cluster ready!"

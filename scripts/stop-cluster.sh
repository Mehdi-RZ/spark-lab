#!/bin/bash

set -e

echo "Stopping Spark Lab cluster..."

docker compose -f docker/docker-compose.yml down

echo "Spark cluster stopped."

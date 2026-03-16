#!/bin/bash

set -e

echo "Stopping Spark single-node..."

docker compose -f docker/docker-compose.single.yml down

echo "Spark single-node stopped."

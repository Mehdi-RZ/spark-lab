#!/bin/bash

set -e

SIZE="all"
BASE_PATH="/opt/spark/data"
KEEP_RUNNING="false"
STARTED_CLUSTER="false"
RUN_MODE="auto"

show_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -s, --size         Dataset size: very_small, small, medium, large, very_large, all (default: all)"
    echo "                     Note: large and very_large are generated on-demand due to time/space requirements"
    echo "  -b, --base-path    Output base path (default: /opt/spark/data)"
    echo "  -m, --mode         Run mode: auto, single, cluster (default: auto)"
    echo "  -k, --keep-running Keep cluster running after generation"
    echo "  -h, --help         Show this help"
    echo ""
    echo "Dataset sizes:"
    echo "  very_small: 100 users, 50 products, 100 orders (quick smoke tests)"
    echo "  small:      1K users, 500 products, 1K orders (basic learning)"
    echo "  medium:     10K users, 5K products, 10K orders (realistic queries)"
    echo "  large:      100K users, 50K products, 1M orders (performance testing)"
    echo "  very_large: 1M users, 500K products, 10M orders (scale testing)"
    echo ""
    echo "Example:"
    echo "  $0 --size small"
    echo "  $0 --size large  # Takes longer, generates 1M+ records"
    echo ""
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--size)
            SIZE="$2"
            shift 2
            ;;
        -b|--base-path)
            BASE_PATH="$2"
            shift 2
            ;;
        -m|--mode)
            RUN_MODE="$2"
            shift 2
            ;;
        -k|--keep-running)
            KEEP_RUNNING="true"
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed. Please install Docker."
    exit 1
fi

if [ "${RUN_MODE}" = "auto" ]; then
    if docker ps --format '{{.Names}}' | grep -qx "spark-single"; then
        RUN_MODE="single"
    elif docker ps --format '{{.Names}}' | grep -qx "spark-master"; then
        RUN_MODE="cluster"
    else
        RUN_MODE="cluster"
        echo "Starting Spark cluster..."
        docker compose -f docker/docker-compose.yml up -d
        STARTED_CLUSTER="true"
        echo "Waiting for cluster to be ready..."
        sleep 5
    fi
elif [ "${RUN_MODE}" = "single" ]; then
    if ! docker ps --format '{{.Names}}' | grep -qx "spark-single"; then
        echo "Error: spark-single is not running. Start it with ./scripts/start-single.sh"
        exit 1
    fi
elif [ "${RUN_MODE}" = "cluster" ]; then
    if ! docker ps --format '{{.Names}}' | grep -qx "spark-master"; then
        echo "Error: spark-master is not running. Start it with ./scripts/start-cluster.sh"
        exit 1
    fi
else
    echo "Error: unknown mode '${RUN_MODE}'. Use auto, single, or cluster."
    exit 1
fi

echo "Generating datasets (${SIZE}) using ${RUN_MODE} mode..."
if [ "${RUN_MODE}" = "single" ]; then
    docker exec -e SPARK_MASTER=local[*] spark-single /opt/spark/bin/spark-submit \
        --master local[*] \
        /opt/spark/apps/generate_data.py \
        --size "${SIZE}" \
        --base-path "${BASE_PATH}"
else
    docker exec -e SPARK_MASTER=spark://spark-master:7077 spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/apps/generate_data.py \
        --size "${SIZE}" \
        --base-path "${BASE_PATH}"
fi

echo ""
echo "✅ Datasets generated successfully!"
echo "Sample data location: ${BASE_PATH}"

if [ "${RUN_MODE}" = "cluster" ] && [ "${STARTED_CLUSTER}" = "true" ]; then
    if [ "${KEEP_RUNNING}" != "true" ]; then
        docker compose -f docker/docker-compose.yml down
        echo ""
        echo "Spark Lab environment stopped."
    else
        echo ""
        echo "Spark Lab environment still running."
    fi
fi

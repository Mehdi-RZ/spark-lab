# Spark Lab

A containerized, production-like Spark testing & learning environment.

## Prerequisites

- Docker
- Docker Compose
- Task: see [taskfile.dev](https://taskfile.dev/installation))

## Quick Start

```bash
# One-time setup
task setup

# Start single-node mode (recommended)
task single:start

# Run your first example
task submit JOB=examples/01-fundamentals/01_hello_spark.py

# Open Jupyter at http://localhost:8888 (token: spark-lab-token)
task jupyter:open
```

## Modes

| Mode | Start | Stop | UI |
|------|-------|------|----|
| Single-node | `task single:start` | `task single:stop` | localhost:4040 |
| Cluster | `task cluster:start` | `task cluster:stop` | localhost:8080 |

## Examples

| Directory | Topics |
|-----------|--------|
| `01-fundamentals/` | SparkSession, RDD, DataFrame, SQL |
| `02-data-processing/` | Map/filter, joins, aggregations, windows, skew |
| `03-performance/` | Caching, partitioning, broadcast, AQE |
| `04-advanced/` | UDFs, complex types, partitioning strategies, streaming |

## Jupyter

URL: http://localhost:8888 | Token: `spark-lab-token`

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").appName("MyApp").getOrCreate()
```

## Commands

```bash
task                  # Show all commands
task status           # Check what's running
task submit JOB=<path> # Submit a job
task generate         # Generate sample data (very_small/small/medium); SIZE=large for bigger sets
task test             # Run validation tests
task logs             # View logs
task stop             # Stop everything
```

## Configuration

Edit `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_VERSION` | 3.5.5 | Spark version |
| `SPARK_WORKER_MEMORY` | 2g | Worker memory |
| `SPARK_WORKER_CORES` | 2 | Worker cores |
| `JUPYTER_TOKEN` | spark-lab-token | Jupyter token |

## Project Structure

```
spark-lab/
├── Taskfile.yml           # All task definitions
├── docker/                # Docker Compose files
├── examples/              # 17 learning examples
├── scripts/               # Helper scripts
├── config/                # Spark configuration
├── notebooks/             # Jupyter notebooks
├── tests/                 # Validation tests
├── utils/                 # Shared utilities
└── docs/                  # Architecture, learning path, troubleshooting
```
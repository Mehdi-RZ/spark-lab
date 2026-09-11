#!/usr/bin/env bash
# =============================================================================
# Render config/spark-defaults.conf from config/spark-defaults.conf.template
# using values defined in .env.
#
# This keeps .env as the single source of truth for executor/app resources:
#   SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_OVERHEAD,
#   SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_MEMORY_OVERHEAD,
#   SPARK_EXECUTOR_CORES, SPARK_DEFAULT_PARALLELISM
#
# Usage:
#   ./scripts/render-config.sh        (or)   task config:render
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ENV_FILE="$ROOT/.env"
TEMPLATE="$ROOT/config/spark-defaults.conf.template"
OUTPUT="$ROOT/config/spark-defaults.conf"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run 'task setup' first." >&2
  exit 1
fi

if [ ! -f "$TEMPLATE" ]; then
  echo "ERROR: $TEMPLATE not found." >&2
  exit 1
fi

# Load .env, only if `envsubst` is available on this host
if ! command -v envsubst >/dev/null 2>&1; then
  echo "ERROR: envsubst is required but not found (install gettext)." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source "$ENV_FILE"
set +a

envsubst \
  '${SPARK_DRIVER_MEMORY} ${SPARK_DRIVER_MEMORY_OVERHEAD} ${SPARK_EXECUTOR_MEMORY} ${SPARK_EXECUTOR_MEMORY_OVERHEAD} ${SPARK_EXECUTOR_CORES} ${SPARK_DEFAULT_PARALLELISM}' \
  < "$TEMPLATE" > "$OUTPUT"

echo "Rendered $OUTPUT from $TEMPLATE"

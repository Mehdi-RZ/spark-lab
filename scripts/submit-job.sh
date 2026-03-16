#!/bin/bash
# =============================================================================
# Spark Job Submission Script with Logging
# =============================================================================
# Submits PySpark jobs to running Spark containers and captures output to files.
# Logs are organized by date: logs/app-logs/YYYY-MM-DD/HHMMSS_jobname.log
#
# Usage:
#   ./scripts/submit-job.sh [options] <script-path>
#
# Options:
#   -m, --master-url   Spark master URL (default: auto-detect)
#   -c, --container    Container name (default: auto-detect)
#   -h, --help         Show this help
#
# Examples:
#   ./scripts/submit-job.sh apps/01-fundamentals/01_hello_spark.py
#   ./scripts/submit-job.sh -m spark://spark-master:7077 apps/my_job.py
#   ./scripts/submit-job.sh -c spark-single apps/my_job.py
#
# Log files are saved to: logs/app-logs/YYYY-MM-DD/HHMMSS_jobname.log
# =============================================================================

set -e

# Configuration
LOG_BASE_DIR="./logs/app-logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    echo ""
    echo "Spark Job Submission Script with Logging"
    echo ""
    echo "Usage: $0 [options] <script-path>"
    echo ""
    echo "Options:"
    echo "  -m, --master-url   Spark master URL (default: auto-detect)"
    echo "  -c, --container    Container name (default: auto-detect)"
    echo "  -h, --help         Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 apps/01-fundamentals/01_hello_spark.py"
    echo "  $0 -m spark://spark-master:7077 apps/my_job.py"
    echo ""
    echo "Logs are saved to: ${LOG_BASE_DIR}/YYYY-MM-DD/HHMMSS_jobname.log"
    echo ""
}

# Extract job name from script path
# Example: apps/01-fundamentals/01_hello_spark.py -> 01_hello_spark
extract_job_name() {
    local script_path="$1"
    basename "$script_path" .py
}

# Detect running Spark container
detect_container() {
    if docker ps --format '{{.Names}}' | grep -qx "spark-single"; then
        echo "spark-single"
    elif docker ps --format '{{.Names}}' | grep -qx "spark-master"; then
        echo "spark-master"
    else
        echo ""
    fi
}

# Detect master URL based on container
detect_master_url() {
    local container="$1"
    case "$container" in
        spark-single)
            echo "local[*]"
            ;;
        spark-master)
            echo "spark://spark-master:7077"
            ;;
        *)
            echo "local[*]"
            ;;
    esac
}

# Normalize script path for container
# Remove 'examples/' prefix if present, since examples/ is mounted at /opt/spark/apps
normalize_script_path() {
    local script_path="$1"
    # Remove leading 'examples/' if present
    if [[ "$script_path" == examples/* ]]; then
        echo "${script_path#examples/}"
    else
        echo "$script_path"
    fi
}

# =============================================================================
# Parse Arguments
# =============================================================================

MASTER_URL=""
CONTAINER=""
SCRIPT_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--master-url)
            MASTER_URL="$2"
            shift 2
            ;;
        -c|--container)
            CONTAINER="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        -*)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            SCRIPT_PATH="$1"
            shift
            ;;
    esac
done

# Validate script path
if [ -z "$SCRIPT_PATH" ]; then
    log_error "Script path is required"
    show_usage
    exit 1
fi

# Check Docker is available
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed. Please install Docker."
    exit 1
fi

# Auto-detect container if not specified
if [ -z "$CONTAINER" ]; then
    CONTAINER=$(detect_container)
    if [ -z "$CONTAINER" ]; then
        log_error "No Spark container is running"
        echo "Start one with:"
        echo "  task single:start    (for single-node mode)"
        echo "  task cluster:start   (for cluster mode)"
        exit 1
    fi
fi

# Auto-detect master URL if not specified
if [ -z "$MASTER_URL" ]; then
    MASTER_URL=$(detect_master_url "$CONTAINER")
fi

# Normalize script path (remove 'examples/' prefix if present)
NORMALIZED_PATH=$(normalize_script_path "$SCRIPT_PATH")

# =============================================================================
# Setup Logging
# =============================================================================

JOB_NAME=$(extract_job_name "$SCRIPT_PATH")
DATE_STAMP=$(date +%Y-%m-%d)
TIME_STAMP=$(date +%H%M%S)
DATE_DIR="${LOG_BASE_DIR}/${DATE_STAMP}"
LOG_FILE="${DATE_DIR}/${TIME_STAMP}_${JOB_NAME}.log"

# Create log directory
mkdir -p "$DATE_DIR" 2>/dev/null || {
    log_warning "Could not create log directory: $DATE_DIR"
    log_warning "Logs will only be shown in terminal"
    LOG_FILE=""
}

# =============================================================================
# Print Job Information
# =============================================================================

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}  Spark Job Submission${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo -e "  Job:       ${GREEN}${JOB_NAME}${NC}"
echo -e "  Script:    ${SCRIPT_PATH}"
echo -e "  Container: ${CONTAINER}"
echo -e "  Master:    ${MASTER_URL}"
echo -e "  Started:   $(date '+%Y-%m-%d %H:%M:%S')"
if [ -n "$LOG_FILE" ]; then
    echo -e "  Log File:  ${LOG_FILE}"
else
    echo -e "  Log File:  ${YELLOW}Terminal only${NC}"
fi
echo ""
echo -e "${CYAN}========================================${NC}"
echo ""

# =============================================================================
# Execute Spark Submit
# =============================================================================

log_info "Submitting job to Spark..."
echo ""

# Build the spark-submit command
# The script path inside container is /opt/spark/apps/<normalized_path>
CONTAINER_SCRIPT_PATH="/opt/spark/apps/${NORMALIZED_PATH}"

# Run spark-submit inside container with output capture
if [ -n "$LOG_FILE" ]; then
    # Capture output to file while showing in terminal
    set +e
    docker exec "$CONTAINER" /opt/spark/bin/spark-submit \
        --master "$MASTER_URL" \
        --conf "spark.app.name=${JOB_NAME}" \
        "$CONTAINER_SCRIPT_PATH" 2>&1 | tee "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
else
    # No file capture
    set +e
    docker exec "$CONTAINER" /opt/spark/bin/spark-submit \
        --master "$MASTER_URL" \
        --conf "spark.app.name=${JOB_NAME}" \
        "$CONTAINER_SCRIPT_PATH"
    EXIT_CODE=$?
    set -e
fi

# =============================================================================
# Print Completion Status
# =============================================================================

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}  Execution Complete${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo -e "  Finished:  $(date '+%Y-%m-%d %H:%M:%S')"

if [ $EXIT_CODE -eq 0 ]; then
    log_success "Job completed successfully"
    if [ -n "$LOG_FILE" ]; then
        echo -e "  Log saved:  ${GREEN}${LOG_FILE}${NC}"
    fi
else
    log_error "Job failed with exit code: ${EXIT_CODE}"
    if [ -n "$LOG_FILE" ]; then
        echo -e "  Check log:  ${LOG_FILE}"
    fi
fi

echo ""
echo -e "${CYAN}========================================${NC}"

exit $EXIT_CODE

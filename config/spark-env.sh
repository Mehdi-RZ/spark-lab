#!/bin/bash
# ============================================================================
# SPARK ENVIRONMENT CONFIGURATION - spark-env.sh
# ============================================================================
#
# This script sets environment variables that configure Spark's runtime behavior.
# It's sourced by Spark's launch scripts before starting the JVM.
#
# DOCUMENTATION FORMAT:
#   Each variable is documented with:
#     WHAT:   What the variable controls
#     WHY:    Why this value was chosen for the lab environment
#     HOW:    How Spark uses this variable
#     IMPACT: Effect on cluster behavior
#     PROD:   How this would differ in production
#
# PRECEDENCE (lowest to highest):
#   1. spark-env.sh (this file)
#   2. Environment variables (export VAR=value)
#   3. Command-line flags
#
# SYNTAX:
#   export VAR=${VAR:-default}  # Use $VAR if set, otherwise use default
#
# ============================================================================


# ============================================================================
# SPARK MASTER CONFIGURATION
# ============================================================================
# The Spark Master is the central coordinator in cluster mode:
#   - Manages worker registration
#   - Schedules applications
#   - Tracks resource availability
#   - Provides the Master UI
#
# In single-node mode, these are not used (master URL is local[*])
# ============================================================================

# WHAT:   Hostname where the Spark Master runs
# WHY:    "spark-master" is the Docker service name, resolvable via Docker DNS
# HOW:    Workers use this to connect to the master
#         spark://spark-master:7077 is the full master URL
# IMPACT: Must match Docker service name in docker-compose.yml
#         Wrong value = workers cannot connect to master
# PROD:   Typically a hostname like "spark-master.prod.internal"
#         Or IP address in static cluster setups
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-spark-master}

# WHAT:   Port for Spark Master RPC communication
# WHY:    7077 is Spark's default master port
# HOW:    Workers connect to this port to register and receive tasks
#         Applications connect to submit jobs
# IMPACT: Must match exposed port in docker-compose.yml
#         Must not conflict with other services
# PROD:   Usually 7077. May use different port if running multiple masters
#         (e.g., for multiple Spark clusters on same network)
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}

# WHAT:   Port for the Spark Master Web UI
# WHY:    8080 is Spark's default master UI port
# HOW:    Access the Master UI at http://master-host:8080
#         Shows: registered workers, running apps, cluster resources
# IMPACT: Must be exposed in docker-compose.yml for external access
#         Conflicts with common web ports (may need to change)
# PROD:   Often behind reverse proxy. May use different port
#         Common alternatives: 18080, 8081, etc.
export SPARK_MASTER_UI_PORT=${SPARK_MASTER_UI_PORT:-8080}

# WHAT:   Port for Spark Connect server (modern client protocol)
# WHY:    15002 is a non-conflicting port for Spark Connect
# HOW:    Spark Connect allows thin clients to connect to remote Spark
#         More efficient than traditional spark-submit for interactive use
# IMPACT: Enables modern Spark Connect protocol
#         Required for some IDE integrations and remote clients
# PROD:   Becoming more common. Port choice depends on environment
#         Default is often 15002
export SPARK_CONNECT_PORT=${SPARK_CONNECT_PORT:-15002}


# ============================================================================
# SPARK WORKER CONFIGURATION
# ============================================================================
# Workers are JVM processes that:
#   - Register with the master
#   - Run executors for submitted applications
#   - Report resource usage and status
#   - Provide individual worker UIs
#
# Each worker can run multiple executors (one per application)
# ============================================================================

# WHAT:   Number of CPU cores each worker offers to the cluster
# WHY:    2 cores per worker is reasonable for a lab environment
#         Allows running 2 workers on a typical dev machine without overload
# HOW:    Worker advertises this many cores to the master
#         Master schedules executors up to this limit
# IMPACT: More cores = more parallelism but more resource contention
#         Fewer cores = limited parallelism
# PROD:   Typically 4-16 cores per worker depending on machine size
#         Rule: Leave 1-2 cores for OS and other processes
#         Example: 16-core machine → 14-15 cores for worker
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}

# WHAT:   Memory each worker offers to the cluster
# WHY:    2GB per worker is safe for lab environment
#         Allows running multiple workers on typical dev machines
# HOW:    Worker advertises this much memory to master
#         Executors are allocated from this pool
# IMPACT: More memory = larger datasets, more caching
#         Less memory = more spilling, smaller datasets
# PROD:   Typically 8-64GB per worker
#         Rule: Leave 20-30% for OS and overhead
#         Example: 64GB machine → 48-52GB for worker
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-2g}

# WHAT:   Port for worker RPC communication
# WHY:    8881 is the first worker port (8882 for second worker, etc.)
# HOW:    Master connects to this port to send task assignments
#         Worker uses this for internal communication
# IMPACT: Each worker needs a unique port
#         Must not conflict between workers
# PROD:   Usually auto-assigned or sequential (8881, 8882, ...)
#         Not typically exposed externally
export SPARK_WORKER_PORT=${SPARK_WORKER_PORT:-8881}

# WHAT:   Port for the worker Web UI
# WHY:    8081 is the first worker UI port (8082 for second, etc.)
# HOW:    Access worker UI at http://worker-host:8081
#         Shows: executors running, memory usage, logs
# IMPACT: Each worker needs unique UI port
#         Must be exposed in docker-compose for external access
# PROD:   Often not exposed externally
#         Accessed via SSH tunnel or internal network only
export SPARK_WORKER_UI_PORT=${SPARK_WORKER_UI_PORT:-8081}


# ============================================================================
# SPARK HISTORY SERVER CONFIGURATION
# ============================================================================
# The History Server reads event logs and provides UIs for completed apps.
# It's a separate process that runs independently of Spark applications.
# ============================================================================

# WHAT:   Port for the History Server Web UI
# WHY:    18080 is Spark's default history server port
# HOW:    Access at http://history-server-host:18080
#         Shows list of completed applications with full UI
# IMPACT: Must be exposed in docker-compose for external access
#         Allows viewing job history after apps terminate
# PROD:   Same port typically. Often behind reverse proxy
#         May run on dedicated server for large organizations
export SPARK_HISTORY_PORT=${SPARK_HISTORY_PORT:-18080}


# ============================================================================
# EVENT LOG CONFIGURATION
# ============================================================================
# Event logs are written by running applications and read by History Server.
# This path must be accessible to both the application and History Server.
# ============================================================================

# WHAT:   Directory where Spark writes event logs
# WHY:    /opt/spark/logs is our mounted logs directory
#         Accessible from all containers via Docker volume
# HOW:    Driver writes JSON events here
#         History Server reads from here
# IMPACT: Must be a shared, persistent location
#         In Docker, this is a mounted volume
# PROD:   Typically HDFS or S3 for durability and sharing
#         hdfs:///spark/history or s3a://bucket/spark-logs
export SPARK_EVENTLOG_DIR=${SPARK_EVENTLOG_DIR:-/opt/spark/logs}


# ============================================================================
# PYTHON / PYSPARK CONFIGURATION
# ============================================================================
# PySpark requires Python on both driver and executors.
# These settings ensure consistent Python versions across the cluster.
# ============================================================================

# WHAT:   Python executable for executor worker processes
# WHY:    python3 ensures Python 3.x (Python 2 is EOL)
# HOW:    When UDFs run, this Python interpreter executes them
# IMPACT: Must be installed on all worker nodes
#         Must match driver Python version for serialization
# PROD:   Often points to virtualenv or conda environment
#         Example: /opt/conda/envs/pyspark/bin/python
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3}

# WHAT:   Python executable for the driver process
# WHY:    Should match PYSPARK_PYTHON for consistency
# HOW:    Your main script runs with this Python interpreter
# IMPACT: Mismatch with executor Python causes serialization errors
# PROD:   Same as PYSPARK_PYTHON for consistency
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3}


# ============================================================================
# JVM CONFIGURATION
# ============================================================================
# Spark runs on the JVM, so Java options affect all Spark processes.
# These settings control heap size, garbage collection, and more.
# ============================================================================

# WHAT:   JVM options for Spark processes
# WHY:    -Xms512m sets initial heap (faster startup)
#         -Xmx2g sets max heap (prevents runaway memory usage)
# HOW:    Applied to driver, master, worker, and executor JVMs
# IMPACT: Lower values = less memory but faster startup
#         Higher values = more memory but slower GC
# PROD:   Typically much larger heaps:
#         - Driver: -Xms4g -Xmx8g
#         - Executors: Set via spark.executor.memory
#         - Workers/Master: -Xms1g -Xmx2g
#         
#         Also add GC tuning:
#         -XX:+UseG1GC
#         -XX:MaxGCPauseMillis=200
#         -XX:InitiatingHeapOccupancyPercent=35
export JAVA_OPTS="-Xms512m -Xmx2g"


# ============================================================================
# ADDITIONAL PRODUCTION CONFIGURATIONS (COMMENTED)
# ============================================================================
# Uncomment and adjust these for production deployments

# --- Spark Master High Availability (ZooKeeper) ---
# export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.zookeeper.url=zk1:2181,zk2:2181,zk3:2181"

# --- Worker directories (for spill and shuffle) ---
# export SPARK_WORKER_DIR=/var/spark/work
# export SPARK_LOCAL_DIRS=/var/spark/shuffle

# --- Logging configuration ---
# export SPARK_LOG_DIR=/var/log/spark

# --- PID file location ---
# export SPARK_PID_DIR=/var/run/spark

# --- Python driver memory (for PySpark) ---
# export PYSPARK_DRIVER_PYTHON_OPTS="memory_limit=4g"

# ============================================================================
# END OF CONFIGURATION
# ============================================================================

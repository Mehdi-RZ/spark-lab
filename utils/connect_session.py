"""
Spark Connect URL Helper
========================

Resolves the Spark Connect endpoint for the Spark Lab cluster.

Notebooks are THIN CLIENTS: they create a regular SparkSession pointing at
the cluster via Spark Connect. All Spark computation happens on the cluster
side (spark-single in single-node mode, spark-master + workers in cluster
mode); the notebook process runs no Spark engine of its own.

Usage (the full, explicit way — nothing hidden):
    >>> import sys
    >>> sys.path.insert(0, "/opt/spark")
    >>>
    >>> from utils.connect_session import get_connect_url
    >>> from pyspark.sql import SparkSession
    >>>
    >>> spark = (
    ...     SparkSession.builder
    ...     .appName("My-Notebook")
    ...     .remote(get_connect_url())   # sc://spark-master:15002
    ...     .getOrCreate()
    ... )

Important:
    - File paths are resolved SERVER-side, so always use the cluster's
      paths (/opt/spark/data/...), not Jupyter-local ones.
    - spark.stop() only closes the client connection; the Spark Connect
      server keeps running in the cluster container.
    - The pyspark client version must match the server's SPARK_VERSION.
      This is enforced by docker/Dockerfile.jupyter.

Environment Variables:
    SPARK_CONNECT_HOST: Spark Connect server hostname (default: spark-single)
    SPARK_CONNECT_PORT: Spark Connect server port (default: 15002)
"""

import os


def get_connect_url() -> str:
    """
    Build the Spark Connect URL from environment variables.

    Docker Compose sets SPARK_CONNECT_HOST to spark-single (single-node
    mode) or spark-master (cluster mode); SPARK_CONNECT_PORT defaults
    to 15002.

    Returns:
        Connect URL in the form "sc://<host>:<port>"
    """
    host = os.getenv("SPARK_CONNECT_HOST", "spark-single")
    port = os.getenv("SPARK_CONNECT_PORT", "15002")
    return f"sc://{host}:{port}"

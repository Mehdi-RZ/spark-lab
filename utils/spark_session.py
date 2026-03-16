"""
Spark Session Manager Module
============================

This module provides centralized SparkSession creation and management
for the Spark Lab environment. It handles different deployment modes
(local vs cluster) and provides consistent configuration.

Usage Examples:
    >>> from utils.spark_session import create_spark_session
    >>>
    >>> # For cluster mode
    >>> spark = create_spark_session(
    ...     app_name="MyApp",
    ...     master="spark://spark-master:7077"
    ... )
    >>>
    >>> # For Spark Connect (modern protocol)
    >>> spark = create_spark_session(use_spark_connect=True)
    >>>
    >>> # For local development
    >>> spark = create_spark_session(master="local[*]")

Production Notes:
    - Always use create_spark_session() instead of manual builder
    - This ensures consistent configuration
    - Handles proper cleanup and error handling
"""

import os
import logging
from typing import Dict, Optional, Union
from pyspark.sql import SparkSession

# Configure module-level logging
logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    use_spark_connect: bool = False,
    config: Optional[Dict[str, Union[str, int, float, bool]]] = None,
) -> SparkSession:
    """
    Create and configure a SparkSession with best practices.

    This function provides a centralized way to create SparkSessions
    with production-ready defaults. It supports multiple deployment
    modes and ensures consistent configuration.

    Args:
        app_name: Name of the Spark application. If not provided,
                 uses environment variable APP_NAME or defaults to "Spark-Lab".
        master: Spark master URL. If not provided and not using Spark Connect,
               uses environment variable SPARK_MASTER or defaults to
               "spark://spark-master:7077".
        use_spark_connect: If True, uses Spark Connect protocol instead
                          of traditional cluster connection. This is the
                          modern recommended approach for client applications.
        config: Dictionary of additional Spark configuration properties.
               These will override any defaults. Example:
               {"spark.sql.shuffle.partitions": "100"}

    Returns:
        Configured SparkSession instance

    Raises:
        Exception: If SparkSession creation fails

    Examples:
        Basic usage with defaults:
        >>> spark = create_spark_session()

        Custom application name:
        >>> spark = create_spark_session(app_name="Data-Analysis")

        Using Spark Connect (recommended for Jupyter/interactive):
        >>> spark = create_spark_session(use_spark_connect=True)

        Local mode for testing:
        >>> spark = create_spark_session(master="local[*]")

        With custom configuration:
        >>> spark = create_spark_session(
        ...     app_name="MyApp",
        ...     config={"spark.sql.shuffle.partitions": "50"}
        ... )

    Environment Variables:
        The following environment variables are used if parameters not provided:
        - APP_NAME: Default application name
        - SPARK_MASTER: Default master URL
        - SPARK_CONNECT_HOST: Spark Connect server hostname
        - SPARK_CONNECT_PORT: Spark Connect server port (default: 15002)

    Best Practices:
        1. Always provide a meaningful app_name for monitoring
        2. Use Spark Connect for interactive development
        3. Set appropriate shuffle partitions based on data size
        4. Enable AQE (Adaptive Query Execution) for better performance
    """
    # Determine application name (priority: parameter > env > default)
    app_name = app_name or os.getenv("APP_NAME", "Spark-Lab")

    # Initialize configuration dictionary
    config = config or {}

    # Determine master URL based on mode
    if use_spark_connect:
        # Spark Connect: Modern gRPC-based protocol
        # Benefits: Better for interactive work, client-only libraries
        spark_connect_host = os.getenv("SPARK_CONNECT_HOST", "spark-master")
        spark_connect_port = os.getenv("SPARK_CONNECT_PORT", "15002")
        master = f"sc://{spark_connect_host}:{spark_connect_port}"
        logger.info(f"Using Spark Connect: {master}")
    elif master is None:
        # Traditional cluster connection
        # Default to local mode if SPARK_MASTER not set
        master = os.getenv("SPARK_MASTER", "local[*]")
        logger.info(f"Using traditional cluster connection: {master}")

    # Ensure directories exist for Spark housekeeping
    # These are used for:
    # - Event logs (for debugging and history server)
    # - Warehouse directory (for Spark SQL tables)
    log_dir = "/opt/spark/logs"
    warehouse_dir = "/opt/spark/data/warehouse"
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(warehouse_dir, exist_ok=True)

    # Build SparkSession with production defaults
    builder = SparkSession.builder.appName(app_name).master(master)

    # Apply custom configurations (if any)
    for key, value in config.items():
        builder = builder.config(key, value)

    # Essential production configurations
    # These optimize performance and enable monitoring

    # 1. SQL warehouse location for managed tables
    builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    # 2. Event logging for debugging and history server
    builder = builder.config("spark.eventLog.enabled", "true")
    builder = builder.config("spark.eventLog.dir", log_dir)

    # 3. Kryo serialization (faster than Java serialization)
    builder = builder.config(
        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    )

    # Create the session
    spark = builder.getOrCreate()

    # Reduce log verbosity (WARN instead of INFO)
    # This makes output more readable for learning
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Created SparkSession: {app_name} on {master}")

    return spark


def get_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    use_spark_connect: bool = False,
    config: Optional[Dict[str, Union[str, int, float, bool]]] = None,
) -> SparkSession:
    """
    Alias for create_spark_session().

    Provides a simpler function name for common use cases.
    See create_spark_session() for full documentation.

    Args:
        app_name: Application name
        master: Master URL
        use_spark_connect: Use Spark Connect protocol
        config: Additional configuration

    Returns:
        Configured SparkSession

    Example:
        >>> from utils.spark_session import get_spark_session
        >>> spark = get_spark_session(app_name="Quick-Job")
    """
    return create_spark_session(
        app_name=app_name,
        master=master,
        use_spark_connect=use_spark_connect,
        config=config,
    )


def stop_spark_session(spark: SparkSession) -> None:
    """
    Gracefully stop a SparkSession.

    Always use this function to stop SparkSessions to ensure
    proper cleanup of resources.

    Args:
        spark: SparkSession to stop

    Example:
        >>> spark = create_spark_session()
        >>> try:
        ...     # Your code here
        ...     pass
        ... finally:
        ...     stop_spark_session(spark)

    Note:
        In production, use try/finally blocks to ensure cleanup:

        try:
            spark = create_spark_session()
            result = process_data(spark)
        finally:
            if spark:
                stop_spark_session(spark)
    """
    if spark:
        app_name = spark.sparkContext.appName
        spark.stop()
        logger.info(f"Stopped SparkSession: {app_name}")


def create_local_spark_session(
    app_name: Optional[str] = None,
    config: Optional[Dict[str, Union[str, int, float, bool]]] = None,
) -> SparkSession:
    """
    Create a local-mode SparkSession (for testing/development).

    This is a convenience function that creates a SparkSession
    running in local mode on all available cores. Useful for:
    - Unit testing
    - Development and debugging
    - Small data processing
    - Learning Spark basics

    Args:
        app_name: Application name (defaults to "Local-Spark")
        config: Additional configuration

    Returns:
        Local-mode SparkSession

    Example:
        >>> from utils.spark_session import create_local_spark_session
        >>>
        >>> # Perfect for testing
        >>> spark = create_local_spark_session(app_name="Unit-Test")
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        >>> print(df.count())
        2
        >>> stop_spark_session(spark)

    Performance Notes:
        - Uses local[*] which utilizes all CPU cores
        - No network overhead (single JVM)
        - Fast startup and shutdown
        - Not suitable for large datasets (> 1GB)
    """
    return create_spark_session(
        app_name=app_name or "Local-Spark",
        master="local[*]",
        use_spark_connect=False,
        config=config,
    )


def get_spark_info(spark: SparkSession) -> Dict[str, str]:
    """
    Get information about the current SparkSession.

    Useful for debugging and monitoring. Returns key information
    about the Spark environment.

    Args:
        spark: Active SparkSession

    Returns:
        Dictionary with Spark information:
        - version: Spark version
        - app_name: Application name
        - master: Master URL
        - default_parallelism: Default parallelism level
        - executor_memory: Memory per executor
        - executor_cores: Cores per executor

    Example:
        >>> spark = create_spark_session()
        >>> info = get_spark_info(spark)
        >>> print(f"Running Spark {info['version']} on {info['master']}")
    """
    sc = spark.sparkContext

    return {
        "version": spark.version,
        "app_name": sc.appName,
        "master": sc.master,
        "default_parallelism": str(sc.defaultParallelism),
        "executor_memory": spark.conf.get("spark.executor.memory", "Not set"),
        "executor_cores": spark.conf.get("spark.executor.cores", "Not set"),
    }


def print_spark_info(spark: SparkSession) -> None:
    """
    Print formatted information about the SparkSession.

    Convenience function to display Spark environment details
    in a readable format.

    Args:
        spark: Active SparkSession

    Example:
        >>> spark = create_spark_session()
        >>> print_spark_info(spark)
        ╔════════════════════════════════════════╗
        ║         Spark Environment              ║
        ╚════════════════════════════════════════╝
        Version: 3.5.5
        App Name: MyApp
        Master: local[*]
        ...
    """
    info = get_spark_info(spark)

    print("\n" + "=" * 50)
    print("         Spark Environment              ")
    print("=" * 50)
    print(f"Version:     {info['version']}")
    print(f"App Name:    {info['app_name']}")
    print(f"Master:      {info['master']}")
    print(f"Parallelism: {info['default_parallelism']}")
    print(f"Exec Memory: {info['executor_memory']}")
    print(f"Exec Cores:  {info['executor_cores']}")
    print("=" * 50 + "\n")


# For testing the module directly
if __name__ == "__main__":
    print("Testing Spark Session Manager...")

    # Test local session creation
    print("\n1. Testing local session...")
    spark = create_local_spark_session(app_name="Test-Local")
    print_spark_info(spark)

    # Test basic operation
    print("\n2. Testing basic operation...")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    print(f"Created DataFrame with {df.count()} rows")

    # Cleanup
    print("\n3. Cleaning up...")
    stop_spark_session(spark)
    print("✓ All tests passed")

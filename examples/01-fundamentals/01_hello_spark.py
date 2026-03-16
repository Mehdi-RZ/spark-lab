"""
Hello Spark - Your First Spark Application
============================================

This is the first example in the Spark Lab learning path. It demonstrates:
- Creating a SparkSession
- Understanding Spark environment information
- Creating a simple DataFrame
- Basic Spark operations
- Proper resource cleanup

LEARNING OBJECTIVES:
- Understand SparkSession as the entry point
- Learn to check Spark configuration
- See the difference between transformations and actions
- Practice proper cleanup patterns

PREREQUISITES:
- Spark Lab environment running (task local:start)
- Basic Python knowledge

DOCUMENTATION:
- SparkSession: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession
- Spark Architecture: docs/architecture.md
"""

import sys
from typing import Optional

# Add the parent directory to Python path for importing utilities
# This allows us to use the centralized SparkSession manager
sys.path.insert(0, "/opt/spark/apps")

from pyspark.sql import SparkSession
from utils.spark_session import (
    create_spark_session,
    stop_spark_session,
    print_spark_info,
)


def demonstrate_spark_session_creation() -> SparkSession:
    """
    Demonstrate creating a SparkSession with proper configuration.

    The SparkSession is the entry point for all Spark functionality.
    It's a unified interface for working with structured data (DataFrames),
    SQL, and streaming.

    Returns:
        Configured SparkSession instance

    Note:
        In production, you would use environment variables or configuration
        files rather than hardcoded values.
    """
    print("\n" + "=" * 60)
    print("STEP 1: Creating SparkSession")
    print("=" * 60)

    # OPTION 1: Using our utility function (RECOMMENDED)
    # This ensures consistent configuration across all examples
    spark = create_spark_session(
        app_name="Hello-Spark-Example",
        # master is determined automatically based on environment
    )
    print("✓ Created SparkSession using utility function")

    # OPTION 2: Manual creation (for reference)
    # spark = SparkSession.builder \
    #     .appName("Hello-Spark") \
    #     .master("local[*]") \
    #     .getOrCreate()
    # print("✓ Created SparkSession manually")

    return spark


def demonstrate_environment_info(spark: SparkSession) -> None:
    """
    Show how to retrieve and display Spark environment information.

    Understanding your Spark environment is crucial for:
    - Debugging issues
    - Optimizing performance
    - Ensuring proper resource allocation

    Args:
        spark: Active SparkSession
    """
    print("\n" + "=" * 60)
    print("STEP 2: Understanding Your Spark Environment")
    print("=" * 60)

    # Retrieve Spark version
    # This helps ensure compatibility with features you want to use
    print(f"\n📊 Spark Version: {spark.version}")
    print("   - Check documentation for version-specific features")

    # Get the SparkContext for low-level information
    # SparkContext is the heart of Spark - it manages the cluster
    sc = spark.sparkContext

    # Application name - important for identifying your job in UI
    print(f"📱 Application Name: {sc.appName}")
    print("   - Shows in Spark UI: http://localhost:4040 or http://localhost:8080")

    # Master URL - tells us where Spark is running
    print(f"🎯 Master URL: {sc.master}")

    if sc.master == "local[*]":
        print("   ℹ️  Running in LOCAL mode (single JVM)")
        print("   - All operations happen on one machine")
        print("   - Great for learning and testing")
    elif "spark://" in sc.master:
        print("   ℹ️  Running in CLUSTER mode (distributed)")
        print("   - Operations distributed across multiple machines")
        print("   - Check worker UIs: http://localhost:8081, http://localhost:8082")

    # Default parallelism - how many tasks can run in parallel
    # This is usually equal to the number of cores available
    parallelism = sc.defaultParallelism
    print(f"⚡ Default Parallelism: {parallelism}")
    print(f"   - Can run {parallelism} tasks simultaneously")
    print("   - Affects performance of parallel operations")

    # Alternative: Use our utility for formatted output
    print("\n--- Detailed Info (via utility) ---")
    print_spark_info(spark)


def demonstrate_dataframe_creation(spark: SparkSession) -> None:
    """
    Demonstrate creating and working with DataFrames.

    DataFrames are the primary abstraction for structured data in Spark.
    They provide:
    - Type safety (with schemas)
    - Optimization (via Catalyst optimizer)
    - SQL-like operations

    Args:
        spark: Active SparkSession
    """
    print("\n" + "=" * 60)
    print("STEP 3: Creating Your First DataFrame")
    print("=" * 60)

    # METHOD 1: Create DataFrame from Python data
    # This is useful for testing with small datasets
    print("\n📦 Method 1: Creating from Python data")

    # Create a list of tuples (rows)
    # Each tuple represents one row of data
    data = [
        (1, "Alice", 25, "Engineering"),
        (2, "Bob", 30, "Marketing"),
        (3, "Charlie", 35, "Engineering"),
        (4, "Diana", 28, "Sales"),
        (5, "Eve", 32, "Engineering"),
        (6, "Frank", 29, "Marketing"),
        (7, "Grace", 27, "Sales"),
        (8, "Henry", 31, "Engineering"),
        (9, "Ivy", 26, "Marketing"),
        (10, "Jack", 33, "Sales"),
    ]

    # Create DataFrame with column names
    # Spark infers the schema automatically
    df = spark.createDataFrame(data, ["id", "name", "age", "department"])

    print(f"✓ Created DataFrame with {len(data)} rows")
    print(f"   Schema: id (int), name (string), age (int), department (string)")

    # METHOD 2: Using range (convenient for testing)
    print("\n📦 Method 2: Using range() for quick testing")
    df_range = spark.range(100)  # Creates DataFrame with single column 'id'
    print(f"✓ Created range DataFrame with {df_range.count()} rows")

    # TRANSFORMATION vs ACTION
    print("\n📚 Understanding Transformations vs Actions")
    print("   - Transformation: Creates new DataFrame (LAZY - no execution)")
    print("   - Action: Triggers actual computation")

    # TRANSFORMATION example (lazy - nothing happens yet)
    df_filtered = df.filter(df.age > 28)
    print(f"   ✓ Created filtered DataFrame (transformation - lazy)")
    print(f"     Filter: age > 28")

    # ACTION example (triggers execution)
    # count() is an action - it actually processes the data
    count = df_filtered.count()
    print(f"   ✓ Counted filtered rows (action): {count}")
    print(f"     This is when Spark actually processes the data!")

    # Show the data
    print("\n📊 Displaying DataFrame contents:")
    print("   (Showing first 20 rows)")
    df.show()

    # Print schema
    print("\n📋 DataFrame Schema:")
    df.printSchema()

    # Some basic statistics
    print("\n📈 Basic Statistics:")
    row_count = df.count()
    print(f"   Total rows: {row_count}")

    # Count by department
    print("\n👥 Count by Department:")
    dept_counts = df.groupBy("department").count()
    dept_counts.show()


def demonstrate_best_practices(spark: SparkSession) -> None:
    """
    Demonstrate Spark best practices and common patterns.

    This section covers important concepts that apply to all
    Spark applications.

    Args:
        spark: Active SparkSession
    """
    print("\n" + "=" * 60)
    print("STEP 4: Best Practices & Key Concepts")
    print("=" * 60)

    print("\n✅ DO's:")
    print("   1. Always use SparkSession.builder for creating sessions")
    print("   2. Set meaningful application names for monitoring")
    print("   3. Use DataFrames instead of RDDs for better optimization")
    print("   4. Chain transformations to minimize actions")
    print("   5. Stop the SparkSession when done")

    print("\n❌ DON'Ts:")
    print("   1. Don't call collect() on large datasets (will crash)")
    print("   2. Don't create multiple SparkSessions in one application")
    print("   3. Don't use show() for large DataFrames (truncates)")
    print("   4. Don't forget to handle errors gracefully")

    print("\n🔍 Monitoring:")
    print("   - Spark UI is your friend!")
    print("   - Local mode: http://localhost:4040")
    print("   - Cluster mode: http://localhost:8080 (master)")
    print("   - Check: Jobs, Stages, Storage, Environment tabs")

    # Demonstrate checking configuration
    print("\n⚙️  Current Configuration:")
    configs_to_check = [
        "spark.app.name",
        "spark.master",
        "spark.sql.adaptive.enabled",
    ]

    for config in configs_to_check:
        value = spark.conf.get(config, "Not set")
        print(f"   {config}: {value}")


def main() -> None:
    """
    Main entry point for the Hello Spark example.

    This function demonstrates a complete Spark workflow:
    1. Create SparkSession
    2. Explore environment
    3. Create and manipulate DataFrames
    4. Clean up resources

    The try/finally pattern ensures proper cleanup even if
    errors occur.
    """
    spark = None

    try:
        # Step 1: Create SparkSession
        spark = demonstrate_spark_session_creation()

        # Step 2: Check environment information
        demonstrate_environment_info(spark)

        # Step 3: Create and work with DataFrames
        demonstrate_dataframe_creation(spark)

        # Step 4: Learn best practices
        demonstrate_best_practices(spark)

        # Success message
        print("\n" + "=" * 60)
        print("✅ SUCCESS: Hello Spark example completed!")
        print("=" * 60)
        print("\nNext steps:")
        print("   1. Check Spark UI to see your application")
        print("      - Local mode: http://localhost:4040")
        print("      - Cluster mode: http://localhost:8080")
        print("\n   2. Try the next example:")
        print("      task submit JOB=examples/01-fundamentals/02_rdd_basics.py")
        print("\n   3. Read the documentation:")
        print("      docs/learning-path.md")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nTroubleshooting:")
        print("   - Check Spark Lab is running: task status")
        print("   - View logs: task logs")
        print("   - See docs/troubleshooting.md")
        raise

    finally:
        # Step 5: Clean up
        # This is CRITICAL - always stop the SparkSession
        if spark:
            print("\n🧹 Cleaning up resources...")
            stop_spark_session(spark)
            print("✓ SparkSession stopped")

        print("\n👋 Goodbye!")


# This allows the script to be run directly or imported
if __name__ == "__main__":
    main()

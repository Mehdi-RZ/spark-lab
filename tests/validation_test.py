#!/usr/bin/env python3
"""
Spark Lab Validation Tests
==========================

This script validates that the Spark Lab environment is properly configured
and all components are working correctly.

Run with: task test
Or manually: python tests/validation_test.py

Tests include:
- Environment setup validation
- Spark installation check
- Basic Spark operations
- Data connectivity
- Spark configuration
- Memory settings
- Serialization
"""

import os
import sys

# Add the path for imports (when running in container)
sys.path.insert(0, "/opt/spark/apps")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Module-level shared SparkSession
# A single JVM is started for the whole test run, instead of one per test.
# ---------------------------------------------------------------------------
_spark: SparkSession = (
    SparkSession.builder.appName("Spark-Lab-Validation")
    .master("local[*]")
    .getOrCreate()
)


def test_spark_installation() -> tuple:
    """Test 1: Verify Spark is installed and accessible."""
    print("\n" + "=" * 60)
    print("TEST 1: Spark Installation")
    print("=" * 60)

    try:
        version = _spark.version
        print(f"✓ Spark version: {version}")
        return True, "Spark installation verified"
    except Exception as e:
        return False, f"Failed to access SparkSession: {e}"


def test_basic_operations() -> tuple:
    """Test 2: Verify basic Spark operations work."""
    print("\n" + "=" * 60)
    print("TEST 2: Basic Spark Operations")
    print("=" * 60)

    try:
        # Test 2.1: Create DataFrame
        data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
        df = _spark.createDataFrame(data, ["id", "name", "age"])
        print(f"✓ Created DataFrame with {df.count()} rows")

        # Test 2.2: Select operation
        result = df.select("name").collect()
        print(f"✓ Select operation: {len(result)} names selected")

        # Test 2.3: Filter operation
        filtered = df.filter(F.col("age") > 25)
        print(f"✓ Filter operation: {filtered.count()} rows with age > 25")

        # Test 2.4: GroupBy / aggregation operation
        grouped = df.groupBy().agg(F.avg("age"))
        print(f"✓ Aggregation operation: avg age = {grouped.collect()[0][0]:.1f}")

        return True, "All basic operations passed"
    except Exception as e:
        return False, f"Basic operations failed: {e}"


def test_data_connectivity() -> tuple:
    """Test 3: Verify data directories are accessible."""
    print("\n" + "=" * 60)
    print("TEST 3: Data Connectivity")
    print("=" * 60)

    data_paths = [
        "/opt/spark/data/users",
        "/opt/spark/data/products",
        "/opt/spark/data/orders",
    ]

    accessible_count = 0
    for path in data_paths:
        if os.path.isdir(path):
            print(f"✓ Data directory accessible: {path}")
            accessible_count += 1
        else:
            print(f"⚠ Data directory not found: {path}")

    if accessible_count > 0:
        return True, f"{accessible_count} data directories accessible"
    return False, "No data directories found. Run: task generate"


def test_configuration() -> tuple:
    """Test 4: Verify Spark configuration."""
    print("\n" + "=" * 60)
    print("TEST 4: Spark Configuration")
    print("=" * 60)

    try:
        configs = ["spark.app.name", "spark.master", "spark.sql.adaptive.enabled"]
        for config in configs:
            value = _spark.conf.get(config, "NOT SET")
            print(f"✓ {config}: {value}")

        return True, "Configuration verified"
    except Exception as e:
        return False, f"Configuration test failed: {e}"


def test_memory_settings() -> tuple:
    """Test 5: Verify memory settings are appropriate."""
    print("\n" + "=" * 60)
    print("TEST 5: Memory Settings")
    print("=" * 60)

    try:
        driver_memory = _spark.conf.get("spark.driver.memory", "NOT SET")
        print(f"✓ Driver memory: {driver_memory}")

        # Verify that a moderately-sized operation completes successfully
        data = [(i, f"user_{i}") for i in range(10000)]
        df = _spark.createDataFrame(data, ["id", "name"])
        print(f"✓ Successfully processed {df.count()} rows")

        return True, "Memory settings adequate"
    except Exception as e:
        return False, f"Memory test failed: {e}"


def test_serialization() -> tuple:
    """Test 6: Verify serialization works."""
    print("\n" + "=" * 60)
    print("TEST 6: Serialization")
    print("=" * 60)

    try:
        serializer = _spark.conf.get("spark.serializer", "NOT SET")
        print(f"✓ Serializer: {serializer}")

        # RDD map requires serialization
        data = [(1, "a"), (2, "b"), (3, "c")]
        rdd = _spark.sparkContext.parallelize(data)
        result = rdd.map(lambda x: (x[0] * 2, x[1].upper())).collect()
        print(f"✓ Serialization test: {len(result)} items processed")

        return True, "Serialization working"
    except Exception as e:
        return False, f"Serialization test failed: {e}"


def run_all_tests() -> int:
    """Run all validation tests and report results."""
    print("\n" + "=" * 60)
    print("SPARK LAB VALIDATION SUITE")
    print("=" * 60)
    print("\nRunning comprehensive environment tests...")

    tests = [
        test_spark_installation,
        test_basic_operations,
        test_data_connectivity,
        test_configuration,
        test_memory_settings,
        test_serialization,
    ]

    results = []
    for test in tests:
        try:
            passed, message = test()
            results.append((test.__name__, passed, message))
        except Exception as e:
            results.append((test.__name__, False, f"Test crashed: {e}"))

    # Stop the shared session after all tests have run
    _spark.stop()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for _, passed, _ in results if passed)
    total_count = len(results)

    for name, passed, message in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name.replace('test_', '').replace('_', ' ').title()}")
        if not passed:
            print(f"   Error: {message}")

    print("\n" + "=" * 60)
    print(f"Results: {passed_count}/{total_count} tests passed")
    print("=" * 60)

    if passed_count == total_count:
        print("\n✅ All tests passed! Environment is ready.")
        print("\nNext steps:")
        print("  task generate     - Generate sample data")
        print("  task submit JOB=examples/01-fundamentals/01_hello_spark.py")
        return 0
    else:
        print("\n❌ Some tests failed. Check the errors above.")
        print("\nTroubleshooting:")
        print("  1. Ensure Docker is running: docker ps")
        print("  2. Check logs: task logs")
        print("  3. Review: docs/troubleshooting.md")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())

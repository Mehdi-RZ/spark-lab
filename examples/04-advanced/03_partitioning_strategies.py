"""
Partitioning Strategies
=======================

Demonstrates disk partitioning and bucketing strategies for writes and reads.
This example complements 03-performance/02_partitioning.py, which focuses on
in-memory partition tuning (repartition / coalesce).

Topics covered:
  1. partitionBy  - Hive-style directory partitioning on write
  2. Multi-level partitionBy
  3. Partition pruning - How Spark skips irrelevant partitions on read
  4. Bucketing     - Pre-sorted, fixed-count buckets that avoid shuffle on join
  5. Dynamic partition overwrite (incremental loads)

Learning objectives:
  - Understand the difference between in-memory repartitioning and disk partitioning
  - Know when to use partitionBy vs bucketing
  - See how Spark optimises queries against partitioned/bucketed tables
"""

import os
import sys
import tempfile

sys.path.insert(0, "/opt/spark/apps")

from pyspark.sql import functions as F

from utils.spark_session import create_spark_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _show_partition_info(df, label: str) -> None:
    print(f"\n  [{label}] Number of RDD partitions: {df.rdd.getNumPartitions()}")


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Demo functions
# ---------------------------------------------------------------------------


def demo_partition_by(spark, base_path: str) -> None:
    """
    partitionBy writes data into a directory hierarchy keyed by column values.

    Directory layout on disk:
        orders_by_status/
          status=DELIVERED/
          status=SHIPPED/
          status=PENDING/
          ...

    Benefits:
      - Partition pruning: WHERE status='DELIVERED' reads only that sub-directory.
      - Easy incremental loads: overwrite a single partition without touching
        the rest of the dataset.

    Trade-offs:
      - High-cardinality columns (e.g. user_id) produce thousands of small
        files — a classic "small files problem". Prefer low-cardinality columns
        like date, status, or country.
    """
    _section("1. partitionBy — Disk-Level Directory Partitioning")

    orders_data = [
        ("ORD-001", 1, "DELIVERED", "2024-01-10", 120.50),
        ("ORD-002", 2, "SHIPPED", "2024-01-11", 89.99),
        ("ORD-003", 3, "PENDING", "2024-01-12", 45.00),
        ("ORD-004", 1, "DELIVERED", "2024-01-13", 200.00),
        ("ORD-005", 4, "CANCELLED", "2024-01-14", 60.00),
        ("ORD-006", 2, "SHIPPED", "2024-01-15", 75.50),
        ("ORD-007", 5, "DELIVERED", "2024-01-16", 310.00),
        ("ORD-008", 3, "PENDING", "2024-01-17", 95.00),
    ]
    orders_df = spark.createDataFrame(
        orders_data, ["order_id", "user_id", "status", "order_date", "amount"]
    )

    out_path = os.path.join(base_path, "orders_by_status")
    print(f"\n  Writing orders partitioned by 'status' to:\n    {out_path}")
    orders_df.write.mode("overwrite").partitionBy("status").parquet(out_path)

    # List the partition directories that were created
    if os.path.isdir(out_path):
        dirs = sorted(d for d in os.listdir(out_path) if not d.startswith("."))
        print(f"\n  Partition directories created ({len(dirs)}):")
        for d in dirs:
            print(f"    {d}/")

    # Full dataset read-back
    all_df = spark.read.parquet(out_path)
    print(f"\n  Full dataset row count: {all_df.count()}")

    # Partition pruning: Spark reads ONLY the DELIVERED sub-directory
    print("\n  Reading only DELIVERED orders (partition pruning):")
    delivered_df = spark.read.parquet(out_path).filter(F.col("status") == "DELIVERED")
    delivered_df.show(truncate=False)
    _show_partition_info(delivered_df, "after pruning")

    print("\n  Physical plan (look for 'PartitionFilters'):")
    delivered_df.explain()


def demo_multi_level_partition_by(spark, base_path: str) -> None:
    """
    Multi-level partitioning: partition by year, then by status.

    Layout:
        orders_multi/
          year=2023/status=DELIVERED/ ...
          year=2024/status=DELIVERED/ ...

    A query WHERE year='2024' AND status='DELIVERED' touches exactly one
    leaf directory regardless of total dataset size.
    """
    _section("2. Multi-Level partitionBy (year + status)")

    data = [
        ("ORD-A1", "DELIVERED", "2023", 100.0),
        ("ORD-A2", "SHIPPED", "2023", 200.0),
        ("ORD-B1", "DELIVERED", "2024", 150.0),
        ("ORD-B2", "PENDING", "2024", 50.0),
        ("ORD-B3", "SHIPPED", "2024", 300.0),
    ]
    df = spark.createDataFrame(data, ["order_id", "status", "year", "amount"])

    out_path = os.path.join(base_path, "orders_multi_partition")
    df.write.mode("overwrite").partitionBy("year", "status").parquet(out_path)

    # Read with both partition filters applied
    result = spark.read.parquet(out_path).filter(
        (F.col("year") == "2024") & (F.col("status") == "SHIPPED")
    )
    print(f"\n  2024 / SHIPPED orders: {result.count()} row(s)")
    result.show(truncate=False)


def demo_bucketing(spark, base_path: str) -> None:
    """
    Bucketing pre-partitions data into a fixed number of buckets based on
    the hash of one or more columns and persists the layout as a managed table.

    Key advantage: when two tables are bucketed on the same column with the
    same bucket count, Spark can perform a sort-merge join WITHOUT a shuffle
    (exchange) step — a major win for large-table joins.

    Limitations:
      - Requires saveAsTable (cannot use just .parquet(path)).
      - The bucket count and key must match on both sides.
      - Best for large, frequently-joined tables (fact / dimension pattern).
    """
    _section("3. Bucketing — Shuffle-Free Joins")

    users_data = [(i, f"user_{i}", i % 5) for i in range(1, 21)]
    orders_data = [(f"ORD-{i:03d}", i % 20 + 1, float(i * 10)) for i in range(1, 31)]

    users_df = spark.createDataFrame(users_data, ["user_id", "username", "tier"])
    orders_df = spark.createDataFrame(orders_data, ["order_id", "user_id", "amount"])

    print("\n  Writing bucketed tables (4 buckets on user_id)...")
    (
        users_df.write.mode("overwrite")
        .bucketBy(4, "user_id")
        .sortBy("user_id")
        .saveAsTable("bucketed_users")
    )
    (
        orders_df.write.mode("overwrite")
        .bucketBy(4, "user_id")
        .sortBy("user_id")
        .saveAsTable("bucketed_orders")
    )

    joined = spark.table("bucketed_users").join(
        spark.table("bucketed_orders"), on="user_id", how="inner"
    )
    print(f"\n  Joined row count: {joined.count()}")
    joined.show(5)

    print("\n  Physical plan (look for SortMergeJoin WITHOUT Exchange):")
    joined.explain()


def demo_partition_overwrite(spark, base_path: str) -> None:
    """
    Dynamic partition overwrite lets you refresh a single partition without
    touching the rest — ideal for incremental / idempotent ETL pipelines.

    Key setting:
        spark.sql.sources.partitionOverwriteMode = dynamic
    """
    _section("4. Dynamic Partition Overwrite (incremental loads)")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    initial_data = [
        ("ORD-001", "2024-01", 100.0),
        ("ORD-002", "2024-01", 200.0),
        ("ORD-003", "2024-02", 150.0),
    ]
    df_initial = spark.createDataFrame(initial_data, ["order_id", "month", "amount"])

    out_path = os.path.join(base_path, "orders_dynamic_overwrite")
    df_initial.write.mode("overwrite").partitionBy("month").parquet(out_path)
    print(f"\n  Initial write: {spark.read.parquet(out_path).count()} rows")

    # Overwrite only 2024-01 — 2024-02 must remain untouched
    new_jan_data = [
        ("ORD-001-v2", "2024-01", 110.0),  # corrected record
        ("ORD-004", "2024-01", 75.0),  # new record
    ]
    df_new = spark.createDataFrame(new_jan_data, ["order_id", "month", "amount"])
    df_new.write.mode("overwrite").partitionBy("month").parquet(out_path)

    final_df = spark.read.parquet(out_path)
    print(f"  After dynamic overwrite: {final_df.count()} rows (2024-02 intact)")
    final_df.orderBy("month", "order_id").show(truncate=False)

    # Restore default
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = create_spark_session(app_name="Partitioning-Strategies")

    # Temp dir so the example works both locally and inside Docker
    with tempfile.TemporaryDirectory() as tmp:
        demo_partition_by(spark, tmp)
        demo_multi_level_partition_by(spark, tmp)
        demo_bucketing(spark, tmp)
        demo_partition_overwrite(spark, tmp)

    print("\n" + "=" * 60)
    print("  Summary: When to use each strategy")
    print("=" * 60)
    print("""
  partitionBy (disk partitioning)
    - Low-cardinality columns (date, status, country, region)
    - Enables partition pruning on reads
    - Great for time-series or range-scoped queries

  Multi-level partitionBy
    - When queries commonly filter on two dimensions (year + region)
    - Watch out for exponential partition counts with high-cardinality keys

  Bucketing
    - Large tables joined frequently on the same high-cardinality key
    - Eliminates shuffle on sort-merge joins
    - Requires saveAsTable (not just a raw parquet write)

  Dynamic partition overwrite
    - Incremental / idempotent ETL pipelines
    - Refresh a single date partition without re-writing the entire dataset
    """)

    spark.stop()


if __name__ == "__main__":
    main()

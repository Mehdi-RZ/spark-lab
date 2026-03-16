"""
Structured Streaming Basics
============================

Demonstrates Spark Structured Streaming using self-contained file-based
sources so that the example runs out-of-the-box in this lab without
requiring an external socket or Kafka cluster.

Topics covered:
  1. Streaming concepts overview (readStream / writeStream / trigger / outputMode)
  2. File-based streaming source (JSON files in a watched directory)
  3. Output modes: append, update, complete
  4. Stateless transformations on a stream (filter, select, withColumn)
  5. Stateful aggregation with watermarking (count + revenue per category)
  6. Writing to a memory sink for interactive inspection

How the file-based stream works:
  - A temporary "source" directory is watched by Structured Streaming.
  - A background thread writes new JSON files into that directory every second,
    simulating a live event feed.
  - Spark picks up new files automatically, applies the query, and writes
    results to a memory sink for easy inspection.

Learning objectives:
  - Understand the readStream / writeStream API
  - Know the difference between append, update, and complete output modes
  - See how watermarking prevents unbounded state growth
  - Run a real streaming query without external infrastructure
"""

import json
import os
import random
import sys
import tempfile
import threading
import time

sys.path.insert(0, "/opt/spark/apps")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils.spark_session import create_spark_session


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def _write_events(source_dir: str, num_batches: int = 6, delay: float = 1.0) -> None:
    """
    Background thread: writes one JSON-lines file per tick into *source_dir*.
    Each file contains 3 random events, simulating a micro-batch feed.
    """
    for batch in range(num_batches):
        events = []
        for i in range(3):
            events.append(
                {
                    "event_id": f"EVT-{batch:03d}-{i}",
                    "category": random.choice(CATEGORIES),
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "event_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )
        file_path = os.path.join(source_dir, f"batch_{batch:03d}.json")
        with open(file_path, "w") as fh:
            for event in events:
                fh.write(json.dumps(event) + "\n")
        time.sleep(delay)


def _await_and_stop(query, max_wait: float = 18.0) -> None:
    """Wait until the query has processed at least one batch, then stop."""
    start = time.time()
    while time.time() - start < max_wait:
        progress = query.lastProgress
        if progress and progress.get("numInputRows", 0) > 0:
            # Give one more second to accumulate a bit more data
            time.sleep(1)
            break
        time.sleep(1)
    query.stop()


# ---------------------------------------------------------------------------
# Demo 1: Stateless stream — filter + projection
# ---------------------------------------------------------------------------


def demo_stateless_stream(spark, source_dir: str, checkpoint_dir: str) -> None:
    """
    The simplest streaming query: read JSON events, apply a filter and
    a projection, and write results to a memory sink.

    Output mode: append — each qualifying row is emitted exactly once.
    """
    _section("1. Stateless Stream — filter + select (outputMode: append)")

    print("""
  readStream watches the source directory for new files.
  The query below keeps only high-value events (amount > 200).
  Results go to an in-memory table called 'high_value_events'.
    """)

    stream_df = spark.readStream.schema(EVENT_SCHEMA).json(source_dir)

    filtered = stream_df.filter(F.col("amount") > 200).select(
        "event_id",
        "category",
        F.round("amount", 2).alias("amount"),
        "event_time",
    )

    query = (
        filtered.writeStream.outputMode("append")
        .format("memory")
        .queryName("high_value_events")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "stateless"))
        .start()
    )

    feeder = threading.Thread(target=_write_events, args=(source_dir,), daemon=True)
    feeder.start()
    _await_and_stop(query, max_wait=18)
    feeder.join(timeout=25)

    print("  Results in 'high_value_events' (amount > 200):")
    spark.sql("SELECT * FROM high_value_events ORDER BY event_time").show(
        truncate=False
    )


# ---------------------------------------------------------------------------
# Demo 2: Stateful aggregation with watermarking
# ---------------------------------------------------------------------------


def demo_stateful_aggregation(spark, source_dir: str, checkpoint_dir: str) -> None:
    """
    Aggregates event counts and total revenue per category using a watermark
    to bound the amount of state Spark holds in memory.

    Watermark: tells Spark it can safely discard state for events whose
    event_time is more than N seconds behind the latest seen event time.
    Without a watermark, late-arriving data would accumulate state forever.

    Output mode: complete — the full aggregation result is re-emitted each trigger.
    """
    _section("2. Stateful Aggregation with Watermarking (outputMode: complete)")

    print("""
  withWatermark("event_time", "10 seconds")
    -> Spark discards state for events more than 10 s late.

  groupBy("category").agg(count, sum, avg)
    -> Stateful aggregation — state grows until the watermark expires it.

  outputMode("complete")
    -> The full result table is re-emitted on every trigger.
    """)

    stream_df = (
        spark.readStream.schema(EVENT_SCHEMA)
        .json(source_dir)
        .withWatermark("event_time", "10 seconds")
    )

    agg = stream_df.groupBy("category").agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )

    query = (
        agg.writeStream.outputMode("complete")
        .format("memory")
        .queryName("category_stats")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "stateful"))
        .start()
    )

    feeder = threading.Thread(
        target=_write_events,
        args=(source_dir,),
        kwargs={"num_batches": 8, "delay": 0.8},
        daemon=True,
    )
    feeder.start()
    _await_and_stop(query, max_wait=20)
    feeder.join(timeout=30)

    print("  Category stats (sorted by total revenue):")
    spark.sql("SELECT * FROM category_stats ORDER BY total_revenue DESC").show(
        truncate=False
    )


# ---------------------------------------------------------------------------
# Reference sections (no running query needed)
# ---------------------------------------------------------------------------


def explain_output_modes() -> None:
    _section("3. Output Modes — Reference")
    print("""
  append  (default)
    Each row is emitted exactly once when it is considered final.
    Valid for: stateless transforms, append-only windowed aggregations
               (with watermark).
    NOT valid for: aggregations without a watermark.

  update
    Only rows that changed since the last trigger are emitted.
    Efficient for sinks that support upserts (e.g. Cassandra, Delta).

  complete
    The entire result table is re-emitted on every trigger.
    Required for aggregations that may update any existing row.
    Memory usage grows with the size of the result set.

  Rule of thumb:
    Stateless filter / project       -> append
    Grouped aggregation              -> complete (or update + watermark)
    Windowed aggregation + watermark -> append (after window is finalised)
    """)


def explain_triggers() -> None:
    _section("4. Trigger Options — Reference")
    print("""
  Trigger.ProcessingTime("5 seconds")   (default)
    Run a micro-batch every 5 seconds.

  Trigger.Once()
    Process all available data in ONE batch, then stop.
    Useful for scheduled / batch-style streaming jobs.

  Trigger.AvailableNow()
    Like Once(), but honours maxFilesPerTrigger — multiple batches.

  Trigger.Continuous("1 second")
    Experimental: near-real-time (~1 ms latency).
    Lower throughput and limited operator support.

  Usage:
    query = df.writeStream \\
        .trigger(processingTime="10 seconds") \\
        .format("parquet") \\
        .start(output_path)
    """)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = create_spark_session(app_name="Streaming-Basics")

    with tempfile.TemporaryDirectory() as tmp:
        source_dir = os.path.join(tmp, "events")
        checkpoint_dir = os.path.join(tmp, "checkpoints")
        os.makedirs(source_dir, exist_ok=True)
        os.makedirs(checkpoint_dir, exist_ok=True)

        demo_stateless_stream(spark, source_dir, checkpoint_dir)

        # Clear source files before the second demo
        for fname in os.listdir(source_dir):
            os.remove(os.path.join(source_dir, fname))

        demo_stateful_aggregation(spark, source_dir, checkpoint_dir)

    explain_output_modes()
    explain_triggers()

    print("\n" + "=" * 60)
    print("  Summary: Structured Streaming key concepts")
    print("=" * 60)
    print("""
  readStream          - Open a streaming DataFrame from a source
  writeStream.start() - Start the streaming query (returns StreamingQuery)
  outputMode          - How results are emitted: append / update / complete
  trigger             - When to process: ProcessingTime / Once / AvailableNow
  watermark           - How much lateness is tolerated; bounds state size
  memory sink         - Writes results to an in-memory table (for testing)
  checkpoint          - Fault-tolerance: saves query progress to durable storage

  Production sources:  Kafka, Kinesis, Delta Lake, files (Parquet / JSON / CSV)
  Production sinks:    Kafka, Delta Lake, JDBC, files, foreachBatch
    """)

    spark.stop()


if __name__ == "__main__":
    main()

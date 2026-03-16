import sys
import time

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Adaptive-Query-Execution")

    data = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (1, "a"), (2, "b")]
    df = spark.createDataFrame(data, ["id", "value"])

    print("Original DataFrame:")
    df.show()

    print("\nDeduplication:")
    deduped = df.dropDuplicates(["id"])
    deduped.show()

    print("\nStateful aggregation (running count):")
    window_spec = Window.orderBy("id")

    stateful_df = df.withColumn("running_count", F.count("value").over(window_spec))
    stateful_df.show()

    print("\nWatermarking for late data:")
    stream_threshold = time.time() + 10

    timed_df = df.withColumn("event_time", F.lit(stream_threshold))

    late_df = timed_df.filter(F.col("event_time") < stream_threshold)
    print(f"After watermark (ignore data after {stream_threshold}):")
    late_df.show()

    spark.stop()


if __name__ == "__main__":
    main()

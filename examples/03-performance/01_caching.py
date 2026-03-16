import sys
import time

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Caching-Optimization")

    df = spark.range(1, 10000000).repartition(200)
    df = df.withColumn("value", F.col("id") * 2 + F.rand())

    print(f"DataFrame: {df.count()} rows, {df.rdd.getNumPartitions()} partitions")

    print("\nWithout caching:")
    start = time.time()
    for i in range(5):
        df.filter(F.col("value") > 1000000).count()
    end = time.time()
    print(f"  Total for 5 iterations: {end - start:.3f} seconds")

    df.cache()
    df.count()

    print("\nWith caching:")
    start = time.time()
    for i in range(5):
        df.filter(F.col("value") > 1000000).count()
    end = time.time()
    print(f"  Total for 5 iterations: {end - start:.3f} seconds")

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

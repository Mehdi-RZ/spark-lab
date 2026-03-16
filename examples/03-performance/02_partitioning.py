import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Partitioning-Optimization")

    print("\n1. Default partitions:")
    df1 = spark.range(10000)
    print(f"  Partitions: {df1.rdd.getNumPartitions()}")

    print("\n2. Coalesce (reduce partitions):")
    df2 = df1.coalesce(4)
    print(f"  Partitions: {df2.rdd.getNumPartitions()}")

    print("\n3. Repartition (increase partitions):")
    df3 = df1.repartition(8)
    print(f"  Partitions: {df3.rdd.getNumPartitions()}")

    print("\n4. Repartition by column:")
    df4 = spark.range(1, 100).withColumn("key", F.col("id") % 5)
    df5 = df4.repartition(10, "key")
    print(f"  Partitions: {df5.rdd.getNumPartitions()}")

    print("\n5. Understanding skew:")
    skewed_data = spark.range(1, 100).withColumn(
        "skewed_key", F.when(F.col("id") < 10, "hot").otherwise("cold")
    )
    df6 = skewed_data.repartition(4, "skewed_key")
    print(f"  Partitions: {df6.rdd.getNumPartitions()}")

    spark.stop()


if __name__ == "__main__":
    main()

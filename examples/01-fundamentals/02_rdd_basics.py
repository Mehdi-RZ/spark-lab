import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="RDD-Basics")
    sc = spark.sparkContext

    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)

    print(f"RDD: {rdd}")
    print(f"Partitions: {rdd.getNumPartitions()}")
    print(f"Count: {rdd.count()}")
    print(f"First element: {rdd.first()}")
    print(f"Take 3: {rdd.take(3)}")

    print("\nMap operation:")
    mapped_rdd = rdd.map(lambda x: x * 2)
    print(f"Mapped: {mapped_rdd.collect()}")

    print("\nFilter operation:")
    filtered_rdd = rdd.filter(lambda x: x > 2)
    print(f"Filtered (>2): {filtered_rdd.collect()}")

    print("\nReduce operation:")
    sum_result = rdd.reduce(lambda a, b: a + b)
    print(f"Sum: {sum_result}")

    print("\nflatMap operation:")
    flatmapped_rdd = rdd.flatMap(lambda x: [x, x * 10, x * 100])
    print(f"FlatMapped: {flatmapped_rdd.collect()}")

    spark.stop()


if __name__ == "__main__":
    main()

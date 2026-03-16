import sys
import time

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Broadcast-Join-Optimization")

    large_data = [(i, f"Item_{i}", i % 3 + 101) for i in range(10000)]
    large_df = spark.createDataFrame(large_data, ["id", "name", "category_id"])

    small_data = [(101, "Premium"), (102, "Standard"), (103, "Basic")]
    small_df = spark.createDataFrame(small_data, ["category_id", "category_name"])

    print("Large DataFrame:")
    large_df.show(5)

    print("\nSmall DataFrame:")
    small_df.show()

    print("\nWithout Broadcast Hint:")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    start1 = time.time()
    result1 = large_df.join(small_df, "category_id", "left")
    result1.count()
    end1 = time.time()
    print(f"  Time: {end1 - start1:.3f} seconds")

    print("\nWith Broadcast Hint:")
    start2 = time.time()
    result2 = large_df.join(F.broadcast(small_df), "category_id", "left")
    result2.count()
    end2 = time.time()
    print(f"  Time: {end2 - start2:.3f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()

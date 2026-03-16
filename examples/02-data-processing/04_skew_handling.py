import sys
import time

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Optimize-Skewed-Join")

    skewed_data = []
    for i in range(1000):
        skewed_user = f"user_{i % 7}"
        skewed_data.append((skewed_user, skewed_user, i % 5))

    skewed_df = spark.createDataFrame(skewed_data, ["user_id", "username", "_score"])

    small_skew_data = []
    for i in range(100):
        small_skew_user = f"user_{i % 7}"
        small_skew_data.append((small_skew_user, small_skew_user, i % 5))

    small_skew_df = spark.createDataFrame(
        small_skew_data, ["user_id", "username", "_score"]
    )

    print("Small dataset:")
    small_skew_df.show()

    print("\nSkewed dataset:")
    skewed_df.show()

    print("\nJoin operation without optimization:")
    start1 = time.time()
    result1 = skewed_df.join(small_skew_df, "user_id")
    result1.count()
    end1 = time.time()
    print(f"  Time without optimization: {end1 - start1:.3f} seconds")

    print("\nSalting technique for skew:")
    skewed_df_with_salt = skewed_df.withColumn(
        "salt",
        F.concat(F.col("user_id"), F.lit("_"), F.lit((F.rand() * 10).cast("int"))),
    )

    start2 = time.time()
    result2 = skewed_df_with_salt.repartition("salt").join(small_skew_df, "user_id")
    result2.count()
    end2 = time.time()
    print(f"  Time with salting: {end2 - start2:.3f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()

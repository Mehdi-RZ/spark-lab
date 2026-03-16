import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="DataFrame-Basics")

    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "David", 40)]

    df = spark.createDataFrame(data, ["id", "name", "age"])

    print("\nDataFrame Operations:")
    df.show()
    print(f"Schema: {df.schema}")
    print(f"Count: {df.count()}")

    print("\nSelect columns:")
    df.select("name", "age").show()

    print("\nFilter:")
    df.filter(df.age > 25).show()

    print("\nGroup By:")
    df.groupBy("name").count().show()

    print("\nSort:")
    df.orderBy("age", ascending=False).show()

    print("\nWithColumn:")
    df.withColumn("age_plus_10", df.age + 10).show()

    spark.stop()


if __name__ == "__main__":
    main()

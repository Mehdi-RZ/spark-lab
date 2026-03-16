import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="SQL-Basics")

    data = [
        (1, "Alice", 25, "Engineer"),
        (2, "Bob", 30, "Manager"),
        (3, "Charlie", 35, "Analyst"),
        (4, "David", 40, "Director"),
    ]

    df = spark.createDataFrame(data, ["id", "name", "age", "role"])

    df.createOrReplaceTempView("employees")

    print("\nSQL Queries:")

    result = spark.sql("SELECT * FROM employees WHERE age > 25")
    result.show()

    avg_age = spark.sql("SELECT AVG(age) as avg_age FROM employees")
    print(f"Average age: {avg_age.collect()}")

    spark.stop()


if __name__ == "__main__":
    main()

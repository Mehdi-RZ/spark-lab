import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Complex-Types")

    complex_data = [
        (1, "Alice", [1, 2, 3]),
        (2, "Bob", [4, 5]),
        (3, "Charlie", [7, 8, 9]),
    ]
    df = spark.createDataFrame(complex_data, ["id", "name", "scores"])

    print("Nested Array Data:")
    df.show(truncate=False)

    print("\nExplode Array:")
    exploded = df.withColumn("score", F.explode("scores"))
    exploded.select("id", "name", "score").show()

    print("\nArray Operations:")
    df.withColumn("array_size", F.size("scores")).show()

    df.withColumn("first_score", F.element_at("scores", 1)).show()

    df.withColumn("scores_json", F.to_json(F.col("scores"))).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Map-Filter-Transformations")

    data = [
        ("apple", 10),
        ("banana", 20),
        ("cherry", 30),
        ("date", 40),
        ("elderberry", 50),
    ]
    df = spark.createDataFrame(data, ["fruit", "price"])

    print("Original DataFrame:")
    df.show()

    print("\nMap transformation (adding tax):")
    tax_df = df.withColumn("price_with_tax", df.price * 1.1)
    tax_df.show()

    print("\nFilter transformation (price > 20):")
    filtered_df = df.filter(df.price > 20)
    filtered_df.show()

    print("\nCombined transformations:")
    result_df = (
        df.filter(df.price > 15)
        .withColumn("discounted_price", df.price * 0.9)
        .withColumn(
            "category",
            F.when(df.price > 30, "Premium")
            .when(df.price > 20, "Standard")
            .otherwise("Basic"),
        )
    )
    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()

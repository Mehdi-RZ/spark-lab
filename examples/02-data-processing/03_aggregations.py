import sys
import random

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Aggregations")

    sales_data = []
    for i in range(100):
        sales_data.append(
            (i, f"Product_{i % 10}", i % 5 + 1, i * 10.0 + random.uniform(-2, 2))
        )

    sales = spark.createDataFrame(sales_data, ["id", "product", "quantity", "revenue"])

    print("Sales Data (sample):")
    sales.show(5)

    print("\nBasic Aggregations:")
    sales.select(
        F.count("*").alias("total_transactions"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_revenue"),
        F.max("revenue").alias("max_revenue"),
        F.min("revenue").alias("min_revenue"),
    ).show()

    print("\nGroup By:")
    sales.groupBy("product").agg(
        F.sum("quantity").alias("total_qty"), F.avg("revenue").alias("avg_rev")
    ).show()

    print("\nMultiple Aggregations:")
    sales.agg(
        F.countDistinct("product").alias("unique_products"),
        F.approx_count_distinct("product").alias("approx_unique"),
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()

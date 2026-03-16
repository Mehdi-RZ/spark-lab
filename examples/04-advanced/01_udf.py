import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils.spark_session import create_spark_session


def categorize_price(price):
    if price < 50:
        return "Low"
    elif price < 100:
        return "Medium"
    else:
        return "High"


def main():
    spark = create_spark_session(app_name="UDF-Example")

    categorize_udf = F.udf(categorize_price, StringType())

    data = [(1, 25.0), (2, 75.0), (3, 150.0), (4, 10.0)]
    df = spark.createDataFrame(data, ["id", "price"])

    print("Original DataFrame:")
    df.show()

    print("\nWith UDF:")
    df.withColumn("price_category", categorize_udf("price")).show()

    print("\nUsing SQL Expression (faster):")
    df.withColumn(
        "price_category_sql",
        F.expr(
            "CASE WHEN price < 50 THEN 'Low' WHEN price < 100 THEN 'Medium' ELSE 'High' END"
        ),
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()

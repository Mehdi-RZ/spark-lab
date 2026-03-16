import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Window-Functions")

    employees_data = [
        (1, "Alice", 50000),
        (2, "Bob", 60000),
        (3, "Charlie", 55000),
        (4, "David", 70000),
        (5, "Eve", 45000),
    ]
    df = spark.createDataFrame(employees_data, ["emp_id", "name", "salary"])

    print("Employees with Salary:")
    df.show()

    window_spec = Window.orderBy(F.col("salary").desc())

    print("\nRow Number:")
    df.withColumn("rank", F.row_number().over(window_spec)).show()

    print("\nRunning Total (cumulative sum):")
    window_spec_unbounded = Window.orderBy("emp_id").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df.withColumn("running_total", F.sum("salary").over(window_spec_unbounded)).show()

    print("\nLead/Lag:")
    df.withColumn("prev_salary", F.lag("salary").over(window_spec)).show()

    spark.stop()


if __name__ == "__main__":
    main()

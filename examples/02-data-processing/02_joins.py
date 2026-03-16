import sys

sys.path.insert(0, "/opt/spark/apps")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session


def main():
    spark = create_spark_session(app_name="Joins-Transformation")

    employees_data = [
        (1, "Alice", 101),
        (2, "Bob", 102),
        (3, "Charlie", 103),
        (4, "David", 104),
    ]
    employees = spark.createDataFrame(employees_data, ["emp_id", "name", "dept_id"])

    departments_data = [
        (101, "Engineering"),
        (102, "Sales"),
        (103, "Marketing"),
        (104, "HR"),
    ]
    departments = spark.createDataFrame(departments_data, ["dept_id", "dept_name"])

    print("Employees:")
    employees.show()

    print("\nDepartments:")
    departments.show()

    print("\nInner Join:")
    inner_join = employees.join(departments, "dept_id").select("name", "dept_name")
    inner_join.show()

    print("\nLeft Join:")
    left_join = employees.join(departments, "dept_id", "left").select(
        "name", "dept_name"
    )
    left_join.show()

    print("\nBroadcast Join (optimized for small tables):")
    broadcast_join = employees.join(F.broadcast(departments), "dept_id")
    broadcast_join.show()

    spark.stop()


if __name__ == "__main__":
    main()

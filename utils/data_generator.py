"""
Data Generator Module
=====================

Provides synthetic dataset generation for the Spark Lab environment.
Generates realistic-looking users, products, and orders data in
configurable sizes and saves them in multiple formats (Parquet, CSV, JSON).

Usage Examples:
    >>> from utils.data_generator import generate_and_save_all_datasets
    >>> from utils.spark_session import create_spark_session
    >>>
    >>> spark = create_spark_session(app_name="Data-Generator")
    >>> generate_and_save_all_datasets(spark, size="small")
    >>> spark.stop()

Available Sizes:
    very_small  - 100 users / 50 products / 100 orders (smoke tests)
    small       - 1K users / 500 products / 1K orders (basic learning)
    medium      - 10K users / 5K products / 10K orders (realistic patterns)
    large       - 100K users / 50K products / 1M orders (performance testing)
    very_large  - 1M users / 500K products / 10M orders (scale testing)
"""

import logging
import os
import random
import string
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Size configuration
# ---------------------------------------------------------------------------

DATA_SIZE_CONFIG = {
    "very_small": {
        "users": 100,
        "products": 50,
        "orders": 100,
        "description": "Very small - for quick smoke tests and basic validation",
    },
    "small": {
        "users": 1000,
        "products": 500,
        "orders": 1000,
        "description": "Small - for basic learning and exploration",
    },
    "medium": {
        "users": 10000,
        "products": 5000,
        "orders": 10000,
        "description": "Medium - for realistic query patterns and joins",
    },
    "large": {
        "users": 100000,
        "products": 50000,
        "orders": 1000000,
        "description": "Large - for performance testing (generate on-demand)",
    },
    "very_large": {
        "users": 1000000,
        "products": 500000,
        "orders": 10000000,
        "description": "Very large - for scale testing (generate on-demand)",
    },
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _random_string(length: int = 10) -> str:
    """Return a random alphanumeric string of the given length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def _random_date(start_date: str = "2020-01-01", end_date: str = "2024-12-31") -> date:
    """Return a random date between *start_date* and *end_date* (inclusive)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).date()


# Keep the old public names as aliases so existing callers are not broken.
generate_random_string = _random_string
generate_random_date = _random_date

# ---------------------------------------------------------------------------
# Dataset generators
# ---------------------------------------------------------------------------


def generate_users_data(spark: SparkSession, num_records: int = 1000) -> DataFrame:
    """
    Generate a synthetic users DataFrame.

    Args:
        spark: Active SparkSession.
        num_records: Number of user rows to generate.

    Returns:
        DataFrame with columns:
        user_id, username, email, age, country, registration_date, is_active
    """
    schema = StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", DateType(), True),
            StructField("is_active", BooleanType(), True),
        ]
    )

    countries = [
        "USA",
        "UK",
        "Germany",
        "France",
        "Japan",
        "Australia",
        "Canada",
        "Brazil",
    ]

    data: List[Tuple] = []
    for i in range(1, num_records + 1):
        username = f"user_{_random_string(8)}"
        email = f"{username}@example.com"
        age = random.randint(18, 75)
        country = random.choice(countries)
        registration_date = _random_date()
        is_active = random.choice([True, False])
        data.append((i, username, email, age, country, registration_date, is_active))

    logger.debug("Generated %d user records", num_records)
    return spark.createDataFrame(data, schema)


def generate_products_data(spark: SparkSession, num_records: int = 500) -> DataFrame:
    """
    Generate a synthetic products DataFrame.

    Args:
        spark: Active SparkSession.
        num_records: Number of product rows to generate.

    Returns:
        DataFrame with columns:
        product_id, product_name, category, price, stock_quantity, rating, brand
    """
    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("rating", DoubleType(), True),
            StructField("brand", StringType(), True),
        ]
    )

    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
        "Beauty",
        "Food",
    ]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

    data: List[Tuple] = []
    for i in range(1, num_records + 1):
        product_name = f"Product_{_random_string(6)}"
        category = random.choice(categories)
        price = round(random.uniform(10.0, 1000.0), 2)
        stock_quantity = random.randint(0, 1000)
        rating = round(random.uniform(1.0, 5.0), 1)
        brand = random.choice(brands)
        data.append((i, product_name, category, price, stock_quantity, rating, brand))

    logger.debug("Generated %d product records", num_records)
    return spark.createDataFrame(data, schema)


def generate_orders_data(
    spark: SparkSession,
    num_records: int = 10000,
    user_ids: Optional[List[int]] = None,
    product_ids: Optional[List[int]] = None,
) -> DataFrame:
    """
    Generate a synthetic orders DataFrame.

    Args:
        spark: Active SparkSession.
        num_records: Number of order rows to generate.
        user_ids: List of valid user IDs to reference. Defaults to range(1, 1001).
        product_ids: List of valid product IDs to reference. Defaults to range(1, 501).

    Returns:
        DataFrame with columns:
        order_id, user_id, product_id, quantity, total_amount, order_date, status
    """
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_date", DateType(), True),
            StructField("status", StringType(), True),
        ]
    )

    statuses = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]

    if user_ids is None:
        user_ids = list(range(1, 1001))
    if product_ids is None:
        product_ids = list(range(1, 501))

    data: List[Tuple] = []
    for _ in range(num_records):
        order_id = f"ORD-{_random_string(10)}"
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        total_amount = round(random.uniform(20.0, 5000.0), 2)
        order_date = _random_date("2022-01-01", "2024-12-31")
        status = random.choice(statuses)
        data.append(
            (order_id, user_id, product_id, quantity, total_amount, order_date, status)
        )

    logger.debug("Generated %d order records", num_records)
    return spark.createDataFrame(data, schema)


def generate_all_datasets(
    spark: SparkSession,
    users_count: Optional[int] = None,
    products_count: Optional[int] = None,
    orders_count: Optional[int] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Generate all three datasets (users, products, orders) in one call.

    Record counts fall back to environment variables USERS_COUNT, PRODUCTS_COUNT,
    and ORDERS_COUNT, and ultimately to sensible defaults if neither is provided.

    Args:
        spark: Active SparkSession.
        users_count: Number of user rows.
        products_count: Number of product rows.
        orders_count: Number of order rows.

    Returns:
        Tuple of (users_df, products_df, orders_df).
    """
    users_count = users_count or int(os.getenv("USERS_COUNT", "1000"))
    products_count = products_count or int(os.getenv("PRODUCTS_COUNT", "500"))
    orders_count = orders_count or int(os.getenv("ORDERS_COUNT", "10000"))

    users_df = generate_users_data(spark, users_count)
    products_df = generate_products_data(spark, products_count)

    user_ids = [row.user_id for row in users_df.select("user_id").collect()]
    product_ids = [row.product_id for row in products_df.select("product_id").collect()]

    orders_df = generate_orders_data(
        spark, orders_count, user_ids=user_ids, product_ids=product_ids
    )

    return users_df, products_df, orders_df


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def save_dataset(
    df: DataFrame,
    output_path: str,
    file_format: str = "parquet",
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame to *output_path* using the specified format.

    Args:
        df: DataFrame to persist.
        output_path: Destination path (local or HDFS/S3).
        file_format: Spark write format (e.g. ``"parquet"``, ``"csv"``, ``"json"``).
        mode: Write mode — ``"overwrite"`` (default), ``"append"``, ``"ignore"``,
              or ``"error"``.
    """
    logger.debug(
        "Saving dataset to %s (format=%s, mode=%s)", output_path, file_format, mode
    )
    df.write.mode(mode).format(file_format).save(output_path)


def load_dataset(
    spark: SparkSession, input_path: str, file_format: str = "parquet"
) -> DataFrame:
    """
    Read a dataset from *input_path*.

    Args:
        spark: Active SparkSession.
        input_path: Source path (local or HDFS/S3).
        file_format: Spark read format (e.g. ``"parquet"``, ``"csv"``, ``"json"``).

    Returns:
        DataFrame loaded from the given path.
    """
    logger.debug("Loading dataset from %s (format=%s)", input_path, file_format)
    return spark.read.format(file_format).load(input_path)


# ---------------------------------------------------------------------------
# Convenience: generate + save in one shot
# ---------------------------------------------------------------------------


def generate_and_save_all_datasets(
    spark: SparkSession, base_path: str = "/opt/spark/data", size: str = "small"
) -> None:
    """
    Generate all datasets for the given *size* and write them to *base_path*.

    Three formats are produced for each dataset:
    - Parquet  → ``<base_path>/<dataset>/<size>/``
    - CSV      → ``<base_path>/<dataset>/<size>.csv/``
    - JSON     → ``<base_path>/<dataset>/<size>.json/``

    Args:
        spark: Active SparkSession.
        base_path: Root directory for output data. Defaults to ``/opt/spark/data``.
        size: One of the keys in :data:`DATA_SIZE_CONFIG`
              (``"very_small"``, ``"small"``, ``"medium"``, ``"large"``, ``"very_large"``).
              Defaults to ``"small"``.
    """
    size_config = DATA_SIZE_CONFIG.get(size, DATA_SIZE_CONFIG["small"])
    logger.info(
        "Generating '%s' datasets (%s)",
        size,
        size_config["description"],
    )

    users_df, products_df, orders_df = generate_all_datasets(
        spark,
        users_count=size_config["users"],
        products_count=size_config["products"],
        orders_count=size_config["orders"],
    )

    # --- Parquet ---
    save_dataset(users_df, f"{base_path}/users/{size}", "parquet")
    save_dataset(products_df, f"{base_path}/products/{size}", "parquet")
    save_dataset(orders_df, f"{base_path}/orders/{size}", "parquet")

    # --- CSV ---
    users_df.coalesce(1).write.mode("overwrite").csv(
        f"{base_path}/users/{size}.csv", header=True
    )
    products_df.coalesce(1).write.mode("overwrite").csv(
        f"{base_path}/products/{size}.csv", header=True
    )
    orders_df.coalesce(1).write.mode("overwrite").csv(
        f"{base_path}/orders/{size}.csv", header=True
    )

    # --- JSON ---
    users_df.coalesce(1).write.mode("overwrite").json(f"{base_path}/users/{size}.json")
    products_df.coalesce(1).write.mode("overwrite").json(
        f"{base_path}/products/{size}.json"
    )
    orders_df.coalesce(1).write.mode("overwrite").json(
        f"{base_path}/orders/{size}.json"
    )

    users_count = users_df.count()
    products_count = products_df.count()
    orders_count = orders_df.count()

    logger.info(
        "Generated '%s' datasets: %d users, %d products, %d orders — saved to %s",
        size,
        users_count,
        products_count,
        orders_count,
        base_path,
    )

import argparse
import sys

sys.path.insert(0, "/opt/spark")

from utils.data_generator import generate_and_save_all_datasets
from utils.spark_session import create_spark_session


VALID_SIZES = ["very_small", "small", "medium", "large", "very_large"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate sample datasets for the Spark Lab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Sizes:
  very_small  100 users /    50 products /    100 orders  (smoke tests)
  small       1K users  /   500 products /     1K orders  (basic learning)
  medium      10K users /    5K products /    10K orders  (realistic patterns)
  large       100K users /  50K products /     1M orders  (performance testing)
  very_large  1M users  / 500K products /    10M orders  (scale testing)
  all         Generates very_small + small + medium
        """,
    )
    parser.add_argument(
        "--size",
        choices=VALID_SIZES + ["all"],
        default="all",
        help="Dataset size to generate (default: all -> very_small + small + medium).",
    )
    parser.add_argument(
        "--base-path",
        default="/opt/spark/data",
        help="Base output path for datasets (default: /opt/spark/data).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(app_name="Spark-Lab-Data-Generator")

    sizes = ["very_small", "small", "medium"] if args.size == "all" else [args.size]
    for size in sizes:
        generate_and_save_all_datasets(spark, base_path=args.base_path, size=size)

    spark.stop()


if __name__ == "__main__":
    main()

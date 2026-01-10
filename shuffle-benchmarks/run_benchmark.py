#!/usr/bin/env python3
"""
Run shuffle size comparison benchmark.

Run this script once per mode (spark, jvm, native) with appropriate spark-submit configs.
Check the Spark UI to compare shuffle sizes between modes.
"""

import argparse
import time
import json

from pyspark.sql import SparkSession


def run_benchmark(spark: SparkSession, data_path: str) -> int:
    """Run the benchmark query and return duration in ms."""

    spark.catalog.clearCache()

    df = spark.read.parquet(data_path)
    row_count = df.count()
    print(f"Number of rows: {row_count:,}")

    # Register as temp views to enable proper aliasing
    df.createOrReplaceTempView("test_data")

    start_time = time.time()

    # Join on different columns to force shuffle on both sides
    # left shuffles by category_id, right shuffles by region_id
    result = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM test_data a
        JOIN test_data b ON a.category_id = b.region_id
    """)

    result_count = result.collect()[0]["cnt"]
    print(f"Join result rows: {result_count:,}")

    duration_ms = int((time.time() - start_time) * 1000)
    return duration_ms


def main():
    parser = argparse.ArgumentParser(
        description="Run shuffle benchmark for a single mode"
    )
    parser.add_argument(
        "--data", "-d",
        required=True,
        help="Path to input parquet data"
    )
    parser.add_argument(
        "--mode", "-m",
        required=True,
        choices=["spark", "jvm", "native"],
        help="Shuffle mode being tested"
    )

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"ShuffleBenchmark-{args.mode.upper()}") \
        .getOrCreate()

    print("\n" + "=" * 80)
    print(f"Shuffle Benchmark: {args.mode.upper()}")
    print("=" * 80)
    print(f"Data path: {args.data}")

    # Print shuffle configuration
    conf = spark.sparkContext.getConf()
    print(f"Shuffle manager: {conf.get('spark.shuffle.manager', 'default')}")
    print(f"Comet enabled: {conf.get('spark.comet.enabled', 'false')}")
    print(f"Comet shuffle enabled: {conf.get('spark.comet.exec.shuffle.enabled', 'false')}")
    print(f"Comet shuffle mode: {conf.get('spark.comet.shuffle.mode', 'not set')}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

    try:
        duration_ms = run_benchmark(spark, args.data)
        print(f"\nDuration: {duration_ms:,} ms")
        print("\nCheck Spark UI for shuffle sizes")

    finally:
        spark.stop()

    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()

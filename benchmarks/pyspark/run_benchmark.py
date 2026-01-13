#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Run shuffle size comparison benchmark.

Run this script once per mode (spark, jvm, native) with appropriate spark-submit configs.
Check the Spark UI to compare shuffle sizes between modes.
"""

import argparse
import time
import json

from pyspark.sql import SparkSession


def run_benchmark(spark: SparkSession, data_path: str, mode: str) -> int:
    """Run the benchmark query and return duration in ms."""

    spark.catalog.clearCache()

    df = spark.read.parquet(data_path)
    row_count = df.count()
    print(f"Number of rows: {row_count:,}")

    start_time = time.time()

    # Repartition to force full shuffle of all columns

    # repartition using round-robin partitioning
    repartitioned = df.repartition(200)

    # repartition using hash partitioning
    # repartitioned = df.repartition(200, "group_key")

    # Write to parquet to force materialization
    output_path = f"/tmp/shuffle-benchmark-output-{mode}"
    repartitioned.write.mode("overwrite").parquet(output_path)
    print(f"Wrote repartitioned data to: {output_path}")

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
        .config("spark.comet.explain.format", "verbose") \
        .config("spark.comet.explainFallback.enabled", "true") \
        .config("spark.comet.logFallbackReasons.enabled", "true") \
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
        duration_ms = run_benchmark(spark, args.data, args.mode)
        print(f"\nDuration: {duration_ms:,} ms")
        print("\nCheck Spark UI for shuffle sizes")

    finally:
        spark.stop()

    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()

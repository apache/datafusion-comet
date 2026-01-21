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
Run PySpark benchmarks.

Run benchmarks by name with appropriate spark-submit configs for different modes
(spark, jvm, native). Check the Spark UI to compare results between modes.
"""

import argparse
import sys

from pyspark.sql import SparkSession

from benchmarks import get_benchmark, list_benchmarks


def main():
    parser = argparse.ArgumentParser(
        description="Run PySpark benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run hash partitioning shuffle benchmark in Spark mode
  python run_benchmark.py --data /path/to/data --mode spark --benchmark shuffle-hash

  # Run round-robin shuffle benchmark in Comet native mode
  python run_benchmark.py --data /path/to/data --mode native --benchmark shuffle-roundrobin

  # List all available benchmarks
  python run_benchmark.py --list-benchmarks
        """
    )
    parser.add_argument(
        "--data", "-d",
        help="Path to input parquet data"
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["spark", "jvm", "native"],
        help="Shuffle mode being tested"
    )
    parser.add_argument(
        "--benchmark", "-b",
        default="shuffle-hash",
        help="Benchmark to run (default: shuffle-hash)"
    )
    parser.add_argument(
        "--list-benchmarks",
        action="store_true",
        help="List all available benchmarks and exit"
    )

    args = parser.parse_args()

    # Handle --list-benchmarks
    if args.list_benchmarks:
        print("Available benchmarks:\n")
        for name, description in list_benchmarks():
            print(f"  {name:25s} - {description}")
        return 0

    # Validate required arguments
    if not args.data:
        parser.error("--data is required when running a benchmark")
    if not args.mode:
        parser.error("--mode is required when running a benchmark")

    # Get the benchmark class
    try:
        benchmark_cls = get_benchmark(args.benchmark)
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        print("\nUse --list-benchmarks to see available benchmarks", file=sys.stderr)
        return 1

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"{benchmark_cls.name()}-{args.mode.upper()}") \
        .getOrCreate()

    try:
        # Create and run the benchmark
        benchmark = benchmark_cls(spark, args.data, args.mode)
        results = benchmark.execute_timed()

        print("\nCheck Spark UI for shuffle sizes and detailed metrics")
        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

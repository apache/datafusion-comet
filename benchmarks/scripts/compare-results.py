#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import sys
from pathlib import Path


def load_result(filepath: str) -> dict:
    with open(filepath, 'r') as f:
        return json.load(f)


def calculate_speedup(spark_duration: float, comet_duration: float) -> float:
    if comet_duration <= 0:
        return float('inf')
    return spark_duration / comet_duration


def print_comparison(spark_result: dict, comet_result: dict, speedup: float, min_speedup: float):
    spark_duration = spark_result['duration_seconds']
    comet_duration = comet_result['duration_seconds']

    print("\n" + "=" * 50)
    print("BENCHMARK COMPARISON")
    print("=" * 50)
    print(f"Query: {spark_result.get('query', 'unknown')}")
    print(f"Spark: {spark_duration:.2f}s")
    print(f"Comet: {comet_duration:.2f}s")
    print(f"Speedup: {speedup:.2f}x")
    print(f"Required: {min_speedup:.2f}x")
    print("-" * 50)

    if speedup >= min_speedup:
        print(f"PASS: {speedup:.2f}x >= {min_speedup:.2f}x")
    else:
        print(f"FAIL: {speedup:.2f}x < {min_speedup:.2f}x")
    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results")
    parser.add_argument("--spark", "-s", required=True, help="Spark result JSON")
    parser.add_argument("--comet", "-c", required=True, help="Comet result JSON")
    parser.add_argument("--min-speedup", type=float, default=1.1, help="Min speedup (default: 1.1)")
    parser.add_argument("--output", "-o", help="Output summary JSON")
    parser.add_argument("--strict", action="store_true", help="Exit with error if below threshold")

    args = parser.parse_args()

    if not Path(args.spark).exists():
        print(f"Error: {args.spark} not found", file=sys.stderr)
        return 1

    if not Path(args.comet).exists():
        print(f"Error: {args.comet} not found", file=sys.stderr)
        return 1

    spark_result = load_result(args.spark)
    comet_result = load_result(args.comet)

    speedup = calculate_speedup(
        spark_result['duration_seconds'],
        comet_result['duration_seconds']
    )

    passed = speedup >= args.min_speedup
    print_comparison(spark_result, comet_result, speedup, args.min_speedup)

    if args.output:
        summary = {
            "spark": spark_result,
            "comet": comet_result,
            "speedup": speedup,
            "min_speedup": args.min_speedup,
            "passed": passed
        }
        with open(args.output, 'w') as f:
            json.dump(summary, f, indent=2)

    if args.strict and not passed:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

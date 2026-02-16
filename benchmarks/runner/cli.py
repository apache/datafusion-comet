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
Single CLI entry point for the unified benchmark runner.

Designed to be the Python script passed to ``spark-submit``.  Subcommands
correspond to benchmark suites (currently: ``tpc``, ``shuffle``).

Usage (via spark-submit)::

    spark-submit ... benchmarks/runner/cli.py tpc --benchmark tpch --data /path ...
    spark-submit ... benchmarks/runner/cli.py shuffle --benchmark shuffle-hash --data /path ...
"""

import argparse
import json
import os
import sys

from benchmarks.runner.spark_session import create_session
from benchmarks.suites import tpc
from benchmarks.suites import shuffle


# ---------------------------------------------------------------------------
# Profiling helpers
# ---------------------------------------------------------------------------

def _maybe_start_profiler(spark, args):
    """Start profiler if ``--profile`` was passed.  Returns profiler or None."""
    if not getattr(args, "profile", False):
        return None
    from benchmarks.runner.profiling import SparkMetricsProfiler

    interval = getattr(args, "profile_interval", 2.0)
    profiler = SparkMetricsProfiler(spark, interval_secs=interval)
    profiler.start()
    return profiler


def _maybe_stop_profiler(profiler, output_dir, name, benchmark):
    """Stop profiler and write CSV if active."""
    if profiler is None:
        return
    profiler.stop()
    csv_path = os.path.join(output_dir, f"{name}-{benchmark}-metrics.csv")
    profiler.write_csv(csv_path)


def _add_profiling_args(parser):
    """Add common profiling flags to a subparser."""
    parser.add_argument(
        "--profile", action="store_true",
        help="Enable Level 1 JVM metrics profiling via Spark REST API",
    )
    parser.add_argument(
        "--profile-interval", type=float, default=2.0,
        help="Profiling poll interval in seconds (default: 2.0)",
    )


# ---------------------------------------------------------------------------
# TPC subcommand
# ---------------------------------------------------------------------------

def _add_tpc_subparser(subparsers):
    """Register the ``tpc`` subcommand with the same args as tpcbench.py."""
    p = subparsers.add_parser(
        "tpc",
        help="Run TPC-H or TPC-DS benchmarks",
        description="TPC-H/TPC-DS benchmark runner for files or Iceberg tables",
    )
    p.add_argument("--benchmark", required=True, help="tpch or tpcds")

    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--data", help="Path to data files")
    source.add_argument("--catalog", help="Iceberg catalog name")

    p.add_argument(
        "--format", default="parquet",
        help="Input file format: parquet, csv, json (only with --data)",
    )
    p.add_argument(
        "--options", type=json.loads, default={},
        help='Spark reader options as JSON, e.g. \'{"header": "true"}\'',
    )
    p.add_argument(
        "--database", default="tpch",
        help="Database containing TPC tables (only with --catalog)",
    )
    p.add_argument("--queries", required=True, help="Path to query SQL files")
    p.add_argument("--iterations", type=int, default=1, help="Number of iterations")
    p.add_argument("--output", required=True, help="Directory for results JSON")
    p.add_argument("--name", required=True, help="Prefix for result file")
    p.add_argument("--query", type=int, help="Run a single query (1-based)")
    p.add_argument("--write", help="Path to save query results as Parquet")
    _add_profiling_args(p)


def _run_tpc(args):
    """Execute the TPC suite."""
    spark = create_session(
        app_name=f"{args.name} benchmark derived from {args.benchmark}",
        spark_conf={},  # configs already set by spark-submit
    )

    profiler = _maybe_start_profiler(spark, args)

    using_iceberg = tpc.register_tables(
        spark,
        benchmark=args.benchmark,
        data_path=args.data,
        catalog=args.catalog,
        database=args.database,
        file_format=args.format,
        reader_options=args.options,
    )

    timings = tpc.run_queries(
        spark,
        benchmark=args.benchmark,
        query_path=args.queries,
        iterations=args.iterations,
        query_num=args.query,
        write_path=args.write,
    )

    results = tpc.build_results(
        spark,
        benchmark=args.benchmark,
        query_path=args.queries,
        data_path=args.data,
        catalog=args.catalog,
        database=args.database,
        using_iceberg=using_iceberg,
        name=args.name,
        timings=timings,
    )

    tpc.write_results(results, args.output, args.name, args.benchmark)
    _maybe_stop_profiler(profiler, args.output, args.name, args.benchmark)
    spark.stop()


# ---------------------------------------------------------------------------
# Shuffle subcommand
# ---------------------------------------------------------------------------

def _add_shuffle_subparser(subparsers):
    """Register the ``shuffle`` subcommand."""
    p = subparsers.add_parser(
        "shuffle",
        help="Run shuffle benchmarks (hash, round-robin)",
        description=(
            "Shuffle benchmark runner.  Tests different partitioning strategies "
            "across Spark, Comet JVM, and Comet Native shuffle implementations."
        ),
    )
    p.add_argument(
        "--benchmark", required=True,
        choices=list(shuffle.BENCHMARKS),
        help="Shuffle benchmark to run",
    )
    p.add_argument("--data", required=True, help="Path to input parquet data")
    p.add_argument(
        "--mode", required=True,
        choices=["spark", "jvm", "native"],
        help="Shuffle mode being tested",
    )
    p.add_argument(
        "--partitions", type=int, default=200,
        help="Number of shuffle partitions (default: 200)",
    )
    p.add_argument("--iterations", type=int, default=1, help="Number of iterations")
    p.add_argument("--output", required=True, help="Directory for results JSON")
    p.add_argument("--name", required=True, help="Prefix for result file")
    _add_profiling_args(p)


def _run_shuffle(args):
    """Execute the shuffle suite."""
    spark = create_session(
        app_name=f"{args.name}-{args.benchmark}-{args.mode.upper()}",
        spark_conf={},  # configs already set by spark-submit
    )

    profiler = _maybe_start_profiler(spark, args)

    timings = shuffle.run_shuffle(
        spark,
        benchmark=args.benchmark,
        data_path=args.data,
        mode=args.mode,
        num_partitions=args.partitions,
        iterations=args.iterations,
    )

    results = shuffle.build_results(
        spark,
        benchmark=args.benchmark,
        data_path=args.data,
        mode=args.mode,
        name=args.name,
        timings=timings,
    )

    shuffle.write_results(results, args.output, args.name, args.benchmark)
    _maybe_stop_profiler(profiler, args.output, args.name, args.benchmark)
    spark.stop()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="benchmark-runner",
        description="Unified benchmark runner for Apache DataFusion Comet",
    )
    subparsers = parser.add_subparsers(dest="suite", required=True)
    _add_tpc_subparser(subparsers)
    _add_shuffle_subparser(subparsers)

    args = parser.parse_args(argv)

    if args.suite == "tpc":
        _run_tpc(args)
    elif args.suite == "shuffle":
        _run_shuffle(args)
    else:
        parser.error(f"Unknown suite: {args.suite}")


if __name__ == "__main__":
    main()

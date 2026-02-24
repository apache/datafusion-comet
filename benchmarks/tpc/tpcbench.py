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
TPC-H / TPC-DS benchmark runner.

Supports two data sources:
  - Files: use --data with --format (parquet, csv, json) and optional --options
  - Iceberg tables: use --catalog and --database to specify the catalog location
"""

import argparse
from datetime import datetime
import json
import os
from pyspark.sql import SparkSession
import time
from typing import Dict


def dedup_columns(df):
    """Rename duplicate column aliases: a, a, b, b -> a, a_1, b, b_1"""
    counts = {}
    new_cols = []
    for c in df.columns:
        if c not in counts:
            counts[c] = 0
            new_cols.append(c)
        else:
            counts[c] += 1
            new_cols.append(f"{c}_{counts[c]}")
    return df.toDF(*new_cols)


def main(
    benchmark: str,
    data_path: str,
    catalog: str,
    database: str,
    iterations: int,
    output: str,
    name: str,
    format: str,
    query_num: int = None,
    write_path: str = None,
    options: Dict[str, str] = None,
    profile: bool = False,
    profile_interval: float = 2.0,
):
    if options is None:
        options = {}

    query_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "queries", benchmark
    )

    spark = SparkSession.builder \
        .appName(f"{name} benchmark derived from {benchmark}") \
        .getOrCreate()

    profiler = None
    if profile:
        from profiling import SparkMetricsProfiler
        profiler = SparkMetricsProfiler(spark, interval_secs=profile_interval)
        profiler.start()

    # Define tables for each benchmark
    if benchmark == "tpch":
        num_queries = 22
        table_names = [
            "customer", "lineitem", "nation", "orders",
            "part", "partsupp", "region", "supplier"
        ]
    elif benchmark == "tpcds":
        num_queries = 99
        table_names = [
            "call_center", "catalog_page", "catalog_returns", "catalog_sales",
            "customer", "customer_address", "customer_demographics", "date_dim",
            "time_dim", "household_demographics", "income_band", "inventory",
            "item", "promotion", "reason", "ship_mode", "store", "store_returns",
            "store_sales", "warehouse", "web_page", "web_returns", "web_sales",
            "web_site"
        ]
    else:
        raise ValueError(f"Invalid benchmark: {benchmark}")

    # Register tables from either files or Iceberg catalog
    using_iceberg = catalog is not None
    for table in table_names:
        if using_iceberg:
            source = f"{catalog}.{database}.{table}"
            print(f"Registering table {table} from {source}")
            df = spark.table(source)
        else:
            # Support both "customer/" and "customer.parquet/" layouts
            source = f"{data_path}/{table}.{format}"
            if not os.path.exists(source):
                source = f"{data_path}/{table}"
            print(f"Registering table {table} from {source}")
            df = spark.read.format(format).options(**options).load(source)
        df.createOrReplaceTempView(table)

    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    results = {
        'engine': 'datafusion-comet',
        'benchmark': benchmark,
        'spark_conf': conf_dict,
    }
    if using_iceberg:
        results['catalog'] = catalog
        results['database'] = database
    else:
        results['data_path'] = data_path

    for iteration in range(iterations):
        print(f"\n{'='*60}")
        print(f"Starting iteration {iteration + 1} of {iterations}")
        print(f"{'='*60}")
        iter_start_time = time.time()

        # Determine which queries to run
        if query_num is not None:
            if query_num < 1 or query_num > num_queries:
                raise ValueError(
                    f"Query number {query_num} out of range. "
                    f"Valid: 1-{num_queries} for {benchmark}"
                )
            queries_to_run = [query_num]
        else:
            queries_to_run = range(1, num_queries + 1)

        for query in queries_to_run:
            spark.sparkContext.setJobDescription(f"{benchmark} q{query}")

            path = f"{query_path}/q{query}.sql"
            print(f"\nRunning query {query} from {path}")

            with open(path, "r") as f:
                text = f.read()
                queries = text.split(";")

                start_time = time.time()
                for sql in queries:
                    sql = sql.strip().replace("create view", "create temp view")
                    if len(sql) > 0:
                        print(f"Executing: {sql[:100]}...")
                        df = spark.sql(sql)
                        df.explain("formatted")

                        if write_path is not None:
                            if len(df.columns) > 0:
                                output_path = f"{write_path}/q{query}"
                                deduped = dedup_columns(df)
                                deduped.orderBy(*deduped.columns).coalesce(1).write.mode("overwrite").parquet(output_path)
                                print(f"Results written to {output_path}")
                        else:
                            rows = df.collect()
                            print(f"Query {query} returned {len(rows)} rows")

                end_time = time.time()
                elapsed = end_time - start_time
                print(f"Query {query} took {elapsed:.2f} seconds")

                query_timings = results.setdefault(query, [])
                query_timings.append(elapsed)

        iter_end_time = time.time()
        print(f"\nIteration {iteration + 1} took {iter_end_time - iter_start_time:.2f} seconds")

    # Write results
    result_str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"{output}/{name}-{benchmark}-{current_time_millis}.json"
    print(f"\nWriting results to {results_path}")
    with open(results_path, "w") as f:
        f.write(result_str)

    if profiler is not None:
        profiler.stop()
        metrics_path = f"{output}/{name}-{benchmark}-metrics.csv"
        profiler.write_csv(metrics_path)
        profiler.snapshot_cgroup_metrics(output, name, benchmark)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TPC-H/TPC-DS benchmark runner for files or Iceberg tables"
    )
    parser.add_argument(
        "--benchmark", required=True,
        help="Benchmark to run (tpch or tpcds)"
    )

    # Data source - mutually exclusive: either file path or Iceberg catalog
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--data",
        help="Path to data files"
    )
    source_group.add_argument(
        "--catalog",
        help="Iceberg catalog name"
    )

    # Options for file-based reading
    parser.add_argument(
        "--format", default="parquet",
        help="Input file format: parquet, csv, json (only used with --data)"
    )
    parser.add_argument(
        "--options", type=json.loads, default={},
        help='Spark reader options as JSON string, e.g., \'{"header": "true"}\' (only used with --data)'
    )

    # Options for Iceberg
    parser.add_argument(
        "--database", default="tpch",
        help="Database containing TPC tables (only used with --catalog)"
    )

    parser.add_argument(
        "--iterations", type=int, default=1,
        help="Number of iterations"
    )
    parser.add_argument(
        "--output", required=True,
        help="Path to write results JSON"
    )
    parser.add_argument(
        "--name", required=True,
        help="Prefix for result file"
    )
    parser.add_argument(
        "--query", type=int,
        help="Specific query number (1-based). If omitted, run all."
    )
    parser.add_argument(
        "--write",
        help="Path to save query results as Parquet"
    )
    parser.add_argument(
        "--profile", action="store_true",
        help="Enable executor metrics profiling via Spark REST API"
    )
    parser.add_argument(
        "--profile-interval", type=float, default=2.0,
        help="Profiling poll interval in seconds (default: 2.0)"
    )
    args = parser.parse_args()

    main(
        args.benchmark,
        args.data,
        args.catalog,
        args.database,
        args.iterations,
        args.output,
        args.name,
        args.format,
        args.query,
        args.write,
        args.options,
        args.profile,
        args.profile_interval,
    )

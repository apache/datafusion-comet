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

"""TPC-H / TPC-DS benchmark suite."""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession


# Table definitions per benchmark
TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]

TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "time_dim", "household_demographics", "income_band", "inventory",
    "item", "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "warehouse", "web_page", "web_returns", "web_sales",
    "web_site",
]

BENCHMARK_META = {
    "tpch":  {"num_queries": 22, "tables": TPCH_TABLES},
    "tpcds": {"num_queries": 99, "tables": TPCDS_TABLES},
}


def dedup_columns(df):
    """Rename duplicate column aliases: a, a, b, b -> a, a_1, b, b_1."""
    counts: Dict[str, int] = {}
    new_cols: List[str] = []
    for c in df.columns:
        if c not in counts:
            counts[c] = 0
            new_cols.append(c)
        else:
            counts[c] += 1
            new_cols.append(f"{c}_{counts[c]}")
    return df.toDF(*new_cols)


def register_tables(
    spark: SparkSession,
    benchmark: str,
    data_path: Optional[str],
    catalog: Optional[str],
    database: str,
    file_format: str,
    reader_options: Optional[Dict[str, str]],
) -> bool:
    """Register TPC tables as temp views.

    Returns True when using Iceberg catalog, False for file-based tables.
    """
    if benchmark not in BENCHMARK_META:
        raise ValueError(f"Invalid benchmark: {benchmark}")
    tables = BENCHMARK_META[benchmark]["tables"]
    using_iceberg = catalog is not None

    for table in tables:
        if using_iceberg:
            source = f"{catalog}.{database}.{table}"
            print(f"Registering table {table} from {source}")
            df = spark.table(source)
        else:
            source = f"{data_path}/{table}.{file_format}"
            print(f"Registering table {table} from {source}")
            df = spark.read.format(file_format).options(**(reader_options or {})).load(source)
        df.createOrReplaceTempView(table)

    return using_iceberg


def run_queries(
    spark: SparkSession,
    benchmark: str,
    query_path: str,
    iterations: int,
    query_num: Optional[int] = None,
    write_path: Optional[str] = None,
) -> Dict[int, List[float]]:
    """Execute TPC queries and return {query_num: [elapsed_secs_per_iter]}."""
    meta = BENCHMARK_META[benchmark]
    num_queries = meta["num_queries"]
    results: Dict[int, List[float]] = {}

    for iteration in range(iterations):
        print(f"\n{'=' * 60}")
        print(f"Starting iteration {iteration + 1} of {iterations}")
        print(f"{'=' * 60}")
        iter_start_time = time.time()

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
                queries_sql = text.split(";")

                start_time = time.time()
                for sql in queries_sql:
                    sql = sql.strip().replace("create view", "create temp view")
                    if len(sql) > 0:
                        print(f"Executing: {sql[:100]}...")
                        df = spark.sql(sql)
                        df.explain("formatted")

                        if write_path is not None:
                            if len(df.columns) > 0:
                                output_path = f"{write_path}/q{query}"
                                deduped = dedup_columns(df)
                                deduped.orderBy(*deduped.columns).coalesce(1).write.mode(
                                    "overwrite"
                                ).parquet(output_path)
                                print(f"Results written to {output_path}")
                        else:
                            rows = df.collect()
                            print(f"Query {query} returned {len(rows)} rows")

                end_time = time.time()
                elapsed = end_time - start_time
                print(f"Query {query} took {elapsed:.2f} seconds")

                results.setdefault(query, []).append(elapsed)

        iter_end_time = time.time()
        print(
            f"\nIteration {iteration + 1} took "
            f"{iter_end_time - iter_start_time:.2f} seconds"
        )

    return results


def build_results(
    spark: SparkSession,
    benchmark: str,
    query_path: str,
    data_path: Optional[str],
    catalog: Optional[str],
    database: str,
    using_iceberg: bool,
    name: str,
    timings: Dict[int, List[float]],
) -> Dict:
    """Assemble the result dict with the same schema as tpcbench.py."""
    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    results: Dict = {
        "engine": "datafusion-comet",
        "benchmark": benchmark,
        "query_path": query_path,
        "spark_conf": conf_dict,
    }
    if using_iceberg:
        results["catalog"] = catalog
        results["database"] = database
    else:
        results["data_path"] = data_path

    # Integer query keys — json.dumps serialises them as strings, matching
    # the format that generate-comparison.py expects (str(query)).
    for query, elapsed_list in timings.items():
        results[query] = elapsed_list

    return results


def write_results(
    results: Dict,
    output_dir: str,
    name: str,
    benchmark: str,
) -> str:
    """Write JSON results file.  Returns the path written."""
    result_str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"{output_dir}/{name}-{benchmark}-{current_time_millis}.json"
    print(f"\nWriting results to {results_path}")
    with open(results_path, "w") as f:
        f.write(result_str)
    return results_path

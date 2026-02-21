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
Convert TPC-H or TPC-DS Parquet data to Iceberg tables.

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
        create-iceberg-tables.py \
        --benchmark tpch \
        --parquet-path /path/to/tpch/parquet \
        --warehouse /path/to/iceberg-warehouse

    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
        create-iceberg-tables.py \
        --benchmark tpcds \
        --parquet-path /path/to/tpcds/parquet \
        --warehouse /path/to/iceberg-warehouse
"""

import argparse
import os
import sys
from pyspark.sql import SparkSession
import time

TPCH_TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "time_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

BENCHMARK_TABLES = {
    "tpch": TPCH_TABLES,
    "tpcds": TPCDS_TABLES,
}


def main(benchmark: str, parquet_path: str, warehouse: str, catalog: str, database: str):
    table_names = BENCHMARK_TABLES[benchmark]

    # Validate paths before starting Spark
    errors = []
    if not os.path.isdir(parquet_path):
        errors.append(f"Error: --parquet-path '{parquet_path}' does not exist or is not a directory")
    if not os.path.isdir(warehouse):
        errors.append(f"Error: --warehouse '{warehouse}' does not exist or is not a directory. "
                       "Create it with: mkdir -p " + warehouse)
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(f"Create Iceberg {benchmark.upper()} Tables") \
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse) \
        .getOrCreate()

    # Set the Iceberg catalog as the current catalog so that
    # namespace operations are routed correctly
    spark.sql(f"USE {catalog}")

    # Create namespace if it doesn't exist
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {database}")
    except Exception:
        # Namespace may already exist
        pass

    for table in table_names:
        parquet_table_path = f"{parquet_path}/{table}.parquet"
        iceberg_table = f"{catalog}.{database}.{table}"

        print(f"Converting {parquet_table_path} -> {iceberg_table}")
        start_time = time.time()

        # Drop table if exists to allow re-running
        spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")

        # Read parquet and write as Iceberg
        df = spark.read.parquet(parquet_table_path)
        df.writeTo(iceberg_table).using("iceberg").create()

        row_count = spark.table(iceberg_table).count()
        elapsed = time.time() - start_time
        print(f"  Created {iceberg_table} with {row_count} rows in {elapsed:.2f}s")

    print(f"\nAll {benchmark.upper()} tables created successfully!")
    print(f"Tables available at: {catalog}.{database}.*")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert TPC-H or TPC-DS Parquet data to Iceberg tables"
    )
    parser.add_argument(
        "--benchmark", required=True, choices=["tpch", "tpcds"],
        help="Benchmark whose tables to convert (tpch or tpcds)"
    )
    parser.add_argument(
        "--parquet-path", required=True,
        help="Path to Parquet data directory"
    )
    parser.add_argument(
        "--warehouse", required=True,
        help="Path to Iceberg warehouse directory"
    )
    parser.add_argument(
        "--catalog", default="local",
        help="Iceberg catalog name (default: 'local')"
    )
    parser.add_argument(
        "--database", default=None,
        help="Database name to create tables in (defaults to benchmark name)"
    )
    args = parser.parse_args()

    database = args.database if args.database else args.benchmark
    main(args.benchmark, args.parquet_path, args.warehouse, args.catalog, database)

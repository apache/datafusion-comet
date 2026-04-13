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
Convert TPC-H or TPC-DS Parquet data to Delta Lake tables.

Usage:
    spark-submit \
        --packages io.delta:delta-spark_2.12:3.3.2 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        create-delta-tables.py \
        --benchmark tpch \
        --parquet-path /path/to/tpch/parquet \
        --warehouse /path/to/delta-warehouse

    spark-submit \
        --packages io.delta:delta-spark_2.12:3.3.2 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        create-delta-tables.py \
        --benchmark tpcds \
        --parquet-path /path/to/tpcds/parquet \
        --warehouse /path/to/delta-warehouse
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession


TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier"
]

TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]


def create_delta_tables(spark, benchmark, parquet_path, warehouse):
    tables = TPCH_TABLES if benchmark == "tpch" else TPCDS_TABLES

    for table_name in tables:
        input_path = os.path.join(parquet_path, table_name)
        output_path = os.path.join(warehouse, table_name)

        if not os.path.exists(input_path) and not input_path.startswith("s3"):
            print(f"  Skipping {table_name}: {input_path} does not exist")
            continue

        print(f"  Converting {table_name}: {input_path} -> {output_path}")
        df = spark.read.parquet(input_path)
        df.write.format("delta").mode("overwrite").save(output_path)
        print(f"    {table_name}: {df.count()} rows written")


def main():
    parser = argparse.ArgumentParser(
        description="Convert TPC Parquet data to Delta Lake tables"
    )
    parser.add_argument(
        "--benchmark", required=True, choices=["tpch", "tpcds"],
        help="Which TPC benchmark to convert"
    )
    parser.add_argument(
        "--parquet-path", required=True,
        help="Path to the TPC Parquet data directory"
    )
    parser.add_argument(
        "--warehouse", required=True,
        help="Path to the Delta warehouse directory"
    )
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"Create Delta {args.benchmark.upper()} Tables") \
        .getOrCreate()

    print(f"Converting {args.benchmark.upper()} tables from Parquet to Delta...")
    create_delta_tables(spark, args.benchmark, args.parquet_path, args.warehouse)
    print("Done.")

    spark.stop()


if __name__ == "__main__":
    main()

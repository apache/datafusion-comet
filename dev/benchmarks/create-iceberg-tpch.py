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
Convert TPC-H Parquet data to Iceberg tables.

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=/path/to/iceberg-warehouse \
        create-iceberg-tpch.py \
        --parquet-path /path/to/tpch/parquet \
        --catalog local \
        --database tpch
"""

import argparse
from pyspark.sql import SparkSession
import time


def main(parquet_path: str, catalog: str, database: str):
    spark = SparkSession.builder \
        .appName("Create Iceberg TPC-H Tables") \
        .getOrCreate()

    table_names = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier"
    ]

    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

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

    print("\nAll TPC-H tables created successfully!")
    print(f"Tables available at: {catalog}.{database}.*")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert TPC-H Parquet data to Iceberg tables")
    parser.add_argument("--parquet-path", required=True, help="Path to TPC-H Parquet data directory")
    parser.add_argument("--catalog", required=True, help="Iceberg catalog name (e.g., 'local')")
    parser.add_argument("--database", default="tpch", help="Database name to create tables in")
    args = parser.parse_args()

    main(args.parquet_path, args.catalog, args.database)

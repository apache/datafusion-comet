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

import argparse
from pyspark.sql import SparkSession
import time

def main(data_path: str, query_path: str, iterations: int):

    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("DataFusion Microbenchmarks") \
        .getOrCreate()

    # Register the tables
    table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
       "customer_address", "customer_demographics", "date_dim", "time_dim", "household_demographics",
       "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns",
       "store_sales", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)

    # read sql file
    print(f"Reading query from path {query_path}")
    with open(query_path, "r") as f:
        sql = f.read().strip()


    durations = []
    for iteration in range(0, iterations):
        print(f"Starting iteration {iteration} of {iterations}")

        start_time = time.time()
        df = spark.sql(sql)
        rows = df.collect()

        print(f"Query returned {len(rows)} rows")
        end_time = time.time()
        duration = end_time - start_time
        print(f"Query took {duration} seconds")

        durations.append(duration)

    # Stop the SparkSession
    spark.stop()

    print(durations)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--query", required=True, help="Path to query file")
    parser.add_argument("--iterations", required=False, default="1", help="How many iterations to run")
    args = parser.parse_args()

    main(args.data, args.query, int(args.iterations))
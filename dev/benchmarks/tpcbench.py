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
from datetime import datetime
import json
from pyspark.sql import SparkSession
import time

# rename same columns aliases
# a, a, b, b -> a, a_1, b, b_1
#
# Important for writing data where column name uniqueness is required
def dedup_columns(df):
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

def main(benchmark: str, data_path: str, query_path: str, iterations: int, output: str, name: str, query_num: int = None, write_path: str = None):

    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName(f"{name} benchmark derived from {benchmark}") \
        .getOrCreate()

    # Register the tables
    if benchmark == "tpch":
        num_queries = 22
        table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    elif benchmark == "tpcds":
        num_queries = 99
        table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
           "customer_address", "customer_demographics", "date_dim", "time_dim", "household_demographics",
           "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns",
           "store_sales", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
    else:
        raise "invalid benchmark"

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)

    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    results = {
        'engine': 'datafusion-comet',
        'benchmark': benchmark,
        'data_path': data_path,
        'query_path': query_path,
        'spark_conf': conf_dict,
    }

    for iteration in range(0, iterations):
        print(f"Starting iteration {iteration} of {iterations}")
        iter_start_time = time.time()

        # Determine which queries to run
        if query_num is not None:
            # Validate query number
            if query_num < 1 or query_num > num_queries:
                raise ValueError(f"Query number {query_num} is out of range. Valid range is 1-{num_queries} for {benchmark}")
            queries_to_run = [query_num]
        else:
            queries_to_run = range(1, num_queries+1)

        for query in queries_to_run:
            spark.sparkContext.setJobDescription(f"{benchmark} q{query}")

            # read text file
            path = f"{query_path}/q{query}.sql"

            print(f"Reading query {query} using path {path}")
            with open(path, "r") as f:
                text = f.read()
                # each file can contain multiple queries
                queries = text.split(";")

                start_time = time.time()
                for sql in queries:
                    sql = sql.strip().replace("create view", "create temp view")
                    if len(sql) > 0:
                        print(f"Executing: {sql}")
                        df = spark.sql(sql)
                        df.explain("formatted")

                        if write_path is not None:
                            # skip results with empty schema
                            # coming across for running DDL stmt
                            if len(df.columns) > 0:
                                output_path = f"{write_path}/q{query}"
                                # rename same column names for output
                                # a, a, b, b => a, a_1, b, b_1
                                # output doesn't allow non unique column names
                                deduped = dedup_columns(df)
                                # sort by all columns to have predictable output dataset for comparison
                                deduped.orderBy(*deduped.columns).coalesce(1).write.mode("overwrite").parquet(output_path)
                                print(f"Query {query} results written to {output_path}")
                            else:
                                print(f"Skipping write: DataFrame has no schema for {output_path}")
                        else:
                            rows = df.collect()
                            print(f"Query {query} returned {len(rows)} rows")

                end_time = time.time()
                print(f"Query {query} took {end_time - start_time} seconds")

                # store timings in list and later add option to run > 1 iterations
                query_timings = results.setdefault(query, [])
                query_timings.append(end_time - start_time)

        iter_end_time = time.time()
        print(f"Iteration {iteration} took {round(iter_end_time - iter_start_time,2)} seconds")

    str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"{output}/{name}-{benchmark}-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    with open(results_path, "w") as f:
        f.write(str)

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--iterations", required=False, default="1", help="How many iterations to run")
    parser.add_argument("--output", required=True, help="Path to write output")
    parser.add_argument("--name", required=True, help="Prefix for result file e.g. spark/comet/gluten")
    parser.add_argument("--query", required=False, type=int, help="Specific query number to run (1-based). If not specified, all queries will be run.")
    parser.add_argument("--write", required=False, help="Path to save query results to, in Parquet format.")
    args = parser.parse_args()

    main(args.benchmark, args.data, args.queries, int(args.iterations), args.output, args.name, args.query, args.write)


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
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

TPCH_Q1 = """
SELECT
    l_returnflag, l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

TPCH_Q6 = """
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
"""

QUERIES = {"q1": TPCH_Q1, "q6": TPCH_Q6}


def load_tables(spark: SparkSession, data_path: str):
    parquet_path = os.path.join(data_path, "parquet")
    if os.path.exists(parquet_path):
        for table in ["lineitem", "part", "orders"]:
            table_path = os.path.join(parquet_path, table)
            if os.path.exists(table_path):
                spark.read.parquet(table_path).createOrReplaceTempView(table)
        return

    schemas = {
        "lineitem": "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment",
    }

    for table, columns in schemas.items():
        tbl_file = os.path.join(data_path, f"{table}.tbl")
        if os.path.exists(tbl_file):
            df = spark.read.csv(tbl_file, sep="|", header=False)
            col_names = columns.split(",")
            for i, col_name in enumerate(col_names):
                df = df.withColumnRenamed(f"_c{i}", col_name)
            if table == "lineitem":
                df = df.withColumn("l_shipdate", df["l_shipdate"].cast("date"))
            df.createOrReplaceTempView(table)


def run_query(spark: SparkSession, query_name: str, query_sql: str) -> dict:
    print(f"Running: {query_name}")

    # Warmup
    spark.sql(query_sql).collect()

    # Timed runs
    durations = []
    for i in range(3):
        start = time.time()
        spark.sql(query_sql).count()
        durations.append(time.time() - start)
        print(f"  Run {i+1}: {durations[-1]:.2f}s")

    return {
        "query": query_name,
        "durations": durations,
        "min_duration_seconds": min(durations),
        "avg_duration_seconds": sum(durations) / len(durations),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", "-q", default="q1", choices=list(QUERIES.keys()))
    parser.add_argument("--data", "-d", required=True)
    parser.add_argument("--mode", "-m", default="spark", choices=["spark", "comet"])
    parser.add_argument("--output", "-o")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"TPC-H-{args.query}-{args.mode}").getOrCreate()

    try:
        load_tables(spark, args.data)
        result = run_query(spark, args.query, QUERIES[args.query])
        result["mode"] = args.mode
        result["duration_seconds"] = result["min_duration_seconds"]
        result["timestamp"] = datetime.utcnow().isoformat() + "Z"

        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
        else:
            print(json.dumps(result, indent=2))

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

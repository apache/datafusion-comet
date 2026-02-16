<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TPC-H / TPC-DS Benchmark Suite

Runs TPC-H (22 queries) or TPC-DS (99 queries) benchmarks against Parquet
files or Iceberg tables.

## Arguments

| Argument        | Required | Default    | Description                                      |
| --------------- | -------- | ---------- | ------------------------------------------------ |
| `--benchmark`   | yes      |            | `tpch` or `tpcds`                                |
| `--data`        | *        |            | Path to Parquet data files                        |
| `--catalog`     | *        |            | Iceberg catalog name (mutually exclusive with `--data`) |
| `--database`    | no       | `tpch`     | Database name (only with `--catalog`)             |
| `--format`      | no       | `parquet`  | File format: parquet, csv, json (only with `--data`) |
| `--options`     | no       | `{}`       | Spark reader options as JSON string               |
| `--queries`     | yes      |            | Path to directory containing `q1.sql` ... `qN.sql` |
| `--iterations`  | no       | `1`        | Number of times to run all queries                |
| `--output`      | yes      |            | Directory for results JSON                        |
| `--name`        | auto     |            | Result file prefix (auto-injected from engine config) |
| `--query`       | no       |            | Run a single query number (1-based)               |
| `--write`       | no       |            | Write query results as Parquet to this path       |
| `--profile`     | no       |            | Enable JVM metrics profiling                      |
| `--profile-interval` | no  | `2.0`      | Profiling poll interval in seconds                |

`*` Either `--data` or `--catalog` is required, but not both.

## Examples

### TPC-H with Comet (standalone cluster)

```bash
export SPARK_HOME=/opt/spark
export SPARK_MASTER=spark://hostname:7077
export COMET_JAR=/path/to/comet.jar
export TPCH_DATA=/mnt/bigdata/tpch/sf100
export TPCH_QUERIES=/mnt/bigdata/tpch/queries

python benchmarks/run.py \
    --engine comet --profile standalone-tpch --restart-cluster \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --iterations 1
```

### TPC-DS with Blaze

```bash
export BLAZE_JAR=/path/to/blaze.jar
export TPCDS_DATA=/mnt/bigdata/tpcds/sf100
export TPCDS_QUERIES=/mnt/bigdata/tpcds/queries

python benchmarks/run.py \
    --engine blaze --profile standalone-tpcds --restart-cluster \
    -- tpc --benchmark tpcds --data $TPCDS_DATA --queries $TPCDS_QUERIES \
       --output . --iterations 1
```

### TPC-H with vanilla Spark (baseline)

```bash
python benchmarks/run.py \
    --engine spark --profile standalone-tpch --restart-cluster \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --iterations 1
```

### TPC-H with Iceberg tables

First, create Iceberg tables from Parquet data:

```bash
export ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.8.1.jar
export ICEBERG_WAREHOUSE=/mnt/bigdata/iceberg-warehouse

$SPARK_HOME/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$ICEBERG_WAREHOUSE \
    benchmarks/create-iceberg-tpch.py \
    --parquet-path $TPCH_DATA --catalog local --database tpch
```

Then run the benchmark with Comet's native Iceberg scanning:

```bash
python benchmarks/run.py \
    --engine comet-iceberg --profile standalone-tpch \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$ICEBERG_WAREHOUSE \
    --conf spark.sql.defaultCatalog=local \
    --restart-cluster \
    -- tpc --benchmark tpch --catalog local --database tpch \
       --queries $TPCH_QUERIES --output . --iterations 1
```

### Run a single query

```bash
python benchmarks/run.py --engine comet --profile local \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --query 1
```

## Output Format

Results are written as JSON with the filename `{name}-{benchmark}-{timestamp_millis}.json`:

```json
{
    "engine": "datafusion-comet",
    "benchmark": "tpch",
    "query_path": "/path/to/queries",
    "spark_conf": { ... },
    "data_path": "/path/to/data",
    "1": [12.34],
    "2": [5.67],
    ...
}
```

Query keys are integers (serialised as strings by `json.dumps`). Each value
is a list of elapsed seconds per iteration. This format is compatible with
`analysis/compare.py` for chart generation.

## Comparing Results

```bash
python -m benchmarks.analysis.compare \
    comet-tpch-*.json spark-tpch-*.json \
    --labels Comet Spark --benchmark tpch \
    --title "TPC-H SF100" --output-dir ./charts
```

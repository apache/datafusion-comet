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

# Shuffle Size Comparison Benchmark

Compares shuffle file sizes between Spark, Comet JVM, and Comet Native shuffle implementations.

## Prerequisites

- Apache Spark cluster (standalone, YARN, or Kubernetes)
- PySpark installed
- Comet JAR built

## Build Comet JAR

```bash
cd /path/to/datafusion-comet
make release
```

## Step 1: Generate Test Data

Generate test data with realistic 50-column schema (nested structs, arrays, maps):

```bash
spark-submit \
  --master spark://master:7077 \
  --executor-memory 16g \
  generate_data.py \
  --output /tmp/shuffle-benchmark-data \
  --rows 10000000 \
  --partitions 200
```

### Data Generation Options

| Option               | Default    | Description                  |
| -------------------- | ---------- | ---------------------------- |
| `--output`, `-o`     | (required) | Output path for parquet data |
| `--rows`, `-r`       | 10000000   | Number of rows               |
| `--partitions`, `-p` | 200        | Number of output partitions  |

## Step 2: Run Benchmark

Run benchmarks and check Spark UI for shuffle sizes:

```bash
SPARK_MASTER=spark://master:7077 \
EXECUTOR_MEMORY=16g \
./run_all_benchmarks.sh /tmp/shuffle-benchmark-data
```

Or run individual modes:

```bash
# Spark baseline
spark-submit --master spark://master:7077 \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode spark

# Comet JVM shuffle
spark-submit --master spark://master:7077 \
  --jars /path/to/comet.jar \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.shuffle.mode=jvm \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode jvm

# Comet Native shuffle
spark-submit --master spark://master:7077 \
  --jars /path/to/comet.jar \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.shuffle.mode=native \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode native
```

## Checking Results

Open the Spark UI (default: http://localhost:4040) during each benchmark run to compare shuffle write sizes in the Stages tab.

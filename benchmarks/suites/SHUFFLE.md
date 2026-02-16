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

# Shuffle Benchmark Suite

Compares shuffle file sizes and performance across Spark, Comet JVM, and
Comet Native shuffle implementations using hash or round-robin partitioning.

## Arguments

| Argument        | Required | Default | Description                          |
| --------------- | -------- | ------- | ------------------------------------ |
| `--benchmark`   | yes      |         | `shuffle-hash` or `shuffle-roundrobin` |
| `--data`        | yes      |         | Path to input Parquet data           |
| `--mode`        | yes      |         | `spark`, `jvm`, or `native`          |
| `--partitions`  | no       | `200`   | Number of shuffle partitions         |
| `--iterations`  | no       | `1`     | Number of iterations                 |
| `--output`      | yes      |         | Directory for results JSON           |
| `--name`        | auto     |         | Result file prefix (auto-injected)   |
| `--profile`     | no       |         | Enable JVM metrics profiling         |
| `--profile-interval` | no  | `2.0`   | Profiling poll interval in seconds   |

## Generating Test Data

Generate a Parquet dataset with a wide schema (100 columns including deeply
nested structs, arrays, and maps):

```bash
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --executor-memory 16g \
    benchmarks/generate_shuffle_data.py \
    --output /tmp/shuffle-benchmark-data \
    --rows 10000000 \
    --partitions 200
```

> **Note**: The data generation script is a standalone PySpark job. It can be
> run with any Spark installation — no engine JARs required.

## Examples

### Hash shuffle — Spark baseline

```bash
python benchmarks/run.py --engine spark-shuffle --profile local \
    -- shuffle --benchmark shuffle-hash --data /tmp/shuffle-data \
       --mode spark --output . --iterations 3
```

### Hash shuffle — Comet JVM

```bash
python benchmarks/run.py --engine comet-jvm-shuffle --profile local \
    -- shuffle --benchmark shuffle-hash --data /tmp/shuffle-data \
       --mode jvm --output . --iterations 3
```

### Hash shuffle — Comet Native

```bash
python benchmarks/run.py --engine comet-native-shuffle --profile local \
    -- shuffle --benchmark shuffle-hash --data /tmp/shuffle-data \
       --mode native --output . --iterations 3
```

### Round-robin shuffle

```bash
python benchmarks/run.py --engine comet-native-shuffle --profile local \
    -- shuffle --benchmark shuffle-roundrobin --data /tmp/shuffle-data \
       --mode native --output . --iterations 3
```

### Run all three modes back-to-back

```bash
for engine_mode in "spark-shuffle spark" "comet-jvm-shuffle jvm" "comet-native-shuffle native"; do
    set -- $engine_mode
    python benchmarks/run.py --engine "$1" --profile local \
        -- shuffle --benchmark shuffle-hash --data /tmp/shuffle-data \
           --mode "$2" --output . --iterations 3
done
```

### With profiling

```bash
python benchmarks/run.py --engine comet-native-shuffle --profile local \
    -- shuffle --benchmark shuffle-hash --data /tmp/shuffle-data \
       --mode native --output . --iterations 3 --profile --profile-interval 1.0
```

## Output Format

Results are written as JSON with the filename `{name}-{benchmark}-{timestamp_millis}.json`:

```json
{
    "engine": "datafusion-comet",
    "benchmark": "shuffle-hash",
    "mode": "native",
    "data_path": "/tmp/shuffle-data",
    "spark_conf": { ... },
    "shuffle-hash": [
        {"duration_ms": 12345, "row_count": 10000000, "num_partitions": 200},
        {"duration_ms": 11234, "row_count": 10000000, "num_partitions": 200},
        {"duration_ms": 10987, "row_count": 10000000, "num_partitions": 200}
    ]
}
```

## Checking Results

Open the Spark UI (default: http://localhost:4040) during each benchmark run
to compare shuffle write sizes in the Stages tab.

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

# PySpark Benchmarks

A suite of PySpark benchmarks for comparing performance between Spark, Comet JVM, and Comet Native implementations.

## Available Benchmarks

Run `python run_benchmark.py --list-benchmarks` to see all available benchmarks:

- **shuffle-hash** - Shuffle all columns using hash partitioning on group_key
- **shuffle-roundrobin** - Shuffle all columns using round-robin partitioning

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

## Step 2: Run Benchmarks

### List Available Benchmarks

```bash
python run_benchmark.py --list-benchmarks
```

### Run Individual Benchmarks

You can run specific benchmarks by name:

```bash
# Hash partitioning shuffle - Spark baseline
spark-submit --master spark://master:7077 \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode spark --benchmark shuffle-hash

# Round-robin shuffle - Spark baseline
spark-submit --master spark://master:7077 \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode spark --benchmark shuffle-roundrobin

# Hash partitioning - Comet JVM shuffle
spark-submit --master spark://master:7077 \
  --jars /path/to/comet.jar \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.shuffle.mode=jvm \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode jvm --benchmark shuffle-hash

# Round-robin - Comet Native shuffle
spark-submit --master spark://master:7077 \
  --jars /path/to/comet.jar \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.exec.shuffle.mode=native \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  run_benchmark.py --data /tmp/shuffle-benchmark-data --mode native --benchmark shuffle-roundrobin
```

### Run All Benchmarks

Use the provided script to run all benchmarks across all modes:

```bash
SPARK_MASTER=spark://master:7077 \
EXECUTOR_MEMORY=16g \
./run_all_benchmarks.sh /tmp/shuffle-benchmark-data
```

## Checking Results

Open the Spark UI (default: http://localhost:4040) during each benchmark run to compare shuffle write sizes in the Stages tab.

## Adding New Benchmarks

The benchmark framework makes it easy to add new benchmarks:

1. **Create a benchmark class** in `benchmarks/` directory (or add to existing file):

```python
from benchmarks.base import Benchmark

class MyBenchmark(Benchmark):
    @classmethod
    def name(cls) -> str:
        return "my-benchmark"

    @classmethod
    def description(cls) -> str:
        return "Description of what this benchmark does"

    def run(self) -> Dict[str, Any]:
        # Read data
        df = self.spark.read.parquet(self.data_path)

        # Run your benchmark operation
        def benchmark_operation():
            result = df.filter(...).groupBy(...).agg(...)
            result.write.mode("overwrite").parquet("/tmp/output")

        # Time it
        duration_ms = self._time_operation(benchmark_operation)

        return {
            'duration_ms': duration_ms,
            # Add any other metrics you want to track
        }
```

2. **Register the benchmark** in `benchmarks/__init__.py`:

```python
from .my_module import MyBenchmark

_BENCHMARK_REGISTRY = {
    # ... existing benchmarks
    MyBenchmark.name(): MyBenchmark,
}
```

3. **Run your new benchmark**:

```bash
python run_benchmark.py --data /path/to/data --mode spark --benchmark my-benchmark
```

The base `Benchmark` class provides:

- Automatic timing via `_time_operation()`
- Standard output formatting via `execute_timed()`
- Access to SparkSession, data path, and mode
- Spark configuration printing

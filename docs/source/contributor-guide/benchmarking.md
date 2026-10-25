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

# Comet Benchmarking Guide

To track progress on performance, we regularly run benchmarks derived from TPC-H and TPC-DS.

The benchmarking scripts are contained [here](https://github.com/apache/datafusion-comet/tree/main/benchmarks/tpc).

Data generation scripts are available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) GitHub repository.

## Current Benchmark Results

- [Benchmarks derived from TPC-DS](benchmark-results/tpc-ds)

## Benchmarking Guides

Available benchmarking guides:

- [Benchmarking on macOS](benchmarking_macos.md)
- [Benchmarking on AWS EC2](benchmarking_aws_ec2)
- [Micro Benchmarking on AWS EC2](benchmarking_micro_ec2.md)
- [TPC-DS Benchmarking with spark-sql-perf](benchmarking_spark_sql_perf.md)

We also have many micro benchmarks that can be run from an IDE located [here](https://github.com/apache/datafusion-comet/tree/main/spark/src/test/scala/org/apache/spark/sql/benchmark).
These can also be run as a suite on a dedicated machine, see
[Micro Benchmarking on AWS EC2](benchmarking_micro_ec2.md). Published results are in
[benchmarks/results/micro](https://github.com/apache/datafusion-comet/tree/main/benchmarks/results/micro).

## Iceberg Bloom-filter writes

`CometIcebergWriteBenchmark` compares Spark and Comet Iceberg writers. Its `bloom` mode compares
native writes with Bloom filters off/on, using the same prepared Parquet source in eight files. Both cases enable native writes and the Bloom folding opt-in; only the table's Bloom
properties differ. Filters cover the bigint, string, and binary columns at the default 1 MiB cap,
without explicit NDV/FPP. The benchmark checks the native plan, row count, and filter presence
before measuring. Table creation and source preparation are outside the timed insert; the Iceberg
commit is included.

Build once, then run the comparison in separate JVMs to collect peak RSS:

```shell
make release PROFILES=-Pspark-4.1
python3 benchmarks/micro/iceberg_bloom_writes.py --spark-profile spark-4.1 --heap 8g
```

The runner reuses the micro-benchmark runner's Maven/JVM settings without rebuilding. Each case
warms up and measures at least five writes of 4,194,304 rows. It prints Spark's timing row and
whole-process peak RSS, and writes individual logs and `summary.json` to the reported directory.
Use `--output-dir` to choose that directory. RSS includes Maven, source generation, validation,
warmup, and writes; it is not the writer's allocation or its peak alone. The cases run in fresh
processes because RSS high-water marks and retained native buffers cannot be reset between cases.
The warehouse is local, so these results do not include object-store latency.

For a quick functional check after a development build, pass
`--allow-debug --rows 32768 --iterations 1`. These runs are marked as smoke checks and do not
provide release-performance measurements. For timing alone, pass `bloom`, `bloom-off`, or
`bloom-on` to the existing benchmark target, optionally followed by rows and minimum iterations:

```shell
make benchmark-org.apache.spark.sql.benchmark.CometIcebergWriteBenchmark bloom
```

Do not measure RSS around that `make` command: its release-build prerequisite includes compilation.

```{toctree}
:hidden:

benchmark-results/tpc-ds
benchmarking_macos
benchmarking_aws_ec2
benchmarking_micro_ec2
benchmarking_spark_sql_perf
```

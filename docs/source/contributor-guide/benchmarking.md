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

- [Benchmarks derived from TPC-DS](benchmark-results/tpc-ds): Comet 1.1.0 and 1.2.0 vs. Spark 4.2.0 and 4.1.3 on Parquet tables at SF 1000

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

## MIN/MAX Reader Filtering

`CometMinMaxBenchmark` compares one `MIN(k)` or `MAX(k)` over an `INT` Parquet column with
`spark.comet.exec.aggregate.dynamicFilter.enabled` disabled (`off`) and enabled (`on`). Run it
with a release build; the make target builds and packages the native library:

```shell
make benchmark-org.apache.spark.sql.benchmark.CometMinMaxBenchmark -- \
  1048576 1,4 ascending,descending,random,constant 5 off,on
```

The arguments are total rows, scan partition counts, layouts, minimum measured iterations, and
mode order. The example uses one million rows, one or four files/tasks, and at least five measured iterations per
case. Use `16384 1,4 ascending,descending,random,constant 3 off,on` for a smoke run. The Spark master
is `local[1]` by default; set `COMET_BENCHMARK_MASTER` to change task concurrency and record it
alongside results.

Files use uncompressed Parquet v1 pages with dictionary encoding disabled, so even constant
values reach the row-group size target. Each file has multiple row groups. Ascending input favors
`MIN` and descending input favors `MAX`; the opposite combinations measure overhead when the bound keeps improving. Random input
uses deterministic hashed keys, while constant input measures repeated values. Page-index pruning
and decoder row filtering are disabled to isolate row-group pruning. Only the key is scanned.

Before timing, the suite checks results against Spark, verifies the direct scan/partial aggregate
plan and task count, and requires attachment in every enabled task. It prints scan output rows,
requested data bytes, reader data and metadata bytes, pruned row groups, and attachment counters
from this untimed execution. The timed cases include query planning and result collection, but
exclude data generation and validation.

Repeat with `on,off` to check case-order effects, including JVM warmup and metadata-cache warmth.
Compare elapsed time as well as scan metrics: fewer rows or data bytes do not guarantee a faster
query, especially for small files or when footer inspection and scheduling dominate. Record the
Comet revision, build flags, Spark/JDK versions, machine, master, arguments, and both mode orders
with any reported results. See [MIN/MAX reader filtering](../user-guide/latest/tuning/operators.md#minmax-reader-filtering)
for supported plans and fallback limits.

## Iceberg Bloom-filter writes

`CometIcebergWriteBenchmark` compares Spark and Comet Iceberg writers. Its `bloom` mode compares
native writes with Bloom filters off/on, using the same prepared Parquet source in eight files. Both cases enable native writes and the Bloom folding opt-in; only the table's Bloom
properties differ. Filters cover the bigint, string, and binary columns at the default 1 MiB cap,
without explicit NDV/FPP. The benchmark checks the native plan, row count, and filter presence
before measuring. Table creation and source preparation are outside the timed insert; the Iceberg
commit is included.

Use the existing benchmark target for timing, optionally followed by rows and minimum iterations:

```shell
make benchmark-org.apache.spark.sql.benchmark.CometIcebergWriteBenchmark bloom
```

Each case warms up and measures at least five writes of 4,194,304 rows by default. Use
`bloom-off` or `bloom-on` to select a single case, or append `32768 1` for a quick functional
check. Performance comparisons require a release build.

To collect peak RSS, build once and run each case in a separate JVM using the existing Maven
invocation. Obtain the shared JVM options from the Makefile:

```shell
make release PROFILES=-Pspark-4.1
export COMET_CONF_DIR="$PWD/conf"
export MAVEN_OPTS="$(make --no-print-directory print-benchmark-args PROFILES=-Pspark-4.1 BENCH_HEAP=8g | sed -n 's/^MAVEN_OPTS=//p')"
cd spark
/usr/bin/time -v ../mvnw exec:java -Dexec.classpathScope=test \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.mainClass=org.apache.spark.sql.benchmark.CometIcebergWriteBenchmark \
  -Dexec.args="bloom-off" -Pspark-4.1
```

Repeat with `bloom-on`. On macOS, use `/usr/bin/time -l` instead of `-v`; its maximum RSS is in
bytes, while GNU time reports KiB. The reported RSS includes Maven, source generation, validation,
warmup, and writes; it is not the writer's allocation or its peak alone. Fresh processes avoid
carrying an earlier case's RSS high-water mark and retained native buffers into the next case.
The warehouse is local, so these results do not include object-store latency.

Do not measure RSS around `make benchmark-*`: its release-build prerequisite includes compilation.

```{toctree}
:hidden:

benchmark-results/tpc-ds
benchmarking_macos
benchmarking_aws_ec2
benchmarking_micro_ec2
benchmarking_spark_sql_perf
```

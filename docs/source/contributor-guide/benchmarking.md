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

## Map lookup dispatch

`CometCodegenDispatchBenchmark --map-lookups` compares dispatcher on, dispatcher off
(Spark projection over a Comet scan), and pure Spark. It checks answers and routes before
timing `m[k]` and `element_at(m, k)`, with persisted DOUBLE/STRUCT keys, 4/64-entry maps,
and lookup-only/mixed projections. Corpus generation is outside the measurements.

Set `JAVA_HOME` to a JDK supported by the selected Spark profile. Build once, then reuse
the release artifacts for separate benchmark JVMs. Forking Java also keeps Hadoop shutdown
hooks outside Maven's disposable application classloader:

```sh
make release PROFILES=-Pspark-4.1
comet_bench_opts=$(make -s print-benchmark-args BENCH_HEAP=4g PROFILES=-Pspark-4.1 | sed -n 's/^MAVEN_OPTS=//p')
export COMET_CONF_DIR="$PWD/conf"
cd spark
../mvnw exec:exec -Pspark-4.1 -Dexec.classpathScope=test \
  -Dexec.executable="$JAVA_HOME/bin/java" \
  -Dexec.args="$comet_bench_opts -classpath %classpath org.apache.spark.sql.benchmark.CometCodegenDispatchBenchmark --map-lookups"
```

Use `--map-lookups --check-only` to validate the complete matrix without timing.
For first use, replace `--map-lookups` at the end with
`--map-lookups --case=get_map_value-double-4-lookup --first-use=dispatch`.
Case IDs combine `get_map_value`/`element_at`, `double`/`struct`, `4`/`64`, and
`lookup`/`mixed`. Repeat each selected case with `dispatch`, `fallback`, and `spark`
in **separate JVMs**, before running another lookup in that JVM. The first two executions
are timed before validation. Spark's compiled-source cache is shared across map sizes
and projection shapes; resetting dispatcher counters does not clear it.

Warmed tables use 1,024 and 65,536 rows, two seconds of warmup and at least two seconds of
timing per arm, including a repeated fallback baseline to expose drift. Every query still
creates new tasks: warmed does not mean task kernel setup is excluded. First-use results
use 1,024 rows and report wall time, JVM-wide Spark compilation metrics, and task kernel
initializations separately. First-minus-second is not an isolated compilation cost.
Repeat measurements on an otherwise idle machine; these end-to-end scan/projection/sink
results do not establish a universal speedup or isolate map traversal from bridge costs.

```{toctree}
:hidden:

benchmark-results/tpc-ds
benchmarking_macos
benchmarking_aws_ec2
benchmarking_micro_ec2
benchmarking_spark_sql_perf
```

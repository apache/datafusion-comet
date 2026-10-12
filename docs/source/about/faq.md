<!---
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

# Frequently Asked Questions

Answers to common questions about choosing, installing, and running Comet. If your question is not
answered here, see [Where can I ask questions or report bugs?](#where-can-i-ask-questions-or-report-bugs)

## Choosing Comet

### How does Comet compare to other open-source Spark accelerators?

Several open-source projects speed up Spark by running query plans in native code. They differ in
the engine they build on and the hardware they need.

- [Apache Gluten](https://gluten.apache.org/) is the closest to Comet in design: a Spark plugin that
  passes the parts of a plan it supports to a native engine, such as the C++ engine Velox. See
  [Comparison of Comet and Gluten](gluten_comparison.md).
- [Apache Auron (incubating)](https://auron.apache.org/), formerly Blaze, is also a Spark plugin
  built on DataFusion. Comet is developed within the DataFusion project itself.
- The [NVIDIA cuDF plugin for Apache Spark](https://github.com/NVIDIA/cudf-spark), formerly the
  RAPIDS Accelerator, runs queries on NVIDIA GPUs. Comet runs on the CPUs your cluster already has.

Gluten and Auron are also adding Flink support, while Comet focuses on Spark alone. Which
accelerator is fastest depends on the workload, so we recommend benchmarking your own jobs.

### Will Comet return the same results as Spark?

That is the goal, and Comet runs Apache Spark's own SQL test suite in CI to check it. By default,
Comet uses implementations that match Spark, including running Spark's own generated code for
expressions whose native implementations behave differently. Those native implementations are
opt-in, one expression at a time. The known differences that remain, such as the handling of strings
that contain invalid UTF-8, are listed in the
[Compatibility Guide](../user-guide/latest/compatibility/index.md). If you find a difference that is
not documented, please [file an issue](https://github.com/apache/datafusion-comet/issues).

### Which Spark and Java versions does Comet support?

Spark 3.4, 3.5, 4.0, 4.1, and 4.2, on Java 17 or later. Spark 3.4 is deprecated. The
[installation guide](../user-guide/latest/installation.md#supported-spark-versions) lists the exact
Spark, Java, and Scala versions, and the
[Spark Version Compatibility](../user-guide/latest/compatibility/spark-versions.md) page lists known
issues for each Spark version.

Comet drops a Spark version some time after the Spark project stops maintaining it. Spark 3.3 and
earlier are no longer supported, and Comet 1.1.0 dropped Java 11. Java 8 cannot be supported because
Apache Arrow's Java library no longer supports it. If you cannot upgrade Spark yet, stay on an earlier
Comet release that supports your version, as the
[versioning policy](versioning_policy.md#apache-spark-version-support) describes.

### Does Comet work with vendor distributions of Spark?

Comet is built and tested against open-source Apache Spark only. Spark distributions from cloud
providers and other vendors can change the internal Spark APIs that Comet calls and add operators
that Comet does not recognize, so Comet may fail with errors such as `NoSuchMethodError`, or leave
much of the work to Spark. To use Comet in the cloud, run open-source Apache Spark there, for example
on Kubernetes as described in the [Kubernetes guide](../user-guide/latest/kubernetes.md).

### Which platforms do the published jars support?

The jars in Maven Central include native libraries for Linux on amd64 and arm64, and need glibc 2.31
or newer. They do not load on distributions with an older glibc, such as RHEL 7 and CentOS 7. On
those systems and on macOS, [build Comet from source](../user-guide/latest/source.md).

For performance, the libraries target CPUs that are common in data centers: `x86-64-v3`, which
includes AVX2, on amd64, and Arm Neoverse N1 or newer cores on arm64. If Comet crashes with `SIGILL`
(illegal instruction), the CPU is older than that. Build Comet from source for it, and please
[open an issue](https://github.com/apache/datafusion-comet/issues) describing your environment.

A jar you build yourself is smaller than the published one because it contains the native library
for one platform only.

## Running Comet

### Why do I get a `ClassNotFoundException` for `CometShuffleManager`?

Spark could not find the Comet jar when it created the shuffle manager. On Spark 3.4 and 3.5,
executors create the shuffle manager before they load jars passed with `--jars` or `--packages`, so
the jar must be on the startup classpath. Either copy it into `$SPARK_HOME/jars` on every machine, or
set `spark.driver.extraClassPath` and `spark.executor.extraClassPath` to its path, as the
[installation guide](../user-guide/latest/installation.md#run-spark-shell-with-comet-enabled) shows.

These two settings take a local file path, not an `hdfs://` or other URL. The JVM silently ignores
classpath entries that do not exist, so the jar must be at that path on every machine that runs a
driver or an executor.

### How much memory does Comet need?

Comet needs Spark's off-heap memory: without `spark.memory.offHeap.enabled=true`, Comet disables
itself and logs a warning. Comet's native operators share the off-heap pool, sized by
`spark.memory.offHeap.size`, with Spark. When the pool runs out, operators that can spill, such as
sorts, aggregations, and shuffle writes, spill to disk, and operators that cannot spill fail the
task.

Comet also uses memory that the pool does not track, which has to fit in
`spark.executor.memoryOverhead`. If the cluster manager kills executors for exceeding their memory
limit, raise the overhead rather than the pool. Each executor logs its native memory use while Comet
runs, which you can use to size these settings. See
[Memory Tuning](../user-guide/latest/tuning/memory.md).

### Why isn't my query faster with Comet?

First check how much of the query Comet runs. Set `spark.comet.explain.fallback.enabled=true` to log
the reasons that parts of each query run in Spark, and see
[Understanding Comet Plans](../user-guide/latest/understanding-comet-plans.md) to read the plan. To
estimate how much of a workload Comet would accelerate without changing how it runs, set
`spark.comet.explain.planOnly.enabled=true`.

Common causes of a small speedup are:

- **Fallbacks inside a stage.** Each switch between Comet and Spark converts data between columnar
  and row formats, which can cost more than Comet saves. See
  [Reducing Row/Columnar Conversion Overhead](../user-guide/latest/tuning/transitions.md).
- **Comet's shuffle manager is not set.** If `spark.shuffle.manager` does not name Comet's shuffle
  manager, Comet disables itself and logs a warning, unless `spark.comet.shuffle.enabled=false`, in
  which case Comet runs but every shuffle runs in Spark. See
  [Shuffle Tuning](../user-guide/latest/tuning/shuffle.md).
- **Too little memory.** Native operators spill to disk when the off-heap pool is too small. See
  [Memory Tuning](../user-guide/latest/tuning/memory.md).
- **Little computation to accelerate.** Queries that spend most of their time reading from storage,
  listing files, or planning, or that process little data, have less to gain.

## Feature Support

### Does Comet support Delta Lake, Apache Hudi, or Apache Paimon tables?

Not yet. Comet accelerates [Apache Iceberg](../user-guide/latest/iceberg.md) tables, with a native
reader that is enabled by default and
[experimental native writes](../user-guide/latest/iceberg-writes.md). It does not accelerate scans of
Delta Lake, Hudi, or Paimon tables, so Spark reads those. Native Delta Lake reads are in development;
see the [roadmap](../contributor-guide/roadmap.md#delta-lake-support). Hudi and Paimon are not on the
roadmap.

### Does Comet accelerate PySpark jobs and Python UDFs?

PySpark DataFrame and SQL queries produce the same physical plans as Scala, so Comet accelerates them
the same way. Set Comet up as the [installation guide](../user-guide/latest/installation.md)
describes. There is no pip package.

For Python UDFs, Comet has experimental support for `mapInArrow` and `mapInPandas` on Spark 4.0 and
later. It keeps their data in Arrow format instead of converting it to rows and back, and is disabled
by default. To enable it, set `spark.comet.exec.pyarrowUDF.enabled=true`; see
[PyArrow UDF Acceleration](../user-guide/latest/pyarrow-udfs.md). Scalar `@pandas_udf` functions are
not accelerated yet, and Python UDFs that do not use Arrow are not planned.

### Does Comet plan to add an API for vectorized Java/Scala UDFs similar to the Rust UDF API?

Not at the moment. Comet already runs ordinary Scala and Java UDFs in its pipeline without code
changes, compiling each call into a loop over a whole batch that the JVM optimizes well. See
[Scala UDF and Java UDF Support](../user-guide/latest/scala_java_udfs.md).

A prototype ([#6697](https://github.com/apache/datafusion-comet/pull/6697)) found that, once the code
generator's overheads were reduced, a vectorized rewrite of a simple function such as `x + 1` saved
only about 3 ns per row. That does not justify asking users to rewrite their functions against
Comet's relocated Arrow classes and rebuild them for every Comet release, so the effort is going into
the code generator instead.

If you have a use case that a function of one row handles poorly, such as working on the UTF-8 bytes
of strings or calling a library that processes whole batches, please describe it in
[#6694](https://github.com/apache/datafusion-comet/issues/6694). For native speed, see the
experimental [Rust UDF API](../user-guide/latest/rust_udfs.md).

### Does Comet support Structured Streaming?

No. Comet targets batch queries and leaves streaming queries to Spark, and streaming support is not
on the roadmap. See [Spark Operator Support](../user-guide/latest/operators.md#not-currently-planned).

### Can I use Comet with a remote shuffle service?

Yes, with [Apache Celeborn](https://celeborn.apache.org/). Comet accelerates scans, filters, and
other operators, and Celeborn's own Spark integration handles the shuffle. With the released Celeborn
0.6.x and 0.7.x clients, Comet's native shuffle cannot run through Celeborn. See
[Using Comet with Apache Celeborn](../user-guide/latest/celeborn.md). Native shuffle with Apache
Uniffle is not supported yet ([#4913](https://github.com/apache/datafusion-comet/issues/4913)).

## Project and Community

### Is there a roadmap?

Yes. The [Comet Roadmap](../contributor-guide/roadmap.md) describes the major items that need
coordination between contributors.

### Where can I ask questions or report bugs?

- Ask questions in [GitHub Discussions](https://github.com/apache/datafusion-comet/discussions), or
  in the Comet channels of the DataFusion
  [Slack and Discord](https://datafusion.apache.org/contributor-guide/communication.html).
- Report bugs and request features in
  [GitHub issues](https://github.com/apache/datafusion-comet/issues).
- Report security vulnerabilities privately, following the
  [ASF security process](https://www.apache.org/security/), not in a public issue.
- Contributors also meet weekly on a video call; see
  [Regular public meetings](../contributor-guide/contributing.md#regular-public-meetings).

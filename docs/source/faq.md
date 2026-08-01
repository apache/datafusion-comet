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

# Frequently Asked Questions

Short answers to the questions we see most often from Comet users, each linking to the guide that
covers the topic in full. As with the rest of the unversioned pages on this site, links point into
the development snapshot of the user guide. If you are running a released version, the equivalent
pages for it are linked from the [User Guide](user-guide/index.md) index.

## Getting Started

### Do I need to change my queries or my code to use Comet?

No. Comet is a Spark plugin. You add the Comet jar to an existing Spark job, set
`spark.plugins=org.apache.spark.CometPlugin`, and Comet rewrites the physical plan behind the scenes.
Your DataFrame and SQL code is unchanged. See the [installation guide](user-guide/latest/installation.md)
for the full set of properties, including the off-heap memory and shuffle manager settings that most
deployments want.

### How do I confirm that Comet is actually accelerating my query?

Three checks, from cheapest to most detailed:

1. Look for `INFO core/src/lib.rs: Comet native library version <version> initialized` in the
   executor logs. If it is absent, the native library never loaded and nothing is being accelerated.
2. Read `spark.conf.get("spark.comet.version")` to confirm which build is loaded.
3. Run `EXPLAIN FORMATTED` on the query and look for `Comet*` operators in the plan. Anything without
   the `Comet` prefix is running on unmodified Spark.

[Understanding Comet Plans](user-guide/latest/understanding-comet-plans.md) explains how to read the
plan and which operator names mean what.

## Performance

### Why is my query no faster, or slower, with Comet enabled?

The usual cause is **fallback**. Comet replaces the operators it supports and leaves the rest as
Spark operators, so a plan can be a mix of both. Every boundary between the two needs a
`CometColumnarToRow` or `CometSparkRowToColumnar` transition, and if a stage has several of these the
conversion cost can exceed what native execution saved.

Start by finding out whether and why fallback is happening:

```
spark.comet.explain.fallback.enabled=true
```

This logs one WARN per query stage listing the reasons that stage could not run in Comet. On Spark
4.0 and newer, `spark.comet.explain.format` surfaces the same information in the Spark SQL UI. The
[configs for inspecting plans](user-guide/latest/understanding-comet-plans.md#configs-for-inspecting-plans-and-fallback)
section covers all five diagnostic configs.

Once you know the reason, the common remedies are:

- **An unsupported operator or expression.** Check [Supported Operators](user-guide/latest/operators.md)
  and [Supported Expressions](user-guide/latest/expressions.md). Some expressions are supported but
  run only when you opt in with `spark.comet.expression.<Name>.allowIncompatible=true`.
- **Many transitions in one stage.** Set `spark.comet.exec.transitionRevert.enabled=true` to have
  Comet revert a whole stage to Spark row execution when the transition count exceeds
  `spark.comet.exec.transitionRevert.maxTransitions`, trading native execution of a few operators for
  eliminating conversion overhead across the stage.
- **Shuffle is still Spark's.** Comet's shuffle is not used unless you set
  `spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager`, which is
  a static config and must be set before the `SparkSession` is created.
- **Too little memory, causing spills.** See the next question.
- **Idle scan tasks.** When Parquet row group sizes are close to `spark.sql.files.maxPartitionBytes`,
  Comet and Spark can disagree about which split owns a row group, leaving some scan tasks with no
  work and reducing effective parallelism. The
  [Parquet native scans](user-guide/latest/tuning.md#parquet-native-scans) section describes the
  symptoms and the `spark.sql.files.maxPartitionBytes` workaround.

The [Tuning Guide](user-guide/latest/tuning.md) covers the remaining knobs: batch size, join strategy,
shuffle compression, and the tokio worker thread count.

### Why does Comet run out of memory on a query that Spark completed?

Comet's native operators allocate from Spark's off-heap pool rather than the JVM heap, so a
configuration that was adequate for Spark may not be adequate once Comet is doing the work. Things to
check, in order:

- `spark.memory.offHeap.enabled=true` and a `spark.memory.offHeap.size` large enough for native
  execution. This is the single most common cause.
- `spark.comet.exec.memoryPool.fraction` set below `1.0`. Comet's memory accounting is not exact, and
  restricting the reservable fraction gives the process headroom.
- The pool type, `spark.comet.exec.memoryPool`. `fair_unified` (the default with off-heap enabled)
  caps each operator at an even share and suits queries where several operators may spill;
  `greedy_unified` is first-come first-served and suits queries with a single spillable operator.
- A lower `spark.comet.batchSize`, which reduces peak memory on wide tables.
- Whether you set `spark.comet.exec.forceShuffledHashJoin=true`. Comet's native hash join requires the
  build side to fit in memory, so forcing it on a large build side can OOM where Spark's
  `SortMergeJoin` would have spilled. Spill support for the hash join is on the
  [roadmap](contributor-guide/roadmap.md#spillable-hash-join).

Note also that `spark.comet.maxTempDirectorySize` (default 100 GB, applied per task) bounds spill
disk usage for native aggregate, sort, and shuffle operators, and exceeding it fails the query. The
[memory tuning](user-guide/latest/tuning.md#memory-tuning) section works through a sizing example.

### Why isn't my Iceberg query faster?

First confirm the native Iceberg scan is being used and that the rest of the plan is not falling back,
using the diagnostics above. Beyond that, two Iceberg-specific points matter:

- Comet's native Iceberg scan reads each task's data files one at a time by default. On tables with
  many small files, or on high-latency object storage, raise
  `spark.comet.scan.icebergNative.dataFileConcurrencyLimit` (default `1`; try 2–8) to overlap I/O at
  the cost of extra memory.
- Iceberg **writes** are not accelerated; they still run through Spark's Iceberg write operator.

See the [Iceberg Guide](user-guide/latest/iceberg.md) for supported features and known limitations.

## Compatibility and Support

### Which Spark, Java, and Scala versions does Comet support?

Comet supports Spark 3.4, 3.5, 4.0, and 4.1 in production builds, with experimental support for the
Spark 4.2 preview. The exact patch versions and the supported JDK and Scala combinations are listed
in [Spark Version Compatibility](user-guide/latest/compatibility/spark-versions.md).

```{warning}
JDK 11 and Spark 3.4 support are deprecated as of the 1.0.0 release and will be **removed in the
1.1.0 release**. We recommend moving to JDK 17 or later and Spark 3.5 or later.
```

Which Spark versions Comet supports is not governed by semantic versioning: adding or removing a
Spark minor is always a Comet minor release. Comet supports a Spark minor for as long as the upstream
Apache Spark project maintains it, with one Comet minor release of deprecation notice before removal.
The rules are in [Apache Spark Version Support](about/versioning_policy.md#apache-spark-version-support).

### Can I use Comet with Spark 3.3 or Java 8?

No. Neither is supported, and there are no plans to add them. If you are on a Spark version a given
Comet release no longer supports, stay on an earlier Comet release until you can upgrade Spark.

### Which operating systems are supported?

The jars published to Maven Central bundle native libraries for Linux only, on both amd64 and arm64.
macOS (Apple Silicon) is supported but requires
[building from source](user-guide/latest/source.md). See
[Supported Operating Systems](user-guide/latest/installation.md#supported-operating-systems).

Note that the published jars target baseline data-center CPUs — `x86-64-v3` on amd64 and
`neoverse-n1` on arm64. If the native library fails with SIGILL (illegal instruction), your CPU
predates that baseline; build from source for your own target.

### Does Comet support Delta Lake, Hudi, or Paimon?

Not today. Comet accelerates Spark's built-in file formats and provides a native Iceberg scan, but it
has no native integration for Delta Lake, Hudi, or Paimon.

Delta Lake support is being actively explored along several tracks — reusing Comet's existing native
Parquet reader for plain Delta tables, a full native Delta scan built on `delta-kernel-rs`, and a
Table Provider API that would let non-Iceberg sources plug in more easily. None of these is committed
yet. [Delta Lake Support](contributor-guide/roadmap.md#delta-lake-support) on the roadmap tracks the
current state and links the relevant issues and pull requests. Users who need native acceleration for
Delta, Hudi, or Paimon today will find broader coverage in Gluten; see the
[comparison with Gluten](about/gluten_comparison.md#table-format-support).

### Do I need a GPU?

No. Comet runs on commodity CPUs. There is no GPU or FPGA requirement and no proprietary hardware
dependency.

### Can Comet return different results than Spark?

Comet's goal is to produce exactly the results Spark produces, and it is **compatible by default**.
Where a native implementation has known semantic differences from Spark, Comet either runs Spark's
own generated code inside the native pipeline (the codegen dispatcher) or falls back to Spark. The
faster but divergent native path is opt-in per expression, via
`spark.comet.expression.<Name>.allowIncompatible=true`.

So you can see different results if you opt in to an `Incompatible` expression or operator, and you
can hit one of the divergences that are still open bugs. The
[Compatibility Guide](user-guide/latest/compatibility/index.md) documents both, including a
[Known result-value divergences](user-guide/latest/compatibility/index.md#known-result-value-divergences)
section that collects the current edge cases in one place. A `Compatible` expression that disagrees
with Spark is a bug — please [report it](#where-do-i-ask-questions-or-report-a-bug).

### Does Comet use SIMD?

Comet does not hand-write SIMD intrinsics. It relies on the vectorized Arrow and DataFusion kernels,
where the compiler auto-vectorizes the tight loops over columnar data, and it selects the instruction
set through the build target: the published amd64 jar is built for `x86-64-v3` (which includes AVX2)
and the arm64 jar for `neoverse-n1`. Building from source with `-Ctarget-cpu=native` lets the compiler
use everything your own CPU offers.

## Project and Community

### How does Comet compare to Gluten?

Both are Spark plugins that hand serialized plans to a native engine — Comet to Apache DataFusion
(Rust), Gluten primarily to Velox (C++). Independent benchmarking by AWS Labs on TPC-DS 3 TB found
similar overall performance. Comet currently has an edge on Spark 4.0 with ANSI mode enabled and
through its codegen dispatcher; Gluten offers broader table format coverage. The
[comparison with Gluten](about/gluten_comparison.md) goes through architecture, governance,
compatibility, and performance in detail.

### What is planned for future releases?

The [Roadmap](contributor-guide/roadmap.md) covers the major items that need coordination between
contributors, including window function gaps, native coverage for codegen-dispatched expressions,
Iceberg V3 support, spillable hash joins, native Parquet and Iceberg writes, and Delta Lake. Comet
targets a minor release every four to six weeks; see the
[Versioning Policy](about/versioning_policy.md) for what each release number promises.

### Where do I ask questions or report a bug?

- **Questions and discussion**: the DataFusion Slack and Discord channels, listed on the
  [Apache DataFusion communication page](https://datafusion.apache.org/contributor-guide/communication.html),
  or [GitHub Discussions](https://github.com/apache/datafusion-comet/discussions).
- **Bugs and feature requests**: [GitHub Issues](https://github.com/apache/datafusion-comet/issues).
  For a bug, please include the Comet version, Spark version, JDK, and the plan or fallback reasons
  where relevant. [Checking the Comet Version](user-guide/latest/installation.md#checking-the-comet-version)
  shows how to get the build metadata that is most useful in a report.
- **Contributing**: start with the [Contributor Guide](contributor-guide/index.md).

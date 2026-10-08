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

# Comet Roadmap

Comet is an open-source project and contributors are welcome to work on any issues at any time, but we find it
helpful to have a roadmap for some of the major items that require coordination between contributors.

## Window Expressions

Native window execution runs by default (`spark.comet.exec.window.enabled`). The ranking functions (`rank`,
`dense_rank`, `row_number`, `percent_rank`, `cume_dist`, `ntile`), value functions (`lag`, `lead`, `nth_value`,
`first_value`, `last_value`), and the `count`, `min`, `max`, `sum`, and `avg` aggregates are accelerated.
Remaining work is to close the gaps that still fall back to Spark: statistical aggregates (`stddev`, `variance`,
`corr`, `covar`) and `collect_list` / `collect_set` as window functions ([#4766]), `RANGE` frames with explicit date or
decimal offsets ([#4834]), `first_value` / `last_value` on `RANGE` frames with a literal offset ([#4835]), and
non-literal `lag` / `lead` default values ([#4268]). See the
[window function compatibility guide](../user-guide/latest/compatibility/operators.md) for the complete list of
supported functions, frames, and fallback cases.

[#4268]: https://github.com/apache/datafusion-comet/issues/4268
[#4766]: https://github.com/apache/datafusion-comet/issues/4766
[#4834]: https://github.com/apache/datafusion-comet/issues/4834
[#4835]: https://github.com/apache/datafusion-comet/issues/4835

## Native Lambda Evaluation

Spark supports higher-order functions on arrays and maps that take a lambda, including `transform`, `exists`,
`forall`, `aggregate`, `zip_with`, `map_filter`, and `map_zip_with`. Comet evaluates these today through a JVM
codegen-dispatch bridge (`CometScalaUDF`, `CometBatchKernelCodegen`) instead of falling back to Spark, but the
lambda body is still interpreted row-at-a-time on the JVM rather than natively in DataFusion. DataFusion added
native higher-order function support (`array_transform`, `array_filter`, `array_any_match`, etc.) that Comet
does not yet use; it's not yet known whether their semantics are Spark-compatible. We'll explore whether
DataFusion's implementations can replace the JVM codegen-dispatch bridge to remove that round-trip and let
these expressions benefit from vectorized native execution.

## Native Coverage for Codegen-Dispatched Expressions

Beyond lambda bodies, a number of built-in Spark scalar expressions (some regular expression, JSON, and datetime
functions, for example) route through the same JVM codegen-dispatch bridge by default, either because their native
DataFusion or `datafusion-spark` implementation has known semantic differences from Spark, or because no native
implementation exists yet. The [expression reference] records which expressions are codegen-dispatched today. The
bridge covers only scalar expressions, so an aggregate function with no Spark-compatible native implementation falls
back to Spark instead. We're exploring closing these gaps so that more expressions run natively by default, which
would remove JVM round-trips beyond those the lambda work above addresses.

[expression reference]: ../user-guide/latest/expressions.md

## Iceberg Table Format V3 Support

Comet's native Iceberg scans read V3 tables, including encrypted tables ([#4991]) and tables with deletion vectors
([#5853]). We want to add the remaining V3 features so that these scans don't fall back to Spark: row lineage
metadata columns, column default values, and the new V3 types (`variant`, `geometry`, `geography`, and `unknown`).
The work is tracked in [#3376], and upstream `iceberg-rust` support in [iceberg-rust #2411]. Native Iceberg scans
also don't support HDFS-backed tables today: Comet's native Iceberg storage layer handles only local files, S3 and
S3-compatible stores, GCS, and OSS, and would need an HDFS `StorageFactory` upstream in `iceberg-storage-opendal`.
We're scoping what that work would take.

[#3376]: https://github.com/apache/datafusion-comet/issues/3376
[#4991]: https://github.com/apache/datafusion-comet/pull/4991
[#5853]: https://github.com/apache/datafusion-comet/pull/5853
[iceberg-rust #2411]: https://github.com/apache/iceberg-rust/issues/2411

## TPC-H and TPC-DS Performance

Comet already delivers substantial speedups over vanilla Spark on TPC-H and TPC-DS; we publish per-query
results for [TPC-DS] with each release. An independent [AWS Labs benchmark] comparing Comet 0.16.0 with
Gluten 1.6.0 on a 3TB TPC-DS workload found that the two accelerators deliver similar overall performance. Increasing
the speedup further and closing the remaining per-query gaps is an ongoing focus, tracked under [#2004] (TPC-H) and
[#2551] (TPC-DS).

[TPC-DS]: benchmark-results/tpc-ds.md
[AWS Labs benchmark]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-gluten-velox-comet-benchmark
[#2004]: https://github.com/apache/datafusion-comet/issues/2004
[#2551]: https://github.com/apache/datafusion-comet/issues/2551

## Upstream Work in DataFusion

A growing number of Spark-compatible expressions live in the `datafusion-spark` crate in the core DataFusion
repository. Comet is migrating its expression implementations to that crate so that they can be shared by other
DataFusion-based projects, and has wired up nearly every function that crate provides ([#4150]). Improvements to
core DataFusion operators (joins, aggregates, window) made in support of Comet also benefit the wider ecosystem.

[#4150]: https://github.com/apache/datafusion-comet/issues/4150

## Spillable Hash Join

Comet's native hash join currently requires the build side to fit entirely in memory. Adding spill-to-disk
support ([#2545]) will allow Comet to handle larger joins without falling back to Spark, improving both reliability
and performance for memory-intensive workloads. Comet's native hash join uses DataFusion's `HashJoinExec`, and a
design for spilling in that operator, behind a flag that is off by default, is proposed upstream in
[datafusion #24768].

[#2545]: https://github.com/apache/datafusion-comet/issues/2545
[datafusion #24768]: https://github.com/apache/datafusion/issues/24768

## Java/Scala UDF Support

Spark users frequently define custom UDFs in Java or Scala. Comet now dispatches scalar `ScalaUDF` expressions
through a JVM codegen bridge (`CometScalaUDF`) instead of always falling back to Spark. Aggregate UDFs, table
UDFs/generators, Hive `GenericUDF`/`SimpleUDF`, and Python UDFs other than `mapInArrow` and `mapInPandas` still fall
back to Spark entirely; Comet's support for those two Python APIs is experimental and disabled by default ([#4234]).
Extending the codegen-dispatch approach to cover these remaining categories will reduce fallbacks further and
allow more queries to run end-to-end in Comet.

[#4234]: https://github.com/apache/datafusion-comet/pull/4234

## Memory Management Improvements

Comet coordinates memory between the JVM and native Rust execution through a custom memory pool. Improving
memory accounting, reservation strategies, and spill integration will reduce out-of-memory errors and allow
Comet to make better use of available resources, especially in multi-query and multi-task environments.

## Native Parquet Writes

Comet has experimental support for native Parquet writes via `InsertIntoHadoopFsRelationCommand`, currently
disabled by default. The goal is to reach correctness and performance parity with Spark's writer so it can be
enabled by default ([#1625]).

[#1625]: https://github.com/apache/datafusion-comet/issues/1625

## Iceberg Table Writes

Comet can now write Iceberg tables natively. The feature is experimental, disabled by default, and controlled by two
settings, the second of which requires the first. The first splits Spark's Iceberg V2 write operator into separate
writer and committer operators, so the query feeding the write becomes visible to AQE and to Comet's columnar rules
([#4658]). The second delegates each task's Parquet write to `iceberg-rust` when the write passes an eligibility
check ([#5361]); writes that don't pass keep Iceberg Java's writer. Merge-on-read writes are not intercepted yet
([#6240]). The remaining work toward the original goal of [#4322], an ETL job that runs end to end in native code, is
tracked in [#5649]: correctness fixes, failure handling that matches Iceberg Java, broader coverage, and enabling
both settings by default ([#5644]). See [Iceberg Writes](iceberg-writes.md) for how the write path works.

[#4322]: https://github.com/apache/datafusion-comet/issues/4322
[#4658]: https://github.com/apache/datafusion-comet/pull/4658
[#5361]: https://github.com/apache/datafusion-comet/pull/5361
[#5644]: https://github.com/apache/datafusion-comet/issues/5644
[#5649]: https://github.com/apache/datafusion-comet/issues/5649
[#6240]: https://github.com/apache/datafusion-comet/issues/6240

## Delta Lake Support

Comet currently supports Spark's built-in file formats and Iceberg, but not Delta Lake ([#174]). The plugin boundary
for an out-of-tree Delta module has landed in inert, gated slices ([#4700], [#4952]), and two read paths build on
it. In the first, which is in review, delta-spark plans the scan and Comet's native Parquet reader executes it,
applying deletion vectors inside the scan; it ships as an opt-in contrib module ([#5365]). The second, still a
draft, is a full native Delta scan built on `delta-kernel-rs` ([#4366]). [#5411] proposes converging the two into
one plugin, with the Parquet path as the default and the kernel path covering what it can't express yet, such as
change data feed. Generalizing Comet's scan-side APIs so that Delta and other non-Iceberg data sources can plug in
more easily is tracked as a Table Provider API abstraction ([#4706]).

[#174]: https://github.com/apache/datafusion-comet/issues/174
[#4366]: https://github.com/apache/datafusion-comet/pull/4366
[#4700]: https://github.com/apache/datafusion-comet/pull/4700
[#4706]: https://github.com/apache/datafusion-comet/issues/4706
[#4952]: https://github.com/apache/datafusion-comet/pull/4952
[#5365]: https://github.com/apache/datafusion-comet/pull/5365
[#5411]: https://github.com/apache/datafusion-comet/issues/5411

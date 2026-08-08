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
`corr`, `covar`) and `collect_list` / `collect_set` as window functions ([#4766]), `GROUPS` frames ([#4836]), `RANGE` frames with explicit date
offsets ([#4834]), `first_value` / `last_value` on `RANGE` frames with a literal offset ([#4835]),
non-literal `lag` / `lead` default values ([#4268]), and `WindowGroupLimitExec` ([#4837]). See the
[window function compatibility guide](../user-guide/latest/compatibility/operators.md) for the complete list of
supported functions, frames, and fallback cases.

[#4268]: https://github.com/apache/datafusion-comet/issues/4268
[#4766]: https://github.com/apache/datafusion-comet/issues/4766
[#4834]: https://github.com/apache/datafusion-comet/issues/4834
[#4835]: https://github.com/apache/datafusion-comet/issues/4835
[#4836]: https://github.com/apache/datafusion-comet/issues/4836
[#4837]: https://github.com/apache/datafusion-comet/issues/4837

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
functions, for example) and aggregate functions route through the same JVM codegen-dispatch bridge by default,
either because their native DataFusion or `datafusion-spark` implementation has known semantic differences from
Spark, or because no native implementation exists yet. See the [compatibility guide] for the current list of
codegen-dispatched expressions. We're exploring closing these gaps so that more expressions run natively by
default, which would reduce JVM round-trips beyond what the lambda and UDF work above already covers.

[compatibility guide]: ../user-guide/latest/compatibility/index.md

## Iceberg Table Format V3 Support

Comet landed its first Iceberg table format V3 feature (native data file decryption, [#4991]), and we want to
add more V3 features to Comet's native Iceberg scans so they don't fall back to Spark. The work is tracked
phase-by-phase in [#3376]: detecting the V3 format and supporting new V3 data types, deletion vector reads, row
lineage, and table encryption. Native deletion vector reads are prototyped in a draft PR ([#4887]), but are
blocked on upstream `iceberg-rust` support, tracked in [iceberg-rust #2792] and [iceberg-rust #2411]. Native Iceberg
scans don't support HDFS-backed tables today: the storage scheme match in `iceberg_scan.rs` only handles `file`,
`s3`/`s3a`, `gs`, and `oss`, and would need an HDFS `StorageFactory` upstream in `iceberg-storage-opendal`. We're
scoping what that work would take.

[#3376]: https://github.com/apache/datafusion-comet/issues/3376
[#4887]: https://github.com/apache/datafusion-comet/pull/4887
[#4991]: https://github.com/apache/datafusion-comet/pull/4991
[iceberg-rust #2411]: https://github.com/apache/iceberg-rust/issues/2411
[iceberg-rust #2792]: https://github.com/apache/iceberg-rust/issues/2792

## TPC-H and TPC-DS Performance

Comet already delivers substantial speedups over vanilla Spark on TPC-H and TPC-DS; we publish per-query
results for [TPC-DS] with each release. An independent [AWS Labs benchmark] comparing Comet 0.16.0 with
Gluten 1.6.0 on a 3TB TPC-DS workload found that the two accelerators deliver similar overall performance. Increasing
the speedup further and closing the remaining per-query gaps is an ongoing focus, tracked under [#2004] (TPC-H) and
[#858] (TPC-DS).

[TPC-DS]: benchmark-results/tpc-ds.md
[AWS Labs benchmark]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-gluten-velox-comet-benchmark
[#858]: https://github.com/apache/datafusion-comet/issues/858
[#2004]: https://github.com/apache/datafusion-comet/issues/2004

## Upstream Work in DataFusion

A growing number of Spark-compatible expressions live in the `datafusion-spark` crate in the core DataFusion
repository. Comet is migrating its expression implementations to that crate so that they can be shared by other
DataFusion-based projects, and is wiring up the functions that crate already provides ([#4150]). Improvements to
core DataFusion operators (joins, aggregates, window) made in support of Comet also benefit the wider ecosystem.

[#4150]: https://github.com/apache/datafusion-comet/issues/4150

## Spillable Hash Join

Comet's native hash join currently requires the build side to fit entirely in memory. Adding spill-to-disk
support will allow Comet to handle larger joins without falling back to Spark, improving both reliability and
performance for memory-intensive workloads.

## Java/Scala UDF Support

Spark users frequently define custom UDFs in Java or Scala. Comet now dispatches scalar `ScalaUDF` expressions
through a JVM codegen bridge (`CometScalaUDF`) instead of always falling back to Spark. Aggregate UDFs, table
UDFs/generators, Python/Pandas UDFs, and Hive `GenericUDF`/`SimpleUDF` still fall back to Spark entirely.
Extending the codegen-dispatch approach to cover these remaining categories will reduce fallbacks further and
allow more queries to run end-to-end in Comet.

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

Comet's native scans accelerate Iceberg reads today, but writes still run entirely through Spark's Iceberg V2 write
operator. [#4322] tracks exploring native acceleration of Iceberg writes, so an ETL job could run end to end in
native code, falling back only where Comet can't match Iceberg Java's functionality. Draft proposals split the
existing V2 write operator into separate "writer" and "committer" operators so the data-file-write portion can be
planned and accelerated the same way as a native Parquet write ([#4658], [#4487]). No design here is committed yet;
we're exploring whether this approach is worth pursuing.

[#4322]: https://github.com/apache/datafusion-comet/issues/4322
[#4487]: https://github.com/apache/datafusion-comet/pull/4487
[#4658]: https://github.com/apache/datafusion-comet/pull/4658

## Delta Lake Support

Comet currently supports Spark's built-in file formats and Iceberg, but not Delta Lake ([#174]). A draft PR reuses
Comet's existing native Parquet reader to accelerate scans of plain Delta tables that use neither deletion vectors
nor column mapping ([#4669]); a separate, larger effort explores a full native Delta scan built on `delta-kernel-rs`
as an out-of-tree contrib module ([#4366]), landing in inert, gated slices ([#4700], [#4952]). Generalizing Comet's
scan-side APIs so that Delta and other non-Iceberg data sources can plug in more easily is tracked as a Table
Provider API abstraction ([#4706]). None of this is committed yet; we're exploring which approach, if any, is worth
pursuing.

[#174]: https://github.com/apache/datafusion-comet/issues/174
[#4366]: https://github.com/apache/datafusion-comet/pull/4366
[#4669]: https://github.com/apache/datafusion-comet/pull/4669
[#4700]: https://github.com/apache/datafusion-comet/pull/4700
[#4706]: https://github.com/apache/datafusion-comet/issues/4706
[#4952]: https://github.com/apache/datafusion-comet/pull/4952

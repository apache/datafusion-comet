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

# Comet Upgrade Guide

This guide lists the behavior changes in each Comet release and the configuration setting that
restores the previous behavior for each one. Read the section for every version between the release
you are upgrading from and the release you are upgrading to.

A **behavior change** is one where the same query, run over the same data, with the same explicitly
set configuration, produces a different result or a different error than it did in the previous
release. Comet's [versioning policy](../../about/versioning_policy.md) permits these in a minor
release only when a `spark.comet.legacy.*` configuration key restores the previous behavior, so
every behavior change below names such a key.

A release's section can also list changes that need no legacy key but can still change what an
existing deployment does, such as a fix that makes Comet apply a setting as documented, or a setting
that was deprecated or removed. Each of these entries names the settings involved.

Two kinds of change are deliberately absent from this guide:

- **Correctness fixes.** Comet's goal is to return the results Apache Spark returns. When Comet
  returns something different for an expression or operator marked `Compatible`, that is a bug, and
  fixing it is a bug fix rather than a behavior change. These fixes appear in the release notes, not
  here. For a fix with an unusually wide blast radius the maintainers may still provide a
  `spark.comet.legacy.*` key, in which case it will be listed below.
- **Changes to which operators run natively.** Whether a given expression runs in Comet or falls
  back to Spark can change in any release. This affects performance, not results.

Additions, new configuration keys, and Apache Spark version support changes are recorded in the
release notes and on the
[Spark Version Compatibility](compatibility/spark-versions.md) page rather than here.

## Legacy Configuration Keys

Every key under `spark.comet.legacy.*` is deprecated from the moment it is added. Each one exists to
give you time to adapt to a behavior change, and may be removed in any future major release, at
which point the newer behavior becomes unconditional.

Treat setting one of these keys as a temporary measure. If you find you cannot stop relying on a
legacy behavior, please open an issue describing your use case so it can be considered before the
key is removed.

## Upgrading to Comet 1.2.0

Comet `1.2.0` makes no behavior changes that need a `spark.comet.legacy.*` key. The changes below
need none either, but check whether any of them applies to your deployment.

### S3 Request Addressing

Comet's native Parquet and CSV scans now address S3 requests the way Hadoop S3A does (native
Iceberg reads go through iceberg-rust's FileIO and are unchanged). With `fs.s3a.path.style.access`
unset or `false` they use virtual-hosted addressing (`<bucket>.<host>`), and with `true` they use
path-style addressing (`<host>/<bucket>`). Comet `1.1.0` and earlier addressed every custom
`fs.s3a.endpoint` path-style whatever the setting said, and applied the setting the wrong way round
when no endpoint was set.

With a custom endpoint and the setting unset or `false`, a read of `s3a://bucket/key` through
`fs.s3a.endpoint=http://minio.internal:9000` now goes to `http://bucket.minio.internal:9000/key`
instead of `http://minio.internal:9000/bucket/key`. A MinIO or Ceph RGW deployment whose DNS does
not resolve the bucket host fails with a DNS error. Set `fs.s3a.path.style.access=true` to keep the
old addressing.

Reads with no custom endpoint change too. With only `spark.hadoop.fs.s3a.endpoint.region=us-east-1`
set, a read of `s3a://my-bucket/key` moves from `https://s3.us-east-1.amazonaws.com/my-bucket/key`
to `https://my-bucket.s3.us-east-1.amazonaws.com/key`. With `fs.s3a.path.style.access=true` and no
endpoint it moves the other way, from virtual-hosted to path-style. Both match Hadoop S3A's
addressing, but a proxy or egress rule that matches on host names will see the new hosts.

Comet still addresses a request path-style on its own when the endpoint's host is an IP address or
the bucket name cannot be a host name. See
[Additional S3 Configuration Options](datasources.md#additional-s3-configuration-options).

### Deprecated and Removed Settings

Using `spark.comet.sparkToColumnar.enabled` and `spark.comet.sparkToColumnar.supportedOperatorList`
to convert ranges, in-memory cached tables, RDDs, queries without a `FROM` clause and Data Source V1
relations that are not file-based to Arrow is deprecated, and will stop working in a future major
release. Each of these now has a config of its own:

| Operator in the list | Config                                       |
| -------------------- | -------------------------------------------- |
| `Range`              | `spark.comet.convert.range.enabled`          |
| `InMemoryTableScan`  | `spark.comet.convert.inMemoryCache.enabled`  |
| `RDDScan`            | `spark.comet.convert.rdd.enabled`            |
| `OneRowRelation`     | `spark.comet.convert.oneRowRelation.enabled` |
| `RowDataSourceScan`  | `spark.comet.convert.rowDataSource.enabled`  |

`spark.comet.sparkToColumnar.supportedOperatorList` now defaults to an empty list. When it is not
set, `spark.comet.sparkToColumnar.enabled=true` still converts the first four, which the list named
by default, and a list that names any of the five still converts it, as before. In both cases the
driver logs a warning that names the config to use instead. Before Spark 4.1, Spark plans a query
without a `FROM` clause as an `RDDScan`, so on those versions `RDDScan` in the list also converted
it. `spark.comet.convert.oneRowRelation.enabled` is on by default, so such a query is now converted
without either setting, and logs no warning. The list remains the way to convert other leaf
operators, such as the scan of a Data Source V2 connector.

Converting a Text scan with `FileSourceScan` or `BatchScan` in the list is deprecated in the same
way, and logs the same warning. Use `spark.comet.convert.text.enabled` instead.

### Iceberg Writes

Comet's Iceberg write path is now switched by one setting, `spark.comet.write.iceberg.enabled`,
which stays off by default. With it on, Comet plans an Iceberg `INSERT INTO`, `INSERT OVERWRITE`,
and copy-on-write `DELETE`, `UPDATE` or `MERGE` as two operators, `IcebergWrite` under
`IcebergCommit`, in place of Spark's single V2 write operator, and writes the data files of each
eligible write natively with iceberg-rust. A write that is not eligible uses iceberg-java's writer,
and iceberg-java commits every write. Before turning it on, see [Iceberg Writes](iceberg.md#writes)
for how natively written files differ from iceberg-java's, the footer reads the native path makes on
S3 or GCS, and how the native writer draws on Comet's off-heap memory pool.

Comet 1.1.0 named this setting `spark.comet.iceberg.write.enabled`, in the testing category, and it
took effect only together with `spark.comet.write.iceberg.splitOperator.enabled`. Comet now ignores
the old name, so a deployment that turned native writes on that way must set
`spark.comet.write.iceberg.enabled=true` to keep them.
`spark.comet.write.iceberg.splitOperator.enabled` is a testing setting, as it was in 1.1.0, but
Comet 1.1.0's Iceberg Writes guide documented it as the switch for the split plan. A deployment
that still sets it to `true` keeps the split plan, with iceberg-java writing the data files, and
on Spark 3.5+ it now also plans merge-on-read writes as the split plan. Unset it to plan Spark's
own operator.

### Memory Pool Limits

The `fair_unified` memory pool, which is the default for `spark.comet.exec.memoryPool`, limited each
of a task's operators to a share of all of `spark.memory.offHeap.size`, while Spark limits each task
to an even share of it among the running tasks. Whenever more than one task was running, the share
was the looser limit, so one operator could take all of its task's memory, and another operator
that cannot spill, such as the final merge of a sort, then failed the task
([#7088](https://github.com/apache/datafusion-comet/issues/7088)). Spark retried such tasks, and the
retries usually succeeded. Comet 1.2.0 divides `spark.memory.offHeap.size` among the tasks running
native plans, as Spark does, and within a task limits only the operators that can spill, each to an
even share of what the operators that cannot spill leave. So sorts, aggregations and the shuffle
writer can spill sooner when several of them run in one task, and the operators that cannot spill
are limited only by Spark, unless the deprecated `spark.comet.exec.memoryPool.fraction` is below
`1.0`. The native Iceberg fanout writer is one of the operators that can spill: it closes files
rather than take memory from the others, so it can write more files.

An operator's limit can now be well below what Spark would grant its task, and a sort can fail
where it used to run. A sort fails with "Not enough memory to continue external sort" instead of
spilling when a single batch is larger than its share, which wide rows, many cores per executor, a
small `spark.memory.offHeap.size` and several operators that can spill in one task make likelier.
It can also fail when operators that cannot spill, such as the build side of a shuffled hash join,
hold the whole of the task's share. To avoid either, give the executors more off-heap memory,
lower `spark.comet.batchSize`, or leave every limit to Spark by setting
`spark.comet.exec.memoryPool=greedy_unified`. See
[Configuring Comet Memory](tuning/memory.md#configuring-comet-memory).

### Native Iceberg Scan File Concurrency

`spark.comet.scan.icebergNative.dataFileConcurrencyLimit` now defaults to 2, where Comet `1.1.0`
defaulted to 1. The native Iceberg scan reads two data files at once within a task, which hides the
latency of opening each file on tables with many small files or on high-latency storage such as S3.
Each file in flight holds its own buffers, so a task can use more off-heap memory, and a query
without an `ORDER BY` can return rows in a different order than before, because batches from the two
files interleave. Spark does not guarantee the order of such a result. To read one file at a time in
the order Spark assigned them, as before, set the setting to `1`. See [Iceberg tuning](iceberg.md#tuning).

### Known Differences From Spark That Are New in 1.2.0

The three differences below were not in Comet `1.1.0`. They come from operators and expressions that
now run natively where `1.1.0` fell back to Spark, so they are listed here although they are not
behavior changes that a legacy key restores. Each one has an issue, and each has a setting that
makes Comet use Spark's implementation instead.

- **`collect_set` over `FLOAT` or `DOUBLE` next to `count(DISTINCT ...)`
  ([#7203](https://github.com/apache/datafusion-comet/issues/7203)).** On Spark before 4.2, a query
  that mixes `count(DISTINCT ...)` with other aggregates, such as
  `SELECT x, count(DISTINCT y), collect_set(z) FROM t GROUP BY x`, now runs `collect_set` natively.
  Comet `1.1.0` ran that query in Spark. Native `collect_set` keeps `-0.0` and `0.0` as two entries
  and merges every `NaN` into one, where Spark collapses the zeros and keeps each `NaN`. The
  [compatibility guide](compatibility/floating-point.md) already lists this difference for
  `collect_set` on its own. Spark 4.2 is not affected. Set `spark.comet.exec.strictFloatingPoint=true`
  to make `collect_set` over floating-point values fall back to Spark.
- **`RANGE` window frames over a `DATE` column with a very large offset
  ([#5022](https://github.com/apache/datafusion-comet/issues/5022)).** A `RANGE` frame with an
  explicit `PRECEDING` or `FOLLOWING` offset over a `DATE` `ORDER BY` column now runs natively,
  where `1.1.0` fell back for every such frame. When the offset is roughly 92 million days (about
  250,000 years) or more, the frame bound overflows the date range, and Comet can put rows with a
  `NULL` date in the frame where Spark does not. Comet falls back above 2,144,550,751 days. Offsets
  of a realistic size are not affected. To avoid it for larger offsets, set
  `spark.comet.exec.window.enabled=false`, which runs every window operator in Spark.
- **Native `min` and `max` over `STRING` or `BINARY` under-report their memory
  ([#6936](https://github.com/apache/datafusion-comet/issues/6936)).** Comet `1.2.0` runs these
  natively, where `1.1.0` fell back. In a hash aggregate, DataFusion's accumulator counts the
  length of each group's current value rather than the memory it holds, so a group whose extreme
  value gets shorter, such as the `min` of URLs where a prefix arrives after a longer value, holds
  more memory than Comet's memory pool sees. The aggregate then spills later than it should, and a
  task can run out of memory. Results are not affected. Spark plans a hash aggregate for this when
  the query also has an aggregate such as `collect_set`, as in
  `SELECT k, min(s), collect_set(tag) FROM t GROUP BY k`. A plain `min` or `max` over a string is
  not affected. Set `spark.sql.execution.useObjectHashAggregateExec=false` to make Spark plan a
  sort aggregate for these queries, which is not affected.

## Upgrading to Comet 1.1.0

Comet `1.1.0` makes no behavior changes that need a `spark.comet.legacy.*` key. The changes below
need none either, but check whether any of them applies to your deployment.

Comet `1.1.0` requires JDK 17 or later. JDK 11 is no longer supported. See
[Installing Comet](installation.md) for the supported Java, Scala, and Spark versions.

### Settings That Now Take Effect as Documented

Comet `1.0.0` misread three size settings. Comet `1.1.0` reads each of them as documented, so a job
that sets one of them can behave differently after the upgrade. In each case, a setting that
already exists restores the old effect.

- `spark.comet.shuffle.native.writeBufferSize` was read in MiB but used as a number of bytes, so
  the native shuffle writer ran with a 1-byte write buffer by default, and a value of `64m` gave it
  64 bytes. The setting is now read in bytes with a default of 1 MiB, and a value with a unit means
  what it says. Each shuffle task holds a few of these buffers in native memory that no memory pool
  tracks, so check any value you set with a unit against `spark.executor.memoryOverhead`. A bare
  number keeps its old meaning, so writing the number without its unit restores the old buffer
  size. Values of 2 GiB or more are now rejected.
- `spark.comet.maxTempDirectorySize` was ignored when it was written with a unit, such as `10g`, and
  the 100 GB default applied instead. It is now enforced, so a query that spills more than that
  amount now fails. Remove the setting to keep the old limit. See
  [Limiting Spill Disk Usage](tuning/memory.md#limiting-spill-disk-usage).
- `spark.memory.offHeap.size` was read as MiB when it was written as a bare number of bytes. The
  `fair_unified` memory pool's per-operator shares were therefore about a million times too large,
  and never limited an operator. The shares are now correct, so operators can spill sooner, and an
  operator that cannot spill can fail when it exceeds its share. A size written with a unit, such
  as `16g`, is unaffected. To get the old behavior back, set
  `spark.comet.exec.memoryPool=greedy_unified`, which leaves every limit to Spark. See
  [Configuring Comet Memory](tuning/memory.md#configuring-comet-memory).

A malformed value of `spark.comet.maxTempDirectorySize` or `spark.comet.explain.native.enabled` now
fails the query instead of being replaced by the default. `spark.comet.debug.enabled`,
`spark.comet.explain.native.enabled` and `spark.comet.tracing.enabled` now also take effect in
Comet's native code when they are written in upper case, such as `TRUE`.

### Memory Pool Limits

In Comet 0.15.0 through 1.0.0, the `fair_unified` memory pool, which is the default for
`spark.comet.exec.memoryPool`, capped the memory of all of a task's operators combined at one
operator's share, because of a bug ([#5961](https://github.com/apache/datafusion-comet/issues/5961)).
Comet 1.1.0 limits each operator to its own share instead, so tasks with several operators can
reserve more memory before they spill than they could in those releases. The difference is largest
on executors that run few tasks at once, where Spark's own limit on each task is loosest. If you
sized executor memory against one of those releases, check that executors still have enough
headroom; see
[Sizing the Overhead from the Memory Usage Log](tuning/memory.md#sizing-the-overhead-from-the-memory-usage-log).

### Conditions for Enabling Comet

Comet needs Spark's off-heap memory to be enabled. `CometPlugin` already disabled Comet when
off-heap memory was disabled, but an application that registered `CometSparkSessionExtensions`
directly with `spark.sql.extensions` skipped that check, and Comet ran in on-heap mode, which exists
only for running tests. Comet `1.1.0` makes the same check on that path, and disables itself with a
warning when off-heap memory is not enabled. To keep using Comet, set
`spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size` when the application starts;
see [Configuring Comet Memory](tuning/memory.md#configuring-comet-memory). Both checks read
`spark.memory.offHeap.enabled` from the SparkContext, so setting it on a `SparkSession.builder`
after the SparkContext exists has no effect.

Comet also now checks the shuffle manager that the application is running, rather than the
session's `spark.shuffle.manager`. A session that named `CometShuffleManager` after the SparkContext
had started with a different shuffle manager used to plan Comet shuffles that failed with a
`ClassCastException`. Such a session now runs without Comet, with a warning.

### Native Iceberg Reads on EKS with IRSA

On EKS with IAM Roles for Service Accounts (IRSA), when `AWS_WEB_IDENTITY_TOKEN_FILE`,
`AWS_ROLE_ARN` and a region are set and the catalog configures no credentials, Comet `1.1.0`'s
native Iceberg scan takes its S3 credentials only from the web-identity role. If that fails, it no
longer falls back to the node role or Pod Identity, as Comet `1.0.0` did. So a cluster whose IRSA
setup is broken, and that was reading S3 as the node role without anyone noticing, now fails native
Iceberg reads with `failed to load signing credential`. The "EKS / IRSA" section of
[S3 Credential Providers](s3-credential-providers.md) explains the change. To go back to the old
credential chain for a catalog, set
`spark.sql.catalog.<catalog>.s3.comet.credential.webIdentity.enabled=false`. A table loaded by path
has no catalog to set that on, so for it set `spark.comet.scan.icebergNative.enabled=false`.

### Deprecated and Removed Settings

`spark.comet.exec.memoryPool.fraction` is deprecated and will be removed in a future major release.
It was documented as leaving room in `spark.memory.offHeap.size` for the native memory that Comet's
memory pools do not track, but it cannot: Spark hands out the whole off-heap pool whatever it is set
to. It keeps working as before, and the driver now logs a warning when it is set. Size
`spark.executor.memoryOverhead` for that memory instead; see
[Configuring Executor Memory Overhead](tuning/memory.md#configuring-executor-memory-overhead).

Comet `1.1.0` removes `spark.comet.memoryOverhead`, `spark.comet.exec.onHeap.memoryPool` and
`spark.comet.shuffle.jvm.memoryFactor`, including its older name
`spark.comet.columnar.shuffle.memory.factor`. They applied only to on-heap mode
(`spark.comet.exec.onHeap.enabled`), which exists for running Spark's SQL tests against Comet and no
longer tracks native memory at all. They were in the testing category, which the
[versioning policy](../../about/versioning_policy.md#testing-and-internal-configurations-are-exempt)
exempts, and Comet ignores them if they are still set.

## Upgrading to Comet 1.0.0

Comet `1.0.0` is the first release under the stable
[versioning policy](../../about/versioning_policy.md). From this release onward, behavior changes
are documented on this page along with the configuration key that reverts each one.

Changes made during the `0.x` series are not recorded here. If you are upgrading from a `0.x`
release, review the release notes for the versions in between.

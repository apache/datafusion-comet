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

### In-Memory Cache Enabled by Default

`spark.comet.exec.inMemoryCache.enabled` now defaults to `true` on Spark 3.5 and later. There, an
application that loads `CometPlugin` now stores what it caches with `CACHE TABLE`, `df.cache()` or
`df.persist()` in Comet's Arrow format instead of Spark's, and Comet scans it natively. On Spark 3.4
the default stays `false`; see [In-Memory Cache](in-memory-cache.md) for why. The format does not
change query results, but it can change performance. Spark's own cache scan can read Comet's format
more slowly than Spark's, which matters when a session turns Comet or its native execution off
after caching, and Comet records a fallback reason on such a scan. See
[In-Memory Cache](in-memory-cache.md#limitations).

The format is chosen once, when the application starts. To keep Spark's format, set
`spark.comet.exec.inMemoryCache.enabled=false` then. Comet also keeps Spark's format without that
setting when the application:

- starts with `spark.comet.enabled` or `spark.comet.exec.enabled` set to `false`.
- leaves Comet shuffle enabled without one of Comet's shuffle managers, so that Comet disables
  itself.
- uses Kryo with `spark.kryo.registrationRequired=true` and has not registered Comet's cached
  batch, because Kryo would reject it. To use Comet's format, register Comet's classes with
  `spark.kryo.registrator=org.apache.comet.CometKryoRegistrator`; see
  [Kryo](in-memory-cache.md#kryo).

An application that sets `spark.sql.cache.serializer` itself keeps the serializer it chose.

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

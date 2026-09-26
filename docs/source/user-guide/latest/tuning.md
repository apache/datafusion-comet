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

# Comet Tuning Guide

Comet provides some tuning options to help you get the best performance from your queries.

## Configuring Tokio Runtime

Comet uses a global tokio runtime per executor process. By default it starts one worker thread per executor core
(`spark.executor.cores`, or the thread count of `local[N]` and `local[*]` masters) and allows up to 512 blocking
threads, which is tokio's default. If `spark.executor.cores` is not set outside local mode, Comet starts a single
worker thread. These values can be overridden using the environment variables `COMET_WORKER_THREADS` and
`COMET_MAX_BLOCKING_THREADS`.

## Adaptive Partial Aggregation

For high-cardinality grouping, Comet can bypass partial hash aggregation when it is not
reducing the number of rows enough. This currently applies only to fused native shuffle-writer
plans whose partial aggregates are grouping-only or single-argument `COUNT`. Low-cardinality
inputs continue to aggregate normally. The SQL metric `rows bypassing partial aggregation`
shows whether skipping occurred.

Eligibility is conservative for the whole fused native plan: any unsupported partial accumulator,
Spark `PartialMerge`, or mixed-mode aggregate disables skipping in that plan. Multi-argument
`COUNT` and other accumulators are not admitted. Distribution-required grouping-only stages
still fully deduplicate, and non-native-shuffle plans retain ordinary aggregation.
The DataFusion testing configuration override does not bypass these safety checks.

DataFusion 55 defaults to probing after 100,000 input rows per partial aggregation
partition and skipping when the number of groups divided by input rows exceeds `0.8`.
To experiment with these thresholds, enable `spark.comet.exec.respectDataFusionConfigs`,
a development and testing option that defaults to `false`. For example, the following
SQL settings pass through the default threshold values, which you can adjust:

```sql
SET spark.comet.exec.respectDataFusionConfigs=true;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_rows_threshold=100000;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_ratio_threshold=0.8;
```

A lower row threshold allows an earlier decision; a lower ratio threshold makes
skipping more likely. Skipping can increase the number of partial states emitted
and the amount of shuffle data, so measure the effect on your workload.

To disable skipping, keep `spark.comet.exec.respectDataFusionConfigs=true` and set
the ratio threshold above the maximum possible groups/input-rows ratio:

```sql
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_ratio_threshold=1.1;
```

These settings only tune eligible plans. Unsupported accumulators and modes remain
disabled even when configuration overrides are enabled.

## Memory Tuning

It is necessary to specify how much memory Comet can use in addition to memory already allocated to Spark. In some
cases, it may be possible to reduce the amount of memory allocated to Spark so that overall memory allocation is
the same or lower than the original configuration. In other cases, enabling Comet may require allocating more memory
than before. See the [Determining How Much Memory to Allocate] section for more details.

Comet needs two things configured: an off-heap pool for it to draw its reservations from, and enough executor memory
overhead to cover the part of its footprint that no pool tracks. See [Configuring Comet Memory] and
[Configuring Executor Memory Overhead].

![Spark and Comet both use the JVM heap and share the off-heap memory pool, and the rest of Comet's native memory has to fit in the executor's memory overhead](../../_static/images/comet-executor-memory.svg)

[Determining How Much Memory to Allocate]: #determining-how-much-memory-to-allocate
[Configuring Comet Memory]: #configuring-comet-memory
[Configuring Executor Memory Overhead]: #configuring-executor-memory-overhead

### Configuring Comet Memory

Comet shares an off-heap memory pool with Spark. The size of the pool is
specified by `spark.memory.offHeap.size`. The pool is a shared _budget_ rather than a shared
allocator: Comet's native operators allocate from the Rust heap rather than from JVM off-heap
memory, but every reservation they make is charged against this same pool, so Comet and Spark's own
off-heap consumers draw down one number.

Comet's memory pool only tracks memory that an operator explicitly reserves, which in practice means the batches
an operator deliberately accumulates: the sort buffer, the build side of a hash join, hash aggregation state, and the
shuffle writer's buffered partitions. Memory that is not reserved is invisible to the pool no matter how much of it
there is. That includes:

- per-batch working memory in expression kernels and Arrow array builders,
- decompression buffers and Parquet reader structures,
- object store request buffers and the async runtime's own machinery,
- Arrow buffers allocated on the JVM side, which no budget covers at all,
- allocator overhead: buffer padding, size-class rounding, fragmentation, and pages the allocator retains after a
  free rather than returning to the operating system.

Reserved memory is therefore a lower bound on what Comet really uses, and how far below it sits depends on the
workload. This is why Comet can stay within the pool's limit and still push the executor past its container limit.
The part that is not counted has to fit in `spark.executor.memoryOverhead`, and each executor logs how large it is
while Comet runs; see [Sizing the Overhead from the Memory Usage Log].

`spark.comet.exec.memoryPool.fraction` is deprecated and does not leave room for it. Spark hands out all of
`spark.memory.offHeap.size` to the tasks that ask for it, whatever the fraction. The `fair_unified` pool applies the
fraction to each task separately, where Spark's own limit of an even share of the pool per running task is tighter
whenever more than one task is running, and the `greedy_unified` pool ignores it.

For more details about Spark off-heap memory mode, please refer to [Spark documentation].

[Spark documentation]: https://spark.apache.org/docs/latest/configuration.html

Comet implements multiple memory pool implementations. The type of pool can be specified with `spark.comet.exec.memoryPool`.

The valid pool types are:

- `fair_unified` (default when `spark.memory.offHeap.enabled=true` is set)
- `greedy_unified`

Both pool types are shared by all the native plans in the same Spark task. A task can run more than
one native plan at a time, for example the native operators on either side of a union or a
coalesce. The shared pool ensures that their combined memory usage stays within the per-task limit.

The `fair_unified` pool prevents operators from using more than an even fraction of the available memory
(i.e. `pool_size / num_consumers`, where `num_consumers` counts the memory consumers registered by all of the task's
native plans). This pool works best when you know beforehand
the query has multiple operators that will likely all need to spill. Sometimes it will cause spills even
when there is sufficient memory in order to leave enough memory for other operators.

Comet 0.15.0 through 1.0.0 capped the memory of all of a task's operators combined at one operator's share, because of a
bug ([#5961](https://github.com/apache/datafusion-comet/issues/5961)). Tasks with several operators can now reserve more
memory before they spill than they could in those releases. The difference is largest on executors that run few tasks
at once, where Spark's own limit on each task is loosest. If you sized executor memory against one of those releases,
check that executors still have enough headroom; see [Sizing the Overhead from the Memory Usage Log].

The `greedy_unified` pool type implements a greedy first-come first-serve limit. This pool works well for queries that do not
need to spill or have a single spillable operator.

[shuffle]: #shuffle
[Advanced Memory Tuning]: #advanced-memory-tuning

### Configuring Executor Memory Overhead

Enabling off-heap memory is not sufficient on its own. Comet also needs room in
`spark.executor.memoryOverhead`.

`spark.memory.offHeap.size` is a budget, and the cluster manager already sizes the executor
container to include it, so the memory that Comet's operators explicitly reserve has room. What does
not have room is everything Comet allocates without reserving it — the untracked categories listed
under [Configuring Comet Memory]. Those allocations are made by the Rust global allocator and live
in the native heap, outside the JVM heap and outside Spark's off-heap allocations, and nothing in
the container sizing accounts for them. The same applies to Comet's JVM-side Arrow buffers.

`spark.executor.memoryOverhead` is the only slack the container has for this, and the JVM's own
non-heap usage — metaspace, code cache, thread stacks, GC structures — is already drawing on it.

Work out what the executor already gets before choosing a value. When
`spark.executor.memoryOverhead` is unset, Spark derives the overhead as
`max(spark.executor.memoryOverheadFactor * spark.executor.memory, 384 MiB)`. The factor defaults to
`0.1`, except for PySpark and SparkR applications submitted to Kubernetes in cluster mode, where it
defaults to `0.4`. On Spark 4.0 and later the floor is configurable through
`spark.executor.minMemoryOverhead`. Setting `spark.executor.memoryOverhead` **replaces** the derived
value rather than adding to it, so a value below what is derived today shrinks the container instead
of growing it.

For a small executor, `2g` is a reasonable starting point. A 4 GiB executor derives only 409 MiB, so
this is a real increase:

```
spark.executor.memoryOverhead=2g
```

A 32 GiB executor, on the other hand, already derives 3276 MiB, and the same setting would take away
1228 MiB. For executors that large, either pick an absolute value above what is derived today, or
raise `spark.executor.memoryOverheadFactor` instead so that the overhead keeps scaling with executor
size:

```
spark.executor.memoryOverheadFactor=0.2
```

Raise the value further if executors are killed by the cluster manager (on Kubernetes,
`ExecutorLostFailure` with exit code 137) rather than failing with a task-level out-of-memory error.
To measure how much Comet needs rather than guessing, see [Sizing the Overhead from the Memory Usage Log].

Note that on Kubernetes and YARN the overhead is added to the container size, so raising it reduces
how many executors fit on a node.

[Sizing the Overhead from the Memory Usage Log]: #sizing-the-overhead-from-the-memory-usage-log

### Sizing the Overhead from the Memory Usage Log

While Comet native plans are running, each executor logs its native memory usage at INFO level,
one line every 10 seconds for the whole executor:

```
Comet native memory usage: allocated 5412.3 MiB, reserved 3890.0 MiB (16 native plans, 8 memory pools)
```

- `allocated` is the memory that Comet's native code has allocated and not yet freed, whether or not
  a pool tracks it.
- `reserved` is the part that Comet's memory pools track. It is charged against
  `spark.memory.offHeap.size`, so the container already has room for it.

The difference between the two, `allocated - reserved`, is Comet's untracked native memory. It is
the part of Comet's footprint that has to fit in `spark.executor.memoryOverhead`, alongside the
JVM's own non-heap memory. To size the overhead from it:

1. Run a representative workload and find the line with the largest difference in each executor's
   log. Take both figures from the same line: they are sampled together, and figures from different
   lines describe different moments. Setting `spark.comet.memory.logInterval=1s` for this run makes a
   short-lived peak less likely to fall between samples.
2. Start from the overhead the executors had before Comet was enabled, which covers the JVM's own
   non-heap memory, and add the largest difference seen on any executor.
3. Add a margin on top. The log can miss the true peak between samples, and neither figure includes
   the allocator's fragmentation and retained pages, memory allocated by native C libraries such as
   zstd, or Comet's Arrow buffers on the JVM side.

For example, a 16 GiB executor derives an overhead of 1638 MiB. If the largest difference in its
log is the 1522.3 MiB in the line above, the overhead needs to be at least 1638 + 1523 = 3161 MiB
before any margin, so `spark.executor.memoryOverhead=4g` would be a reasonable setting.

The executor also logs a warning when its native memory looks larger than its container allows:
when the difference, plus everything in use in Spark's off-heap memory pool (which includes Comet's
reservations), exceeds `spark.memory.offHeap.size` plus the memory overhead. This counts the part of
the off-heap pool that nothing has acquired at that moment, which untracked memory can occupy until
Spark hands it out, so a quiet log is not a sign that the overhead is large enough: size it from the
largest difference as described above. The overhead also has to hold the JVM's own non-heap memory,
so by the time the warning appears the executor has likely outgrown its container. It warns the first time this
happens, and again each time it happens after dropping back below. The overhead it uses is
`spark.executor.memoryOverhead` if set, otherwise `spark.executor.memoryOverheadFactor` of
`spark.executor.memory` with a minimum of `spark.executor.minMemoryOverhead`, as Spark sizes the
default container. There is no warning in local mode.

Look more closely before raising the overhead if the difference keeps growing through a run rather
than levelling off: native memory that is not being released will exhaust any overhead eventually.
The executor logs one more line after its last native plan finishes, and an `allocated` figure there
that grows from one query to the next points the same way.

`spark.comet.memory.logInterval` is read when an executor starts its first Comet native plan, so set
it when the application is submitted. Set it to `0` to turn the log off.

### Determining How Much Memory to Allocate

Generally, increasing the amount of memory allocated to Comet will improve query performance by reducing the
amount of time spent spilling to disk, especially for aggregate, join, and shuffle operations. Allocating insufficient
memory can result in out-of-memory errors. This is no different from allocating memory in Spark and the amount of
memory will vary for different workloads, so some experimentation will be required.

Here is a real-world example, based on running benchmarks derived from TPC-H, running on a single executor against
local Parquet files using the 100 GB data set.

Baseline Spark Performance

- Spark completes the benchmark in 632 seconds with 8 cores and 8 GB RAM
- With less than 8 GB RAM, performance degrades due to spilling
- Spark can complete the benchmark with as little as 3 GB of RAM, but with worse performance (744 seconds)

Comet Performance

- Comet requires at least 5 GB of RAM, but performance at this level
  is around 340 seconds, which is significantly faster than Spark with any amount of RAM
- Comet running in off-heap with 8 cores completes the benchmark in 295 seconds, more than 2x faster than Spark
- It is worth noting that running Comet with only 4 cores and 4 GB RAM completes the benchmark in 520 seconds,
  providing better performance than Spark for half the resource

It may be possible to reduce Comet's memory overhead by reducing batch sizes or increasing number of partitions.

### Batch Size

Comet processes data in columnar batches. The batch size is controlled by `spark.comet.batchSize` (default
`8192` rows). Larger batches generally improve throughput by amortizing per-batch overhead, but they also
increase peak memory usage — a batch holds all projected columns in Arrow format at once. Reduce this value
if you see frequent spilling or out-of-memory errors on wide tables; increase it (for example to `16384`) on
narrow tables when memory is plentiful.

`spark.comet.shuffle.jvm.batchSize` controls the batch size used when the JVM columnar shuffle writer
flushes sorted spill files. It must not exceed `spark.comet.batchSize`.

### Limiting Spill Disk Usage

Native operators that spill to disk (aggregate, sort, shuffle) are bounded by
`spark.comet.maxTempDirectorySize` (default 100 GB). The operators of one Comet native plan share
the limit. A Spark task can run more than one native plan at a time, for example the native
operators on either side of a union or a coalesce, so an executor running `N` concurrent tasks may
use more than `N` times this value on shared local disks. If the limit is reached, further spills
fail and the query errors out. Raise this on workloads with large sort/aggregate/shuffle spills, or
lower it to protect executors on shared disks, remembering that the total across an executor is a
multiple of this value.

## Parquet Reader Tuning

### Filter Pushdown / Late Materialization

Setting `spark.comet.parquet.rowFilterPushdown.enabled=true` pushes filter evaluation into the Parquet
decode step and lazily materializes projected columns for surviving rows. This can significantly reduce
CPU and memory when the filter is highly selective on a small subset of columns. It is disabled by default
because it can hurt when the filter is not selective or when most columns must be read anyway. Row-group,
page-index, and bloom-filter pruning happen regardless of this flag whenever Spark's
`spark.sql.parquet.filterPushdown` is on.

### Parquet Native Scans

Spark and DataFusion's native Parquet scans use different rules to decide which row groups belong to a
given scan range (split). Spark assigns a row group to a split if the row group's start offset falls
within `[split.start, split.start + split.length)`, guaranteeing that every task Spark plans reads at
least one row group when the file layout permits. DataFusion's `prune_by_range` also checks whether a
row group's start offset falls within the split's byte range, but because row group sizes are not aligned
with Spark's split boundaries, the two systems can disagree on which split "owns" a given row group.

When a file contains row groups whose sizes are close to `spark.sql.files.maxPartitionBytes`, this
mismatch can leave some Comet scan tasks with no row groups to read. Those tasks still load Parquet
metadata but return zero rows, while neighboring tasks end up reading more row groups than Spark
intended. The overall effect is that Comet uses only a fraction of the parallelism that Spark planned
for the scan stage, and end-to-end scan latency increases even though the total amount of data read
is unchanged.

Symptoms to look for:

- A subset of scan tasks completes almost immediately and reports 0 input rows, while the remaining
  tasks read noticeably more rows than the equivalent Spark tasks would.
- The Comet scan stage has the same number of planned tasks as Spark but a much lower count of tasks
  that actually do work.

Workaround: lower `spark.sql.files.maxPartitionBytes` so that each split is smaller than a single row
group. For example, if the file's row groups are around 120 MB and `spark.sql.files.maxPartitionBytes`
is left at the 128 MB default, most splits will contain at most one row group boundary and the
mismatch is amplified; setting `spark.sql.files.maxPartitionBytes` below 120 MB (for example, 64 MB)
distributes row groups across more splits and reduces the number of idle tasks. Smaller values produce
more splits overall, so some idle tasks may remain — tune the value against your file layout.

See [#3817](https://github.com/apache/datafusion-comet/issues/3817#issuecomment-4193279630) for a
worked example and further discussion.

## Iceberg Scan Tuning

Comet's native Iceberg scan (`spark.comet.scan.icebergNative.enabled`, enabled by default) reads each
task's data files one at a time by default. For tables with many small files or high-latency storage,
increase `spark.comet.scan.icebergNative.dataFileConcurrencyLimit` (default `1`; values of 2–8 are
suggested) to overlap I/O across files at the cost of extra memory.

## Optimizing Sorting on Floating-Point Values

Comet normalizes NaN payloads and signed zeros in scalar `FLOAT` and `DOUBLE` ordering keys, so `ORDER BY`, window
ordering and range partitioning on them match Spark and stay native even with
`spark.comet.exec.strictFloatingPoint=true`. Only the comparison key is normalized; returned values keep their original
NaN representation and zero sign.

Floating-point values nested in arrays, structs, or maps are compared with Arrow's raw total ordering instead, which can
differ from Spark when the data contains both zero and negative zero, or more than one NaN representation. This is likely
an edge case that is not of concern for many users. Setting `spark.comet.exec.strictFloatingPoint=true` makes those
nested cases fall back to Spark, and they can be forced back onto the native path with
`spark.comet.expression.SortOrder.allowIncompatible=true`.

`sort_array` is separate. It sorts array elements rather than ordering rows, and its elements are compared with Arrow's
raw total ordering, so `spark.comet.exec.strictFloatingPoint=true` makes it fall back even for a scalar floating-point
element type. Use `spark.comet.expression.SortArray.allowIncompatible=true` to keep it native.

## Optimizing Joins

Spark often chooses `SortMergeJoin` over `ShuffledHashJoin` for stability reasons. If the build-side of a
`ShuffledHashJoin` is very large then it could lead to OOM in Spark.

Vectorized query engines tend to perform better with `ShuffledHashJoin`, so for best performance it is often preferable
to configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`. Comet does not yet provide spill-to-disk for
`ShuffledHashJoin` so this could result in OOM. Also, `SortMergeJoin` may still be faster in some cases. It is best
to test with both for your specific workloads.

To configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`, set `spark.comet.exec.forceShuffledHashJoin=true`.

### Join Runtime Filters

Set `spark.comet.exec.join.dynamicFilter.enabled=true` to try experimental native hash join runtime
filtering. It is disabled by default. Eligible joins are inner joins with one direct signed integer
key (`TINYINT`, `SMALLINT`, `INT`, or `BIGINT`) and one native partition per input within each task.
Both broadcast and shuffled hash joins support either Spark build side. Unsupported joins keep
their existing execution path.

Once the build completes, its key domain filters probe batches before the hash probe. Eligible
native Parquet readers also use the domain to prune row groups. Reader attachment can pass through
direct-column `IS NOT NULL` checks, including conjunctions, and remaps columns when the scan itself
projects the file schema. The original null checks and residual runtime filter remain in place.
The original join still verifies matches, including any hash collisions admitted by the filter.
Standalone projections, other filter expressions, and limits prevent reader attachment.

To preserve schema-conversion and timestamp-overflow errors, runtime reader pruning is disabled for
each file whose projected or statically filtered columns require schema adaptations beyond direct
column mappings or literal values. This conservative check also disables reader pruning for allowed
`INT32` to `BIGINT` promotion and for projecting a subset of a struct's fields, even when those
adaptations cannot fail. Nested column pruning still reads only the requested struct fields. Scans
with supplied file statistics also skip reader attachment. These cases still use runtime filtering
on decoded batches.

Filters stay within the task's native plan and do not propagate across Spark exchanges or JVM/Arrow
boundaries. A shuffled hash join can still filter probe batches after shuffle, but it cannot send
its filter back to an earlier scan stage. Compare the [runtime-filter and scan metrics](metrics.md#hash-joins)
with the setting disabled to distinguish reduced hash-probe work from reader I/O savings.

## Shuffle

Comet provides accelerated shuffle implementations that can be used to improve the performance of your queries.

To enable Comet shuffle, set the following configuration in your Spark configuration:

```
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.comet.shuffle.enabled=true
```

`spark.shuffle.manager` is a Spark static configuration which cannot be changed at runtime.
It must be set before the Spark context is created. You can enable or disable Comet shuffle
at runtime by setting `spark.comet.shuffle.enabled` to `true` or `false`.
Once it is disabled, the configured shuffle manager handles ordinary Spark shuffle dependencies
without Comet's shuffle implementation.

### Shuffle Implementations

Comet provides two shuffle implementations: Native Shuffle and Columnar Shuffle. Comet will first try to use Native
Shuffle and if that is not possible it will try to use Columnar Shuffle. If neither can be applied, it will fall
back to Spark for shuffle operations.

#### Native Shuffle

Comet provides a fully native shuffle implementation, which generally provides the best performance. Native shuffle
supports `HashPartitioning`, `RangePartitioning`, and `SinglePartition`, plus `RoundRobinPartitioning` when enabled
(see [Round-Robin Partitioning](compatibility/operators.md#round-robin-partitioning)). Range partitioning keys must be
scalar types. Hash partitioning keys must be scalar types unless
`spark.comet.shuffle.native.partitioning.hash.nested.enabled=true`, which also admits struct, array, and (Spark 4.0
and later) map keys. That setting is disabled by default until the performance of the nested hashing paths has been
measured. Columns that are not partitioning keys may contain complex types like maps, structs, and arrays.

#### Columnar (JVM) Shuffle

Comet Columnar shuffle is JVM-based and supports `HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`, and
`SinglePartition`. This shuffle implementation supports complex data types as partitioning keys.

By default, Comet will convert a Spark `ShuffleExchangeExec` to columnar shuffle even when the shuffle's child is a
non-Comet (Spark) plan. The benefit is that the next query stage can start as native Comet execution, since the
shuffle output is already in Arrow format. The cost is a row to columnar conversion at the shuffle boundary on the
write side. To restrict columnar shuffle to cases where the child is already a Comet plan, set
`spark.comet.shuffle.convertFromSparkPlan.enabled=false`. Shuffles whose child is a Spark plan will then be left
as native Spark shuffles, which avoids the row to columnar conversion but means the downstream stage will also start
on Spark.

#### Automatic Revert to Spark Shuffle

When a Comet columnar shuffle ends up between a partial and a final aggregate that Comet could not convert (both
remain Spark `HashAggregateExec` or `ObjectHashAggregateExec` operators), Comet reverts it to Spark's built-in shuffle.
Keeping columnar shuffle between the two row-based aggregates would add `row -> Arrow -> shuffle -> Arrow -> row`
conversions with no Comet consumer on either side to benefit from columnar output. Other shuffles between non-Comet
operators are not reverted.

This shifts the affected shuffles from Comet's off-heap memory pool back to the JVM execution memory pool. Clusters
tuned for a small JVM heap may see `ExternalSorter` spills on queries where this revert fires. Shuffle I/O may also
grow marginally because Spark's row-based serializer generally compresses less well than Comet's Arrow IPC format.

Each revert is logged at `INFO` level on the driver as `Reverting Comet columnar shuffle to Spark shuffle between
<parent> and <child>`, which lets you correlate any unexpected behavior with this optimization.

This optimization is enabled by default and can be disabled by setting
`spark.comet.shuffle.revertRedundantColumnar.enabled=false`, in which case Comet will keep the columnar shuffle
even when both of those aggregates run on Spark.

### Remote Shuffle with Celeborn

Applications using Apache Celeborn can use Comet's composite shuffle manager to retain ordinary
Spark/Celeborn shuffle while accelerating other operators with Comet.

Native shuffle also requires reliable completion tracking for in-flight payloads. Released Celeborn
0.6.0 and 0.7.0 clients do not provide the required guarantee, so these versions retain ordinary
Spark/Celeborn shuffle even when `spark.comet.shuffle.mode=native`. Native shuffle support for
these clients requires a safe Celeborn push-completion API. The following settings request
native shuffle when the client passes Comet's compatibility checks:

```properties
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager
spark.comet.exec.enabled=true
spark.comet.shuffle.enabled=true
spark.comet.shuffle.mode=native
spark.celeborn.client.spark.stageRerun.enabled=true
```

Set the shuffle manager and Celeborn configuration before creating the Spark context. Celeborn is
an optional application dependency, not bundled with Comet: provide a compatible Celeborn Spark
client matching the application's Spark and Scala versions on both the driver and executors,
alongside the Comet JAR. Keep the application's existing Celeborn service configuration.

Native Celeborn shuffle requires explicit `spark.comet.shuffle.mode=native`. The default `auto`
mode and `jvm` mode retain ordinary Spark shuffle through the delegated Celeborn manager; they do
not select Comet's JVM columnar shuffle. Comet execution can still accelerate other operators.
With native mode enabled, exchanges with unsupported children, data types, or partitioning also
retain the ordinary Spark/Celeborn shuffle path. The local `CometShuffleManager` keeps its existing
native-to-columnar fallback behavior.

Stage reruns must remain enabled so failed or ambiguous map attempts can recover through a new
Celeborn shuffle generation. Native RSS does not support `spark.io.encryption.enabled=true`;
encrypted applications retain ordinary Spark/Celeborn shuffle instead. Do not disable encryption
required by the application to enable native RSS. Eligibility uses the manager's application-time
configuration, including Celeborn's effective defaults and legacy aliases, rather than later SQL
session overrides.

Celeborn's fallback policy remains application-owned. An effective
`spark.celeborn.client.spark.shuffle.fallback.policy=ALWAYS`, or an `AUTO` partition-count
threshold that the exchange reaches, keeps the exchange on Spark. Worker availability and quota
can still cause Celeborn to choose local fallback during registration. Once an exchange has been
planned as native, Comet rejects that local handle and fails the registration: native Arrow frames
cannot be passed to Spark's ordinary local shuffle writer. Set
`spark.celeborn.client.spark.shuffle.fallback.policy=NEVER` only if the application also wants
Celeborn to prohibit local fallback for ordinary Spark shuffles.

Native frames retain Comet's configured compression; the raw Celeborn client path bypasses
Celeborn's additional row compression and decompression. Use
`spark.comet.shuffle.rss.maxFrameBytes` and `spark.comet.shuffle.rss.maxInFlightBytes` to bound
encoded frame size and executor-side push admission. The defaults are 64 MiB and 512 MiB,
respectively. Admission includes Arrow encoding workspace as well as overlapping native, JNI,
and client frame copies. An uncompressed frame needs roughly seven times its size plus schema
and codec overhead. Compression reduces the transmitted bytes but still needs uncompressed
encoding workspace.

Comet splits large batches between rows. If a single row, its schema, or its encoding workspace
cannot fit the remote limits, Comet abandons the remote shuffle and materializes a replacement
using its local shuffle writer before downstream tasks can consume the exchange. The replacement
has a separate shuffle and scheduling identity, so late remote results cannot overwrite or skip
local map output, and remote stage failures cannot abort the replacement. Independent exchanges
can materialize concurrently; readers wait for their storage decisions before execution. Runtime
output statistics count only the selected destination. All reads and retries for the replacement
use local files and Spark's block transfer
service, including normal recovery after later fetch failures. Native operators and Comet's
Arrow shuffle format are preserved, and remote admission limits remain enforced. Once remote
output has been published, subsequent failures use the existing Spark/Celeborn recovery path;
Comet does not change that shuffle's destination. Local fallback uses executor disk. When `spark.dynamicAllocation.enabled=true`, native Celeborn shuffle requires
`spark.shuffle.service.enabled=true` or `spark.dynamicAllocation.shuffleTracking.enabled=true`
(the Spark default) so those files remain available. Applications using dynamic allocation with
both settings disabled retain ordinary Spark/Celeborn shuffle, even if remote reliable storage or
decommissioning enables dynamic allocation. Executor shutdown preserves fallback files for the
external shuffle service; explicit shuffle unregister retains the normal local cleanup behavior.
AQE reducer coalescing and mapper-range reads are supported, but Celeborn physical-skew chunk reads
are not.

### Shuffle Compression

`spark.comet.shuffle.compression.codec` controls the codec used to compress shuffle data written by
both Comet's native shuffle and the JVM columnar shuffle writer. Supported values are `lz4` (default),
`zstd`, and `snappy`. LZ4 favors CPU efficiency; ZSTD produces smaller shuffle files at higher CPU cost —
useful when shuffle I/O or network bandwidth is the bottleneck. When ZSTD is selected, the level is
controlled by `spark.comet.shuffle.compression.zstd.level` (default `1`).

`spark.shuffle.compress=false` disables compression for Comet's native shuffle only. It has no effect on
the JVM columnar shuffle writer, which always compresses spill files with the codec above. Disabling
compression on the native path may result in faster shuffle times in certain environments, such as
single-node setups with fast NVMe drives, at the expense of increased disk space usage.

## Reducing Row/Columnar Conversion Overhead

When a query stage contains many operators that fall back to Spark row-based execution, Comet may insert
repeated columnar-to-row and row-to-columnar conversions that dominate stage runtime. Set
`spark.comet.exec.transitionRevert.enabled=true` to have Comet revert the entire stage to Spark row execution
when the number of columnar-to-row transitions exceeds
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`). This trades native execution of a small
subset of operators for eliminating conversion overhead across the stage. A stage is not reverted when it holds a
native aggregate whose intermediate buffer Spark cannot exchange with Comet across a stage boundary, because
reverting it would split that aggregate between the two engines.

### Wide or Deeply Nested Schemas

The cost of each conversion also grows sharply with schema shape: for wide or deeply nested schemas,
columnar-to-row conversion is especially expensive because the conversion work scales with the number of
columns and nested fields. If profiling shows these conversions dominating a query over such a schema, set
`spark.comet.exec.transitionRevert.enabled=true` and lower
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`) to `1`. Note that this reverts every stage that
exceeds the threshold to Spark row-based execution — Comet removes the stage's native operators rather than running a
mix of native and fallback operators joined by repeated conversions — which can be cheaper than paying the
expensive conversions again and again.

## Metrics Overhead

The SQL metrics described in [Metrics](metrics.md) are always collected. Setting `spark.comet.metrics.enabled=true`
additionally publishes plan-coverage counters (`operators.native`, `operators.spark`, `queries.planned`,
`transitions`, and `acceleration.ratio`) through Spark's metrics system under the `comet` source. It is disabled by
default because it walks every executed plan on the driver after each query, and the counters are only useful with an
external sink (for example Prometheus) configured. This setting must be applied before the `SparkSession` is created.

## Explain Plan

For an explanation of Comet plan output, the configs that control it, and how
fallback to Spark works, see [Understanding Comet Plans](understanding-comet-plans.md).

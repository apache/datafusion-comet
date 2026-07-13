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

Comet uses a global tokio runtime per executor process using tokio's defaults of one worker thread per core and a
maximum of 512 blocking threads. These values can be overridden using the environment variables `COMET_WORKER_THREADS`
and `COMET_MAX_BLOCKING_THREADS`.

It is recommended that `COMET_WORKER_THREADS` be set to the number of executor cores. This may not be necessary
in some environments, such as Kubernetes, where the number of cores allocated to a pod will already be equal to the
number of executor cores.

## Memory Tuning

It is necessary to specify how much memory Comet can use in addition to memory already allocated to Spark. In some
cases, it may be possible to reduce the amount of memory allocated to Spark so that overall memory allocation is
the same or lower than the original configuration. In other cases, enabling Comet may require allocating more memory
than before. See the [Determining How Much Memory to Allocate] section for more details.

[Determining How Much Memory to Allocate]: #determining-how-much-memory-to-allocate

### Configuring Comet Memory

Comet shares an off-heap memory pool with Spark. The size of the pool is
specified by `spark.memory.offHeap.size`.

Comet's memory accounting isn't 100% accurate and this can result in Comet using more memory than it reserves,
leading to out-of-memory exceptions. To work around this issue, it is possible to
set `spark.comet.exec.memoryPool.fraction` to a value less than `1.0` to restrict the amount of memory that can be
reserved by Comet.

For more details about Spark off-heap memory mode, please refer to [Spark documentation].

[Spark documentation]: https://spark.apache.org/docs/latest/configuration.html

Comet implements multiple memory pool implementations. The type of pool can be specified with `spark.comet.exec.memoryPool`.

The valid pool types are:

- `fair_unified` (default when `spark.memory.offHeap.enabled=true` is set)
- `greedy_unified`

Both pool types are shared across all native execution contexts within the same Spark task. When
Comet executes a shuffle, it runs two native execution contexts concurrently (e.g. one for
pre-shuffle operators and one for the shuffle writer). The shared pool ensures that the combined
memory usage stays within the per-task limit.

The `fair_unified` pool prevents operators from using more than an even fraction of the available memory
(i.e. `pool_size / num_reservations`). This pool works best when you know beforehand
the query has multiple operators that will likely all need to spill. Sometimes it will cause spills even
when there is sufficient memory in order to leave enough memory for other operators.

The `greedy_unified` pool type implements a greedy first-come first-serve limit. This pool works well for queries that do not
need to spill or have a single spillable operator.

[shuffle]: #shuffle
[Advanced Memory Tuning]: #advanced-memory-tuning

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

`spark.comet.columnar.shuffle.batch.size` controls the batch size used when the JVM columnar shuffle writer
flushes sorted spill files. It must not exceed `spark.comet.batchSize`.

### Limiting Spill Disk Usage

Native operators that spill to disk (aggregate, sort, shuffle) are collectively bounded by
`spark.comet.maxTempDirectorySize` (default 100 GB per executor). If the limit is reached, further spills
fail and the query errors out. Raise this on workloads with large sort/aggregate/shuffle spills, or lower
it to protect executors on shared disks.

## Parquet Reader Tuning

### Parallel I/O

Comet's native Parquet reader can issue overlapping range reads within a single file, which is often the
dominant win when reading from object storage (S3, GCS, ADLS). It is enabled by default via
`spark.comet.parquet.read.parallel.io.enabled=true`, with the thread pool sized by
`spark.comet.parquet.read.parallel.io.thread-pool.size` (default `16` threads per executor). If your
executors have fewer cores or you are reading from local disk, lower this value; if you are reading many
small files from high-latency storage, raise it. When multiple ranges are close together, Comet coalesces
them (`spark.comet.parquet.read.io.mergeRanges`, delta `spark.comet.parquet.read.io.mergeRanges.delta`,
default 8 MB) to reduce request count on cloud storage.

### Filter Pushdown / Late Materialization

Setting `spark.comet.parquet.rowFilterPushdown.enabled=true` pushes filter evaluation into the Parquet
decode step and lazily materializes projected columns for surviving rows. This can significantly reduce
CPU and memory when the filter is highly selective on a small subset of columns. It is disabled by default
because it can hurt when the filter is not selective or when most columns must be read anyway. Row-group,
page-index, and bloom-filter pruning happen regardless of this flag whenever Spark's
`spark.sql.parquet.filterPushdown` is on.

## Iceberg Scan Tuning

When using the native Iceberg scan (`spark.comet.scan.icebergNative.enabled=true`), each task reads its
data files one at a time by default. For tables with many small files or high-latency storage, increase
`spark.comet.scan.icebergNative.dataFileConcurrencyLimit` (values of 2–8 are suggested) to overlap I/O
across files at the cost of extra memory.

## Optimizing Sorting on Floating-Point Values

Sorting on floating-point data types (or complex types containing floating-point values) is not compatible with
Spark if the data contains both zero and negative zero. This is likely an edge case that is not of concern for many users
and sorting on floating-point data can be enabled by setting `spark.comet.expression.SortOrder.allowIncompatible=true`.

## Optimizing Joins

Spark often chooses `SortMergeJoin` over `ShuffledHashJoin` for stability reasons. If the build-side of a
`ShuffledHashJoin` is very large then it could lead to OOM in Spark.

Vectorized query engines tend to perform better with `ShuffledHashJoin`, so for best performance it is often preferable
to configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`. Comet does not yet provide spill-to-disk for
`ShuffledHashJoin` so this could result in OOM. Also, `SortMergeJoin` may still be faster in some cases. It is best
to test with both for your specific workloads.

To configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`, set `spark.comet.exec.replaceSortMergeJoin=true`.

## Shuffle

Comet provides accelerated shuffle implementations that can be used to improve the performance of your queries.

To enable Comet shuffle, set the following configuration in your Spark configuration:

```
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.comet.exec.shuffle.enabled=true
```

`spark.shuffle.manager` is a Spark static configuration which cannot be changed at runtime.
It must be set before the Spark context is created. You can enable or disable Comet shuffle
at runtime by setting `spark.comet.exec.shuffle.enabled` to `true` or `false`.
Once it is disabled, Comet will fall back to the default Spark shuffle manager.

### Shuffle Implementations

Comet provides two shuffle implementations: Native Shuffle and Columnar Shuffle. Comet will first try to use Native
Shuffle and if that is not possible it will try to use Columnar Shuffle. If neither can be applied, it will fall
back to Spark for shuffle operations.

#### Native Shuffle

Comet provides a fully native shuffle implementation, which generally provides the best performance. Native shuffle
supports `HashPartitioning`, `RangePartitioning` and `SinglePartitioning` but currently only supports primitive type
partitioning keys. Columns that are not partitioning keys may contain complex types like maps, structs, and arrays.

#### Columnar (JVM) Shuffle

Comet Columnar shuffle is JVM-based and supports `HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`, and
`SinglePartitioning`. This shuffle implementation supports complex data types as partitioning keys.

By default, Comet will convert a Spark `ShuffleExchangeExec` to columnar shuffle even when the shuffle's child is a
non-Comet (Spark) plan. The benefit is that the next query stage can start as native Comet execution, since the
shuffle output is already in Arrow format. The cost is a row to columnar conversion at the shuffle boundary on the
write side. To restrict columnar shuffle to cases where the child is already a Comet plan, set
`spark.comet.exec.shuffle.convertFromSparkPlan.enabled=false`. Shuffles whose child is a Spark plan will then be left
as native Spark shuffles, which avoids the row to columnar conversion but means the downstream stage will also start
on Spark.

#### Automatic Revert to Spark Shuffle

When a Comet columnar shuffle ends up between two non-Comet operators (for example, a partial/final hash aggregate
pair that Comet could not convert), Comet reverts it to Spark's built-in shuffle. Keeping columnar shuffle between
two row-based operators would add `row -> Arrow -> shuffle -> Arrow -> row` conversions with no Comet consumer on
either side to benefit from columnar output.

This shifts the affected shuffles from Comet's off-heap memory pool back to the JVM execution memory pool. Clusters
tuned for a small JVM heap may see `ExternalSorter` spills on queries where this revert fires. Shuffle I/O may also
grow marginally because Spark's row-based serializer generally compresses less well than Comet's Arrow IPC format.

Each revert is logged at `INFO` level on the driver as `Reverting Comet columnar shuffle to Spark shuffle between
<parent> and <child>`, which lets you correlate any unexpected behavior with this optimization.

This optimization is enabled by default and can be disabled by setting
`spark.comet.exec.shuffle.revertRedundantColumnar.enabled=false`, in which case Comet will keep the columnar shuffle
even when both its parent and child are non-Comet operators.

### Shuffle Compression

By default, Comet's native shuffle compresses shuffle files with LZ4. Compression can be disabled by setting
`spark.shuffle.compress=false`, which may result in faster shuffle times in certain environments, such as
single-node setups with fast NVMe drives, at the expense of increased disk space usage.

The codec used by Comet's native shuffle is controlled by `spark.comet.exec.shuffle.compression.codec`. Supported
values are `lz4` (default), `zstd`, and `snappy`. LZ4 favors CPU efficiency; ZSTD produces smaller shuffle files
at higher CPU cost — useful when shuffle I/O or network bandwidth is the bottleneck. When ZSTD is selected, the
level is controlled by `spark.comet.exec.shuffle.compression.zstd.level` (default `1`).

### Reducing Row/Columnar Conversion Overhead

When a query stage contains many operators that fall back to Spark row-based execution, Comet may insert
repeated columnar-to-row and row-to-columnar conversions that dominate stage runtime. Set
`spark.comet.exec.transitionRevert.enabled=true` to have Comet revert the entire stage to Spark row execution
when the number of columnar-to-row transitions exceeds
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`). This trades native execution of a small
subset of operators for eliminating conversion overhead across the stage.

## Metrics Overhead

Comet exposes rich native operator metrics for observability (see [Metrics](metrics.md)), but they are
disabled by default because traversing the Spark plan on every task adds measurable overhead, and metrics
require an external sink (for example Prometheus) to be useful. Enable them with
`spark.comet.metrics.enabled=true` when you have a metrics sink configured. This setting must be applied
before the `SparkSession` is created.

## Explain Plan

For an explanation of Comet plan output, the configs that control it, and how
fallback to Spark works, see [Understanding Comet Plans](understanding-comet-plans.md).

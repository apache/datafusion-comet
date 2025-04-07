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

## Parquet Scans

Comet currently has three distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation. These scans are described in more detail in the
[Compatibility Guide].

[Compatibility Guide]: compatibility.md

When using `native_datafusion` or `native_iceberg_compat`, there are known performance issues when pushing filters
down to Parquet scans. Until this issue is resolved, performance can be improved by setting
`spark.comet.sql.parquet.filterPushdown=false`.

## Memory Tuning

It is necessary to specify how much memory Comet can use in addition to memory already allocated to Spark. In some
cases, it may be possible to reduce the amount of memory allocated to Spark so that overall memory allocation is
the same or lower than the original configuration. In other cases, enabling Comet may require allocating more memory
than before. See the [Determining How Much Memory to Allocate] section for more details.

[Determining How Much Memory to Allocate]: #determining-how-much-memory-to-allocate

Comet supports Spark's on-heap (the default) and off-heap mode for allocating memory. However, we strongly recommend
using off-heap mode. Comet has some limitations when running in on-heap mode, such as requiring more memory overall,
and requiring shuffle memory to be separately configured.

### Configuring Comet Memory in Off-Heap Mode

The recommended way to allocate memory for Comet is to set `spark.memory.offHeap.enabled=true`. This allows
Comet to share an off-heap memory pool with Spark, reducing the overall memory overhead. The size of the pool is
specified by `spark.memory.offHeap.size`. For more details about Spark off-heap memory mode, please refer to
Spark documentation: https://spark.apache.org/docs/latest/configuration.html.

### Configuring Comet Memory in On-Heap Mode

When running in on-heap mode, Comet memory can be allocated by setting `spark.comet.memoryOverhead`. If this setting
is not provided, it will be calculated by multiplying the current Spark executor memory by
`spark.comet.memory.overhead.factor` (default value is `0.2`) which may or may not result in enough memory for
Comet to operate. It is not recommended to rely on this behavior. It is better to specify `spark.comet.memoryOverhead`
explicitly.

Comet supports native shuffle and columnar shuffle (these terms are explained in the [shuffle] section below).
In on-heap mode, columnar shuffle memory must be separately allocated using `spark.comet.columnar.shuffle.memorySize`.
If this setting is not provided, it will be calculated by multiplying `spark.comet.memoryOverhead` by
`spark.comet.columnar.shuffle.memory.factor` (default value is `1.0`). If a shuffle exceeds this amount of memory
then the query will fail.

[shuffle]: #shuffle

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

- Comet requires at least 5 GB of RAM in off-heap mode and 6 GB RAM in on-heap mode, but performance at this level
  is around 340 seconds, which is significantly faster than Spark with any amount of RAM
- Comet running in off-heap with 8 cores completes the benchmark in 295 seconds, more than 2x faster than Spark
- It is worth noting that running Comet with only 4 cores and 4 GB RAM completes the benchmark in 520 seconds,
  providing better performance than Spark for half the resource

It may be possible to reduce Comet's memory overhead by reducing batch sizes or increasing number of partitions.

### SortExec

Comet's SortExec implementation spills to disk when under memory pressure, but there are some known issues in the
underlying DataFusion SortExec implementation that could cause out-of-memory errors during spilling. See
https://github.com/apache/datafusion/issues/14692 for more information.

Workarounds for this problem include:

- Allocating more off-heap memory
- Disabling native sort by setting `spark.comet.exec.sort.enabled=false`

## Advanced Memory Tuning

### Configuring spark.executor.memoryOverhead in On-Heap Mode

In some environments, such as Kubernetes and YARN, it is important to correctly set `spark.executor.memoryOverhead` so
that it is possible to allocate off-heap memory when running in on-heap mode.

Comet will automatically set `spark.executor.memoryOverhead` based on the `spark.comet.memory*` settings so that
resource managers respect Apache Spark memory configuration before starting the containers.

### Configuring Off-Heap Memory Pools

Comet implements multiple memory pool implementations. The type of pool can be specified with `spark.comet.exec.memoryPool`.

The valid pool types for off-heap mode are:

- `unified` (default when `spark.memory.offHeap.enabled=true` is set)
- `fair_unified`

Both of these pools share off-heap memory between Spark and Comet. This approach is referred to as
unified memory management. The size of the pool is specified by `spark.memory.offHeap.size`.

The `unified` pool type implements a greedy first-come first-serve limit. This pool works well for queries that do not
need to spill or have a single spillable operator.

The `fair_unified` pool type prevents operators from using more than an even fraction of the available memory
(i.e. `pool_size / num_reservations`). This pool works best when you know beforehand
the query has multiple operators that will likely all need to spill. Sometimes it will cause spills even
when there is sufficient memory in order to leave enough memory for other operators.

### Configuring On-Heap Memory Pools

When running in on-heap mode, Comet will use its own dedicated memory pools that are not shared with Spark.

The type of pool can be specified with `spark.comet.exec.memoryPool`. The default setting is `greedy_task_shared`.

The valid pool types for on-heap mode are:

- `greedy`
- `greedy_global`
- `greedy_task_shared`
- `fair_spill`
- `fair_spill_global`
- `fair_spill_task_shared`
- `unbounded`

Pool types ending with `_global` use a single global memory pool between all tasks on same executor.

Pool types ending with `_task_shared` share a single memory pool across all attempts for a single task.

Other pool types create a dedicated pool per native query plan using a fraction of the available pool size based on number of cores
and cores per task.

The `greedy*` pool types use DataFusion's [GreedyMemoryPool], which implements a greedy first-come first-serve limit. This
pool works well for queries that do not need to spill or have a single spillable operator.

The `fair_spill*` pool types use DataFusion's [FairSpillPool], which prevents spillable reservations from using more
than an even fraction of the available memory sans any unspillable reservations
(i.e. `(pool_size - unspillable_memory) / num_spillable_reservations`). This pool works best when you know beforehand
the query has multiple spillable operators that will likely all need to spill. Sometimes it will cause spills even
when there was sufficient memory (reserved for other operators) to avoid doing so. Unspillable memory is allocated in
a first-come, first-serve fashion

The `unbounded` pool type uses DataFusion's [UnboundedMemoryPool], which enforces no limit. This option is useful for
development/testing purposes, where there is no room to allow spilling and rather choose to fail the job.
Spilling significantly slows down the job and this option is one way to measure the best performance scenario without
adjusting how much memory to allocate.

[GreedyMemoryPool]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/struct.GreedyMemoryPool.html
[FairSpillPool]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/struct.FairSpillPool.html
[UnboundedMemoryPool]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/struct.UnboundedMemoryPool.html

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

Comet provides a fully native shuffle implementation, which generally provides the best performance. However,
native shuffle currently only supports `HashPartitioning` and `SinglePartitioning` and has some restrictions on
supported data types.

#### Columnar (JVM) Shuffle

Comet Columnar shuffle is JVM-based and supports `HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`, and
`SinglePartitioning`. This shuffle implementation supports more data types than native shuffle.

### Shuffle Compression

By default, Spark compresses shuffle files using LZ4 compression. Comet overrides this behavior with ZSTD compression.
Compression can be disabled by setting `spark.shuffle.compress=false`, which may result in faster shuffle times in
certain environments, such as single-node setups with fast NVMe drives, at the expense of increased disk space usage.

## Explain Plan

### Extended Explain

With Spark 4.0.0 and newer, Comet can provide extended explain plan information in the Spark UI. Currently this lists
reasons why Comet may not have been enabled for specific operations.
To enable this, in the Spark configuration, set the following:

```shell
-c spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo
```

This will add a section to the detailed plan displayed in the Spark SQL UI page.

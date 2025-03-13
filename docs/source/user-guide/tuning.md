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

Here is a real-world example, based on running benchmarks derived from TPC-H.

**TODO: this section is a work-in-progress**

The following table shows performance of Spark compared to Comet in both Off-Heap and On-Heap modes. The table shows
total query time for TPC-H @ 100GB. Smaller is better.

| Total Executor Memory (GB) | Spark | Comet Off-Heap | Comet On-Heap |
| -------------------------- | ----- | -------------- | ------------- |
| 1                          | OOM   | OOM            | OOM           |
| 2                          | OOM   | OOM            | OOM           |
| 3                          | 744   | OOM            | OOM           |
| 4                          | 739   | OOM            | OOM           |
| 5                          | 681   | 342            | OOM           |
| 6                          | 665   |                | 344           |
| 7                          | 657   |                | 340           |
| 8                          | 632   | 295            | 334           |
| 9                          | 623   |                |               |
| 10                         | 622   |                |               |

TODO: WIP conclusions:

- Spark can complete the benchmark with as little as 3GB but shows best performance at 9-10 GB
- When Comet is enabled, Spark needs at least 5 GB of memory but provides a ~2x improvement in performance for that level of memory allocation
- With Comet enabled, performance with 5 GB is 1.8x faster than Spark with 9-10 GB
- TODO run Comet with half the CPUs and show same performance? i.e. demonstrate same performance for half the cost
- TODO does reducing batch size reduce the amount of memory needed? maybe this goes under advanced memory tuning

## Advanced Memory Tuning

## Configuring spark.executor.memoryOverhead

In some environments, such as Kubernetes and YARN, it is important to correctly set `spark.executor.memoryOverhead` so
that it is possible to allocate off-heap memory.

Comet will automatically set `spark.executor.memoryOverhead` based on the `spark.comet.memory*` settings so that
resource managers respect Apache Spark memory configuration before starting the containers.

### Configuring Off-Heap Memory Pools

Comet implements multiple memory pool implementations. The type of pool can be specified with `spark.comet.exec.memoryPool`.

The valid pool types are:

- `unified` (default when `spark.memory.offHeap.enabled=true` is set)
- `fair_unified`

The `unified` pool type implements a greedy first-come first-serve limit. This pool works well for queries that do not
need to spill or have a single spillable operator.

The `fair_unified` pool type prevents operators from using more than an even fraction of the available memory
(i.e. `pool_size / num_reservations`). This pool works best when you know beforehand
the query has multiple operators that will likely all need to spill. Sometimes it will cause spills even
when there is sufficient memory in order to leave enough memory for other operators.

### Configuring On-Heap Memory Pools

When running in on-heap mode, Comet will use its own dedicated memory pools that are not shared with Spark.

The type of pool can be specified with `spark.comet.exec.memoryPool`. The default setting is `greedy_task_shared`.

The valid pool types are:

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

### Shuffle Mode

Comet provides three shuffle modes: Columnar Shuffle, Native Shuffle and Auto Mode.

#### Auto Mode

`spark.comet.exec.shuffle.mode` to `auto` will let Comet choose the best shuffle mode based on the query plan. This
is the default.

#### Columnar (JVM) Shuffle

Comet Columnar shuffle is JVM-based and supports `HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`, and
`SinglePartitioning`. This mode has the highest query coverage.

Columnar shuffle can be enabled by setting `spark.comet.exec.shuffle.mode` to `jvm`. If this mode is explicitly set,
then any shuffle operations that cannot be supported in this mode will fall back to Spark.

#### Native Shuffle

Comet also provides a fully native shuffle implementation, which generally provides the best performance. However,
native shuffle currently only supports `HashPartitioning` and `SinglePartitioning`.

To enable native shuffle, set `spark.comet.exec.shuffle.mode` to `native`. If this mode is explicitly set,
then any shuffle operations that cannot be supported in this mode will fall back to Spark.

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

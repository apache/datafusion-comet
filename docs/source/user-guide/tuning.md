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

# Tuning Guide

Comet provides some tuning options to help you get the best performance from your queries.

## Memory Tuning

### Unified Memory Management with Off-Heap Memory

The recommended way to share memory between Spark and Comet is to set `spark.memory.offHeap.enabled=true`. This allows
Comet to share an off-heap memory pool with Spark. The size of the pool is specified by `spark.memory.offHeap.size`. For more details about Spark off-heap memory mode, please refer to Spark documentation: https://spark.apache.org/docs/latest/configuration.html.

### Dedicated Comet Memory Pools

Spark uses on-heap memory mode by default, i.e., the `spark.memory.offHeap.enabled` setting is not enabled. If Spark is under on-heap memory mode, Comet will use its own dedicated memory pools that
are not shared with Spark. This requires additional configuration settings to be specified to set the size and type of
memory pool to use.

The size of the pool can be set explicitly with `spark.comet.memoryOverhead`. If this setting is not specified then
the memory overhead will be calculated by multiplying the executor memory by `spark.comet.memory.overhead.factor`
(defaults to `0.2`).

The type of pool can be specified with `spark.comet.exec.memoryPool`. The default setting is `greedy_task_shared`.

The valid pool types are:

- `greedy`
- `greedy_global`
- `greedy_task_shared`
- `fair_spill`
- `fair_spill_global`
- `fair_spill_task_shared`

Pool types ending with `_global` use a single global memory pool between all tasks on same executor.

Pool types ending with `_task_shared` share a single memory pool across all attempts for a single task.

Other pool types create a dedicated pool per native query plan using a fraction of the available pool size based on number of cores 
and cores per task.

The `greedy*` pool types use DataFusion's [GreedyMemoryPool], which implements a greedy first-come first-serve limit. This
pool works well for queries that do not need to spill or have a single spillable operator.

The `fair_spill*` pool types use DataFusion's [FairSpillPool], which prevents spillable reservations from using more
than an even fraction of the available memory sans any unspillable reservations
(i.e. `(pool_size - unspillable_memory) / num_spillable_reservations)`). This pool works best when you know beforehand
the query has multiple spillable operators that will likely all need to spill. Sometimes it will cause spills even
when there was sufficient memory (reserved for other operators) to avoid doing so. Unspillable memory is allocated in
a first-come, first-serve fashion

[GreedyMemoryPool]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/struct.GreedyMemoryPool.html
[FairSpillPool]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/struct.FairSpillPool.html


### Determining How Much Memory to Allocate

Generally, increasing memory overhead will improve query performance, especially for queries containing joins and
aggregates.

Once a memory pool is exhausted, the native plan will start spilling to disk, which will slow down the query.

Insufficient memory allocation can also lead to out-of-memory (OOM) errors.

## Configuring spark.executor.memoryOverhead

In some environments, such as Kubernetes and YARN, it is important to correctly set `spark.executor.memoryOverhead` so
that it is possible to allocate off-heap memory.

Comet will automatically set `spark.executor.memoryOverhead` based on the `spark.comet.memory*` settings so that
resource managers respect Apache Spark memory configuration before starting the containers.

Note that there is currently a known issue where this will be inaccurate when using Native Memory Management because it
does not take executor concurrency into account. The tracking issue for this is
https://github.com/apache/datafusion-comet/issues/949.

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

Comet provides two shuffle implementations.

#### Native Shuffle

Comet Native Shuffle reads columnar batches and repartitions them before writing the shuffled columnar data
to the output file. Native Shuffle supports `HashPartitioning` and `SinglePartition`. Only primitive types 
are supported for partitioning expressions (`Boolean`, `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, 
`Decimal`, `Date`, `Timestamp`, `String`, and `Binary`).

Native shuffle is enabled by default and can be disabled by setting `spark.comet.exec.shuffle.native.enabled=false`.

#### Columnar Shuffle

Comet Columnar Shuffle is used for cases where Native Shuffle is not supported. Columnar Shuffle supports
`HashPartitioning`, `RangePartitioning`, `RoundRobinPartitioning` and `SinglePartition` and supports complex
types for hash-partitioning expressions in addition to the primitive types supported by Native Shuffle.

Columnar Shuffle inserts a `ColumnarToRowExec` transition on the input data (this does not appear in the query
plan) and delegates the partitioning to Spark. The partitioned output rows are then converted back into columnar
format before being written to the shuffle output file.

Columnar shuffle is enabled by default and can be disabled by setting `spark.comet.exec.shuffle.columnar.enabled=false`.

### Shuffle Compression

By default, Comet compresses shuffle files using `lz4` compression. Comet also supports `zstd` and `snappy` 
compression. Different compression algorithms have different trade-offs between CPU overhead and compression ratio.
The compression algorithm can be specified by setting `spark.comet.exec.shuffle.compression.codec` to `lz4`, `zstd`, 
or `snappy`. 

When using `zstd`, the compression level can be specified with `spark.comet.exec.shuffle.compression.zstd.level`.

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

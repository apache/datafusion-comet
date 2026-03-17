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

The `fair_unified` pool types prevents operators from using more than an even fraction of the available memory
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

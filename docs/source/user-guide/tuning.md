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

Comet shares an off-heap memory pool between Spark and Comet. This requires setting `spark.memory.offHeap.enabled=true`. 
If this setting is not enabled, Comet will not accelerate queries and will fall back to Spark.

Each executor will have a single memory pool which will be shared by all native plans being executed within that
process, and by Spark itself. The size of the pool is specified by `spark.memory.offHeap.size`.

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

## Explain Plan
### Extended Explain
With Spark 4.0.0 and newer, Comet can provide extended explain plan information in the Spark UI. Currently this lists
reasons why Comet may not have been enabled for specific operations.
To enable this, in the Spark configuration, set the following:
```shell
-c spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo
```
This will add a section to the detailed plan displayed in the Spark SQL UI page.
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

Comet provides two options for memory management:

- **Unified Memory Management** shares an off-heap memory pool between Spark and Comet. This is the recommended option.
- **Native Memory Management** leverages DataFusion's memory management for the native plans and allocates memory independently of Spark.

### Unified Memory Management

This option is automatically enabled when `spark.memory.offHeap.enabled=true`.

Each executor will have a single memory pool which will be shared by all native plans being executed within that
process, and by Spark itself. The size of the pool is specified by `spark.memory.offHeap.size`.

### Native Memory Management

This option is automatically enabled when `spark.memory.offHeap.enabled=false`.

Each native plan has a dedicated memory pool.

By default, the size of each pool is `spark.comet.memory.overhead.factor * spark.executor.memory`. The default value
for `spark.comet.memory.overhead.factor` is `0.2`.

It is important to take executor concurrency into account. The maximum number of concurrent plans in an executor can
be calculated with `spark.executor.cores / spark.task.cpus`.

For example, if the executor can execute 4 plans concurrently, then the total amount of memory allocated will be
`4 * spark.comet.memory.overhead.factor * spark.executor.memory`.

It is also possible to set `spark.comet.memoryOverhead` to the desired size for each pool, rather than calculating
it based on `spark.comet.memory.overhead.factor`.

If both `spark.comet.memoryOverhead` and `spark.comet.memory.overhead.factor` are set, the former will be used.

Comet will allocate at least `spark.comet.memory.overhead.min` memory per pool.

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

## Metrics

Comet metrics are not directly comparable to Spark metrics in some cases.

`CometScanExec` uses nanoseconds for total scan time. Spark also measures scan time in nanoseconds but converts to
milliseconds _per batch_ which can result in a large loss of precision. In one case we saw total scan time
of 41 seconds reported as 23 seconds for example.

<!--
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

# Comet Plugin Architecture

## Overview

The Comet plugin enhances Spark SQL by introducing optimized query execution and shuffle mechanisms leveraging
native code. It integrates with Spark's plugin framework and extension API to replace or extend Spark's
default behavior.

---

# Plugin Components

## Comet SQL Plugin

The entry point to Comet is the `org.apache.spark.CometPlugin` class, which is registered in Spark using the following
configuration:

```
--conf spark.plugins=org.apache.spark.CometPlugin
```

The plugin has a driver component, `CometDriverPlugin`, and an executor component, `CometExecutorPlugin`.

`CometDriverPlugin` runs once, when the `SparkContext` starts and before any `SparkSession` exists, so it can set static
configuration that cannot be changed once a session has been created. It first sets `spark.comet.version` to the Comet
version. If neither `spark.memory.offHeap.enabled` nor `spark.comet.exec.onHeap.enabled` is `true`, it logs a warning
and skips the remaining steps. Otherwise it:

- Appends `CometSparkSessionExtensions` to `spark.sql.extensions`, unless it is already listed.
- Sets `spark.sql.cache.serializer` to Comet's `ArrowCachedBatchSerializer` when
  `spark.comet.exec.inMemoryCache.enabled=true`, unless the application has chosen a different serializer.
- Registers `CometSource` with Spark's metrics system and adds `CometMetricsListener` to
  `spark.sql.queryExecutionListeners` when `spark.comet.metrics.enabled=true`.
- Logs a warning for settings that are likely to cause problems, such as an unset `spark.executor.memoryOverhead`.

The plugin does not change any executor memory setting. The [Tuning Guide](../user-guide/latest/tuning.md) covers how
to size them.

When the driver or an executor stops, the plugin shuts down Comet's native tokio runtime in that JVM.

`CometSparkSessionExtensions` can also be registered without the plugin, through `spark.sql.extensions` or
`SparkSession.Builder.withExtensions`. Most of Comet's test suites and the Spark SQL tests enable Comet this way, so
none of the driver plugin's steps run for them.

## CometSparkSessionExtensions

On initialization, this class registers one physical plan optimization rule with Spark: `CometRule`. It runs whenever
a query stage is being planned during Adaptive Query Execution, and runs once for the entire plan when Adaptive Query
Execution is disabled.

`CometRule` is two phases, applied in order: scan conversion (`CometScanRule`), then operator conversion
(`CometExecRule`). The order matters, because operator conversion builds its native plan up from the nodes that scan
conversion produces. Each phase is described below.

### Phase 1: CometScanRule

`CometScanRule` replaces any Parquet scans with Comet operators. There are different paths for Spark v1 and v2 data sources.

When reading from Parquet v1 data sources, Comet replaces `FileSourceScanExec` with a `CometScanExec`, and for v2
data sources, `BatchScanExec` is replaced with `CometBatchScanExec`. In both cases, Comet replaces Spark's Parquet
reader with a custom vectorized Parquet reader. This is similar to Spark's vectorized Parquet reader used by the v2
Parquet data source but leverages native code for decoding Parquet row groups directly into Arrow format.

Comet only supports a subset of data types and will fall back to Spark's scan if unsupported types
exist. Comet can still accelerate the rest of the query execution in this case because `CometSparkToColumnarExec` will
convert the output from Spark's scan to Arrow arrays. Note that both `spark.comet.exec.enabled=true` and
`spark.comet.convert.parquet.enabled=true` must be set to enable this conversion.

Refer to the [Supported Spark Data Types](https://datafusion.apache.org/comet/user-guide/datatypes.html) section
in the contributor guide to see a list of currently supported data types.

### Phase 2: CometExecRule

This rule traverses bottom-up from the original Spark plan and attempts to replace each operator with a Comet equivalent.
For example, a `ProjectExec` will be replaced by `CometProjectExec`.

When replacing a node, various checks are performed to determine if Comet can support the operator and its expressions.
If an operator, expression, or data type is not supported by Comet then the reason will be stored in a tag on the
underlying Spark node and the plan will not be converted.

Comet does not support partially replacing subsets of the plan within a query stage because this would involve adding
transitions to convert between row-based and columnar data between Spark operators and Comet operators and the overhead
of this could outweigh the benefits of running parts of the query stage natively in Comet.

## Query Execution

Once the plan has been transformed, any consecutive native Comet operators are combined into a `CometNativeExec` which contains
a protocol buffer serialized version of the plan (the serialization code can be found in `QueryPlanSerde`).

Spark serializes the physical plan and sends it to the executors when executing tasks. The executors deserialize the
plan and invoke it.

When `CometNativeExec` is invoked, it will pass the serialized protobuf plan into
`Native.createPlan`, which invokes the native code via JNI, where the plan is then deserialized.

In the native code there is a `PhysicalPlanner` struct (in `planner.rs`) which converts the deserialized plan into an
Apache DataFusion `ExecutionPlan`. In some cases, Comet provides specialized physical operators and expressions to
override the DataFusion versions to ensure compatibility with Apache Spark.

The leaf nodes in the physical plan are always `ScanExec`. Each JVM-sourced input is exported once as an
[Arrow C Stream](https://arrow.apache.org/docs/format/CStreamInterface.html) (`org.apache.arrow.c.ArrowArrayStream`),
and `ScanExec` pulls each input batch through a single C callback rather than making a JNI call per batch. The
input could be a Comet native Parquet scan, a Spark exchange, or another native plan.

`CometNativeExec` creates a `CometExecIterator` and applies this iterator to the input RDD
partitions. Each call to `CometExecIterator.next()` will invoke `Native.executePlan`. Once the plan finishes
executing, the resulting Arrow batches are imported into the JVM using Arrow FFI.

## Shuffle

Comet integrates with Spark's shuffle mechanism, optimizing both shuffle writes and reads. Comet's shuffle manager
must be registered with Spark using the following configuration:

```
--conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
```

### Shuffle Writes

For shuffle writes, a `ShuffleMapTask` runs in the executors. This task contains a `ShuffleDependency` that is
broadcast to all of the executors. It then passes the input RDD to `ShuffleWriteProcessor.write()` which
requests a `ShuffleWriter` from the shuffle manager, and this is where it gets a Comet shuffle writer.

Comet has two shuffle implementations, native shuffle and JVM columnar shuffle.
[When Native Shuffle is Used](native_shuffle.md#when-native-shuffle-is-used) describes how Comet chooses between them.

For native shuffle, `CometNativeShuffleWriter` runs one native plan per task, with a `ShuffleWriter` operator at the
root. When the exchange's child is a native Comet subtree, that subtree becomes the writer's child, so the operators
that produce the shuffle input and the writer run in the same native plan, and no batch crosses into the JVM between
them. Otherwise, for example when the exchange's child is `CometSparkToColumnarExec`, the writer's child is a scan that
reads batches from the JVM. The writer partitions the batches and writes them in Arrow IPC format. See
[Native Shuffle](native_shuffle.md) for details.

JVM columnar shuffle takes rows instead, converting a Comet child's output with `ColumnarToRowExec`. It assigns
partitions with Spark's partitioner, buffers the rows in memory pages, and calls native code to encode them to Arrow
IPC. See [JVM Shuffle](jvm_shuffle.md) for details.

### Shuffle Reads

For shuffle reads, `CometShuffledBatchRDD` requests a `ShuffleReader` from the shuffle manager and gets a
`CometBlockStoreShuffleReader`, which fetches blocks with Spark's `ShuffleBlockFetcherIterator`. Both shuffle
implementations write the same Arrow IPC block format, so the same reader serves both. When a native plan consumes the
shuffle output and `spark.comet.shuffle.directRead.enabled` is `true`, the default, the compressed blocks are passed
to that plan, which decodes them itself. Otherwise `NativeBatchDecoderIterator` decodes each block in native
code through JNI, and Arrow FFI imports the result into the JVM as a `ColumnarBatch`. See
[Read Path](native_shuffle.md#read-path) for details.

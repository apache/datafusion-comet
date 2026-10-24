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
  It leaves Spark's serializer in place where Comet could not scan its format in the Comet pipeline or Kryo would
  reject it; see [In-Memory Cache](../user-guide/latest/in-memory-cache.md).
- Registers `CometSource` with Spark's metrics system and adds `CometMetricsListener` to
  `spark.sql.queryExecutionListeners` when `spark.comet.metrics.enabled=true`.
- Logs a warning for settings that are likely to cause problems, such as an unset `spark.executor.memoryOverhead`.

The plugin does not change any executor memory setting. The [Memory Tuning](../user-guide/latest/tuning/memory.md) guide covers how
to size them.

When the driver or an executor stops, the plugin shuts down Comet's tokio runtime in that JVM.

When `spark.eventLog.enabled` is `true`, the executor plugin also carries each sample of the executor's native memory
usage log to the driver plugin. For each executor, the driver plugin keeps the sample with the most untracked memory
and the last sample of every minute, and posts them to Spark's listener bus as `CometExecutorMemoryUsage` events so
that the event log records them. A listener on a listener bus queue of its own posts them at the executor's first
heartbeat a minute or more after the first of them arrived, which also ends an idle executor's minute, and posts what
the driver plugin holds when an executor is removed and when the application ends, since an executor that goes away
sends nothing more. See
[Reading the Memory Usage Log from the Event Log](../user-guide/latest/tuning/memory.md#reading-the-memory-usage-log-from-the-event-log).

`CometSparkSessionExtensions` can also be registered without the plugin, through `spark.sql.extensions` or
`SparkSession.Builder.withExtensions`. Most of Comet's test suites and the Spark SQL tests enable Comet this way, so
none of the driver plugin's steps run for them.

## CometSparkSessionExtensions

On initialization, this class registers Comet's rules with Spark. The main one is `CometRule`, which converts the
plan. When Adaptive Query Execution is disabled, it runs once for the entire plan, before Spark inserts columnar
transitions. When Adaptive Query Execution is enabled, it runs as a query stage preparation rule.
The class also registers rules that run after the transitions are inserted, such as
`RevertNativeForTransitionHeavyStages` and `EliminateRedundantTransitions`, Adaptive Query Execution stage optimizer
rules on Spark 3.5 and later, and the `IcebergWriteStrategy` planner strategy. The comment on
`CometSparkSessionExtensions` describes where each rule runs.

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
underlying Spark node and that operator will not be converted.

Comet can replace part of a query stage. After `CometRule` runs, Spark inserts a `ColumnarToRowExec` where a
row-based operator reads columnar output, and Comet's post-transition rules then run in order. First, when
`spark.comet.exec.transitionRevert.enabled=true`, `RevertNativeForTransitionHeavyStages` reverts a query stage to Spark
if it has more columnar-to-row transitions than `spark.comet.exec.transitionRevert.maxTransitions`, because the overhead
of the transitions can outweigh the benefits of running part of the stage in Comet. It keeps the stage in Comet when
reverting it is not safe, for example when Spark and Comet would have to exchange aggregate state. Then
`EliminateRedundantTransitions` removes transitions that are not needed and replaces the rest that read Comet output
with `CometColumnarToRowExec`. It uses the Rust-implemented `CometNativeColumnarToRowExec` instead when
`spark.comet.exec.columnarToRow.native.enabled=true` (off by default) and that converter supports the schema. A schema
that contains `VariantType` keeps Spark's `ColumnarToRowExec`.

## Query Execution

Once the plan has been transformed, consecutive `CometNativeExec` operators are combined into a single block, which contains
a protocol buffer serialized version of the plan (the serialization code can be found in `QueryPlanSerde`). The write
operators are the exception: the child of `CometWriteFilesExec`, `CometNativeWriteExec`, or `CometIcebergWriteExec`
starts a block of its own.

Spark serializes the physical plan and sends it to the executors when executing tasks. The executors deserialize the
plan and invoke it.

When `CometNativeExec` is invoked, it will pass the serialized protobuf plan into
`Native.createPlan`, which invokes the native code via JNI, where the plan is then deserialized.

In the native code there is a `PhysicalPlanner` struct (in `planner.rs`) which converts the deserialized plan into an
Apache DataFusion `ExecutionPlan`. In some cases, Comet provides specialized physical operators and expressions to
override the DataFusion versions to ensure compatibility with Apache Spark.

Not every expression in a Rust plan runs in Rust. When an expression has no Rust implementation, or its Rust
implementation does not match Spark for the given input, its serde can route it through the JVM codegen dispatcher
instead of falling back to Spark (see the
[Implementation legend](../user-guide/latest/expressions.md#implementation-legend)). The Rust plan then evaluates the
expression with `JvmScalarUdfExpr`, which passes each batch's input columns through JNI and the Arrow C Data Interface
to `CometUdfBridge`, which hands them to `CometScalaUDFCodegen` on the JVM. There, Spark's own generated code for the expression, compiled into a batch
kernel by `CometBatchKernelCodegen`, runs over the Arrow vectors and returns an Arrow column. Scala and Java UDFs take
the same path. The operator around the expression stays Rust-implemented, so this JVM code runs inside the Comet
pipeline. `spark.comet.exec.scalaUDF.codegen.enabled`, which is on by default, turns the dispatcher on. When it is off,
these expressions fall back to Spark.

A leaf node in the physical plan that takes Arrow batches from the JVM is a `ScanExec`. Each such input is exported once as an
[Arrow C Stream](https://arrow.apache.org/docs/format/CStreamInterface.html) (`org.apache.arrow.c.ArrowArrayStream`),
and `ScanExec` pulls each input batch through a single C callback rather than making a JNI call per batch. The
input could be a broadcast, a shuffle read decoded through JNI and imported into the JVM, the output of
`CometSparkToColumnarExec`, or the output of an earlier Comet block. When direct shuffle read is enabled and the input
is a Comet shuffle whose output types Comet supports, the input is instead a `ShuffleScanExec`, which pulls compressed
Arrow IPC blocks from the JVM and decodes them in Rust (see [Shuffle Reads](#shuffle-reads)). Operators that run their
own Rust plans, such as `CometTakeOrderedAndProjectExec`, read their shuffle input as batches through `ScanExec`. The
Parquet, Iceberg, and CSV scans read their files in Rust.

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
root. When the exchange's child is a `CometNativeExec` subtree, that subtree becomes the writer's child, so the operators
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

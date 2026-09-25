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

# Iceberg Writes

This document describes how Comet writes Iceberg tables. It covers the split-operator plan that
replaces Spark's single write operator, how a write is admitted to the native writer, what crosses
the JVM/native boundary in each direction, who owns cleanup when a task fails, and how to test a
change to any of it.

For the user-facing view (configuration, the eligibility table, and the accepted differences from
iceberg-java's output), see [Iceberg Writes](../user-guide/latest/iceberg-writes.md). This page
does not repeat those lists; it explains the code that implements them.

## Overview

Two flags, each of which builds on the one before it:

| Flag                                              | What it changes                                                                                                                         |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.write.iceberg.splitOperator.enabled` | The plan shape. Spark's single V2 write operator becomes `IcebergCommit` over `IcebergWrite`. iceberg-java still writes the data files. |
| `spark.comet.iceberg.write.enabled`               | Who writes the data files. An eligible `IcebergWrite` becomes `CometIcebergWrite`, which writes Parquet with iceberg-rust.              |

The native flag does nothing without the split flag, because it converts a node only the split plan
creates. Both default to `false`. The roadmap for making them the default, and the criteria for it,
are tracked in [#5644](https://github.com/apache/datafusion-comet/issues/5644) under the epic
[#5649](https://github.com/apache/datafusion-comet/issues/5649).

One rule runs through the whole native path: **the native writer must produce the outcome
iceberg-java would have produced, or decline.** iceberg-java is the reference for every data file,
manifest entry, partition value and failure behaviour. Where Comet cannot show that it reproduces
iceberg-java for some table or setting, the write falls back to iceberg-java at plan time. The
consequences of getting this wrong are asymmetric. A decline costs performance on one write. A
native write that differs from iceberg-java writes a table that every later reader, Comet or not,
has to live with.

A second rule follows from the first: **decide at plan time, never mid-write.** Once the plan
contains a `CometIcebergWrite`, there is no runtime switch back to the JVM writer. Anything the
native side could reject at execution time (a storage scheme, a type, a partition spec, a
reflective accessor the executor needs) has to be rejected by the planning gate first, or the user
gets a failed query instead of a fallback.

## The Split-Operator Plan

Spark plans an Iceberg write as one physical operator (`AppendDataExec`, `ReplaceDataExec` and so
on) that runs the input query, writes the files, and commits, all outside AQE. The split plan
replaces it with two operators so that the write's input becomes an ordinary query stage.

```text
IcebergCommit                 driver: collect task commit messages, BatchWrite.commit
+- IcebergWrite               executors: write data files, emit one commit message per task
   +- <input query>           scans, projects, exchanges, sorts; now visible to AQE and Comet
```

| Component                                                                       | Location                                           | Role                                                                                                                                                                                                                                    |
| ------------------------------------------------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IcebergWriteStrategy`                                                          | `spark/src/main/scala/org/apache/comet/iceberg/`   | Planner strategy. Matches `AppendData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`, `ReplaceData` (and Iceberg's own `ReplaceIcebergData`, which Iceberg 1.5.2 plans on Spark 3.4) whose `Write` is an Iceberg `SparkWrite`. |
| `IcebergWriteLogical`                                                           | same                                               | Logical anchor for the writer, so AQE re-plans re-emit only the writer and not a second committer.                                                                                                                                      |
| `IcebergWriteExec`                                                              | `spark/src/main/scala/org/apache/spark/sql/comet/` | JVM writer. Runs iceberg-java's `DataWriter` per task and returns the serialized `WriterCommitMessage` as one binary row.                                                                                                               |
| `IcebergCommitExec`                                                             | same                                               | Driver committer. A `V2CommandExec`, so `run()` is memoized and the commit happens once.                                                                                                                                                |
| `IcebergReplaceDataShim`, `IcebergRefreshCacheShim`, `IcebergDriverMetricsShim` | `spark/src/main/spark-*/org/apache/comet/iceberg/` | Version differences: Spark 4.x operation-coded `ReplaceData` rows, cache refresh by name on 4.1+, driver metric reporting.                                                                                                              |

Things to know before changing this layer:

- **One `BatchWrite` is shared.** `buildTwoOp` calls `write.toBatch` once and hands the same
  instance to both operators. Iceberg's commit-time validation must see the instance the writer
  wrote through, and `toBatch()` returns a new one per call.
- **The commit message column is the contract between the two operators.** Both `IcebergWriteExec`
  and `CometIcebergWriteExec` emit a single non-null `BINARY` column named
  `iceberg_commit_message`, holding a Java-serialized `WriterCommitMessage`. `IcebergCommitExec`
  does not know which writer produced it.
- **Messages are collected per task as tasks finish** (`sparkContext.runJob` with a result
  handler), not with `executeCollect`. That is how a failed job still knows which tasks completed,
  so it can delete their files.
- **Commit-coordinator writes are not intercepted.** Iceberg's `SparkWrite` never asks for one; the
  check in `buildTwoOp` is defensive.
- **What is not intercepted:** merge-on-read (`WriteDelta`), streaming writes, and CTAS/RTAS on
  Spark 3.4. Those keep Spark's plan.

## From `IcebergWrite` to `CometIcebergWrite`

`CometExecRule` converts an `IcebergWriteExec` with the `CometIcebergNativeWrite` operator serde
when `spark.comet.iceberg.write.enabled` is on. Two arms in `CometExecRule` handle it: one unwraps
the double conversion AQE can produce when it re-fires write planning over a sub-tree that already
contains a `CometIcebergWriteExec`, and the other calls `convertToComet`.

`CometIcebergNativeWrite.requiresNativeChildren` is `true`. The native writer consumes Arrow
batches from its child over FFI, so the conversion is declined unless the child is already a Comet
native operator. This is why a write fed by a `LocalTableScanExec` (`INSERT ... VALUES`, a local
DataFrame) stays on the JVM writer unless `spark.comet.exec.localTableScan.enabled` is also set.

`CometIcebergWriteExec` is row-based (it emits the commit message row) over a columnar child, so
Spark inserts a columnar-to-row transition beneath it. `EliminateRedundantTransitions` removes that
transition in a dedicated pass that runs before its main `transformUp`, and the scaladoc on
`stripIcebergWriteInputTransition` explains why the order matters. Do not try to suppress the
transition by making the write a `ColumnarToRowTransition`: Spark then skips the whole subtree
below it and never inserts the transitions the rest of the plan needs
([#5689](https://github.com/apache/datafusion-comet/issues/5689), visible only with AQE off).

`CometIcebergWriteExec` carries its own serialized plan (`serializedPlanOpt` serializes `nativeOp`
on demand), so `CometExecRule` resets `firstNativeOp` at the write and lets its child start a
separate native block.

## The Eligibility Gate

`CometIcebergNativeWrite.getSupportLevel` evaluates an ordered list of `TriggerRule`s against a
`TriggerContext` and reports the first reason it finds as `Unsupported`. A rule is a function from
the context to `Option[String]`: `None` means the rule has no objection.

The gate has these properties, and a new rule has to keep them:

- **It is an allow list.** It accepts only configurations shown to produce iceberg-java's output.
  Unknown `write.parquet.*` keys, any `parquet.*` table property, and any `parquet.*` key in the
  session Hadoop configuration are declined, including keys a future Iceberg version adds. When you
  support a new setting, add it to the vetted set and translate it; do not widen a prefix.
- **It reads the effective configuration.** `TriggerContext.properties` is the table's properties
  overlaid with `SparkWrite.writeProperties`, which is where iceberg-java resolves per-write options
  and `spark.sql.iceberg.*` session overrides. Reading table properties alone misses those.
- **It checks instantiated state as well as properties.** A catalog or a custom `TableOperations`
  can install a `FileIO` or an `EncryptionManager` without any property changing, so
  `requireRecognizedTableFileIO` and `requirePlaintextEncryptionManager` look at `table.io()` and
  `table.encryption()` themselves.
- **It fails closed.** A reflection lookup that cannot answer returns a reason, not `None`, and
  `getSupportLevel` turns any non-fatal exception into `Unsupported`. `convert` does the same for
  failures while building the proto.
- **It covers what the executor will need.** `requireExecutorReflectionResolvable` resolves every
  iceberg-java class, method and constructor that the executor-side commit-message assembly calls
  reflectively. An Iceberg release that moves one of them becomes a plan-time fallback rather than
  a task failure after the data is written.
- **It agrees with the native side.** When the gate and the native code both interpret the same
  input (a location's scheme, a partition spec, a column type), they must interpret it the same
  way. A gate that is more permissive than the native code turns a fallback into a failed query
  ([#6140](https://github.com/apache/datafusion-comet/issues/6140) is an example: the gate and
  `scheme_of` split a location's scheme differently). Prefer sharing one implementation, or pin the
  pair with a test that feeds both the same inputs.

Every rule is pinned by `CometIcebergWriteDetectionSuite`. Add a case there for any new rule, for
both the accepted and the declined side. Update the eligibility table in the user guide in the same
change. Whether each existing restriction is permanent is tracked in
[#5643](https://github.com/apache/datafusion-comet/issues/5643).

## What Crosses the Boundary

### JVM to native: the `IcebergWrite` proto

`buildIcebergWriteProto` builds an `IcebergWrite` message (`native/proto/src/proto/operator.proto`)
on the driver. Almost all of it is the per-write `IcebergWriteCommon`:

- the write schema and partition spec as Iceberg JSON (`SchemaParser` / `PartitionSpecParser`),
  taken from the `SparkWrite` rather than the table, so a concurrent schema change cannot alter
  what this write produces
- the data location, the operation id (the `SparkWrite` query id, used in file names), the target
  file size, and the writer mode (unpartitioned, fanout or clustered, resolved the way `SparkWrite`
  chooses its own writer)
- `IcebergParquetWriteSettings`, translated from the effective properties by
  `IcebergWriteProtoTranslation`
- the sort order id, which the native side ignores and the JVM stamps onto the files afterwards
- `catalog_properties` for the native `FileIO`: the table's `FileIO.properties()` merged over the
  `fs.s3a.*` settings translated from the Hadoop configuration, the same translation the native
  scan uses

The per-task `partition_id` and `task_attempt_id` are stamped onto a copy of the proto inside the
task closure in `CometIcebergWriteExec.doExecuteColumnar`. The native side refuses to run without
them, since defaulting them would make every task write the same file names.

`catalog_properties` can carry credentials. `CometIcebergWriteExec.stringArgs` deliberately prints
only the data location and writer mode, because the default `argString` would put the protobuf's
text dump, secrets included, into `explain()`, the SQL UI and the event log. Keep it that way when
adding fields.

On Spark 4.x, copy-on-write `DELETE`/`UPDATE`/`MERGE` rows arrive with an operation code and file
metadata columns around the data columns. `dropNonDataColumns` inserts a native `Projection` that
keeps only the write schema's columns. This is only equivalent to the JVM writer while format
version 3 is declined, because on v3 iceberg-java reads row-lineage fields from those metadata
columns.

### Native execution

The planner builds `IcebergWriteExec` (`native/core/src/execution/operators/iceberg_write.rs`) over
the FFI scan of the child's batches. `run_write_task` builds iceberg-rust's writer stack once per
task:

```text
ParquetWriterBuilder -> RollingFileWriterBuilder -> DataFileWriterBuilder
  -> UnpartitionedWriter | FanoutWriter | ClusteredWriter
```

Points where Comet adapts iceberg-rust to match iceberg-java:

- **Location generation.** `CometLocationGenerator` (`iceberg_partition_path.rs`) replaces
  iceberg-rust's `DefaultLocationGenerator` and renders partition directories the way iceberg-java's
  `PartitionSpec.partitionToPath` does. iceberg-rust's own rendering differs for several types, and
  panics for pre-1970 `timestamptz`. Float and double values use Comet's Java `Double.toString`
  renderer (`java_float_string`), shared with `cast(float as string)`. The partition type is
  resolved once when the generator is built, because `LocationGenerator::generate_location` cannot
  return an error.
- **File names.** `file_name_prefix` embeds the partition id, the task attempt id and the operation
  id, so a retried or speculative attempt never reuses another attempt's file names.
- **Row pacing.** iceberg-java's rolling writer checks the target file size every 1000 rows of the
  current file. iceberg-rust checks once per `write` call. `RowPacer` hands the writer rows in
  `ROWS_DIVISOR` (1000) row units per file, so both writers roll on the same grid.
- **Field ids and casting.** `decorate_batch_with_field_ids` casts each batch to the
  field-id-annotated Arrow schema derived from the Iceberg schema, with `safe: false`, so a type
  mismatch fails the task instead of writing NULLs.
- **Deterministic output order.** iceberg-rust's `FanoutWriter` closes its writers out of a
  `HashMap`, so the fanout path sorts its `DataFile`s by path before returning them
  ([#5776](https://github.com/apache/datafusion-comet/issues/5776)). Manifest order becomes the row
  order of an unordered read, so any map iteration that reaches the output needs the same care.

`FileIO` comes from `load_file_io` in `iceberg_common.rs`, shared with the native scan. It picks
the storage backend from the data location's scheme and wires in Comet's S3 credential bridge when
one is configured. For writes the bridge fails closed: if a configured provider cannot initialize,
the task fails rather than writing with the default credential chain.

### Native to JVM: the task payload

Each task emits exactly one batch with one row and two `BINARY` columns (`build_output_schema`):

1. The task's `DataFile`s encoded as an Iceberg v2 data manifest, written by iceberg-rust's
   `ManifestWriter` into an in-process `memory:` `FileIO`. Empty when the task wrote no files.
2. Every location the task's writers were handed (`encode_locations`: a big-endian count, then a
   length-prefixed UTF-8 string per location).

`CometIcebergWriteExec.doExecute` then, per task:

1. takes cleanup ownership of the locations from column 2 (see below)
2. decodes the manifest with Iceberg's own `ManifestFiles.read` (`decodeManifestToDataFiles`)
3. rebuilds each `DataFile`'s metrics with iceberg-java's `ParquetUtil.footerMetrics` and
   `MetricsConfig.forTable`, reading each written file's footer (`rebuildDataFilesWithJavaMetrics`).
   Only float/double NaN counts and bounds are carried over from the native writer, because the
   footer does not have them.
4. stamps the write's sort order id, which iceberg-rust does not set
5. builds a genuine `SparkWrite$TaskCommit` reflectively, reports Spark output metrics the way
   iceberg-java's `TaskCommit` does, and serializes it as the commit message

Rebuilding metrics on the JVM is the main reason manifest parity holds: metrics modes, truncation,
the inferred-column cap and list/map bounds suppression are decided by iceberg-java's code. When
touching the native writer, do not try to make iceberg-rust's own metrics authoritative; the JVM
discards them. The price is one footer read per written file.

## Failure Handling and Cleanup Ownership

A failed attempt must leave no data files behind, as iceberg-java's `DataWriter.abort()` does, and a
failed job must not commit anything. The files are always owned by exactly one side:

| Phase                                                               | Owner                        | Mechanism                                                                                                                                                                        |
| ------------------------------------------------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Writing, closing writers, encoding the manifest, building the batch | Native                       | `TrackingLocationGenerator` records every location. `AbortOnDrop` deletes them on an error, and also when the plan is dropped mid-write, from inside or outside a Tokio runtime. |
| After the batch reaches the JVM, until the task succeeds            | JVM task                     | `WrittenFileCleanup`, a task failure listener registered before the payload is read, takes the locations before the manifest is decoded.                                         |
| Job failure after some tasks completed                              | Driver (`IcebergCommitExec`) | Calls `BatchWrite.abort` with the completed messages, then deletes their files through the table's `FileIO`.                                                                     |
| Commit failure                                                      | Iceberg                      | `SparkWrite.abort` on the genuine `TaskCommit` messages, the same as the stock path.                                                                                             |

The handoff between the first two rows is why the payload carries the locations separately from the
manifest: a failure decoding the manifest would otherwise lose the list of files to delete. All
deletion is best effort and logged. It must never replace the original exception, and anything it
misses is unreferenced and reclaimed by Iceberg's `remove_orphan_files`.

The same planning-time rule applies to failures: a failure that happens because the native side
rejected something the gate admitted is a bug in the gate, even if cleanup works.

## Matching iceberg-java

Changes to the native path are measured against iceberg-java's output in three tiers, which are the
same tiers the user guide's "accepted divergences" section uses:

1. **Logical content and manifest metadata must match.** Row data, partition values, record
   counts, metrics, spec ids and sort order ids. A difference here outlives the write, because later
   readers prune on it. There are two documented exceptions, both analyzed as unable to change a
   pruning decision.
2. **Physical file layout may differ** where no reader bases a decision on it: footer key-value
   metadata, `created_by`, encodings, compressed bytes, row-group and file roll points, file names,
   and fanout file order.
3. **Anything else is a fallback.** If the native writer cannot match tier 1 for some table or
   setting, add a gate rule rather than documenting a new divergence.

A new accepted divergence belongs in the user guide's list with the reasoning for why no reader can
observe it, not only in a code comment.

Useful places to look when checking parity:

- `CometIcebergWriteActionSuite` writes the same data through both writers into sibling tables and
  compares rows, `readable_metrics` and partition paths.
- iceberg-rust's own code, at the pinned revision. iceberg-rust makes different choices from
  iceberg-java in places that matter to the table's contents, for example grouping partition keys
  with `OrderedFloat`, which treats `-0.0` and `0.0` as equal
  ([#6138](https://github.com/apache/datafusion-comet/issues/6138)). Check how iceberg-rust
  compares, hashes and renders values, not only what it writes.

### The iceberg-rust pin

`native/Cargo.toml` pins `iceberg` and `iceberg-storage-opendal` to a git revision, not a release.
Some Comet code depends on iceberg-rust behaviour that is not a stable API. For example,
`clustered_write_err` recognizes the clustered writer's unsorted-input error by its message text and
restates it as iceberg-java's error, which applications match on. A pin bump can change file bytes, manifests or partition layout, so treat it as a change to
the writer: run the write suites and the Iceberg Spark tests. The pin policy is tracked in
[#5645](https://github.com/apache/datafusion-comet/issues/5645).

## Testing

| Suite                                                            | What it covers                                                                                                                                                                              |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CometIcebergWriteActionSuite`                                   | End-to-end writes through the split plan and the native writer: parity with iceberg-java, row-level DML, partition evolution, file order, cleanup on task and job failure, AQE re-planning. |
| `CometIcebergWriteDetectionSuite`                                | One case per eligibility rule, accepted and declined.                                                                                                                                       |
| `CometIcebergSystemFunctionSuite`                                | Native `bucket`, `truncate`, `years`/`months`/`days`/`hours`, which keep a partitioned write's hash distribution and sort native end to end.                                                |
| `CometIcebergRewriteActionSuite`                                 | Iceberg's `rewrite_data_files` with the split plan and the native writer.                                                                                                                   |
| `IcebergWriteProtoTranslationSuite`                              | Translation of properties into `IcebergParquetWriteSettings` and the writer mode.                                                                                                           |
| Rust tests in `iceberg_write.rs` and `iceberg_partition_path.rs` | File rolling on the 1000-row grid, fanout order, clustered input checks, cleanup guard, manifest round trip, partition path rendering.                                                      |
| `CometIcebergWriteBenchmark`                                     | Native versus iceberg-java for unpartitioned, clustered, fanout and copy-on-write delete writes. It checks each arm's plan before timing it.                                                |

The Comet suites run against the Iceberg version each Spark profile pins in `spark/pom.xml`: 1.5.2
for Spark 3.4, 1.8.1 for 3.5, 1.10.0 for 4.0 and 4.2, and 1.11.0 for 4.1. Only the default profile
runs on every pull request. Behaviour that differs between Iceberg versions, such as reflection
targets, metrics conventions and partition path spelling, needs a test that runs on each.

When writing a native-write test:

- **Assert that the native writer ran.** Every fallback is silent to the query, so a passing
  comparison can mean both sides used iceberg-java. Use `assertNativeWriteEngages`, or collect
  `CometIcebergWriteExec` from the captured plans. Enable the writer with `withNativeEnabled`, which
  also turns on `localTableScan` so `INSERT ... VALUES` input is native.
- **Compare against iceberg-java, not against expected literals.** Write the same rows through the
  JVM writer into a sibling table and compare.
- **Run plan-shape and transition changes with AQE off as well as on.** The suites run with AQE on
  by default, and several write-path bugs only appeared with it off. Iceberg's own extension tests
  pick AQE on or off at random per session, so a bug that depends on AQE shows up there as
  intermittent.
- **Use enough partitions to catch ordering bugs.** With two partitions, a random order is right
  half the time. The fanout order test uses eight.
- **Check storage state for failure tests**, not only the table: list the files under the data
  location and compare them with what the manifests reference.

The upstream Iceberg Spark tests also run with both flags and `localTableScan` enabled (see
[Running Iceberg Spark Tests](iceberg-spark-tests.md)). They are a broad regression net, but they do
not assert which writer ran, and Comet's fallback reasons do not appear in their CI logs, so a green
run is not evidence that the native writer handled a given test
([#6148](https://github.com/apache/datafusion-comet/issues/6148)).

## Pitfalls

Each of these has caused a bug on this path:

- **A permissive gate is a failed query.** Anything the native side rejects at execution has to be
  rejected at plan time, using the same interpretation of the input.
- **Dropped settings are silent.** A table, `FileIO` or Hadoop setting the native writer does not
  read produces a successful write that ignores it. Fail closed on unknown settings in every
  namespace that reaches the writer, as the `gs://` path does
  ([#5637](https://github.com/apache/datafusion-comet/issues/5637),
  [#6139](https://github.com/apache/datafusion-comet/issues/6139)).
- **Map iteration order leaks.** It reaches file names, manifest order and read order.
- **Values that compare equal in Rust may not in Java.** Signed zeros and NaN under `OrderedFloat`,
  and string or float rendering in partition paths.
- **Partition evolution leaves `void` fields behind.** A v1 spec keeps a dropped partition field as
  a `void` transform, whose source column may later be dropped from the schema. Resolving the spec
  against the schema then fails
  ([#5691](https://github.com/apache/datafusion-comet/issues/5691),
  [#5693](https://github.com/apache/datafusion-comet/issues/5693),
  [#6141](https://github.com/apache/datafusion-comet/issues/6141)).
- **Plan rewrites must keep the write node.** Rules that restore Spark operators from a Comet
  node's `originalPlan` have to handle the write execs, whose `originalPlan` today is their child
  ([#5719](https://github.com/apache/datafusion-comet/issues/5719)).
- **The kill switch must still work.** Code that runs for Iceberg writes has to respect
  `spark.comet.enabled`, so that disabling Comet restores Spark's own plan
  ([#6142](https://github.com/apache/datafusion-comet/issues/6142)).

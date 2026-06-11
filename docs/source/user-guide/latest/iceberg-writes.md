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

# Accelerating Apache Iceberg V2 Writes using Comet

## Overview

Comet accelerates Iceberg V2 copy-on-write table writes in two complementary layers:

1. **Split-operator plan** (on by toggle). Comet rewrites Iceberg's single
   `V2ExistingTableWriteExec` command into a pair of operators — a **committer** and a
   **writer** — so that AQE (and Comet's columnar rules) can see and re-optimise the
   data sub-query. The actual file-writing work still runs through Iceberg's stock JVM writer.
2. **Native parquet write** (further toggle). Once the split-operator plan is in place, the
   writer can be swapped for a native variant that delegates the per-task parquet write to
   [iceberg-rust](https://github.com/apache/iceberg-rust). The committer / table-metadata
   update / cache-refresh stay on the JVM exactly as before.

Both layers fall back to plain Iceberg-Java when they can't safely apply — and both layers are
configured per-session, so you can opt into the split plan but skip the native write while it
burns in.

## What changes about the Iceberg plan

### Iceberg's stock V2 plan

```
V2ExistingTableWriteExec(write, query)        ← single V2 command; commit + write together
└── <query>                                    ← data sub-query (scans, shuffles, ...)
```

This is a single command. Spark's `InsertAdaptiveSparkPlan` runs it as a leaf from AQE's
perspective — AQE never sees the data sub-query inside, so Comet's columnar rules can't convert
the scans / shuffles inside it either.

### Comet's split plan

```
IcebergCommitExec(batchWrite, refreshCache)   ← committer; runs BatchWrite.commit()
└── AdaptiveSparkPlanExec                      ← AQE bubble (Spark inserts this)
    └── IcebergWriteExec(batchWrite)    ← writer (UnaryExecNode); per-task write
        └── <query>                            ← data sub-query: now visible to AQE / Comet
```

The committer keeps Iceberg's commit semantics intact. The writer is a normal
`UnaryExecNode`, so AQE wraps it whenever the data sub-query has a shuffle, and Comet's
standard `transformUp` rules can convert the scans / projects / sorts / exchanges inside the
AQE bubble to their Comet counterparts (`CometScan`, `CometProject`, `CometColumnarExchange`,
…).

### What flows between the two execs

The writer produces **one row per Spark task** with a single `BINARY` column:
Java-serialised bytes of an Iceberg `SparkWrite$TaskCommit(DataFile[])`. The committer
`executeCollect()`s that RDD, deserialises each row's bytes back into a `WriterCommitMessage`,
and hands the resulting array to `BatchWrite.commit(messages)` — the same call Iceberg-Java
would have made internally.

## Configuration

Standard Comet + Iceberg setup ([`iceberg.md`](iceberg.md)) plus two toggles for the write-side
behaviour:

```
# Standard Comet / Iceberg wiring
spark.plugins=org.apache.spark.CometPlugin
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<name>.type=hadoop                              # or hive / glue / rest / ...
spark.sql.catalog.<name>.warehouse=...

# Comet write toggles (both off by default; can be turned on independently)
spark.comet.write.iceberg.splitOperator.enabled=true              # layer 1: split-operator plan
spark.comet.write.iceberg.nativeAcceleration=true                 # layer 2: native parquet write
```

`IcebergSparkSessionExtensions` is **mandatory on Spark 3.4** for `UPDATE` / `MERGE` on V2 tables —
Spark 3.4's stock planner rejects `UPDATE TABLE` on V2 sources, and Iceberg's analyzer extension
(`RewriteUpdateTable` / `RewriteMergeIntoTable`) is what provides the rewrite. On Spark 3.5+ the
extension is optional but recommended (Spark added native row-level operation support in 3.5).
`INSERT INTO` / `INSERT OVERWRITE` / `DELETE FROM` work without the extension on every Spark
version.

If `splitOperator.enabled=false`, Comet leaves Iceberg's stock plan alone — every write goes
straight through Iceberg-Java.

## Spark version compatibility

Copy-on-write coverage is identical across versions (`UPDATE` / `MERGE` on 3.4 requires
`IcebergSparkSessionExtensions`). Merge-on-read (`WriteDelta`) is intentionally left on Spark's
default path: the per-task `DeltaWriter` is row-dispatched and no native acceleration is
planned, so routing it through the split-operator plan would add planning complexity for no
realisable benefit.

| Capability                                  | 3.4   | 3.5 | 4.0 |
| ------------------------------------------- | ----- | --- | --- |
| `INSERT INTO` (`AppendData`)                | ✅    | ✅  | ✅  |
| `INSERT OVERWRITE` (static + dynamic)       | ✅    | ✅  | ✅  |
| `DELETE FROM` copy-on-write (`ReplaceData`) | ✅    | ✅  | ✅  |
| `UPDATE` / `MERGE INTO` copy-on-write       | ✅[¹] | ✅  | ✅  |

- [¹] Requires `IcebergSparkSessionExtensions` (see Configuration above).

Native parquet write coverage is more selective:

| Write                                   | Spark 3.4   | Spark 3.5   | Spark 4.0   |
| --------------------------------------- | ----------- | ----------- | ----------- |
| `AppendData` (`INSERT INTO`)            | native      | native      | native      |
| `OverwriteByExpression` (static)        | native      | native      | native      |
| `OverwritePartitionsDynamic`            | native      | native      | native      |
| `ReplaceData` (CoW `DELETE` / `UPDATE`) | native      | native      | native      |
| `ReplaceData` (CoW `MERGE`)             | fallback[²] | fallback[²] | fallback[²] |

- [²] MERGE flows through Spark's (or Iceberg 1.5.2's) `MergeRowsExec`, which per-row dispatches
  the matched / not-matched / not-matched-by-source clauses. There's no native equivalent in
  Comet _yet_ — this is planned (see Known limitations). The scan / join / sort / shuffle parts
  still run native; only the per-row merge dispatch and the file write stay JVM.

## Fallback triggers

Before accepting a write into the native path, the serde checks for things iceberg-rust either
doesn't support or implements differently from iceberg-java. The first matching trigger short-
circuits the conversion and the plan stays on the JVM two-op path.

Each row attributes the gap to the layer that's the actual root cause: **iceberg-rust** (the
high-level Iceberg writer / commit / metadata logic) or **parquet-rs** (the underlying Apache
Parquet Rust encoder that iceberg-rust drives).

| Property / state                                      | Falls back when …                                                                                                                   | Why                                                                                                                                                                                                                                                                                        |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| resolved write format                                 | `SparkWrite.format` (i.e. effective `write-format` option overlaid on `write.format.default`) is not `parquet`                      | **iceberg-rust**: writer stack is parquet-only today. The check reads the resolved format, so per-write `option("write-format", "orc")` is honoured even when the table default is parquet.                                                                                                |
| `write.object-storage.enabled`                        | `true`                                                                                                                              | **iceberg-rust**: no hashed-prefix object-storage location generator                                                                                                                                                                                                                       |
| `write.location-provider.impl`                        | set                                                                                                                                 | **iceberg-rust**: can't load a Java `LocationProvider` class                                                                                                                                                                                                                               |
| `format-version`                                      | `>= 3`                                                                                                                              | **iceberg-rust**: V3 row lineage / DVs / variant types not implemented                                                                                                                                                                                                                     |
| `encryption.*` (any key)                              | set                                                                                                                                 | **iceberg-rust + parquet-rs**: no Parquet modular encryption support in either layer                                                                                                                                                                                                       |
| `write.metadata.metrics.default`                      | mentions `counts`                                                                                                                   | **iceberg-rust**: doesn't implement Iceberg's `counts`-without-bounds metrics mode. parquet-rs supports stats on or off but has no "value counts only" knob, so iceberg-rust would need to track and emit those counts itself; it doesn't.                                                 |
| `write.metadata.metrics.default`                      | `none`                                                                                                                              | **iceberg-rust**: always populates `column_sizes` / `value_counts` / `null_value_counts` / bounds from the parquet footer regardless of the requested mode; iceberg-java's `none` emits an empty `DataFile.metrics`, so a native write would produce strictly richer manifests.            |
| `write.metadata.metrics.column.<col>`                 | any column set to `counts`                                                                                                          | **iceberg-rust**: same as default `counts`, per-column                                                                                                                                                                                                                                     |
| `write.metadata.metrics.column.<col>`                 | any column set to `none`                                                                                                            | **iceberg-rust**: same as default `none`, per-column                                                                                                                                                                                                                                       |
| `write.parquet.bloom-filter-max-bytes`                | explicitly set                                                                                                                      | **iceberg-rust**: doesn't enforce iceberg-java's bloom-filter byte cap. parquet-rs has no global cap parameter on `WriterProperties`, so iceberg-rust would need to clamp NDV / FPP itself before passing them through.                                                                    |
| `write.parquet.bloom-filter-enabled.column.<col>`     | any column set to `true`                                                                                                            | **iceberg-rust**: bloom-filter sizing diverges from iceberg-java (same root cause as above — no cap clamping)                                                                                                                                                                              |
| `write.parquet.row-group-check-min-record-count`      | set to a non-default value (default `100`)                                                                                          | **parquet-rs**: row-group close cadence is byte-driven and doesn't expose parquet-mr's per-row-count knobs, so iceberg-rust can't honour this property                                                                                                                                     |
| `write.parquet.row-group-check-max-record-count`      | set to a non-default value (default `10000`)                                                                                        | **parquet-rs**: same as above                                                                                                                                                                                                                                                              |
| `write.parquet.stats-enabled.column.<col>`            | any column override set **and** the running Iceberg version defines `TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX` (1.10.0+) | **parquet-rs**: no per-column statistics-enabled override matching parquet-mr's. The property was only added in Iceberg 1.10.0; on 1.5.2 / 1.8.1 Iceberg-Java ignores it too, so there's nothing to diverge from and the write is **not** gated on those versions.                         |
| `parquet.enable.dictionary`                           | set                                                                                                                                 | **parquet-rs**: explicit dictionary override is not translated to the native writer settings; parquet-rs would fall back to its own default and diverge from parquet-mr.                                                                                                                   |
| `write.metadata.metrics.max-inferred-column-defaults` | the number of projected field IDs exceeds this limit (default `100`) **and** no explicit `write.metadata.metrics.default` is set    | **iceberg-rust**: doesn't apply `MetricsModes.None` to columns past this limit. Iceberg only applies that inferred-column truncation when no default mode is configured (a user-set default applies to all columns regardless of count), so the gate is skipped once a default is present. |

> `write.parquet.page-version` is intentionally **not** a fallback trigger: no Iceberg version Comet
> targets (1.5.2 / 1.8.1 / 1.10.0) maps a table property to the Parquet writer version — it is
> hardwired to `PARQUET_1_0`. Setting it is a no-op in Iceberg-Java exactly as in the native path.
> | `io-impl` (catalog property) | set | **iceberg-rust**: native FileIO is selected by URI scheme (`OpenDalStorageFactory`), so an explicit Java `FileIO` class can't be honoured. |
> | Resolved data-location URI scheme | not in `{file, memory, s3, s3a, gs, oss}` | **iceberg-rust**: `iceberg_common::storage_factory_for` resolves only the listed schemes; `hdfs`, `abfs`/`abfss`, `wasb`/`wasbs`, and similar would crash at write time. |

Every trigger has a pinning test in `CometIcebergWriteDetectionSuite`.

## Known limitations and planned work

- **CoW MERGE — native acceleration planned.** The split-operator plan already applies (scan,
  join, sort, shuffle run native); only the per-row merge dispatch (`MergeRowsExec`) and the
  file write itself stay JVM. The path forward is well-understood: add a `CometMergeRowsExec`
  that mirrors Spark's instruction-driven dispatch model (`Keep` / `Discard` / `Split` over
  matched / not-matched / not-matched-by-source clauses, plus a bitmap-backed cardinality
  check). Tracked.

- **`sort_order_id` is always stamped `0` (unsorted).** This matches Iceberg-Java for every
  version Comet targets: the Spark `SparkWrite$WriterFactory` in Iceberg 1.5.2 / 1.8.1 / 1.10.0
  builds `SparkFileWriterFactory` **without** `.dataSortOrder(...)`, so a stock batch append stamps
  `sort_order_id = 0` on committed data files even for a table that has a non-default sort order.
  The native writer does the same. (Iceberg 1.11+ adds
  `SparkWriteConf.outputSortOrderId(writeRequirements)` and wires the resolved id into the writer
  factory; Comet reflects that resolver when present, so behaviour stays correct if the pinned
  runtime is bumped.)

- **Nested (list/map) column bounds differ from Iceberg-Java.** iceberg-java's
  `ParquetUtil.shouldStoreBounds` suppresses `lower_bounds` / `upper_bounds` for any field reached
  through a repeated (list or map) field, so collection-element primitives get no manifest bounds.
  iceberg-rust's `MinMaxColAggregator` computes bounds for every leaf primitive — including
  list-element / map-key / map-value fields — so a native write to a schema containing list or map
  columns produces **extra** `lower_bounds` / `upper_bounds` entries for those nested field IDs that
  Iceberg-Java would omit. `column_sizes` / `value_counts` / `null_value_counts` match. The extra
  bounds are not used by Iceberg's metrics evaluators (predicates can't address list/map elements),
  so the functional risk is low, but the manifest is not byte-identical to the JVM path for nested
  schemas. Strict parity would require stripping those nested-field bounds before manifest encoding
  (or in iceberg-rust); not yet done.

## Tests

- **`CometIcebergWriteActionSuite`** — end-to-end scenarios against a temporary Hadoop catalog
  covering the copy-on-write V2 logical-write variants on both the split-operator-only and
  full-native paths, plus parity vs Iceberg-Java, the disabled-config fallback, the
  commit-once invariant, and the documented native-engagement-vs-fallback contract for MERGE.
  Passes on Spark 3.4 / 3.5 / 4.0 with `IcebergSparkSessionExtensions` loaded.
- **`CometIcebergWriteDetectionSuite`** — tests that build a planned-but-not-executed
  `IcebergWriteExec` per fall-back trigger and assert
  `CometIcebergNativeWrite.getSupportLevel` returns `Unsupported` with the expected reason
  fragment, plus a positive baseline (clean parquet V2 table → `Compatible`).
- **`IcebergWriteProtoTranslationSuite`** — unit tests pinning the JVM-side property → proto
  translation table-by-table.

## Abort behaviour

The writer calls `writer.abort()` per task on failure to release task-level resources. The
committer (`IcebergCommitExec.run()`) calls `BatchWrite.abort(messages)` if `commit()` raises.

If task writers stage files locally but their commit messages never reach the driver (e.g.
driver crash mid-collect), the staged Parquet files become **unreferenced orphans**. Iceberg's
catalog-level `RemoveOrphanFiles` action reaps these on the next maintenance run. Schedule
`RemoveOrphanFiles` if you want to avoid storage drift on failed writes. This diverges from
Spark's per-task abort behaviour and is a deliberate trade-off in favour of a simpler
driver-side commit loop.

---

# Developer notes

The sections below document architectural decisions, interactions with Spark internals, and
load-bearing implementation details. They're not required reading for _using_ the writer — only
for working on it.

## How the split survives AQE re-planning

AQE calls `reOptimize(inputPlan.logicalLink.get)` every time a query stage materialises and
re-runs the planner on the result. The trick is making sure the writer stays in
place and the commit stays exactly one — not zero, not two.

Two design choices keep this stable:

1. **File writer exec uses a stable logical anchor.** `IcebergWriteLogical` is a
   Catalyst `UnaryNode` that wraps the data sub-query plus the `BatchWrite` reference. The
   planner's `setLogicalLink` pins the writer to this anchor (not to the surrounding
   `ReplaceData` / `AppendData` / etc. logical node) so AQE's `reOptimize` re-plans only the
   data query. Without the anchor AQE would either re-fire the surrounding Iceberg write
   logical node (duplicating the commit) or strip the writer away (no write
   happens).
2. **The `BatchWrite` instance is shared between committer and writer.** Iceberg's
   `Write.toBatch()` returns a freshly-constructed `BatchWrite` on every call; for CoW DML the
   instance holds the scan state and emitted-file tracking, so the writer's writer
   factory and the committer **must** operate on the same instance for `OverwriteFiles`'
   serialisable-isolation validation to walk the right snapshot range. The strategy calls
   `toBatch` once and threads the result through both execs.

## Other AQE / planning interactions

- **AQE re-fire produces a fresh `IcebergWriteExec` over the existing `CometIcebergWriteExec`.**
  Without protection this would stack two writers. `CometExecRule`'s `IcebergWriteExec` case
  detects this (via `unwrapToCometIcebergWrite`) and returns the existing writer instead —
  mirroring the `DataWritingCommandExec(WriteFilesExec(CometNativeWriteExec))` unwrap V1
  parquet uses.
- **`CometIcebergWriteExec` is a block boundary in the serialisation loop.** The post-convert
  serialization loop in `CometExecRule` resets `firstNativeOp = true` after
  `CometIcebergWriteExec` (alongside `CometNativeWriteExec`) so the upstream Comet chain's
  outermost node gets `convertBlock()` called on it and populates its `serializedPlanOpt`.
  Without this, an upstream `CometProject` reaches execution without a serialised plan and
  `doExecuteColumnar` throws.
- **`CometMetricNode.fromCometPlan` stops at non-Comet descendants.** AQE-stage nodes
  (`AQEShuffleReadExec`) can have a null `session` field when constructed off the planning
  thread; their lazy `metrics` val NPEs on `sparkContext`. They're not ours to update anyway,
  so the metric tree walk just stops at the boundary.

## Native acceleration internals

The high-level shape: the JVM-side serde marshals everything iceberg-rust needs (table
properties, write schema, partition spec, target file size, writer mode, per-task IDs, …) into
a single proto, and on each task iceberg-rust returns one `BINARY` row containing an Iceberg V2
data manifest (Avro container bytes). The JVM-side commit decodes those bytes back into
`DataFile`s via `ManifestFiles.read` and feeds them to `BatchWrite.commit(messages)`. So
everything iceberg-java would do post-write (snapshot id assignment, manifest list aggregation,
commit retries, cache refresh) is untouched — only the per-task parquet emission moved.

Notable details:

- **Per-task IDs are stamped onto the proto.** `CometIcebergWriteExec.doExecuteColumnar` reads
  `TaskContext.getPartitionId()` and `taskAttemptId()` and rebuilds the `IcebergWrite` proto's
  `partition_id` / `task_attempt_id` per task before serialising. Without this every task mints
  the same filename prefix (`00000-00000-<op_id>`) and OpenDAL's `CompleteWriter` trips on the
  resulting concurrent-write byte-count drift. Mirrors `CometNativeWriteExec`'s parquet pattern.
- **Spark 4.x `ReplaceData` carries extra columns** that the native writer can't pass through
  as-is; `CometIcebergNativeWrite.maybeProjectReplaceDataColumns` splices a DataFusion
  `Projection` between the FFI scan and `IcebergWrite` to strip them. The method docstring
  has the full mechanics.

## Iceberg 1.5.2 (Spark 3.4) reflection skew

Iceberg 1.5.2 differs in a handful of small ways from 1.8+. All of them are papered over with
reflection in `IcebergReflection` rather than per-version source shims:

- **`useFanoutWriter` field renamed.** 1.5.2 calls it `partitionedFanoutEnabled`. The
  `getUseFanoutWriterFromSparkWrite` helper tries the new name first and falls back to the old.
- **`GenericManifestFile` constructor.** Iceberg 1.6 added a 3-arg form
  `(InputFile, int, long)` that takes V3's `firstRowId`. 1.5.2 only has the 2-arg form
  `(InputFile, int)`. `newDataManifestFile` tries the 3-arg form first and falls back.
- **Null `snapshotId` rejected on read.** Iceberg 1.5.2's
  `InheritableMetadataFactory.fromManifest` throws "Cannot read from ManifestFile with null
  (unassigned) snapshot ID". 1.8+ relaxed that check. The helper reflectively writes a `0L`
  placeholder onto `GenericManifestFile.snapshotId` after construction; it's overwritten by
  `BatchWrite.commit` later and never reaches storage.
- **Separate logical write node.** Iceberg 1.5.2 ships its own `ReplaceIcebergData` logical
  node because Spark 3.4 lacks native row-level operation support. Field shape is identical to
  Spark 3.5+'s stock `ReplaceData`, so `IcebergWriteStrategy` matches it by FQCN and the
  extracted tuple reuses the existing dispatcher.

## Why a custom logical anchor (`IcebergWriteLogical`)

Without `IcebergWriteLogical`, AQE's `reOptimize` would either:

- **Vanish** the writer, if its logical link points at the data sub-query (which
  carries no write semantics). The re-planned tree would have no writer at all.
- **Duplicate** the committer, if the writer's logical link points at the
  surrounding logical write (`ReplaceData` / `AppendData` / …). The strategy would re-fire the
  entire two-op tree on every AQE iteration, each iteration emitting a fresh
  `IcebergCommitExec` — and Iceberg's `OverwriteFiles` validation then sees the prior
  iteration's file as a newly-added conflicting file and fails.

`IcebergWriteLogical` sits between the two and lets the strategy re-emit a fresh
`IcebergWriteExec` on every iteration without touching the committer.

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

# Planning: from logical plan to per-partition tasks

## The hook point

Spark's logical plan after analysis contains a `LogicalRelation` wrapping a
`HadoopFsRelation` whose `fileFormat` is a `DeltaParquetFileFormat`. Comet's
existing `CometScanRule` runs as a strategy rewrite: it looks at each scan
node and decides whether to replace it with a native equivalent.

The Delta contrib adds one arm to that rule via reflection. From
`spark/.../CometScanRule.scala`, inside the per-`FileSourceScanExec` match
on `HadoopFsRelation`:

```scala
DeltaIntegration.transformV1IfDelta(plan, session, scanExec, r) match {
  case Some(handled) => return handled
  case None          => // proceed with vanilla logic
}
```

`DeltaIntegration` is a thin Scala object in *core* that:

1. On first call, reflectively looks up
   `org.apache.comet.contrib.delta.DeltaScanRule#transformV1IfDelta`
   with signature `(SparkPlan, SparkSession, FileSourceScanExec, HadoopFsRelation)`
2. Caches the resolved `Method` handle in a `@volatile var`
3. Invokes it and returns `Option[SparkPlan]` — `Some(handled)` if the
   contrib either claimed the scan or declined it via its own `withInfo`
   fallback marker; `None` if the relation isn't a Delta relation at all

If the classpath lookup fails (default build, no contrib), the cached value
is `Some(None)` — a "definitely not present" marker — and subsequent calls
short-circuit. This is the only point in core that knows about Delta.

`DeltaScanRule.transformV1IfDelta` in the contrib first checks whether the
relation's `fileFormat` is `DeltaParquetFileFormat` (via reflection, no
compile-time delta-spark dependency), then applies a series of gates
(covered in [06-fallback-and-ops.md](06-fallback-and-ops.md)). If all gates pass, it returns a
`CometScanExec` marker that flows through the standard
`CometExecRule.convertToComet` path and ultimately routes through
`CometDeltaNativeScan` for proto serialisation.

## What `CometDeltaNativeScanExec` looks like

It's a `LeafExecNode` with these responsibilities:

- Hold the original `relation`, `output`, `dataFilters`, `partitionFilters`,
  plus a `dppFilters: Seq[Expression]` list for DPP
- Lazily produce `commonBytes` (the common proto block) and `allTasks` (the
  resolved kernel-rs task list) — both `@transient lazy val`s
- At `doExecuteColumnar()` time, apply any now-resolved DPP filters against
  `allTasks`, serialise per-partition task bytes, and route through Comet's
  existing native exec path

The kernel-rs scan runs once on first access of `allTasks`; the result is
reused. DPP filters are deliberately applied *after* that lazy val (inside
`doExecuteColumnar`) rather than baked into `allTasks` — at planning time
the DPP subquery is still a `SubqueryAdaptiveBroadcastExec` placeholder,
so the actual partition values are not known yet.

## kernel-rs scan resolution

`delta-kernel-rs` is the Delta team's official Rust crate for parsing Delta
metadata. The interaction looks like:

```rust
let engine = engine_cache.get_or_create(table_root, storage_config)?;
let table = Table::try_from_uri(table_root)?;
let snapshot = table.snapshot(engine.as_ref(), version_hint)?;
let scan = snapshot.scan_builder()
    .with_schema(read_schema)
    .with_predicate(pushable_filters)
    .build()?;

let mut acc = RawEntryAcc { entries: vec![], row_tracking: …, next_idx: 0 };
scan.scan_data(engine.as_ref())?.for_each(|batch| {
    visit_scan_files(batch, &mut acc, |entry, ctx| { ctx.entries.push(entry); });
});
```

The `RawEntry` we accumulate carries everything needed to build a per-file
task: the parquet path, partition values, column-mapping maps, optional DV
descriptor, and (newly) the `baseRowId` and `defaultRowCommitVersion` for
row tracking. These are extracted from kernel's `fileConstantValues` by
downcasting the underlying `RecordBatch` (kernel's `ScanFile` doesn't expose
them directly).

The `engine_cache` is keyed by `(scheme, authority, DeltaStorageConfig)`.
Caching matters because `DefaultEngine<TokioBackgroundExecutor>` spawns one OS
thread per executor on creation; without a cache, hundreds of scans per minute
leaked threads faster than tokio reaped them, eventually tripping
`pthread_create EAGAIN`.

## The proto split: common + per-task

A naive serialisation would embed the entire `Vec<RawEntry>` into a single
proto and have Catalyst's plan-closure capture include it in every partition.
For a 5000-file scan with 200 partitions, that's 5000 × 200 = 1M task records
shipped around even though each partition only needs its own slice.

The split:

- **`DeltaScanCommon`** (fields shared across all partitions): table root,
  read schema, partition schema, pushable filters, column mapping mode, the
  set of "emit" flags (which synthetic columns are wanted), and
  `final_output_indices` for reorder
- **`DeltaScanTask[]`** (per partition): the list of files this partition
  reads, with path, partition values, optional DV descriptor, optional
  baseRowId, optional defaultRowCommitVersion

The common block goes into the `OpStruct::DeltaScan` variant of the operator
proto. The per-partition task arrays ride alongside via Comet's existing
`PlanDataInjector` mechanism: at task scheduling time, the executor receives
its partition index and looks up the matching byte array. This is the same
mechanism the plain parquet scan uses for its `FilePartition` payload.

## Synthetic columns: detection and emit flags

After all gates pass, the planner looks at `scan.requiredSchema` and detects
Delta's "synthetic" columns by name:

- `__delta_internal_row_index` (UInt64): per-file physical row index
- `__delta_internal_is_row_deleted` (Int32, 0/1): whether the row is masked
  by a DV
- `row_id` (Int64): Delta row-tracking row ID
- `row_commit_version` (Int64): Delta row-tracking commit version

For each detected synthetic, an `emit_*` flag goes into `DeltaScanCommon`. The
native side reads these flags to decide whether to wrap the parquet output
with `DeltaSyntheticColumnsExec` (see [03-native-execution.md](03-native-execution.md)).

When synthetics ARE detected but are NOT a contiguous suffix of
`requiredSchema` (e.g. caller wants `[col_a, row_id, col_b]` not
`[col_a, col_b, row_id]`), we compute `final_output_indices` — a permutation
that the native side applies via a final `ProjectionExec` reorder. This lets
callers project synthetics in any position without forcing a JVM-side
projection.

## Column mapping

Delta supports three column-mapping modes:

- `none`: physical parquet column names match logical
- `name`: physical names are random strings (`col-1a2b…`); a JSON map in
  the table schema maps logical → physical
- `id`: physical names are random strings; parquet field IDs identify columns

The contrib handles both `name` and `id`:

- **`name` mode**: we rewrite the logical names into a `physicalSchema`,
  pass that to kernel-rs and the parquet reader. After the parquet read,
  a `ProjectionExec` renames physical → logical for the synthetic-column
  detection downstream. The rename projection runs BEFORE the synthetic
  wrap so name lookups by logical name work.
- **`id` mode**: we walk the logical `StructType` and on every `StructField`
  (including nested struct/array/map element fields) we translate the
  `delta.columnMapping.id` metadata key to `PARQUET:field_id`. The parquet
  reader's `ParquetField` resolution then matches by ID, not name.

The translator is recursive and handles `StructType`, `ArrayType`, `MapType`
element types because Delta annotates IDs on every field, not just top-level
columns.

## DPP and partition pruning

If the plan above includes a `DynamicPruningExpression`, the actual partition
values aren't known at plan time — they arrive after the broadcast side of a
join finishes. `CometDeltaNativeScanExec` carries `dppFilters` as a
constructor field, and re-applies them against `allTasks` inside
`doExecuteColumnar` (not in a `lazy val`, so the broadcast result is fresh
on each execution). The resulting per-partition task list is then proto-
serialised through the same encoding path as the static case.

## What's serialised vs computed at execute time

| Computed at plan time (driver) | Computed at execute time (driver, lazy) |
|---|---|
| Schema resolution | DPP-resolved partition values |
| Gate evaluation | kernel-rs file list |
| Column-mapping translation | per-partition task byte arrays |
| Synthetic-column detection | task packing into partitions |
| Emit flags + `final_output_indices` | proto encoding |

Nothing in this list happens per-batch on the executor; the executor's only
job is to deserialise the proto and run the resulting DataFusion plan.

---

**Navigation** · [← 01 Overview](01-overview.md) · [↑ Index](README.md) · Next → [03 Native execution](03-native-execution.md)

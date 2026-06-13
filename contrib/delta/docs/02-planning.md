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

`DeltaIntegration` is a thin Scala object in core's `org.apache.comet.rules`
package (`spark/.../rules/DeltaIntegration.scala`) that:

1. On first call, reflectively looks up the contrib module
   `org.apache.comet.contrib.delta.DeltaScanRule$` and binds its
   `transformV1IfDelta` method with signature
   `(SparkPlan, SparkSession, FileSourceScanExec, HadoopFsRelation)`
2. Caches the resolved `(module, Method)` binding in a `@volatile var`
3. Invokes it and returns `Option[SparkPlan]` — `Some(handled)` if the
   contrib either claimed the scan or declined it via its own
   `withFallbackReason` fallback marker; `None` if the relation isn't a Delta
   relation at all

If the classpath lookup fails (default build, no contrib), the cached value is
the "not present" sentinel and subsequent calls short-circuit. This object is
the only point in core that knows about Delta. (`DeltaIntegration` also carries
the `isCdfRelation` / `transformCdf` Change Data Feed bridge — see
[03-native-execution.md](03-native-execution.md).)

`DeltaScanRule.transformV1IfDelta` in the contrib first checks whether the
relation's `fileFormat` is `DeltaParquetFileFormat` (via reflection, no
compile-time delta-spark dependency), then applies a series of gates
(covered in [06-fallback-and-ops.md](06-fallback-and-ops.md)). If all gates pass, it returns a
`CometDeltaScanMarker` — a `LeafExecNode` that wraps the original, untouched
`FileSourceScanExec` (preserving its `logicalLink`) and carries the
Delta-specific planning info in a `DeltaScanMetadata` field. `CometExecRule`
detects the marker by type (`DeltaIntegration.isDeltaScanMarker`) and routes it
through `convertToComet` with the `CometDeltaNativeScan` serde. If conversion
declines, the marker falls back to executing the wrapped vanilla Spark scan, so
leaving it in the plan is always safe.

## What `CometDeltaNativeScanExec` looks like

It's a `CometLeafExec` (mixing in `CometScanWithPlanData`) with these
responsibilities:

- Hold the serialised common `nativeOp`, `output`, the `originalPlan`
  (`FileSourceScanExec`, for `logicalLink`), the `tableRoot`, the resolved
  `taskListBytes`, a `dppFilters: Seq[Expression]` list for DPP, the
  `partitionSchema`, and an `oneTaskPerPartition` flag
- Lazily produce `commonBytes` (the common proto block) and `allTasks` (the
  resolved kernel-rs task list, decoded from `taskListBytes`) — both
  `@transient lazy val`s
- At `doExecuteColumnar()` time, apply any now-resolved DPP filters against
  `allTasks`, pack them into per-partition task byte arrays (`perPartitionData`),
  and route through Comet's native exec path. Per-partition bytes are injected
  per Spark partition via `DeltaPlanDataInjector` (core's `PlanDataInjector`
  machinery), keyed by `sourceKey`

`oneTaskPerPartition` forces `packTasks` to emit one group (= one DataFusion
partition) per file, which DV / row-tracking / per-file `_metadata` reads
require (Spark consumes one DataFusion partition per Spark partition, so packing
multiple files into one partition would drop the 2nd+ files' rows).

The kernel-rs scan runs once on first access of `allTasks`; the result is
reused. DPP filters are deliberately applied _after_ that lazy val (inside
`doExecuteColumnar`) rather than baked into `allTasks` — at planning time
the DPP subquery is still a `SubqueryAdaptiveBroadcastExec` placeholder,
so the actual partition values are not known yet.

## kernel-rs scan resolution

`delta-kernel-rs` is the Delta team's official Rust crate for parsing Delta
metadata. The driver does **not** resolve the snapshot in Scala; instead
`CometDeltaNativeScan.convert` calls the native `Native.planDeltaScan` JNI
entry point, which runs kernel on the driver to enumerate the snapshot's scan
files and returns a `DeltaScanTaskList` proto. Per scan file, kernel ships:

- the parquet **path**, size, record count, partition values, and
  modification time;
- a `transform_json` — kernel's physical→logical `Expression` for that file
  (partition injection, column-mapping relabel including nested, row-tracking),
  serialised so the executor can rebuild it without an in-process `Snapshot`;
- a **DV descriptor** (not the decoded vector — the executor decodes it via
  `dv_reader`);
- the `baseRowId` and `defaultRowCommitVersion` constants for row tracking.

Kernel-built **physical and logical Arrow schemas** for the data columns ride
on `DeltaScanCommon` (`kernel_physical_schema` / `kernel_logical_schema`),
shipped once per scan rather than per file. The driver projects them from the
snapshot so field-ids are preserved; the executor uses them verbatim (no
Comet-side reconstruction — see "Column mapping" below).

There is **no AddFiles detour for regular reads** (#80): regular reads always
take the kernel-enumeration path. DML rewrites whose `FileIndex` is a
`TahoeBatchFileIndex` (an exact subset of touched files) still use
kernel-enumerate + a Delta path-filter to the touched paths; if not every
touched file is matched by kernel, the contrib declines to vanilla Spark (#81)
rather than reading a wrong file set. For a batch-file-index path the data-column
schemas are fetched via the schema-only `Native.planDeltaReadSchemas`.

The native engine used by `planDeltaScan` is cached
(`get_or_create_engine`, keyed by storage config). Caching matters because
`DefaultEngine<TokioBackgroundExecutor>` spawns one OS thread per executor on
creation; without a cache, hundreds of scans per minute leaked threads faster
than tokio reaped them, eventually tripping `pthread_create EAGAIN`.

## The proto split: common + per-task

A naive serialisation would embed the entire `Vec<RawEntry>` into a single
proto and have Catalyst's plan-closure capture include it in every partition.
For a 5000-file scan with 200 partitions, that's 5000 × 200 = 1M task records
shipped around even though each partition only needs its own slice.

The split:

- **`DeltaScanCommon`** (fields shared across all partitions): table root,
  required schema, partition schema, object-store options, session timezone,
  the kernel-built data-column schemas (`kernel_physical_schema` /
  `kernel_logical_schema`), the `synthesize_in_worker` flag, the
  `dv_file_name_prefix`, the CDF fields (`cdf_read`, `cdf_start_version`,
  `cdf_end_version`), the `emit_*` flags, and `final_output_indices` for reorder
- **`DeltaScanTask[]`** (per partition): the list of files this partition
  reads, with path, size, record count, partition values, optional DV
  descriptor, the per-file `transform_json`, and optional `baseRowId` /
  `defaultRowCommitVersion`

The common block goes into the `OpStruct::DeltaScan` variant of the operator
proto (`delta_scan = 118`). The per-partition task arrays ride alongside via
`DeltaPlanDataInjector` (built on core's `PlanDataInjector` machinery): at task
scheduling time, the executor receives its partition index and looks up the
matching byte array, keyed by the scan's `sourceKey`. This is the same shape
the plain parquet scan uses for its `FilePartition` payload.

## Synthetic columns: in-worker synthesis

After all gates pass, the planner looks at `scan.requiredSchema` and detects
Delta's "synthetic" columns by name:

- `__delta_internal_row_index` (UInt64): per-file physical row index
- `__delta_internal_is_row_deleted` (Int32, 0/1): whether the row is masked
  by a DV
- `row_id` (Int64): Delta row-tracking row ID
- `row_commit_version` (Int64): Delta row-tracking commit version

For each detected synthetic an `emit_*` flag is set on `DeltaScanCommon`, and
the planner sets `synthesize_in_worker = true`. In synthesize mode (the only
native path now, #82) `DeltaKernelScanExec` produces the **entire** output
itself — data, partitions, and every synthetic — assembling the output schema
BY NAME (`row_index` / `row_id` arrive as kernel metadata columns in the read;
`is_row_deleted` by inverting the decoded DV; `row_commit_version` and the
Spark `_metadata.*` columns as per-file constants). There is no separate
synthetic-columns exec stacked on a parquet scan — that
`DeltaSyntheticColumnsExec` was deleted. See
[03-native-execution.md](03-native-execution.md).

Because the executor assembles output by name, `required_schema` in the proto
IS the full output (`scan.output`, synthetics not stripped) and
`final_output_indices` is left empty — no positional reorder is needed. (The
`final_output_indices` permutation machinery and the non-synthesize
`computeFinalOutputIndices` branch survive in the serde for completeness, but
are unused on the current synthesize path.)

## Column mapping

Delta supports three column-mapping modes:

- `none`: physical parquet column names match logical
- `name`: physical names are random strings (`col-1a2b…`); a JSON map in
  the table schema maps logical → physical
- `id`: physical names are random strings; parquet field IDs identify columns

Column mapping (both `name` and `id`, including nested struct/array/map element
fields) is resolved **by kernel**, not by Comet (#76). The driver projects the
snapshot and ships kernel's physical and logical Arrow schemas
(`kernel_physical_schema` / `kernel_logical_schema`) on `DeltaScanCommon`;
those schemas carry the correct physical names and `PARQUET:field_id`s at every
nesting level. The executor hands them verbatim to kernel's reader, which
matches file columns by field-id and applies the per-file `transform_json`
(kernel's physical→logical expression) to relabel to logical names. The old
Comet-side column-mapping physicalisation (rewriting logical names into a
`physicalSchema`, translating `delta.columnMapping.id` to `PARQUET:field_id`,
and a post-read rename `ProjectionExec`) has been dropped. Nested column
mapping (#47) and zero-data-column reads (#48) work.

## DPP and partition pruning

If the plan above includes a `DynamicPruningExpression`, the actual partition
values aren't known at plan time — they arrive after the broadcast side of a
join finishes. `CometDeltaNativeScanExec` carries `dppFilters` as a
constructor field, and re-applies them against `allTasks` inside
`doExecuteColumnar` (not in a `lazy val`, so the broadcast result is fresh
on each execution). The resulting per-partition task list is then proto-
serialised through the same encoding path as the static case.

## What's serialised vs computed at execute time

| Computed at plan time (driver)                      | Computed at execute time (driver, lazy) |
| --------------------------------------------------- | --------------------------------------- |
| Schema resolution                                   | DPP-resolved partition values           |
| Gate evaluation                                     | kernel-rs file list (`allTasks`)        |
| Kernel physical/logical schema shipping             | per-partition task byte arrays          |
| Synthetic-column detection + `synthesize_in_worker` | task packing into partitions            |
| `emit_*` flags                                      | proto encoding                          |

Nothing in this list happens per-batch on the executor; the executor's only
job is to deserialise the proto and run the resulting DataFusion plan.

---

**Navigation** · [← 01 Overview](01-overview.md) · [↑ Index](README.md) · Next → [03 Native execution](03-native-execution.md)

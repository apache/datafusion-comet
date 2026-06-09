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

# Native execution: from proto bytes to Arrow batches

The Delta read path executes natively through **delta-kernel-rs** ("kernel-read",
the only read path). Each Delta data file is read through kernel's own
primitives — kernel does the parquet read, applies the physical→logical
transform (column mapping incl. nested, partition injection, row-tracking), and
applies the deletion vector — and `DeltaKernelScanExec` synthesises any
remaining Delta virtual columns in-worker. The companion design doc for the
kernel-read refactor is
[10-iceberg-style-kernel-read.md](10-iceberg-style-kernel-read.md); a coherence
audit is in [11-kernel-read-coherence-audit.md](11-kernel-read-coherence-audit.md).

## Entry point

When a Spark executor runs its partition, Comet's native exec framework decodes
the operator proto and hits the `OpStruct::DeltaScan` arm. The dispatcher in
core is a thin shim:

```
native/core/src/execution/planner/delta_scan.rs   (~72 lines, #[cfg(feature = "contrib-delta")])
```

It only computes the requested + partition Arrow schemas (core owns the
proto→arrow schema converter) and delegates everything Delta-specific to the
contrib crate:

```
comet_contrib_delta::planner::plan_delta_scan   (contrib/delta/native/src/planner.rs)
```

The shim lives in core (not the contrib crate) because the schema converter is
core's `pub(crate)` and a `contrib → core` dependency would cycle with core's
optional `contrib-delta` dep. Default builds carry zero Delta surface.

`plan_delta_scan` (contrib) builds the `ExecutionPlan`. The rest of this
document walks through that.

> Kernel enumeration of the snapshot's scan files happens on the **driver** via
> the `Native.planDeltaScan` JNI entry point (Scala class
> `org.apache.comet.contrib.delta.Native`, Rust symbol
> `Java_org_apache_comet_contrib_delta_Native_planDeltaScan` in
> `contrib/delta/native/src/jni.rs`), which returns a `DeltaScanTaskList`
> proto. The executor never reconstructs the kernel `Snapshot`; it works from
> the serializable per-file inputs the driver shipped (path / size / DV
> descriptor / partition values / `transform_json`) plus the kernel schemas on
> `DeltaScanCommon`.

## The plan

`plan_delta_scan` builds the kernel-read `ExecutionPlan`. It:

1. Splits `required_schema` (data ++ partition, in order) into data fields and
   partition fields by partition-column name.
2. Decodes the kernel-built physical / logical Arrow schemas from
   `common.kernel_physical_schema` / `kernel_logical_schema` (Arrow IPC), and
   sets `needs_transform` when any physical name diverges from its logical name
   (column mapping is active). For a zero-data-column or CDF read both schemas
   are empty.
3. Maps each `DeltaScanTask` into a `KernelScanFile` (path, size, record count,
   DV descriptor, partition values, `transform_json`, base row id, default row
   commit version, byte range).
4. Builds the storage config (object-store options, S3 bucket for per-bucket
   credentials).
5. Constructs a single `DeltaKernelScanExec`, in synthesise mode
   (`common.synthesize_in_worker`), and — only if `common.final_output_indices`
   is non-empty — wraps it in a `ProjectionExec` to reorder columns. On the
   current synthesise path the executor assembles output by name, so
   `final_output_indices` is empty and no reorder projection is added.

There is **no** stacked synthetic-columns exec and **no** DV-filter exec in the
current design: `DeltaSyntheticColumnsExec` was deleted (#82). The single
`DeltaKernelScanExec` produces the full output itself.

## `DeltaKernelScanExec`

Source: `contrib/delta/native/src/kernel_scan.rs`.

`DeltaKernelScanExec` is an `ExecutionPlan` that reports
`Partitioning::UnknownPartitioning(1)` — it executes a single DataFusion
partition. The Spark side (`CometDeltaNativeScanExec`) splits files across Spark
partitions and injects each partition's subset via `DeltaPlanDataInjector`, so
`self.files` already holds just this partition's files. For DV / row-tracking /
per-file `_metadata` reads the Spark side sets `oneTaskPerPartition`, injecting
one file per Spark partition, so row positions are computed against a single
file.

`execute()` reads its files eagerly (the blocking kernel reads run on the
calling task thread) and emits the collected batches through a
`RecordBatchStreamAdapter`. Output-row counts are recorded via
`BaselineMetrics`. There are three read modes:

### Regular read (synthesise mode)

`read_all_synthesize` reads each file via kernel and produces **all** output
columns BY NAME (`assemble_synthesized` / `assemble_output_batch`):

- **Data columns** come from the kernel read. Kernel matches file columns by
  `PARQUET:field_id` (carried on the shipped physical schema) and applies the
  per-file `transform_json` (kernel's physical→logical `Expression`) via
  `transform_to_logical`, so column-mapping relabel (incl. nested),
  partition injection, and row-tracking materialisation all come from kernel.
- **`__delta_internal_row_index`** and **`row_id`** arrive as kernel metadata
  columns in the read (carried in the physical/logical schemas + transform).
- **`__delta_internal_is_row_deleted`** is produced engine-side by inverting the
  decoded DV. When `is_row_deleted` is requested, all rows are kept and flagged;
  otherwise the DV rows are dropped inline.
- **`row_commit_version`** and the Spark **`_metadata.*`** columns
  (`file_path`, `file_name`, `file_size`, `file_block_start`,
  `file_block_length`, `file_modification_time`, `base_row_id`,
  `default_row_commit_version`) are per-file constants.

The DV is decoded executor-side from the serializable DV descriptor via
`dv_reader::read_dv_indexes` (NOT from a kernel `DvInfo`, which is not
serializable). Because kernel pins the same Arrow version as Comet (arrow-58),
the kernel batch IS a Comet batch — no cross-version bridge.

A **zero-data-column read** (partition-only count, literal projection) reads no
parquet: the row count is driven from `record_count` (Delta `numRecords`, with a
parquet-footer fallback), an empty-data batch of that length is emitted, and the
by-name assembler fills partition + per-file-constant synthetics.

### By-name output assembly

`assemble_output_batch` builds the final `output_schema` batch (data ++
partition, Spark order) from a kernel batch. For each output field it:

- takes the matching column from the kernel batch when present (data column, or
  a partition column the shipped transform already injected), casting to the
  output field's exact type **including nested field names** — kernel names
  nested fields by Spark convention (e.g. a list element `"element"`), but the
  proto's `output_schema` may carry empty/different nested names, so the cast is
  a metadata-only relabel that is a no-op when types match;
- otherwise fills a partition column as a typed constant from
  `partition_values` (typed NULL when absent);
- otherwise errors (a requested data column kernel didn't produce).

This is why column ordering needs no positional `ProjectionExec`: assembly is by
name in `output_schema` order.

### Change Data Feed read

A CDF read (`common.cdf_read`) has no per-file tasks. `read_all_cdf` reconstructs
kernel's `TableChanges(start, end)` from `cdf_start_version` / `cdf_end_version`
and calls `TableChanges::execute()`, then reconciles each batch to
`output_schema` by name (kernel emits data + partitions + `_change_type` /
`_commit_version` / `_commit_timestamp`). See the CDF interception path below.

## Change Data Feed interception (JVM side)

`readChangeFeed` produces a `RowDataSourceScanExec` over Delta's
`DeltaCDFRelation`. `CometExecRule` matches it by relation class name
(`DeltaIntegration.isCdfRelation`) and routes it through
`DeltaIntegration.transformCdf` → `CometDeltaNativeScan.convertCdf` →
`CometDeltaCdfScanExec`, which drives `TableChanges` natively. The inclusive
`[start, end]` range is split into up to `spark.comet.delta.cdf.maxPartitions`
(default 8) contiguous sub-ranges, each read by an independent native
`TableChanges` call as one Spark partition — staying a single `CometNativeExec`
(not a `CometUnionExec`, which would make a downstream native shuffle
ineligible). CDF is read kernel-natively; it is NOT declined and was never
handled via a synthetic-columns exec.

## The output stream

What leaves the topmost exec is an `Arc<dyn ExecutionPlan>` whose `execute()`
returns an Arrow `RecordBatchStream`. Comet's existing native executor framework
consumes that stream, moves the batches across JNI into the JVM via the Arrow C
Data Interface, and hands them to Spark as `ColumnarBatch`es.

There is nothing Delta-specific in the cross-JNI machinery. As far as the JVM is
concerned, the result looks like any other Comet native scan.

## Error handling at the native edge

Failures at any layer (parquet decode error, DV file checksum mismatch,
schema-adaptor mismatch) propagate up as DataFusion `DataFusionError`s. Native
parquet read failures are surfaced as a typed `SparkError::CannotReadFile`
(rather than string-matching), which the JVM side wraps with
`FAILED_READ_FILE.NO_HINT` including the file path — matching Spark's standard
error surface for parquet read failures.

If the failure happens at the kernel-rs layer on the driver (during
`planDeltaScan` enumeration), we never get to native execution. The contrib's
`transformV1IfDelta` throws; `DeltaIntegration` logs a warning and declines via
`withFallbackReason`, falling back to Spark's Delta reader. See
[06-fallback-and-ops.md](06-fallback-and-ops.md) for the full catalogue.

## What this stack does NOT do

- **No vectorised expression evaluation here.** Anything above the scan (joins,
  aggregates, post-scan projections from the user's query) goes through Comet's
  regular operator stack, not this contrib.
- **No write-side anything.** No commit logic, no `_delta_log` writes, no
  protocol upgrade checks. Reads only. (DML rewrites — DELETE/UPDATE/MERGE — use
  this read path via Spark's regenerated plans, but the write back to
  `_delta_log` is Delta's code.)
- **No streaming-source semantics.** Each batch read resolves to a single Delta
  snapshot version (CDF reads a version range). Structured Streaming's
  `DeltaSource`/`DeltaSink` paths fall back to Spark.

---

**Navigation** · [← 02 Planning](02-planning.md) · [↑ Index](README.md) · Next → [04 Design decisions](04-design-decisions.md)

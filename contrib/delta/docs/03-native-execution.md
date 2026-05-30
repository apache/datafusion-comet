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

## Entry point

When a Spark executor processes its partition, it calls into JNI with the
encoded proto. The relevant symbol is

```
Java_org_apache_comet_Native_planDeltaScan
```

declared in `contrib/delta/native/src/jni.rs`. The function:

1. Decodes the `DeltaScan` proto into `(DeltaScanCommon, Vec<DeltaScanTask>)`
2. Calls `build_plan` in `native/core/src/execution/planner/contrib_delta_scan.rs`
3. Returns a pointer to the `Arc<dyn ExecutionPlan>` to be wired into Comet's
   existing native executor framework

`build_plan` is where the wrapping stack gets assembled. The rest of this
document walks through that stack.

## The wrapping stack

Conceptually:

```
DataSourceExec(ParquetSource over the file list)
    ↓  (optional, if column mapping mode requires it AND physical/logical names differ)
ProjectionExec  (physical → logical rename)
    ↓  (exactly one of the two, depending on what the surrounding plan asks for)
DeltaSyntheticColumnsExec    ── if any emit_* flag is set
        ── OR ──
DeltaDvFilterExec            ── else if any task has a DV
    ↓  (optional, if synthetics are present and not a suffix of required_schema)
ProjectionExec  (reorder via final_output_indices)
```

Each layer is added only when needed; the simplest case (no DV, no CM, no
synthetics) is just `DataSourceExec`.

The synthetic-columns exec and the DV-filter exec are **mutually exclusive**.
This is intentional: when synthetics are emitted, the surrounding Delta plan
(UPDATE/DELETE/MERGE rewrite) wants `is_row_deleted` populated and ALL rows
kept so it can decide what to do with each row itself. When synthetics are
NOT emitted, the standard read path wants deleted rows dropped inline. The
two needs never coincide.

### Layer 1: `ParquetSource`

This is DataFusion's existing parquet reader. We build a `FileScanConfig`
from the per-task file lists, passing:

- Partition values as `wrap_partition_value_in_dict` columns
- Pushable filters as `PhysicalExpr` (already translated by the planner)
- `with_field_id(true)` when `common.use_field_id` is set, so the reader
  matches by `PARQUET:field_id` rather than by name

**FileGroup layout**. When any `emit_*` flag is on (or any task carries a
DV), every file gets its own `FileGroup`. This matters because both
`DeltaSyntheticColumnsExec` and `DeltaDvFilterExec` index per-partition
state vectors (deleted-row indexes, base row IDs, commit versions) by the
DataFusion partition index passed to `execute()`. Each `FileGroup` becomes
one partition, so one-file-per-group is what makes "partition index = file
index" hold.

When neither synthetics nor DVs are involved, files can pack into shared
groups for better parallelism.

### Layer 2: column-mapping rename projection (runs BEFORE either layer 3 or 4)

When `column_mapping_mode = "name"` (or `id` if physical names still differ),
the parquet read produced columns under their physical names (e.g.
`col-1a2b3c`). The downstream layers expect *logical* names. We insert a
`ProjectionExec` that renames physical → logical right after the parquet
source.

The rename runs before synthetics for two reasons:

- Synthetic columns have fixed names (`row_id`, `__delta_internal_*`)
  that are never CM-renamed; we want the parquet output already in logical
  form when we append synthetics
- The synthetic exec's input-schema check uses logical names; running rename
  first makes that check correct

### Layer 3a: `DeltaDvFilterExec` (when no synthetics are requested)

If any task in the partition has a non-empty `deleted_row_indexes` (computed
by kernel-rs on the driver from the DV file) AND no `emit_*` flag is set, we
wrap with this filter exec. It:

1. Maintains a `current_row_offset: u64` across batches (assumes
   physical-order input)
2. For each incoming batch, walks the sorted `deleted_row_indexes` and builds
   a `BooleanArray` mask
3. Returns the masked batch (or skips empty batches entirely)

Two safeguards:

- `maintains_input_order() = [true]` — declares to optimisers that we depend
  on input order
- `benefits_from_input_partitioning() = [false]` — declares that we don't
  want a `RepartitionExec` inserted upstream

Without these, a future optimiser rule that inserts a repartition above the
parquet source would silently reshuffle rows and the offset-based filter
would produce garbage. The DV filter would still "work" without errors —
it'd just delete the wrong rows.

### Layer 3b: `DeltaSyntheticColumnsExec` (when any emit flag is set)

This is the most Delta-specific piece. Source: `contrib/delta/native/src/synthetic_columns.rs`.

The exec appends up to four columns onto the parquet output:

| Column | Type | How it's computed |
|---|---|---|
| `__delta_internal_row_index` | UInt64 | per-file row counter, starts at 0, increments by batch size |
| `__delta_internal_is_row_deleted` | Int32 (0/1) | walks the per-task DV sorted indexes against the current row offset |
| `row_id` | Int64 | `task.base_row_id + physical_row_index` (per file) |
| `row_commit_version` | Int64 | `task.default_row_commit_version` (constant per file) |

State is per **DataFusion partition**, not per file. Each `FileGroup` becomes
its own DataFusion partition (because `need_per_file_groups = true` whenever
any `emit_*` flag is set — see Layer 1), so each file gets its own
`execute(partition_idx, ...)` call with a fresh `DeltaSyntheticColumnsStream`.
Inside that stream the state is `{current_row_offset, next_delete_idx,
base_row_id, default_row_commit_version}`, all looked up by `partition_idx`
from the per-partition vectors threaded down by the planner. After each
batch:

- `current_row_offset += batch.num_rows()`
- `next_delete_idx` advances past any DV indexes consumed in this batch
  (this writeback was a review-fix — earlier versions re-walked from 0 each
  batch)

There is no explicit "task finished" reset. State doesn't need to reset
because each partition's stream object only ever sees rows from one file.

**Why we synthesise rather than read from a materialised column**. Delta
*can* materialise `row_id` / `row_commit_version` into the parquet files at
write time, in which case we'd just read them directly. But Delta only
materialises them when row tracking has been on since the file was written —
files written before row tracking was enabled have a `baseRowId` table-level
constant and we must compute `row_id` arithmetically. Our path covers both
cases uniformly: the planner sets `emit_row_id = true` only when materialisation
is NOT available; when materialisation IS available, the column comes through
the parquet read like any other column.

### Layer 4: reorder projection

`required_schema` might want synthetics in non-suffix positions. The driver
computed `final_output_indices` (a permutation of `[0..n]`) and put it in the
proto. If the indices aren't the identity, we wrap with a final
`ProjectionExec` that reorders columns. Identity → skip.

The driver's assertion `assert(emitIdx >= 0)` ensures we never compute an
out-of-bounds permutation; if a synthetic is in `required_schema` but its
emit flag wasn't set (somehow), we fail fast on the JVM side rather than
producing wrong output natively.

## The output stream

What leaves the topmost exec is an `Arc<dyn ExecutionPlan>` whose
`execute()` returns an Arrow `RecordBatchStream`. Comet's existing native
executor framework consumes that stream, moves the batches across JNI into
the JVM via the Arrow C Data Interface, and hands them to Spark as
`ColumnarBatch`es.

There is nothing Delta-specific in the cross-JNI machinery. As far as the
JVM is concerned, the result looks like any other Comet native scan.

## Error handling at the native edge

Failures at any layer (parquet decode error, DV file checksum mismatch,
schema-adaptor mismatch) propagate up as DataFusion `DataFusionError`s and
are converted to Java `RuntimeException`s by the JNI shim. The JVM-side
wrapper in `ShimSparkErrorConverter.wrapNativeParquetError` recognises
parquet-flavoured errors and wraps them with `FAILED_READ_FILE.NO_HINT`
including the file path — this matches Spark's standard error surface for
parquet read failures.

If the failure happens at the kernel-rs layer on the driver (during plan
construction), we never get to native execution. The planner catches the
error, calls `withInfo(plan, "delta-kernel-rs error: …")`, and falls back
to Spark's Delta reader. See [06-fallback-and-ops.md](06-fallback-and-ops.md) for the full
catalogue.

## What this stack does NOT do

- **No vectorised expression evaluation here.** Filters that get pushed
  into `ParquetSource` use DataFusion's PhysicalExpr, but anything above
  the scan (joins, aggregates, post-scan projections from the user's
  query) goes through Comet's regular operator stack, not this contrib.
- **No write-side anything.** No commit logic, no `_delta_log` writes,
  no protocol upgrade checks. Reads only.
- **No streaming-source semantics.** Each plan invocation resolves to a
  single Delta snapshot version. Structured Streaming's
  `DeltaSource`/`DeltaSink` paths fall back to Spark.

---

**Navigation** · [← 02 Planning](02-planning.md) · [↑ Index](README.md) · Next → [04 Design decisions](04-design-decisions.md)

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

# Elimination evaluation: can the custom code rely on kernel / are the core changes still needed?

After the kernel-read refactor (delta-kernel 0.24 / arrow-58, legacy ParquetSource path
deleted), two questions were raised: (1) can each remaining **custom (non-kernel) piece**
in the contrib be dropped in favour of relying on delta-kernel directly, and (2) is each
**core change** the contrib originally required still needed? This is the evaluation.

## Headline

- **Custom pieces (kernel-path):** the surface shrank further since this evaluation was
  first written. Column-mapping **physicalisation** was dropped (#76: kernel ships its own
  `physical_schema()`/`logical_schema()` and `transform_to_logical` relabels), and the
  separate `DeltaSyntheticColumnsExec` was **deleted** (#82: `DeltaKernelScanExec` now
  synthesizes all output columns in-worker). What remains (INT96 custom read +
  `align_batch_to_schema`, the `dv_reader` DV decode, in-worker synthesis, partition
  injection) is blocked on the same two upstream delta-kernel gaps — there is no
  reader-options / INT96 hook, and the per-file scan data (`ScanFile` / `ScanMetadata` /
  transform `Expression` / `DvInfo`) is not serializable, so we can't ship it over JNI and
  call kernel's `Scan::execute` executor-side. **Two upstream asks** would collapse most of
  it (below).
- **Core changes (the 8 extracted PRs + Delta hooks):** **all stay.** None were
  old-path-only. Four are general Comet fixes; the rest are still exercised by the
  kernel-read path.
- **Removable dead code: already removed.** The dead proto fields + Scala emission left
  behind by the #50 old-path deletion (`data_schema`, `projection_vector`, `data_filters`
  on `DeltaScanCommon`, plus the `physicalFileDataSchema` / `physicaliseRequiredField`
  emission and the `build_delta_partitioned_files` / `ColumnMappingFilterRewriter` planner
  helpers) were removed in #71. The proto numbers are now `reserved` (operator.proto
  `reserved 2, 4, 5` in `DeltaScanCommon`, `reserved 13` for the old `column_mappings`).

## Cluster 1 — custom kernel-path pieces (#55–61)

Evidence cross-checked against delta-kernel 0.24 source.

| #   | Custom piece                                                                                                              | Verdict                               | Why it can't be delegated yet                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| --- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 55  | INT96 custom read (`read_file_via_kernel` + `coerce_int96_to_micros`)                                                     | **KEEP — blocked upstream**           | Kernel's `ParquetHandler::read_parquet_files` exposes no `ArrowReaderOptions` / INT96-coercion / supplied-schema hook (`engine/mod.rs` `reader_options()` is fixed). INT96 overflows i64-nanos at read, so it must be coerced _during_ the read.                                                                                                                                                                                                                                                                                                           |
| 56  | `align_batch_to_schema`                                                                                                   | **KEEP — blocked by #55**             | This is the fixup (reorder/cast/null-fill) kernel's reader does internally via `fixup_parquet_read` (`pub(crate)`). It's only redundant if we use kernel's reader — which #55 prevents. Falls away the moment we can delegate the read.                                                                                                                                                                                                                                                                                                                    |
| 57  | `dv_reader` DV decode                                                                                                     | **KEEP — blocked upstream**           | `DvInfo` doesn't derive Serialize/Deserialize and `DeletionVectorDescriptor.deletion_vector` is `pub(crate)`, so the DV can't cross JNI / be rebuilt executor-side. We decode from our own serializable proto descriptor via the public `descriptor.row_indexes()`.                                                                                                                                                                                                                                                                                        |
| 58  | in-worker synthesis (row-tracking / `_metadata` / `is_row_deleted`) in `DeltaKernelScanExec`                              | **KEEP (slimmed) — blocked upstream** | The separate `DeltaSyntheticColumnsExec` is **deleted** (#82); `DeltaKernelScanExec` now produces all output columns by name in `synthesize` mode. `row_index` / `row_id` come from kernel metadata columns; `is_row_deleted` / `row_commit_version` / Spark `_metadata.*` are per-file constants assembled in-worker. Kernel has `FieldTransformSpec::GenerateRowId` / `MetadataDerivedColumn`, but `FieldTransformSpec` isn't serializable and Spark's `_metadata.*` virtual columns aren't a kernel transform output, so the assembly stays Comet-side. |
| 59  | partition injection (kernel `transform_to_logical`, fallback `append_partition_columns` / `parse_delta_partition_scalar`) | **KEEP — partly delegated**           | When the per-file `transform_json` is present kernel injects partitions via its transform (delegated); the Comet-side `append_partition_columns` is only the fallback for the identity-transform path. Full delegation still needs serializable per-file scan data executor-side.                                                                                                                                                                                                                                                                          |
| 60  | column-mapping physicalisation — **DROPPED (#76)**                                                                        | **REMOVED, delegated to kernel**      | The recursive `physicalise_field` schema-rebuild is gone. The driver projects the snapshot and ships kernel's own `Scan::physical_schema()` / `logical_schema()` (`scan.rs`, field-ids preserved); the executor relabels via `transform_to_logical`. Nested column mapping works (#47). `build_struct_column_mappings` survives only as a top-level logical→physical list carried driver-side on `DeltaScanTaskList`, not an executor physicalisation tree.                                                                                                |
| 61  | **umbrella:** per-file exec → `Scan::execute`                                                                             | **KEEP — blocked upstream**           | `Scan::execute` is driver-only (does log replay), and `ScanFile` / `ScanMetadata` / the transform `Expression` aren't serializable. This is the keystone: making them serializable folds #56/#59 and most of #58 into kernel (#60 is already delegated).                                                                                                                                                                                                                                                                                                   |

**Two upstream delta-kernel asks** (filing these unblocks the shrink):

1. **Reader options / INT96 coercion hook** on `ParquetHandler` (or a `read_parquet_files`
   variant accepting a supplied schema / `coerce_int96`). Unblocks #55 → #56.
2. **Serializable per-file scan data** — `ScanFile`, `ScanMetadata`, the transform
   `Expression`, and `DvInfo` deriving Serialize/Deserialize (and `deletion_vector` made
   `pub`), so the executor can reconstruct and call a kernel read directly. Unblocks
   #57/#59/#61 and most of #58 (#60 is already delegated to kernel's schemas).

Until then, the custom per-file pipeline is necessary, not incidental. The in-worker
synthesis (#58) likely stays partly regardless (Spark `_metadata` semantics + `is_row_deleted`
are Comet/Spark concerns kernel doesn't model).

## Cluster 2 — the 8 extracted core PRs (#62–69)

**All KEEP; none old-path-only.**

| #   | PR                                              | Verdict                       | Why                                                                                                               |
| --- | ----------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| 62  | #4523 GetStructField parent null mask           | KEEP — general                | nested-struct correctness for any plan                                                                            |
| 63  | #4524 non-UTF-8 `get_string` lossy              | KEEP — general                | shuffle serialization robustness                                                                                  |
| 64  | #4525 decline V1 scans on bad FS schemes        | KEEP — kernel-coupled         | the scheme check gates both V1 parquet and Delta (kernel's object_store engine has the same supported-scheme set) |
| 65  | #4531 rebalance deep AND/OR                     | KEEP — general                | protobuf recursion-depth guard for any predicate                                                                  |
| 66  | #4532 materialize ConstantColumnVector          | KEEP — kernel-coupled         | partition/synthetic constants still cross to native; general export robustness                                    |
| 67  | #4533 decline CreateArray nullability-divergent | KEEP — general                | DataFusion `make_array` correctness for any plan                                                                  |
| 68  | #4535 O(1) PlanDataInjector                     | KEEP — kernel-coupled         | the kernel path still uses split-mode per-partition task injection (`DeltaPlanDataInjector`)                      |
| 69  | #4536 FAILED_READ_FILE                          | KEEP — **now kernel-coupled** | `map_file_read_error` emits `SparkError::CannotReadFile`; flipped from old-path to kernel-read dependency         |

## Cluster 3 — Delta core hooks (#70) and this session's proto/Scala (#71)

**#70 Delta core hooks — all KEEP** (they are how a Delta scan is detected and how its tasks
reach the native exec): `DeltaIntegration`, the `CometExecRule` Delta arm, `CometDeltaScanMarker`,
`DeltaPlanDataInjector` (split-mode injection), the `CometScanRule` Delta call, and the
`operators.scala` injector registration. `CometPlanAdaptiveDynamicPruningFilters` is
KEEP-but-shrinkable (its Delta arm is transport-agnostic; covered by `CometDeltaDppReproSuite`).

**#71 proto/Scala dead code — DONE (removed).** This evaluation originally flagged the #50
old-path residual for removal; #71 carried it out. The proto numbers are now `reserved` in
`DeltaScanCommon` (operator.proto: `reserved 2, 4, 5` covering the old `data_schema` /
`data_filters` / `projection_vector`, and `reserved 13` for the old `column_mappings`), and
the Scala emitters / planner helpers are gone:

| Item                                                                | State now                                                                                                    |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| proto `data_schema` / `data_filters` / `projection_vector` (Delta)  | **removed** — `reserved 2, 4, 5` in `DeltaScanCommon`                                                        |
| `physicalFileDataSchemaFields` / `physicaliseRequiredField` (Scala) | **removed** — no longer present in `CometDeltaNativeScan.scala`                                              |
| `physicalise_field` recursive schema rebuild (native `scan.rs`)     | **removed** (#76)                                                                                            |
| `build_delta_partitioned_files` / `ColumnMappingFilterRewriter`     | **removed** from `planner.rs` (only stale name mentions remain in `jni.rs` comments)                         |
| proto `kernel_read` (field 25)                                      | **removed** — `reserved 25` in operator.proto; kernel-read is unconditional, `planner.rs` no longer reads it |

> Note: the `data_schema` / `projection_vector` fields that still exist in operator.proto
> live on **`NativeScanCommon`** / **`CsvScan`** — unrelated messages, not Delta.

Still actively read by the contrib `plan_delta_scan` (keep): `required_schema` (pure-logical),
`partition_schema`, `kernel_physical_schema` / `kernel_logical_schema` (kernel's shipped
schemas), `synthesize_in_worker`, `final_output_indices`, `cdf_read` + version range,
`dv_file_name_prefix`, `session_timezone`, `table_root`, and `object_store_options`. The
top-level logical→physical `column_mappings` (no longer a recursive tree) is carried
driver-side on `DeltaScanTaskList`, not consumed executor-side.

With #71 landed, this evaluation no longer marks any code for removal — the kernel-read
surface is at its current floor pending the two upstream delta-kernel asks above.

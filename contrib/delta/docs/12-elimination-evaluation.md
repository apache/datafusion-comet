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

- **Custom pieces (kernel-path):** none can be removed *purely in our code today*. Almost
  all are blocked on the same two upstream delta-kernel gaps — there is no reader-options /
  INT96 hook, and the per-file scan data (`ScanFile` / `ScanMetadata` / transform
  `Expression` / `DvInfo`) is not serializable, so we can't ship it over JNI and call
  kernel's `Scan::execute` executor-side. The custom per-file pipeline exists precisely to
  work around these. **Two upstream asks** would collapse most of it (below).
- **Core changes (the 8 extracted PRs + Delta hooks):** **all stay.** None were
  old-path-only. Four are general Comet fixes; the rest are still exercised by the
  kernel-read path.
- **Only concrete removable code:** the dead proto fields + Scala emission left behind by
  the #50 old-path deletion (`data_schema`, `projection_vector`, `data_filters`, and the
  `physicalFileDataSchema` / `physicaliseRequiredField` emission that feeds them).

## Cluster 1 — custom kernel-path pieces (#55–61)

Evidence cross-checked against delta-kernel 0.24 source.

| # | Custom piece | Verdict | Why it can't be delegated yet |
|---|---|---|---|
| 55 | INT96 custom read (`read_file_via_kernel` + `coerce_int96_to_micros`) | **KEEP — blocked upstream** | Kernel's `ParquetHandler::read_parquet_files` exposes no `ArrowReaderOptions` / INT96-coercion / supplied-schema hook (`engine/mod.rs` `reader_options()` is fixed). INT96 overflows i64-nanos at read, so it must be coerced *during* the read. |
| 56 | `align_batch_to_schema` | **KEEP — blocked by #55** | This is the fixup (reorder/cast/null-fill) kernel's reader does internally via `fixup_parquet_read` (`pub(crate)`). It's only redundant if we use kernel's reader — which #55 prevents. Falls away the moment we can delegate the read. |
| 57 | `dv_reader` DV decode | **KEEP — blocked upstream** | `DvInfo` doesn't derive Serialize/Deserialize and `DeletionVectorDescriptor.deletion_vector` is `pub(crate)`, so the DV can't cross JNI / be rebuilt executor-side. We decode from our own serializable proto descriptor via the public `descriptor.row_indexes()`. |
| 58 | `DeltaSyntheticColumnsExec` (row-tracking / `_metadata` / `is_row_deleted`) | **KEEP — blocked upstream** | Kernel *has* `FieldTransformSpec::GenerateRowId` / `MetadataDerivedColumn`, but `FieldTransformSpec` isn't serializable, the transform `Expression` is a non-serializable `Arc`, and Spark's `_metadata.*` virtual columns aren't a kernel transform output. Output-shape concern that crosses the executor boundary. |
| 59 | partition injection (`append_partition_columns` / `parse_delta_partition_scalar`) | **KEEP — blocked by #58/#61** | Kernel injects partitions via `MetadataDerivedColumn` in the transform — but only reachable if we can ship a serializable transform and run kernel's evaluator executor-side. |
| 60 | column-mapping physicalisation (`scan.rs` mapping tree + `physicalise_field`) | **KEEP — blocked by #61** | Kernel already computes the physical/logical schemas (`Scan::physical_schema()` / `logical_schema()`) and the rename transform internally; we rebuild them only because we can't ship kernel's `ScanMetadata` to the executor. |
| 61 | **umbrella:** per-file exec → `Scan::execute` | **KEEP — blocked upstream** | `Scan::execute` is driver-only (does log replay), and `ScanFile` / `ScanMetadata` / the transform `Expression` aren't serializable. This is the keystone: making them serializable folds #56/#59/#60 and most of #58 into kernel. |

**Two upstream delta-kernel asks** (filing these unblocks the shrink):
1. **Reader options / INT96 coercion hook** on `ParquetHandler` (or a `read_parquet_files`
   variant accepting a supplied schema / `coerce_int96`). Unblocks #55 → #56.
2. **Serializable per-file scan data** — `ScanFile`, `ScanMetadata`, the transform
   `Expression`, and `DvInfo` deriving Serialize/Deserialize (and `deletion_vector` made
   `pub`), so the executor can reconstruct and call a kernel read directly. Unblocks
   #57/#59/#60/#61 and most of #58.

Until then, the custom per-file pipeline is necessary, not incidental. `DeltaSyntheticColumnsExec`
likely stays partly regardless (Spark `_metadata` semantics + `is_row_deleted` are Comet/Spark
concerns kernel doesn't model).

## Cluster 2 — the 8 extracted core PRs (#62–69)

**All KEEP; none old-path-only.**

| # | PR | Verdict | Why |
|---|---|---|---|
| 62 | #4523 GetStructField parent null mask | KEEP — general | nested-struct correctness for any plan |
| 63 | #4524 non-UTF-8 `get_string` lossy | KEEP — general | shuffle serialization robustness |
| 64 | #4525 decline V1 scans on bad FS schemes | KEEP — kernel-coupled | the scheme check gates both V1 parquet and Delta (kernel's object_store engine has the same supported-scheme set) |
| 65 | #4531 rebalance deep AND/OR | KEEP — general | protobuf recursion-depth guard for any predicate |
| 66 | #4532 materialize ConstantColumnVector | KEEP — kernel-coupled | partition/synthetic constants still cross to native; general export robustness |
| 67 | #4533 decline CreateArray nullability-divergent | KEEP — general | DataFusion `make_array` correctness for any plan |
| 68 | #4535 O(1) PlanDataInjector | KEEP — kernel-coupled | the kernel path still uses split-mode per-partition task injection (`DeltaPlanDataInjector`) |
| 69 | #4536 FAILED_READ_FILE | KEEP — **now kernel-coupled** | `map_file_read_error` emits `SparkError::CannotReadFile`; flipped from old-path to kernel-read dependency |

## Cluster 3 — Delta core hooks (#70) and this session's proto/Scala (#71)

**#70 Delta core hooks — all KEEP** (they are how a Delta scan is detected and how its tasks
reach the native exec): `DeltaIntegration`, the `CometExecRule` Delta arm, `CometDeltaScanMarker`,
`DeltaPlanDataInjector` (split-mode injection), the `CometScanRule` Delta call, and the
`operators.scala` injector registration. `CometPlanAdaptiveDynamicPruningFilters` is
KEEP-but-shrinkable (its Delta arm is transport-agnostic; covered by `CometDeltaDppReproSuite`).

**#71 proto/Scala — concrete removable dead code** (the #50 deletion residual). These are
populated by the Scala but **never read by the native kernel path**:

| Item | Set (Scala) | Read (native) | Action |
|---|---|---|---|
| proto `data_schema` (field 2) | `CometDeltaNativeScan.scala` ~1015 | none | **remove** |
| proto `projection_vector` (field 5) | ~1094 | none | **remove** |
| proto `data_filters` (field 4) | `addPushedDataFilters` | none (kernel does its own log-replay pruning) | **remove** |
| `physicalFileDataSchemaFields` (Scala) | feeds `data_schema` | — | **remove** |
| `physicaliseRequiredField` / `physicaliseDataTypePreserving` (Scala) | feed the above | — | shrink/remove once `data_schema` is gone |
| proto `kernel_read` (field 25) | always true now | `delta_scan.rs` errors if false | keep (or fold into deltaNative.enabled later) |

`required_schema` (pure-logical), `column_mappings` (recursive), `partition_schema`, the
synthesis flags, `final_output_indices`, `session_timezone`, `table_root`, and
`object_store_options` are all actively read by `plan_delta_kernel_scan` — keep.

This #71 cleanup is the natural completion of the #50 old-path deletion and is the only
code this evaluation marks for removal. It also overlaps the still-pending `planner.rs`
dead helpers (`build_delta_partitioned_files`, `ColumnMappingFilterRewriter`) noted in the
#50 commit.

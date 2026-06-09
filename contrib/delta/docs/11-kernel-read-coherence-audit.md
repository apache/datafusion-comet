# Kernel-read design-coherence audit

This audit checks each design decision in the iceberg-style kernel-read path
(see `10-iceberg-style-kernel-read.md`) against two references:

- **Iceberg** — Comet's `IcebergScanExec` (`native/core/src/execution/operators/iceberg_scan.rs`),
  the sibling "read the files ourselves and hand the batches back into a Comet
  plan" integration.
- **The rest of Comet** — the main parquet path (`native/core/src/parquet/`:
  `parquet_exec.rs`, `schema_adapter.rs`, `cast_column.rs`, `parquet_support.rs`)
  and the structured-error plumbing (`native/common/src/error.rs`, the
  `ShimSparkErrorConverter` shims).

The goal is coherence and reuse: every decision should either reuse an existing
Comet mechanism or have a recorded reason it can't.

## Verdict

The kernel-read path is **coherent with both references**. It reuses Comet's
existing pieces wherever the arrow-version boundary allows, and the two places it
"goes custom" (the parquet read and the pre-bridge schema reconciliation) match
exactly what Iceberg and the main parquet path already do. One cleanup item fell
out of the INT96 work (dead field-id remap); it is recorded for the deletion
phase (#50).

## Decision-by-decision

| Decision                                                          | Reference behaviour                                                                                           | Kernel-read path                                                                                                                  | Coherent?                                                                                                 |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Read the data files ourselves** (not via Comet's `ParquetExec`) | Iceberg reads via `iceberg::arrow::ArrowReaderBuilder` then hands batches back (`iceberg_scan.rs`)            | `read_file_via_kernel` reads via kernel's storage handler + arrow-57 parquet reader                                               | ✅ Same pattern as Iceberg                                                                                |
| **INT96 → microseconds**                                          | Main path sets `coerce_int96="us"` (`parquet_exec.rs:208`)                                                    | `coerce_int96_to_micros` + `ArrowReaderOptions::with_schema` (ported from DataFusion's `coerce_int96_to_resolution` for arrow-57) | ✅ Same semantics as the main path                                                                        |
| **Schema reconciliation** (reorder / cast / null-fill)            | Main path + Iceberg use `SparkSchemaAdapter` / `SparkPhysicalExprAdapterFactory` (`schema_adapter.rs`)        | `align_batch_to_schema`, reorder by name + `cast` + `new_null_array`                                                              | ✅ Small standalone helper — see note below                                                               |
| **Arrow version bridge**                                          | No precedent (Comet is single-version arrow-58; Iceberg's reader is arrow-58 too)                             | **None** — delta-kernel 0.24 shares Comet's arrow-58, so kernel `RecordBatch`es are Comet batches                                 | ✅ No bridge needed (was an FFI transmute under kernel 0.19's arrow-57; deleted when the kernel upgraded) |
| **Read-error → `FAILED_READ_FILE`**                               | Main path emits typed `SparkError::CannotReadFile`; shim → `cannotReadFilesError` (#4536, `error.rs:541/636`) | `map_file_read_error` emits the same typed error (sibling to the DV path's `map_dv_error_to_datafusion`)                          | ✅ Same typed-error mechanism                                                                             |
| **Deletion vectors**                                              | —                                                                                                             | Reuses the existing `dv_reader` (decoded selection vector, not a non-serializable `DvInfo`)                                       | ✅ Reuse                                                                                                  |
| **Row-tracking / `_metadata`**                                    | —                                                                                                             | Reuses `DeltaSyntheticColumnsExec` stacked on the kernel exec (composition, like the old path stacked it on the parquet read)     | ✅ Reuse                                                                                                  |
| **Partition columns**                                             | —                                                                                                             | Reuses `parse_delta_partition_scalar`; splits `required_schema` into data (read) + partition (injected)                           | ✅ Reuse                                                                                                  |
| **Column-mapping transform**                                      | —                                                                                                             | Reuses kernel's `transform_to_logical` (identity `Transform` relabels physical→logical via the schema pair)                       | ✅ Reuse                                                                                                  |
| **Single output partition**                                       | —                                                                                                             | One DataFusion partition; the Spark side (`CometDeltaNativeScanExec.oneTaskPerPartition`) does per-file splitting                 | ✅ Matches the existing Spark-side split model                                                            |

### Note: why `align_batch_to_schema` stays a small standalone helper

Comet's `SparkSchemaAdapter` / `SparkPhysicalExprAdapterFactory` /
`CometCastColumnExpr` / `spark_parquet_convert` stack is the canonical
schema-reconciliation machinery, and Iceberg reuses it (the
`IcebergStreamWrapper` runs `SparkPhysicalExprAdapterFactory` over every batch).

`align_batch_to_schema` covers a deliberately small slice of that: reorder by
name, `cast` to the target type (which also covers the `Timestamp(us, None) →
Timestamp(us, "UTC")` tz relabel — verified by
`align_batch_to_schema_reorders_and_casts`), and null-fill schema-evolution
columns. It runs **before** kernel's `transform_to_logical` (which does the
column-mapping relabel / partition injection / row-tracking), so it operates on
the physical batch in `physical_schema` layout.

Since the kernel upgrade to 0.24, kernel and Comet share arrow-58, so there is
no longer a version boundary preventing reuse of `SparkSchemaAdapter` here — the
adapter is a candidate to replace this helper if richer reconciliation is needed
(nested-struct field selection, case-insensitive / field-id matching). For the
current top-level scope it is heavier than warranted (it is `PhysicalExpr`- and
`FileScanConfig`-oriented), so the small helper stays. When nested column
mapping lands (#47), reuse `SparkSchemaAdapter` / model the additions on
`parquet_support.rs::parquet_convert_struct_to_struct` rather than re-inventing.

## Resolved: arrow bridge + field-id remap (kernel 0.24 upgrade)

The original audit flagged the field-id remap in `arrow_bridge.rs`
(`with_kernel_field_ids`, `numericize_field_ids`) as dead at read time: the INT96
fix replaced kernel's parquet reader (which matched columns by `parquet.field.id`
for id-mode) with our own read that projects by **physical name**, which is
correct because the driver renames the read schema to `cm.physical_name` (the
parquet column name, a unique UUID).

The kernel 0.24 / arrow-58 upgrade resolved this wholesale: `arrow_bridge.rs` —
the FFI transmute bridge **and** the now-dead field-id remap — was **deleted**,
along with the `object_store_kernel` 0.12 rename hack. The contrib read surface
no longer carries any arrow-version-bridging code or `unsafe`. The remaining
old-path deletion (the non-kernel `plan_delta_scan` branch, the physicalisation,
dead proto fields) is still tracked by #50.

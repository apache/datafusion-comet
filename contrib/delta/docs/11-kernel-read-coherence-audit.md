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

| Decision | Reference behaviour | Kernel-read path | Coherent? |
|---|---|---|---|
| **Read the data files ourselves** (not via Comet's `ParquetExec`) | Iceberg reads via `iceberg::arrow::ArrowReaderBuilder` then hands batches back (`iceberg_scan.rs`) | `read_file_via_kernel` reads via kernel's storage handler + arrow-57 parquet reader | ✅ Same pattern as Iceberg |
| **INT96 → microseconds** | Main path sets `coerce_int96="us"` (`parquet_exec.rs:208`) | `coerce_int96_to_micros` + `ArrowReaderOptions::with_schema` (ported from DataFusion's `coerce_int96_to_resolution` for arrow-57) | ✅ Same semantics as the main path |
| **Schema reconciliation** (reorder / cast / null-fill) | Main path + Iceberg use `SparkSchemaAdapter` / `SparkPhysicalExprAdapterFactory` (`schema_adapter.rs`), **arrow-58 only** | `align_batch_to_schema` (arrow-57), reorder by name + `cast` + `new_null_array` | ✅ Custom by necessity — see note below |
| **Arrow version bridge** | No precedent (Comet is single-version arrow-58; Iceberg's reader is arrow-58 too) | Arrow C Data Interface, `mem::transmute` between the two crates' ABI-identical FFI structs, size-asserted (`arrow_bridge.rs`) | ✅ Standard cross-version bridge; delta-kernel's arrow-57 pin is the only reason it's needed |
| **Read-error → `FAILED_READ_FILE`** | Main path emits typed `SparkError::CannotReadFile`; shim → `cannotReadFilesError` (#4536, `error.rs:541/636`) | `map_file_read_error` emits the same typed error (sibling to the DV path's `map_dv_error_to_datafusion`) | ✅ Same typed-error mechanism |
| **Deletion vectors** | — | Reuses the existing `dv_reader` (decoded selection vector, not a non-serializable `DvInfo`) | ✅ Reuse |
| **Row-tracking / `_metadata`** | — | Reuses `DeltaSyntheticColumnsExec` stacked on the kernel exec (composition, like the old path stacked it on the parquet read) | ✅ Reuse |
| **Partition columns** | — | Reuses `parse_delta_partition_scalar`; splits `required_schema` into data (read) + partition (injected) | ✅ Reuse |
| **Column-mapping transform** | — | Reuses kernel's `transform_to_logical` (identity `Transform` relabels physical→logical via the schema pair) | ✅ Reuse |
| **Single output partition** | — | One DataFusion partition; the Spark side (`CometDeltaNativeScanExec.oneTaskPerPartition`) does per-file splitting | ✅ Matches the existing Spark-side split model |

### Note: why `align_batch_to_schema` is custom rather than `SparkSchemaAdapter`

Comet's `SparkSchemaAdapter` / `SparkPhysicalExprAdapterFactory` /
`CometCastColumnExpr` / `spark_parquet_convert` stack is the canonical
schema-reconciliation machinery, and Iceberg reuses it (the
`IcebergStreamWrapper` runs `SparkPhysicalExprAdapterFactory` over every batch).
It is, however, **arrow-58 only** — it rewrites arrow-58 `PhysicalExpr`s over
arrow-58 schemas.

The kernel-read reconciliation has to happen **before** the arrow-57→58 bridge,
because kernel's `transform_to_logical` (column mapping, partition injection,
row-tracking) consumes the arrow-57 batch in `physical_schema` layout. Reusing
the arrow-58 adapter would mean bridging an un-reconciled batch first, then
reconciling — losing the single-copy read and coupling the kernel path to
Comet's arrow-58 expression machinery across the version boundary.

So `align_batch_to_schema` is the arrow-57, pre-bridge analogue of the
`SparkSchemaAdapter`'s top-level behaviour: reorder by name, `cast` to the target
type (which also covers the `Timestamp(us, None) → Timestamp(us, "UTC")` tz
relabel — verified by `align_batch_to_schema_reorders_and_casts`), and null-fill
schema-evolution columns. It deliberately does **not** reimplement the adapter's
nested-struct field selection or case-insensitive / field-id matching, because
nested column mapping is still guarded to the old path (#47) and kernel supplies
the physical names. When nested support lands, model the additions on
`parquet_support.rs::parquet_convert_struct_to_struct` rather than re-inventing.

## Cleanup finding (for #50)

The INT96 fix replaced kernel's parquet reader (which matched columns by
`parquet.field.id` for id-mode) with our own read that projects by **physical
name**. This is correct for id-mode because the driver renames the read schema to
`cm.physical_name`, which equals the parquet column name (unique UUIDs). As a
consequence, the field-id remap machinery in `arrow_bridge.rs`
(`with_kernel_field_ids`, `numericize_field_ids`) is now **dead at read time** —
nothing consults `parquet.field.id` anymore. It is harmless (a no-op for
name-mode schemas) and still exercised by its own unit tests, so it is left in
place for now and removed as part of the old-path deletion (#50), not piecemeal.

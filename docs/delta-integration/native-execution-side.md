# Native (Rust) execution side

Counterpart to [scala-driver-side.md](scala-driver-side.md). This doc
walks the Rust modules under `native/core/src/delta/` and the
Delta-specific touch points in `native/core/src/execution/`.

## Brief Comet-native recap

For Spark engineers new to Comet's native runtime:

  - **`native/core`** is a `cdylib` loaded into the JVM at runtime.
    Comet's JNI surface is small: planner construction + plan execution
    + a handful of utility entry points. The Rust planner takes a
    protobuf-encoded operator tree and builds a DataFusion
    `ExecutionPlan`.
  - **`SparkPlan`** is Comet's wrapper around an `ExecutionPlan`,
    carrying the Spark plan id and child-plan bookkeeping.
  - **`init_datasource_exec`** is Comet's entry point into
    DataFusion's `ParquetSource`. Every Comet scan (plain parquet,
    Iceberg-compat, Delta) eventually calls it. The Delta path adds a
    couple of wrappers around the result.
  - **`OpStruct`** is the tagged union arm of the operator proto. The
    Delta operator lives at `OpStruct::DeltaScan`; the planner has a
    match arm for it at `planner.rs` ~1351.

## Module layout

```
native/core/src/delta/
├── mod.rs             — public API surface and module preamble
├── engine.rs          — DefaultEngine builder (kernel + object_store_kernel)
├── error.rs           — DeltaError / DeltaResult
├── jni.rs             — Java_org_apache_comet_Native_planDeltaScan
├── predicate.rs       — Catalyst-proto Expr → kernel Predicate
├── scan.rs            — plan_delta_scan / list_delta_files (log replay)
└── integration_tests.rs

native/core/src/execution/
├── operators/delta_dv_filter.rs — DeltaDvFilterExec
└── planner.rs                   — OpStruct::DeltaScan arm + ColumnMappingFilterRewriter + build_delta_partitioned_files

native/core/src/parquet/
└── ignore_missing_file_source.rs — FileSource decorator for ignoreMissingFiles
```

## Why the `delta/` module is quarantined

`delta-kernel-rs` depends on **arrow 57** and **object_store 0.12**.
Comet's main native graph is on **arrow 58** and **object_store 0.13**.
Mixing the two would either force a downgrade of Comet (a non-starter)
or cause type clashes at the planner boundary.

The branch resolves this with a *workspace-level dependency rename*:
in `native/Cargo.toml` the kernel's `object_store` is brought in as
`object_store_kernel = { package = "object_store", version = "0.12", … }`.
The kernel's `arrow` is the same crate name, so duplication of the
crate at two different versions is allowed by Cargo. The result is
two parallel, isolated subgraphs:

  - The `delta_kernel`, `object_store_kernel`, and the kernel's `arrow`
    57 crates *only* reachable from `crate::delta::*`.
  - Everything else uses `arrow` 58 and `object_store` 0.13.

To enforce the boundary at the type level, **no kernel-typed value
escapes `crate::delta`**. The public API surface (`scan.rs`'s
`DeltaFileEntry`, `DeltaScanPlan`) only uses plain Rust types
(`String`, `i64`, `u64`, `Vec`, `HashMap<String, String>`, our own
structs). The kernel's `Snapshot`, `Engine`, `ScanFile`, `DvInfo`,
`Predicate`, `Url` all stay inside `delta/`.

## `delta/engine.rs` — credential-aware engine builder

`create_engine(url, config)` builds a kernel `DefaultEngine` over an
`object_store_kernel::ObjectStore` chosen by URL scheme:

  - `s3://` / `s3a://` → `AmazonS3Builder` with bucket from
    `url.host_str()`, plus access key / secret / session token /
    region / endpoint / path-style flag from `DeltaStorageConfig`.
  - `az://` / `azure://` / `abfs://` / `abfss://` →
    `MicrosoftAzureBuilder` with account name + access key / bearer
    token.
  - `file://` → `LocalFileSystem`.
  - other → `DeltaError::UnsupportedScheme`.

`DeltaStorageConfig` is a flat field-per-knob struct rather than a
generic map: validation lives at the JNI boundary
(`extract_storage_config` in `jni.rs`), which probes both kernel-style
keys (`aws_access_key_id`) and Hadoop-style keys (`fs.s3a.access.key`)
to populate it.

## `delta/scan.rs` — log replay and DV materialisation

The single function the rest of Comet calls is `plan_delta_scan` (or
`plan_delta_scan_with_predicate` for the kernel-pruning case):

  1. **`normalize_url(url_str)`** — guarantees a trailing slash.
     Critical because kernel does `table_root.join("_delta_log/")` and
     `Url::join` *replaces* the last path segment when the base doesn't
     end in `/`. Also handles three input shapes: real URLs (`s3://…`,
     `file://…`), Hadoop's broken single-slash form (`file:/…`), and
     bare local paths (canonicalised + converted via
     `Url::from_directory_path`).
  2. `Snapshot::builder_for(url).at_version(v)?.build(engine)?` →
     pinned snapshot.
  3. **Reader-feature gate** (`unsupported_features`): currently
     constructed empty. Column mapping, type widening, row tracking,
     and DV are *all* supported. Things explicitly *not* treated as
     fallback-worthy: `change_data_feed`, `in_commit_timestamps`,
     `iceberg_compat_v1/v2`, `append_only`. The `Vec<String>` ride is
     kept on the wire for forward compatibility.
  4. **Column mapping**: when `Snapshot.table_properties().column_mapping_mode`
     is `id` or `name`, walk the snapshot's schema and collect
     `(field.name, ColumnMetadataKey::ColumnMappingPhysicalName)` into
     a `Vec<(logical, physical)>`. Empty otherwise.
  5. **`scan_builder().with_predicate(...).build()`** — kernel's scan
     planner. The predicate is the kernel-translated form (see
     `predicate.rs` below).
  6. **`scan.scan_metadata(engine)?`** + `meta.visit_scan_files(...)` —
     iterate active files. Per file, capture path, size, modification
     time, partition values map, record-count stat, and (importantly)
     `DvInfo`.
  7. **DV materialisation**: for any file with `dv_info.has_vector()`
     true, call `dv_info.get_row_indexes(engine, &table_root_url)?`
     to materialise the bitmap (inline or on-disk DV file) into a
     sorted `Vec<u64>`. This happens once per query, on the driver,
     so DV bytes never travel to executors.

The end product is a `DeltaScanPlan` of plain Rust types, ready to be
serialised at the JNI boundary.

## `delta/predicate.rs` — Catalyst-proto Expr → kernel Predicate

Comet already has a Catalyst Expr → DataFusion `PhysicalExpr` translator
on the executor side. For *driver-side* file pruning kernel needs the
predicate in **kernel's own `Predicate` AST**, which is much narrower
(it's only the subset usable for stats-based file skipping).

`catalyst_to_kernel_predicate_with_names(expr, column_names)` walks
the Comet `spark_expression::Expr` tree:

  - `BoundReference(idx)` → kernel `Expression::Column(name)` using
    `column_names[idx]` (the array passed in from
    `Native.planDeltaScan`).
  - Literal scalars → kernel scalar.
  - `EqualTo / NotEqualTo / Lt / LtEq / Gt / GtEq` → matching kernel
    binary predicate.
  - `In / NotIn` → kernel `Predicate::In`.
  - `And / Or / Not` → recursive translation.
  - `IsNull / IsNotNull` → kernel `Predicate::IsNull`.
  - `Cast` → unwrapped (kernel handles type coercion via stats), then
    recurses on the child.

Anything kernel can't represent translates to `None`, in which case
the caller falls back to "no predicate" — kernel returns the whole
file list and Comet still gets row-group-level pruning via DataFusion
later.

## `delta/jni.rs` — the JNI entry point

`Java_org_apache_comet_Native_planDeltaScan(env, _class, table_url,
snapshot_version, storage_options, predicate_bytes, column_names)`:

  1. Decode each JNI arg: `table_url` (JString → `String`),
     `snapshot_version` (`-1` → `None` for "latest"),
     `storage_options` (`JMap<String,String>` →
     `DeltaStorageConfig`), `predicate_bytes` (`JByteArray` → optional
     `Vec<u8>`), `column_names` (`String[]` → `Vec<String>`).
  2. Decode the predicate bytes via prost into a
     `spark_expression::Expr`, then translate via
     `catalyst_to_kernel_predicate_with_names`. On decode failure: log
     and continue with no predicate (correctness preserved, perf
     degraded).
  3. Call `plan_delta_scan_with_predicate(...)`.
  4. Build a `Vec<DeltaPartitionValue>` per file. Translate
     **physical → logical** partition keys when column mapping is
     active (kernel returns partition values keyed by physical names;
     Comet's wire format uses logical names so the native planner's
     `partition_schema` lookup matches).
  5. Build a `Vec<DeltaScanTask>` per file. `file_path` is computed by
     `resolve_file_path(table_root, entry.path)`:
     - If `entry.path` already has a URI scheme (`has_uri_scheme`
       check), pass through verbatim. This handles SHALLOW CLONE
       AddFile paths that Delta stores in Hadoop's `Path.toUri.toString`
       form (`file:/abs/...` with single slash).
     - Otherwise join with `table_root` (with or without trailing
       slash).
  6. Build the `DeltaScanTaskList` proto, encode, return as `byte[]`.

`base_row_id` / `default_row_commit_version` are intentionally left
unset on the kernel path. Kernel 0.19.x consumes those internally for
its `TransformSpec` and doesn't surface them on `ScanFile`. The
pre-materialised-index path on the Scala side fills them in from
`AddFile` directly when row tracking is enabled.

## `parquet/ignore_missing_file_source.rs`

`Spark`'s `spark.sql.files.ignoreMissingFiles` says: if a file
disappears between planning and read, return zero rows for it instead
of failing the whole stage. DataFusion has no equivalent built-in.

`IgnoreMissingFileSource` wraps any `Arc<dyn FileSource>` and returns
an `IgnoreMissingFileOpener` that catches the per-file
`object_store::Error::NotFound` from the underlying opener's first
range request and turns it into an empty `RecordBatchStream`. No
prefetch, no extra IO — only the request DataFusion was already going
to make is intercepted.

The Delta serde wires this in via the `ignore_missing_files` field of
`DeltaScanCommon`, plumbed through `init_datasource_exec` on the
native side.

## `execution/planner.rs` — `OpStruct::DeltaScan` arm

Lines ~1351–1572 are the heart of the native side. Walkthrough:

### 1. Decode common header

```
let common = scan.common.as_ref().ok_or_else(|| ...)?;
let required_schema = convert_spark_types_to_arrow_schema(common.required_schema);
let mut data_schema = convert_spark_types_to_arrow_schema(common.data_schema);
let partition_schema = convert_spark_types_to_arrow_schema(common.partition_schema);
```

### 2. Apply column mapping to `data_schema`

For each entry in `common.column_mappings`, replace the field name in
`data_schema` with the physical name. `required_schema` stays in
logical names — that's what Spark expects upstream operators to see.

### 3. Empty-task fast path

If `scan.tasks.is_empty()` (e.g. all partitions pruned away), return an
`EmptyExec` immediately. Spark still needs the right schema to plumb
upstream operators.

### 4. Build the data filters

For each `Expr` in `common.data_filters`:
  - `self.create_expr(expr, required_schema)` — Comet's standard
    expression translator builds a DataFusion `PhysicalExpr` against
    the *logical* schema.
  - When column mapping is active, `ColumnMappingFilterRewriter` walks
    the expression tree and renames every `Column(logical)` to
    `Column(physical)` so the eventual filter applies against the
    physical-named batch.

### 5. Build PartitionedFiles

`build_delta_partitioned_files(tasks, partition_schema, session_tz)`:

  - One `PartitionedFile` per `DeltaScanTask`.
  - `file_path` from the task; `file_size` and optional
    `byte_range_start`/`end` populated from the task.
  - `partition_values` decoded from the per-task `partition_values`
    list using the partition schema's types and the session timezone
    for date/timestamp parsing. Delta stores partition values as
    strings in the log; Catalyst expects typed `Scalar`s.

### 6. Group by DV presence

```
for (file, task) in files.into_iter().zip(scan.tasks.iter()) {
    if task.deleted_row_indexes.is_empty() { non_dv_files.push(file); }
    else { file_groups.push(vec![file]); deleted_indexes_per_group.push(task.deleted_row_indexes.clone()); }
}
if !non_dv_files.is_empty() { file_groups.push(non_dv_files); deleted_indexes_per_group.push(Vec::new()); }
```

The 1:1 invariant between DV'd file and `FileGroup` partition is what
makes the running-row-offset DV applicator simple and correct (see
`DeltaDvFilterExec` below).

### 7. Build the parquet exec

`init_datasource_exec` is Comet's bridge to DataFusion's
`ParquetSource`. Threaded through:

  - `required_schema`, `data_schema`, `partition_schema`.
  - The object-store URL prepared via `prepare_object_store_with_configs`.
  - `file_groups` (one or many DV groups + one combined non-DV group).
  - `projection_vector` (mapped from the proto).
  - `data_filters` (rewritten to physical names if column mapping).
  - `default_values: None` — Phase 4 (column mapping) didn't need
    column-default support; columns the parquet reader can't find
    become NULL via DataFusion's schema adapter.
  - `session_timezone`, `case_sensitive`.
  - `encryption_enabled: false` here — encryption support for Delta
    is wired through a different code path on the JVM side (the
    encryption hadoop conf is broadcast to executors and consulted by
    `CometExecRDD`).
  - `ignore_missing_files` from the common.

### 8. Wrap in `DeltaDvFilterExec` if any DVs

Skipped entirely when `deleted_indexes_per_group.iter().all(|v|
v.is_empty())` so the common "no DVs in use" case has zero per-batch
overhead.

### 9. Add a column-mapping rename Projection

When column mapping is active, `init_datasource_exec`'s output schema
carries physical names (because that's what `data_schema` uses).
Upstream operators reference logical names. A `ProjectionExec` aliases
each physical column to its logical name. Without this rename, MERGE's
codegen and many other DataFusion expression paths fail with "Unable
to get field named ..." against the physical-named batch.

The rename is built positionally with a physical→logical lookup map
(`scan_out` zipped with `required_schema`) so a length mismatch
between the two schemas can't accidentally fall back to the physical
name.

## `execution/operators/delta_dv_filter.rs` — DeltaDvFilterExec

The DV applicator. Construction takes:
  - `input: Arc<dyn ExecutionPlan>` — the `init_datasource_exec` parquet exec.
  - `deleted_row_indexes_by_partition: Vec<Vec<u64>>` — sorted DV row
    indexes per partition. Length must match
    `input.output_partitioning().partition_count()`.

At execute time, for partition `i`:
  - If `deleted_row_indexes_by_partition[i].is_empty()`, the stream is
    a pass-through — no per-batch cost beyond the wrapping.
  - Otherwise, a `DeltaDvFilterStream` keeps a running absolute row
    offset, builds a `BooleanArray` mask per `RecordBatch` (binary
    search into the sorted DV indexes per row), and applies
    `arrow::compute::filter_record_batch`.

The 1:1 partition-to-file invariant from the planner arm is what makes
the simple running-offset strategy correct: each partition emits the
batches of exactly one parquet file in physical order, so absolute row
offsets are well-defined for the whole partition.

## `ColumnMappingFilterRewriter`

`planner.rs` ~3115. A `TreeNodeRewriter` for `Arc<dyn PhysicalExpr>`
that:

  - Identifies `Column(name, idx)` nodes whose name appears in the
    `logical_to_physical` map.
  - Rebuilds them with the physical name and the same field index.

Run on each filter after `create_expr` so the predicate compares
against the physical-named columns the parquet reader actually
projects.

## Memory pools

`native/core/src/execution/memory_pools/{config,mod}.rs` got a small
update to support a Delta-specific config knob
(`spark.comet.scan.deltaNative.dataFileConcurrencyLimit`) that affects
per-task parallelism inside the parquet reader. The pool itself is the
existing Comet greedy-or-fair pool — only the per-task fan-out
parameter changed.

## `execution/expressions/temporal.rs` and other small touches

  - `temporal.rs`: a small fix to `Hour` to handle TIMESTAMP_NTZ
    correctly, exposed by Delta's NTZ partition values.
  - `expressions/mod.rs` and `spark-expr/src/datetime_funcs/hours.rs`:
    a fully native `hour` implementation needed for DPP/predicate
    evaluation on Delta partition columns.
  - `conversion_funcs/string.rs`: a reproduction-fix for the
    `Utf8 <= Int32` mismatch that triggered some of the column-mapping
    fallbacks. The fix is in plain Comet; the integration just exposed
    it.

## What this side does NOT do

  - **No kernel calls on executors.** All log replay + DV
    materialisation happen on the driver via the JNI entry point.
    Executors only see the final `DeltaScanTask` list and apply DVs
    (per-batch) and per-file projections.
  - **No object-store work outside the kernel quarantine** for
    reading the log. Once parquet reads start, they go through Comet's
    main `object_store 0.13` path via `prepare_object_store_with_configs`.
  - **No retry / repair logic.** A missing parquet file under
    `ignoreMissingFiles=true` is silently skipped; otherwise the
    failure propagates straight up. Delta's reader has the same
    semantics.

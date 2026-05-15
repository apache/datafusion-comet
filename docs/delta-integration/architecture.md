# Architecture

This document gives the bird's-eye view: what the components are, where
they live, and how they hand off to each other for a single query.

## Components by module

```
spark/src/main/scala/
├── org/apache/comet/
│   ├── CometConf.scala                       ← three new config knobs
│   ├── CometSparkSessionExtensions.scala     ← CometDeltaDvConfigRule (Delta DV-strategy auto-flip)
│   ├── delta/
│   │   ├── DeltaReflection.scala             ← class-name probes + reflection helpers (no compile-time spark-delta dep)
│   │   └── RowTrackingAugmentedFileIndex.scala ← FileIndex wrapper that injects baseRowId / defaultRowCommitVersion as constant cols
│   ├── parquet/CometParquetFileFormat.scala  ← physicalName resolution for column-mapping reads
│   ├── rules/CometScanRule.scala             ← detection + DV wrapper stripping + nativeDeltaScan() route + row-tracking rewrite
│   └── serde/operator/CometDeltaNativeScan.scala ← convert(): file-list assembly, partition pruning, file splitting, DeltaScan proto build
└── org/apache/spark/sql/comet/
    ├── CometDeltaNativeScanExec.scala         ← physical exec, split-mode per-partition serialization, DPP, metrics
    ├── CometExecRDD.scala                     ← setInputFileForDeltaScan (InputFileBlockHolder for embedded scans)
    └── operators.scala                        ← DeltaPlanDataInjector (driver→executor common+per-partition assembly)

native/
├── proto/src/proto/operator.proto            ← DeltaScan, DeltaScanCommon, DeltaScanTask, DeltaScanTaskList, DeltaPartitionValue, DeltaColumnMapping
├── core/src/
│   ├── delta/                                ← QUARANTINED: own dep subtree (delta_kernel + arrow 57 + object_store 0.12)
│   │   ├── mod.rs                            ← public surface
│   │   ├── engine.rs                         ← DefaultEngine builder, DeltaStorageConfig
│   │   ├── scan.rs                           ← plan_delta_scan / list_delta_files (log replay + DV materialisation)
│   │   ├── predicate.rs                      ← Catalyst-proto Expr → kernel Predicate
│   │   ├── error.rs                          ← DeltaError / DeltaResult
│   │   └── jni.rs                            ← Java_org_apache_comet_Native_planDeltaScan entry point
│   ├── execution/operators/delta_dv_filter.rs ← per-batch DV applicator wrapping a child ParquetSource exec
│   ├── execution/planner.rs                  ← OpStruct::DeltaScan arm: build_delta_partitioned_files + ColumnMappingFilterRewriter + init_datasource_exec
│   └── parquet/ignore_missing_file_source.rs ← FileSource decorator implementing ignoreMissingFiles
```

The two physical boundaries are: **JVM ↔ Rust JNI** (driver-side log
replay, encryption hadoop conf, executor-side native plan exec) and
**Rust ↔ delta-kernel-rs** (an entire dep subtree quarantined inside
`native/core/src/delta/` so kernel's `arrow 57` and
`object_store 0.12` never touch Comet's main `arrow 58` /
`object_store 0.13` graphs).

## End-to-end query lifecycle

The lifecycle below assumes a typical
`SELECT a, b FROM delta_table WHERE part_col = 'X' AND a > 5` query.

### 1. Logical optimization

`CometSparkSessionExtensions.injectOptimizerRule` adds
**`CometDeltaDvConfigRule`** — a no-op-on-the-plan rule that, when the
logical plan contains a Delta scan and Comet's Delta path is enabled,
sets `spark.databricks.delta.deletionVectors.useMetadataRowIndex=false`
on the session. This must happen before Delta's
`PreprocessTableWithDVsStrategy` runs during logical-to-physical
conversion, so Delta picks the older Project+Filter DV strategy rather
than the opaque metadata-row-index strategy.

### 2. Physical planning — Spark's normal pipeline

Spark + Delta produce a physical plan containing a `FileSourceScanExec`
backed by a Delta `HadoopFsRelation` and either a `TahoeLogFileIndex`,
`PreparedDeltaFileIndex`, `TahoeBatchFileIndex`, or a CDF index. For
DV-bearing tables, Delta's own `PreprocessTableWithDVsStrategy` wraps
the scan in:

```
ProjectExec(userOutput,
  FilterExec(__delta_internal_is_row_deleted = 0,
    ProjectExec([userCols, is_row_deleted, _metadata structs],
      FileSourceScanExec(output includes is_row_deleted))))
```

### 3. CometScanRule — detection and DV stripping

Injected via `extensions.injectQueryStagePrepRule`. Two phases run:

**Phase A — `stripDeltaDvWrappers`** (gated on
`COMET_DELTA_NATIVE_ENABLED`):
  - Walks the plan from the leaves up.
  - When it finds the `Project(Filter(__delta_internal_is_row_deleted =
    0, …))` shape, decides whether the underlying scan can be
    DV-accelerated by Comet:
      - If yes: rebuild the scan without the synthetic column and drop
        the wrapping Project + Filter. DV row indexes will flow over
        the wire on each task and be applied by `DeltaDvFilterExec`.
      - If no (e.g. `TahoeBatchFileIndex` with DV-bearing AddFiles, or a
        `PreparedDeltaFileIndex` whose required schema already carries
        the synthetic column): record the inner `FileSourceScanExec` in
        `dvProtectedScans` and leave the wrapper intact. Phase B will
        skip those scans so Delta's reader supplies the synthetic column
        and the original filter applies the DV.

**Phase B — `transformScan`** for each `FileSourceScanExec` /
`BatchScanExec`:
  - For Delta scans (detected via `DeltaReflection.isDeltaFileFormat`),
    declines accelerating any DV-protected scan; for the rest, declines
    on `input_file_name` references and routes the survivors through
    `nativeDeltaScan`.
  - `nativeDeltaScan` runs the long decline checklist
    (see [features-and-limitations.md](features-and-limitations.md#cases-where-comet-declines-acceleration))
    and, on success, returns a `CometScanExec(scanWithMappedSchema,
    session, SCAN_NATIVE_DELTA_COMPAT)`. This is *not* yet the final
    operator — it's a marker that Comet's exec rule will recognise and
    hand to `CometDeltaNativeScan.convert()` for actual serde.

The two side-effects of `nativeDeltaScan` worth highlighting:
  - **Column-mapping metadata reattach** — `withDeltaColumnMappingMetadata`
    fetches the authoritative Delta `Snapshot.schema` via reflection
    (`DeltaReflection.extractSnapshotSchema`) and reattaches the per-field
    `delta.columnMapping.physicalName` metadata that
    `HadoopFsRelation` strips during construction.
  - **Row-tracking rewrite** — `applyRowTrackingRewrite` rewrites any
    `row_id` / `row_commit_version` references in the scan's required
    schema to read the materialised physical column, then wraps the scan
    in a `Project(coalesce(materialised, baseRowIdCol +
    _tmp_metadata_row_index))`. Achieved by swapping the underlying
    `FileIndex` for a `RowTrackingAugmentedFileIndex` that synthesises
    `baseRowId` / `defaultRowCommitVersion` as constant per-file partition
    values.

### 4. CometExecRule — convert() to native

When the planner reaches the `CometScanExec[native_delta_compat]` marker,
`CometExecRule.convertToComet` calls `CometDeltaNativeScan.convert()`.
This is where the heavy lifting on the JVM happens:

  1. **Resolve table root** via `DeltaReflection.extractTableRoot`
     (`HadoopFsRelation.location.rootPaths.head.toUri.toString` in the
     general case).
  2. **Translate Spark data filters to a kernel-readable proto** so
     kernel can do stats-based file pruning during log replay.
     Multiple filters are combined as `AND(f1, AND(f2, ...))`. The
     translation happens in two stages: Catalyst → Comet's
     `spark.expression.Expr` proto via `exprToProto`, then proto →
     kernel `Predicate` on the Rust side
     (`crate::delta::predicate::catalyst_to_kernel_predicate_with_names`,
     using a column-names array to turn `BoundReference` indices back
     into kernel column names).
  3. **Fetch the active file list.** Two paths:
      - *Pre-materialised file index* (`TahoeBatchFileIndex` /
        `CdcAddFileIndex` / `TahoeRemoveFileIndex`): build a
        `DeltaScanTaskList` directly from the index's `addFiles` via
        `buildTaskListFromAddFiles`. Bypasses kernel because kernel
        replay against the pinned snapshot would return a *different*
        file set (the snapshot's full active list) than the small set the
        index has pre-narrowed for MERGE / streaming / CDC.
      - *Regular scan* (`TahoeLogFileIndex` /
        `PreparedDeltaFileIndex`): call `Native.planDeltaScan` (JNI),
        which delegates to `delta::scan::plan_delta_scan_with_predicate`.
        Returns a list of files plus DV-materialised deleted-row indexes
        plus the snapshot's logical→physical column mapping plus a
        list of unsupported reader features.
  4. **Reader-feature gate.** If kernel reports any unsupported reader
     features for this snapshot, decline.
  5. **Static partition pruning** in `prunePartitions`: build a Catalyst
     `InterpretedPredicate` over `partitionSchema`, evaluate against
     each task's `partition_values` map. DPP filters
     (`DynamicPruningExpression`) are excluded here and deferred to
     execution time.
  6. **File splitting** in `splitTasks`: any task whose `file_size`
     exceeds the per-scan `maxSplitBytes` is replaced by a sequence of
     byte-range chunks. Mirrors `FilePartition.maxSplitBytes` exactly so
     `files.maxPartitionBytes`, `files.openCostInBytes`, and
     `files.minPartitionNum` take effect.
  7. **Build the `DeltaScanCommon` proto** (schemas, projection vector,
     filters, storage options, column mappings, time-travel snapshot
     version, session timezone, case-sensitivity).
  8. **Stash the filtered task list** into `CometDeltaNativeScan
     .lastTaskListBytes` (a thread-local) for `createExec` to pick up.
  9. Build the operator with **`common` set, `tasks` empty** — the proto
     that crosses the wire to executors carries only `common`. Tasks are
     injected per-partition by `DeltaPlanDataInjector` at execution time.

### 5. CometDeltaNativeScanExec — the physical operator

`createExec` constructs a `CometDeltaNativeScanExec` carrying:
  - the `nativeOp` (with empty tasks, common populated);
  - the original `output: Seq[Attribute]`;
  - the `tableRoot` (for `nodeName` / debugging);
  - the **full task-list bytes** (set aside as `taskListBytes`);
  - any DPP filters that survived planning-time pruning;
  - the `partitionSchema` (needed to evaluate DPP at execution time).

At execution time:
  1. **`buildPerPartitionBytes`** re-applies DPP to the cached
     `allTasks` (so AQE-broadcast values are picked up) and bin-packs
     the survivors into Spark partitions via `packTasks` (mirrors Spark's
     `FilePartition` packing — `openCostInBytes` + `maxPartitionBytes`).
  2. Each partition's task list is serialised as its own
     `DeltaScan { tasks=[…] }` proto. The common header travels once;
     each partition gets only its slice of files.
  3. **`CometExecRDD`** is launched with a `commonByKey` map and a
     `perPartitionByKey` map. Both are keyed by `sourceKey` (a stable
     hash over `tableRoot | snapshot_version | required_schema |
     filters | projection_vector | column_mappings`) so multiple Delta
     scans of the same plan (e.g. self-join across two snapshot
     versions) don't collide.

### 6. DeltaPlanDataInjector — re-assembly on the executor

When the executor deserialises the native plan, it walks the operator
tree and calls `DeltaPlanDataInjector.inject` for each `DeltaScan` node
that has zero tasks but has a `common`. The injector:
  - Looks up the parsed `DeltaScanCommon` (cached by `commonBytes`
    `ByteBuffer` to avoid re-parsing the same header for every
    partition of the same scan).
  - Parses this partition's `DeltaScan { tasks=[…] }` slice.
  - Reattaches: `op.toBuilder.setDeltaScan(setCommon(common)
    .addAllTasks(tasksOnly.tasksList))`.

The result is the protobuf shape the native planner expects, but
constructed from two byte streams instead of one giant blob.

### 7. native/core OpStruct::DeltaScan — physical plan build

The native side (`planner.rs` ~1351) takes the assembled `DeltaScan`
proto and builds a DataFusion `ExecutionPlan`:

  1. Convert `data_schema` / `required_schema` / `partition_schema` from
     Spark types to Arrow types.
  2. **Apply column mapping** to `data_schema`: for each top-level
     logical→physical mapping in `column_mappings`, rename the field so
     `ParquetSource` projects by the parquet file's actual column names.
     `required_schema` keeps logical names (Spark conventions); the
     schema-adapter chain bridges the two at row-group decode time.
  3. Build PhysicalExpr filters from the proto data filters; if column
     mapping is active, run them through `ColumnMappingFilterRewriter`
     to translate Column references from logical to physical names.
  4. **Build `PartitionedFile`s** via `build_delta_partitioned_files`,
     which decodes per-task partition values into Arrow scalars
     (string→typed using the session timezone for timestamps, etc.).
  5. **DV file-grouping**: each task with non-empty
     `deleted_row_indexes` becomes its own `FileGroup`; non-DV tasks
     are merged into one combined group. The two arrays
     `file_groups: Vec<Vec<PartitionedFile>>` and
     `deleted_indexes_per_group: Vec<Vec<u64>>` are built side-by-side
     and stay 1:1.
  6. Register the object store via `prepare_object_store_with_configs`
     (one URL is enough — every file under a Delta table shares the
     same root scheme).
  7. Construct the parquet exec via `init_datasource_exec` (the same
     entry point Comet's iceberg-compat scan uses).
  8. **Wrap in `DeltaDvFilterExec`** if any partition has DVs;
     otherwise pass through.
  9. **Add a rename `ProjectionExec`** if column mapping is active so
     the physical-named output of `ParquetSource` reaches upstream
     operators with logical names (otherwise expressions referencing
     logical names would fail with "Unable to get field named ...").

### 8. DeltaDvFilterExec — applying deletion vectors

For each partition `i` (which under DV is exactly one parquet file), it:
  - Receives `RecordBatch`es from the parquet reader.
  - Maintains a running absolute row offset.
  - Builds a `BooleanArray` mask per batch — `true` at row positions
    *not* in `deleted_row_indexes_by_partition[i]`.
  - Calls `arrow::compute::filter_record_batch`.

The DV indexes were materialised once on the driver
(`DvInfo::get_row_indexes` inside `plan_delta_scan`) and travel as a
sorted `Vec<u64>` on the wire.

## What kernel does (and doesn't do)

`delta-kernel-rs` (vendored at the version pinned in `native/Cargo.toml`)
is responsible for:

  - Reading the Delta `_delta_log/` (commit JSON files +
    checkpoints) for a given snapshot version.
  - Returning the active file list with per-file partition values,
    record-count stats, and DV info.
  - Materialising DV bitmaps (inline + on-disk) into sorted row-index
    vectors — Comet calls `DvInfo::get_row_indexes` once per DV-bearing
    file, on the driver.
  - Translating the table's column-mapping mode + per-field
    physical-name metadata.
  - Reporting which Delta reader features are *in use* for the snapshot
    (the basis of Comet's reader-feature gate).
  - Building an `ObjectStore` for the table's URL via
    `object_store_kernel` (the renamed object_store 0.12 dep that lives
    only inside `native/core/src/delta/`).

Kernel does **not** read any parquet files for Comet — that work always
goes through DataFusion's `ParquetSource` so Comet's tuned encrypted /
unencrypted / row-group-pruning logic stays in play.

## What CometDeltaNativeScan does NOT do

  - It does not write to Delta tables.
  - It does not maintain its own snapshot cache; every plan rebuilds the
    scan from scratch (kernel does have its own caches).
  - It does not propagate DPP through Delta's CDC indexes (CDC reads use
    pre-materialised AddFiles already pinned at planning time).
  - It does not track parquet column-statistics back into Delta's log
    (Delta writers do that; Comet only reads).

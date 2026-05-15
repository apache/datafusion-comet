# Scala / JVM driver side

This document is a code-level tour of every Scala component the branch
adds or substantially edits. Read it alongside the source — file:line
references are exact for the branch tip.

## Brief Comet-internals recap

For the benefit of Spark engineers new to Comet:

  - **`CometSparkSessionExtensions`** registers the Catalyst extensions
    that Comet uses: a `ColumnarRule`, two `QueryStagePrepRule`s, and
    (new in this branch) one `OptimizerRule`. Spark calls these as part
    of its standard pipeline — Comet doesn't fork Spark's analyzer.
  - **`CometScanRule`** is a `Rule[SparkPlan]` that visits every
    `FileSourceScanExec` / `BatchScanExec` and replaces it with a
    `CometScanExec` (or a more specialised wrapper) when supported. The
    rule decides *where* the boundary between Spark and Comet sits.
  - **`CometExecRule`** comes second; it walks the post-CometScanRule
    tree, calls each operator's serde `convert()` to build a native
    operator proto, and ultimately wraps the whole subtree in a
    `CometNativeExec` that owns the proto and dispatches it to the
    Rust planner via `CometExecRDD`.
  - **`CometOperatorSerde[T]`** is the common interface implemented by
    every per-operator serde (e.g. `CometNativeScan` for
    `iceberg_compat` plain-parquet, `CometIcebergNativeScan` for
    Iceberg, **`CometDeltaNativeScan`** for Delta). `convert()` returns
    an `Operator` proto; `createExec()` builds the Spark physical
    operator that owns it.
  - **`Operator` protobuf** (in `native/proto/src/proto/operator.proto`)
    is the wire format between the JVM driver and the Rust planner.
    Every Comet physical operator sits at exactly one `OpStruct` arm
    on the native side.
  - **Plan-data injectors** (`PlanDataInjector` in `operators.scala`)
    handle Comet's "split-mode" serialization: when a single operator
    needs to send a large per-partition payload separately from its
    common header, the injector reassembles the two on the executor
    side. Comet's native scan and Iceberg native scan already do this;
    `DeltaPlanDataInjector` is new in this branch.

With that map in mind, the rest of the document goes file by file.

---

## `CometConf.scala`

Three new entries:

```
val SCAN_NATIVE_DELTA_COMPAT = "native_delta_compat"

COMET_DELTA_NATIVE_ENABLED                 // default false
COMET_DELTA_DATA_FILE_CONCURRENCY_LIMIT    // default 1
COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE // default true
```

`SCAN_NATIVE_DELTA_COMPAT` is a *scan implementation tag* used in
`CometScanExec`'s constructor — same role as the existing
`SCAN_NATIVE_DATAFUSION` and `SCAN_NATIVE_ICEBERG_COMPAT` tags. It
identifies that this `CometScanExec` came from the Delta path, so the
matching serde (`CometDeltaNativeScan`) gets dispatched in the
`CometOperatorSerdeRegistry`.

## `CometSparkSessionExtensions.scala` — `CometDeltaDvConfigRule`

The added optimizer rule (lines ~64–136) is a no-op on the plan: it
returns its input unchanged. Its only side effect is conditionally
calling `session.conf.set("spark.databricks.delta.deletionVectors
.useMetadataRowIndex", "false")`.

The intricate guard logic exists because Delta has two DV read
strategies:

  - **Older / legacy** (`useMetadataRowIndex=false`): Delta's
    `PreprocessTableWithDVsStrategy` injects a
    `Project + Filter(__delta_internal_is_row_deleted = 0)` subtree.
    Comet can detect that pattern and rewrite it.
  - **Newer / default** (`useMetadataRowIndex=true`): Delta wraps the
    `FileFormat` in a `DeletionVectorBoundFileFormat` that applies the
    DV opaquely *inside* the parquet reader. Comet's parquet reader
    isn't that file format and can't intercept the DV.

So Comet *needs* the older strategy to be in effect, but it's only
allowed to override the conf when:
  - the user / test hasn't explicitly set it (probed via `getConfString`
    + try/catch because reading an unset key throws);
  - the session is *not* a `spark.newSession()` derived session (those
    don't inherit conf — overriding would silently invalidate the
    parent session's intent);
  - `COMET_DELTA_NATIVE_ENABLED` is true;
  - the plan actually contains a Delta scan;
  - the plan does not reference `__delta_internal_is_row_deleted` in any
    operator's output (that's a *raw* DV-read test path where the user
    wants `useMetadataRowIndex=true`).

The probe uses `LogicalRelation(hfr: HadoopFsRelation, …)` plus
`DeltaReflection.isDeltaFileFormat` so it has no compile-time spark-delta
dependency. The whole rule is wrapped in a top-level `try` that catches
NonFatal so a missing `spark-delta` JAR just makes the rule a no-op.

## `delta/DeltaReflection.scala`

The single most important design choice in the integration: Comet has
**no compile-time dependency on `spark-delta`**. Every interaction with
Delta types goes through this object.

Delta's API surface churns across versions — adding a compile-time dep
would force per-version shims. Class names and Spark's stable APIs
(`HadoopFsRelation`, `FileIndex`, `V2Scan`) have been the only stable
surfaces, so:

  - `isDeltaFileFormat(ff)` — class-name match on
    `org.apache.spark.sql.delta.DeltaParquetFileFormat` plus subclass
    check (catches `DeletionVectorBoundFileFormat`).
  - `extractTableRoot(relation)` — pulls the URI out of the
    `HadoopFsRelation`'s `location.rootPaths.head` via
    `pathToSingleEncodedUri`. The encoder uses `Path.toUri.toString`
    (the *full*, `%`-double-encoded URI form) rather than
    `Path.toString` (the once-decoded form). This matters for Delta
    tests whose temp-dir prefix is the literal `spark%dir%prefix`:
    Spark's actual on-disk dir is `spark%25dir%25prefix-uuid` (with
    `%25` four chars literal), so the URI must double-encode to
    `spark%2525dir%2525prefix-uuid` for the native side's
    percent-decode to recover the literal name. The double encoding is
    a no-op for ordinary table paths.
  - `extractSnapshotSchema(relation)` / `extractSnapshotVersion(relation)`
    / `extractMetadataConfiguration(relation)` — reflection into Delta's
    `TahoeFileIndex.snapshotAtAnalysis` (or `getSnapshot`, depending on
    Delta version) to pull the authoritative `Metadata` action.
  - `isBatchFileIndex(loc)` / `extractBatchAddFiles(loc)` — reflection
    on `TahoeBatchFileIndex` and friends to pull pre-materialised
    AddFile lists for MERGE/UPDATE/DELETE/CDC paths.
  - `castPartitionString(strOpt, dataType, sessionTz)` — Delta stores
    every partition value as a string in the log. This helper handles
    the Spark-side type cast respecting session timezone (TIMESTAMP),
    NTZ semantics (`TIMESTAMP_NTZ`), and the various date/decimal
    string formats Delta emits. Used by both `prunePartitions` and
    `synthesizedFilePartitions`.
  - `IsRowDeletedColumnName` / `RowIndexColumnName` — the synthetic
    column names Delta's `DeltaParquetFileFormat` injects. Comet pattern
    matches against these strings to detect DV / row-index reads.
  - `PhysicalNameMetadataKey` — the StructField metadata key
    `delta.columnMapping.physicalName`.
  - `RowTrackingFileInfo` + `extractAddFileRowTrackingInfo` — the
    per-file row-tracking metadata used by
    `RowTrackingAugmentedFileIndex`.

All reflection is wrapped in `try { … } catch { case NonFatal => None }`
so a class missing in a future Delta release just makes the relevant
detection return `None` and the scan falls back instead of crashing.

## `delta/RowTrackingAugmentedFileIndex.scala`

A `FileIndex` decorator. Delta exposes `_metadata.row_id` /
`_metadata.row_commit_version` as row-level metadata columns that are
either materialised in the parquet file or synthesised on the fly as
`baseRowId + parquet_row_index`.

Comet doesn't have a per-row code path that knows about Delta's row-id
encoding, so it lifts the per-file constants
(`AddFile.baseRowId` / `defaultRowCommitVersion`) into **partition
values** by:

  1. Wrapping the underlying `FileIndex` (`TahoeLogFileIndex`,
     `PreparedDeltaFileIndex`, …).
  2. Splitting each `PartitionDirectory` into one PD per file so each
     file's `baseRowId` / `defaultRowCommitVersion` can travel as that
     file's own constant.
  3. Adding two `LongType` synthetic partition fields
     (`baseRowIdColumnName` / `defaultRowCommitVersionColumnName`) to
     the partition schema.

`CometScanRule.applyRowTrackingRewrite` then wraps the scan in a
`Project(coalesce(materialised_row_id,
baseRowIdCol + _tmp_metadata_row_index))` and the same for
`row_commit_version`. The `_tmp_metadata_row_index` column is requested
from the parquet reader by setting Spark's
`FileSourceGeneratedMetadataStructField` mechanism, which Comet's
`CometParquetFileFormat.substituteGeneratedMetadataFields` recognises
and translates into the parquet `__index_level_0__` row index.

## `parquet/CometParquetFileFormat.scala` (changes only)

`substituteGeneratedMetadataFields` (~lines 200–245) gained a Delta
codepath:

  - It already substituted Spark's `FileSourceGeneratedMetadataStructField`
    columns into their physical equivalents.
  - The branch adds a parallel substitution for any field whose
    `metadata` carries `delta.columnMapping.physicalName`. The key is
    hard-coded as a string here — there's a comment to keep in sync with
    `DeltaReflection.PhysicalNameMetadataKey` — so the parquet module
    has zero compile-time coupling to the delta module.

Net effect: any time a Delta scan falls back to the
`CometScanExec[native_delta_compat]` route (which still uses
`CometParquetFileFormat`, not the native Delta scan), column-mapping
physical names are still respected for top-level scalar columns.

## `rules/CometScanRule.scala` (changes)

The biggest single Scala file in the branch. The Delta-specific surface
is ~700 lines added. Key call sites:

### `_apply` — top-level entry

  - **Phase A** (only when `COMET_DELTA_NATIVE_ENABLED`):
    `stripDeltaDvWrappers(plan, dvProtectedScans)`. Walks the plan from
    the leaves up, finds Delta's
    `Project(Filter(is_row_deleted = 0, …))` shape, decides per-scan
    whether Comet can handle DVs natively, and either rewrites the
    subtree to a clean scan or registers the inner scan in
    `dvProtectedScans` so phase B leaves it for Delta.
  - **Phase B**: `stripped.transform { case scan if isSupportedScanNode
    => transformScan }`. Standard Comet scan-rewrite, with one extra
    case at the top — DV-protected scans return early with a
    `withInfo(scan, "Leaving scan to Delta so its DV filter above can
    apply deletion vectors")`.

### `stripDeltaDvWrappers`

The decision tree for a `Project(Filter(EqualTo(is_row_deleted, 0)),
inner)` match:

  1. Find the underlying Delta `FileSourceScanExec` via
     `collectDeltaScanBelow` (single-child descend).
  2. `scanBelowFallsBackForDvs(inner)` — true when:
     - the scan is a `TahoeBatchFileIndex` whose AddFiles carry DVs
       (Comet's pre-materialised path doesn't apply DVs yet, *and*
       routing through kernel would return a different file set), OR
     - the scan's required schema already contains the synthetic
       `__delta_internal_is_row_deleted` column (Delta's strategy has
       already injected it; routing through Comet would lose the
       Filter).
  3. If yes: leave the wrapper, register the scan in
     `dvProtectedScans`. Phase B will skip it.
  4. If no: `findAndStripDeltaScanBelow` rebuilds the
     `FileSourceScanExec` with `output = userOutput` (intentionally
     anchored on the outer Project's output, not the inner scan's, so
     leaked `_metadata` struct attrs from Delta's inner Project don't
     widen the output and break sibling-Union shape assertions).

### `transformV1Scan` — Delta route

  1. Detect Delta via `DeltaReflection.isDeltaFileFormat`.
  2. Decline if `input_file_name` / `input_file_block_*` appears in the
     enclosing plan (`InputFileBlockHolder` is set thread-locally; Comet
     populates it for the *outer* scan in `setInputFileForDeltaScan` but
     doesn't trace expression nesting).
  3. Otherwise call `nativeDeltaScan`.

### `nativeDeltaScan` — the big decline checklist

In order:

  1. `COMET_DELTA_NATIVE_ENABLED` + `COMET_EXEC_ENABLED` both on.
  2. Encryption config supported.
  3. Schema supported (`isSchemaSupported(scanExec, SCAN_NATIVE_DELTA_COMPAT, r)`).
  4. Not `TahoeLogFileIndexWithCloudFetch` (Databricks-runtime variant).
  5. Filesystem scheme in supported set
     (`file/s3/s3a/gs/gcs/abfss/abfs/wasbs/wasb/oss`).
  6. Cross-FS shallow clone scheme sanity-check (decline malformed
     `s:/path` URIs that Delta tests sometimes mint).
  7. `withDeltaColumnMappingMetadata(scanExec)` — reattach
     `delta.columnMapping.physicalName` metadata onto each `StructField`
     by joining against the authoritative Delta Snapshot schema.
     `HadoopFsRelation` strips this on construction, so without
     reattach `CometParquetFileFormat`'s name-substitution sees nothing
     to do.
  8. Column-mapped + complex columns ⇒ decline (the *fallback*
     `CometScanExec` route, used for non-`CometDeltaNativeScan`-eligible
     code paths, doesn't physical-name-rewrite nested types).
  9. Delta synthetic columns in scan output ⇒ decline.
  10. Row-tracking rewrite (`applyRowTrackingRewrite`) — see below.
  11. On success: return `CometScanExec(scanWithMappedSchema, session,
      SCAN_NATIVE_DELTA_COMPAT)`.

### `applyRowTrackingRewrite`

  - Returns `None` if the scan has no row-tracking columns at all (fast
    path).
  - Returns `Some(None)` (decline) if `delta.enableRowTracking=false`
    or no materialised physical name is in metadata.
  - Otherwise: replaces each `row_id` / `row_commit_version` field in
    the required schema with the materialised physical name, wraps the
    underlying `FileIndex` in a `RowTrackingAugmentedFileIndex` so the
    per-file `baseRowId` / `defaultRowCommitVersion` arrive as
    constant partition columns, and wraps the scan in a `ProjectExec`
    whose output expressions are `coalesce(materialised, baseRowId +
    _tmp_metadata_row_index)` for `row_id` and
    `coalesce(materialised, defaultRowCommitVersion)` for the version.

## `serde/operator/CometDeltaNativeScan.scala` — the `convert()` workhorse

The serde implementation responsible for assembling the
`OperatorOuterClass.Operator` proto. Key sections (file:line into the
branch):

  - **~76:** Resolve `tableRoot` via `DeltaReflection.extractTableRoot`.
  - **~88:** Belt-and-suspenders gate on `IsRowDeletedColumnName` (the
    primary gate is in `CometScanRule`).
  - **~108:** Collect storage options via
    `NativeConfig.extractObjectStoreOptions`. Keyed off the table root
    URI, *not* `inputFiles.head`, because the latter can include
    Delta-test-only test prefixes that aren't URI-safe.
  - **~123:** Pin the snapshot version via
    `DeltaReflection.extractSnapshotVersion`. The `PreparedDeltaFileIndex`
    `toString` looks like `Delta[version=N, …]` — parsing this avoids
    triggering Delta's `cachedState` (which flips
    `stateReconstructionTriggered = true` and breaks ChecksumSuite).
  - **~134:** Translate Spark `dataFilters` to Comet `Expr` proto via
    `exprToProto`, combine into one AND tree.
  - **~166:** Build the column-names array used native-side to resolve
    `BoundReference` indices to kernel column names.
  - **~180:** Branch on file-index family:
    - `TahoeBatchFileIndex` (or any class `isBatchFileIndex` matches):
      - `extractBatchAddFiles(...)` returns the pre-materialised list.
      - If any AddFile carries a DV ⇒ decline (Phase 1 limitation;
        `CometScanRule.scanBelowFallsBackForDvs` keeps Delta's filter
        in place above so DVs still apply correctly).
      - Otherwise build the `DeltaScanTaskList` directly via
        `buildTaskListFromAddFiles`, doing physical→logical
        partition-key translation when the table is column-mapped.
    - Otherwise: `nativeLib.planDeltaScan(tableRoot, snapshotVersion,
      storageOptions, predicateBytes, columnNames)` — the JNI call
      that ends up in `delta::jni::Java_org_apache_comet_Native_planDeltaScan`.
  - **~262:** Augment the parsed `DeltaScanTaskList` with column
    mappings. The kernel path already populates these from
    `Snapshot.schema()`; the pre-materialised path doesn't, so we
    derive them ourselves from the snapshot schema's StructField
    metadata. Skipped when the table's `delta.columnMapping.mode` is
    unset / `none` — some Delta test helpers attach
    `delta.columnMapping.physicalName` even to non-column-mapped tables
    (a test bug we can't fix), so honoring it would corrupt reads of
    those tables.
  - **~319:** Reader-feature gate.
  - **~341:** `prunePartitions(tasks, scan, partitionSchema)` — apply
    static partition filters as an `InterpretedPredicate` against each
    task's `partition_values` row. DPP filters
    (`DynamicPruningExpression`) are excluded here — execution-time
    re-eval handles them.
  - **~353:** `splitTasks(scan, filteredTasks)` — split files larger
    than `maxSplitBytes` into byte-range chunks. DV row indexes are
    copied to every chunk (the native side filters by absolute row
    index regardless of which chunk produced them).
  - **~357:** Build `DeltaScanCommon` proto:
    - `source` (debug name), `tableRoot`, `snapshotVersion`,
      `sessionTimezone`, `caseSensitive`, `ignoreMissingFiles`,
      `dataFileConcurrencyLimit`.
    - **Schemas**: `data_schema` is `relation.dataSchema` minus
      partition columns (Delta includes partition columns in
      dataSchema; physical parquet files don't). `required_schema` is
      the user-visible projection. `partition_schema` is
      `relation.partitionSchema`.
    - **Column-mapping nesting**: when column mapping is active, run
      `physicaliseStructField` / `physicaliseDataType` over the
      `data_schema` fields so every nested struct/array/map name is
      translated to its physical name. `required_schema` stays in
      logical names — the schema-adapter chain in DataFusion bridges
      the two at row-group decode.
    - **`projection_vector`**: maps each output position to either an
      index in `fileDataSchemaFields` (data column) or
      `fileDataSchemaFields.length + partitionIdx` (partition column).
      Mirrors Spark's `FileSourceScanExec`'s split between
      `requiredSchema` and the implicit partition tail.
    - **`data_filters`**: gated on
      `spark.sql.parquet.filterPushdown` and
      `COMET_RESPECT_PARQUET_FILTER_PUSHDOWN`. Filters that reference
      nested-access expressions (`GetArrayItem`, `GetMapValue`, etc.)
      are skipped — DataFusion's pushdown produces "Invalid comparison
      operation" against the file schema, and Spark still evaluates the
      filter post-scan so correctness is unaffected.
    - **`object_store_options`**: full pass-through.
    - **`column_mappings`**: top-level logical→physical pairs only.
  - **~519:** Build `DeltaScan { common: …, tasks: [] }` — tasks
    intentionally omitted. `lastTaskListBytes.set(filteredTaskList
    .toByteArray)` stashes the per-partition payload for `createExec`.

### `createExec`

Pulls the task-list bytes back out of the thread-local, captures the
DPP filters (the `PlanExpression`-bearing subset of
`partitionFilters`), and instantiates `CometDeltaNativeScanExec`.

### Helper functions

  - `buildTaskListFromAddFiles`: builds a `DeltaScanTaskList` from a
    `Seq[ExtractedAddFile]`. Honors absolute paths (URI-scheme
    detection) and joins relative paths against `tableRoot`.
    Translates physical→logical partition keys when needed.
  - `physicaliseStructField` / `physicaliseDataType`: recursive rename
    of every nested field name to its
    `delta.columnMapping.physicalName` metadata value.
  - `maxSplitBytes(scan, fileSizes)`: byte-perfect mirror of Spark's
    `FilePartition.maxSplitBytes`.
  - `splitTasks`: chunk large files into byte ranges; DV row indexes
    are copied verbatim to every chunk.
  - `prunePartitions`: build an `InterpretedPredicate` over the
    partition schema, evaluate against each task's per-partition
    values via `DeltaReflection.castPartitionString`. Excludes DPP.

## `comet.CometDeltaNativeScanExec` (in `org.apache.spark.sql.comet`)

The Spark physical operator that owns one Delta-native scan.

  - **Construction**: takes `nativeOp` (with empty `tasks`),
    `taskListBytes` (the full per-scan task-list to be split per
    partition at execution time), `originalPlan` (kept for
    `relation.sparkSession` / debug), `tableRoot`, `dppFilters`, and
    `partitionSchema`.
  - **`numPartitions` / `outputPartitioning`**: derived from
    `planningPerPartitionBytes.length` so AQE has a partition count to
    plan against. The execution-time count *can* differ if AQE
    materialises new DPP values — `outputPartitioning =
    UnknownPartitioning(...)` so Spark doesn't assume positional
    relationships.
  - **`buildPerPartitionBytes()`**: re-applies DPP via `applyDppFilters`
    (handles unresolved `SubqueryAdaptiveBroadcastExec` by skipping
    pruning for the batch — correct but slower) then bin-packs into
    Spark partitions via `packTasks` (replicates Spark's
    `FilePartition` packing using the session conf for
    `openCostInBytes`, `maxPartitionBytes`, and `minPartitionNum`).
  - **`metrics`**: registers `output_rows` (and `numOutputRows` alias
    for the streaming `ProgressReporter`), `num_splits`, `total_files`
    (and `numFiles` alias for Delta partition-pruning tests), and
    `dv_files`.
  - **`doExecuteColumnar`**: rebuilds `execPerPartitionBytes` so DPP
    sees the latest broadcast values, threads through encryption hadoop
    conf when required, and invokes `CometExecRDD` with `commonByKey`
    keyed by `sourceKey`.
  - **`sourceKey`**: stable hash over `tableRoot |
    snapshotVersion | required_schema | data_filters |
    projection_vector | column_mappings`. Critical for self-joins
    across snapshot versions — without `snapshot_version` in the key,
    `ReuseExchangeAndSubquery` would collapse the two scans into one
    shuffle and silently return v0 rows for both sides.
  - **`synthesizedFilePartitions`**: backstop for Delta tests
    (`DeltaSinkSuite`, partition-pruning checks) that introspect the
    plan via `inputRDDs.head.asInstanceOf[FileScanRDD].filePartitions`.
    The dev/diffs/delta/<ver>.diff patches Delta's helper to fall
    through to this method so the same assertions keep passing.

## `comet.CometExecRDD` — `setInputFileForDeltaScan`

Comet's normal RDD doesn't populate Spark's
`InputFileBlockHolder`, but Delta's MERGE/UPDATE/DELETE commands rely
on `select(input_file_name()).distinct()` to identify touched files.
The new helper (in `CometExecRDD.scala` ~117–215) pulls the per-partition
`DeltaScan` bytes out of the plan-data map, finds the file path, and
calls `InputFileBlockHolder.set(...)` once per partition. No-op for
non-Delta plans.

## `comet.operators.DeltaPlanDataInjector`

`PlanDataInjector` is Comet's split-mode interface (`canInject`,
`getKey`, `inject`). The Delta one:

  - `canInject(op)` — true when the op is a `DeltaScan` with empty
    tasks but populated common.
  - `getKey(op)` — derived from
    `CometDeltaNativeScanExec.computeSourceKey(op)`, identical to the
    driver-side key so the per-partition map and the executor lookup
    agree.
  - `inject(op, commonBytes, partitionBytes)` — caches parsed
    `DeltaScanCommon` in a `LinkedHashMap`-backed LRU (max 16 entries,
    keyed by the bytes' `ByteBuffer`) so multi-partition queries don't
    re-parse the same header per partition. Then `addAllTasks` from
    the per-partition slice and rebuild the operator.

`operators.scala` ~86 registers `DeltaPlanDataInjector` alongside the
existing `IcebergPlanDataInjector` and `NativeScanPlanDataInjector`.

## A note on shims

`spark/src/main/spark-{3.4,3.5,4.0}/org/apache/spark/sql/comet/shims/
ShimSparkErrorConverter.scala` — the only Spark-version-specific shim
the branch touches. Adds a no-op converter for the
`DeltaAnalysisException` lineage so error-rethrow paths stay version
clean.

## Summary

The Scala side does the *hard* work: detect Delta, resolve the snapshot
without triggering Delta's state-reconstruction caches, decide between
the kernel path and the pre-materialised path, translate filters,
attach column-mapping metadata that `HadoopFsRelation` strips, prune
and split tasks to match Spark's `FilePartition` behavior, deal with
DPP / AQE, and pack everything into the split-mode protobuf the native
side can replay.

The native side, by comparison, is a relatively thin glue layer
between Comet's existing parquet exec, kernel's log-replay output, and
DataFusion's operator graph. See [native-execution-side.md](native-execution-side.md).

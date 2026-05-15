# Features & Limitations

## Scope

The branch accelerates **reads** of Delta Lake tables. Writes (`INSERT`,
`UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `VACUUM`, etc.) continue to use
Spark + Delta entirely — Comet only intervenes on the read side, so any
write-side scan that happens *as part of* a write (e.g. MERGE's source-vs-
target join) is the part that gets accelerated.

The reader is gated on `spark.comet.scan.deltaNative.enabled=true`. With
the flag off, Comet behaves as if the integration weren't compiled in —
its planner rules see Delta scans, classify them as a non-Comet file
format (since Comet has no compile-time `spark-delta` dependency), and
leave them alone.

## Configuration knobs

All defined in `common/src/main/scala/org/apache/comet/CometConf.scala`.

| Config | Default | Effect |
|---|---|---|
| `spark.comet.scan.deltaNative.enabled` | `false` | Master switch for the entire Delta path (planner rules, serde, native execution). |
| `spark.comet.scan.deltaNative.dataFileConcurrencyLimit` | `1` | Per-task parallelism inside the native parquet reader. Raise to 2–8 for I/O-bound workloads on tables with many small files. |
| `spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature` | `true` | When the kernel-reported reader-feature set lists anything Comet doesn't handle on this snapshot, decline acceleration rather than risk silent miscompares. |

The branch *also* manipulates one Delta-side config automatically (see
`CometSparkSessionExtensions.CometDeltaDvConfigRule`):

  - **`spark.databricks.delta.deletionVectors.useMetadataRowIndex`** is
    set to `false` for the duration of any logical plan that contains a
    Delta scan we'd accelerate. This forces Delta to use its older
    `Project + Filter(__delta_internal_is_row_deleted = 0)` strategy for
    DV-bearing reads, which Comet can detect and rewrite. The override is
    skipped when the user / test has explicitly set the conf, when the
    plan already references the synthetic `is_row_deleted` column (raw
    DV-read tests), or when the session is a derived `spark.newSession`
    that may have inherited intent from its parent.

## Detection — when Comet will *try* to accelerate

A scan reaches the native Delta path when **all** of the following hold:

1. Comet is loaded (`spark.plugins=org.apache.spark.CometPlugin`).
2. `spark.comet.scan.deltaNative.enabled=true`.
3. The plan contains a `FileSourceScanExec` whose `relation.fileFormat`
   passes `DeltaReflection.isDeltaFileFormat` (i.e. the class name is
   `org.apache.spark.sql.delta.DeltaParquetFileFormat` or a subclass —
   no compile-time dependency).
4. The scan does *not* hit any of the explicit decline conditions listed
   below.

When all four hold, the scan is rewritten into a `CometDeltaNativeScanExec`
that owns a `DeltaScan` operator proto plus a side-channel byte array
of per-file tasks. When any condition fails, the original
`FileSourceScanExec` is left in place and Spark + Delta read it normally.

## Supported reads

### File index families

| Delta file index | Path |
|---|---|
| `TahoeLogFileIndex` (regular reads, time travel) | kernel log replay via `Native.planDeltaScan` |
| `PreparedDeltaFileIndex` (shallow-clone reads, materialised lists) | kernel log replay (with extra path-validation guards for cross-FS clone mocks) |
| `TahoeBatchFileIndex` (MERGE / UPDATE / DELETE post-join scans) | pre-materialised AddFile path (skips kernel — see [edge-cases](edge-cases-and-fallbacks.md#why-pre-materialised-file-indexes-skip-kernel)) |
| `CdcAddFileIndex` / `TahoeRemoveFileIndex` / `TahoeChangeFileIndex` (CDF) | pre-materialised AddFile path; CDC metadata columns flow through partition-value channel |

### Reader features

  - All primitive types, complex types (struct, array, map at arbitrary
    nesting), schema evolution.
  - Time travel: `VERSION AS OF` and `TIMESTAMP AS OF`.
  - Column mapping modes `none`, `id`, and `name`. Including
    `ALTER TABLE RENAME COLUMN` (metadata-only) and nested-field physical
    names through structs / arrays / maps.
  - Deletion vectors (inline + on-disk DV files), including DV
    replacement across multiple commits and `delta.enableDeletionVectors=
    true` tables. DV row indexes are materialised by kernel on the driver
    and sent over the wire on each `DeltaScanTask`.
  - Row tracking (`delta.enableRowTracking=true`). `_metadata.row_id` /
    `_metadata.row_commit_version` are produced by reading Delta's
    materialised physical columns when present, falling back to a
    `coalesce(materialised, baseRowId + parquet_row_index)` synthesis
    via the `RowTrackingAugmentedFileIndex` wrapper.
  - Type widening (`delta.enableTypeWidening=true`) — relies on
    DataFusion's parquet schema adapter to cast the file's stored type to
    the table's current widened type at read time.
  - Change Data Feed (`option("readChangeFeed", "true")`) reads via the
    pre-materialised path; CDC metadata columns ride the partition-value
    channel.
  - Predicate pushdown at two levels: (1) **file-level** via kernel's
    log-stats pruning, predicate translated by
    `catalyst_to_kernel_predicate_with_names`; (2) **row-group-level** via
    DataFusion's `ParquetSource` page-index / row-group stats.
  - Static partition pruning (Catalyst `partitionFilters`) at planning
    time and Dynamic Partition Pruning (DPP) at execution time, with
    correct AQE handling — DPP filters that arrive as
    `SubqueryAdaptiveBroadcastExec` are deferred to `doExecuteColumnar`
    and re-evaluated once AQE materialises the broadcast.
  - File splitting: large parquet files are bin-packed into multiple
    tasks per Spark partition (`packTasks`) and individual files larger
    than `files.maxPartitionBytes` are split into byte-range chunks
    (`splitTasks`), matching Spark's `FilePartition.splitFiles`.
  - Filesystems: local (`file://`), S3 (`s3://`, `s3a://`),
    GCS (`gs://`, `gcs://`), Azure (`abfs://`, `abfss://`,
    `wasb://`, `wasbs://`), and Aliyun OSS (`oss://`).
  - Parquet encryption — when the encryption config is supported by
    Comet's parquet path, the encryption hadoop conf is broadcast to
    executors and the per-file path list is forwarded so each file can
    be decrypted.

### Storage credentials

Driver-side: extracted from the table's hadoop conf via
`NativeConfig.extractObjectStoreOptions` and forwarded to kernel through
`DeltaStorageConfig`. Both kernel-style keys (`aws_access_key_id`,
`aws_region`, …) and Hadoop-style keys (`fs.s3a.access.key`,
`fs.s3a.endpoint`, `fs.s3a.path.style.access`, `fs.s3a.session.token`,
`fs.s3a.endpoint.region`) are recognised. Azure account-name / access-key
/ bearer-token are supported by kernel-style keys only.

## Cases where Comet declines acceleration

The branch is biased toward **correctness over coverage**. Whenever a
particular plan-level or table-level signal makes the native path
risky, Comet declines and the scan stays as `FileSourceScanExec` so
Spark + Delta produce the result. Each decline emits a
`withInfo(scanExec, "…")` annotation that explainFallback surfaces in
`EXPLAIN`.

The full list of decline conditions, with the file:line where each is
enforced:

| Decline | Where | Reason |
|---|---|---|
| `spark.comet.scan.deltaNative.enabled=false` | `CometScanRule.scala` ~505 | Master switch off. |
| `spark.comet.exec.enabled=false` | `CometScanRule.scala` ~512 | Native execution required. |
| Encryption config unsupported | `CometScanRule.scala` ~521 | Comet's encryption support has a narrower config range than Spark's. |
| Schema unsupported by Comet's type system | `CometScanRule.scala` ~525 | Routed through the same `isSchemaSupported` checker as the native Iceberg-compat path. |
| `TahoeLogFileIndexWithCloudFetch` | `CometScanRule.scala` ~545 | Databricks-runtime variant; not exercised by OSS Delta and never validated. |
| Filesystem scheme not in supported set | `CometScanRule.scala` ~561 | Currently `file/s3/s3a/gs/gcs/abfss/abfs/wasbs/wasb/oss`. Schemes that need additional code (e.g. `hdfs`) decline. |
| Cross-filesystem shallow clone with malformed `s:/path` | `CometScanRule.scala` ~582 | Delta tests sometimes mint single-slash test-only URIs; Comet declines rather than failing at the object-store layer. |
| Column-mapped table whose required schema contains complex (struct / array / map) columns *that fall back to the non-native serde route* | `CometScanRule.scala` ~627 | The CometScanExec fallback path doesn't physical-name-rewrite nested types; declines to avoid `Utf8 <= Int32` type mismatches. |
| `__delta_internal_is_row_deleted` / `__delta_internal_row_index` in scan output | `CometScanRule.scala` ~656 | Synthetic columns produced only by `DeltaParquetFileFormat`; Comet's reader has no equivalent. |
| Row-tracking columns referenced but `delta.enableRowTracking=false` | `CometScanRule.scala` ~758 | Table isn't producing the columns at all. |
| Row-tracking columns referenced but no materialised physical names in metadata | `CometScanRule.scala` ~771 | Pure `baseRowId + row_index` synthesis (no materialised column to coalesce against) is left for a follow-up. |
| `input_file_name()` / `input_file_block_*` in plan | `CometScanRule.scala` ~358 | Best-effort: `CometExecRDD.setInputFileForDeltaScan` populates `InputFileBlockHolder` for the *outer* scan, but expressions buried under projections in the same plan still need coverage. |
| Parquet read of a `_delta_log/` checkpoint file | `CometScanRule.scala` ~387 | Delta's own error-message contract on broken checkpoints is asserted by tests; Comet's generic "Data read failed" loses that context. |
| `TahoeBatchFileIndex` AddFile carries a DeletionVectorDescriptor (DV-bearing MERGE/UPDATE/DELETE post-join scan) | `CometDeltaNativeScan.scala` ~209 | Pre-materialised path doesn't yet thread DV indexes through; declines so the Delta-injected DV filter above continues to work. |
| Reflection failure extracting AddFiles from a batch index | `CometDeltaNativeScan.scala` ~222 | Defensive — keeps tests robust against Delta-version skew. |
| Snapshot reader-feature gate trips | `CometDeltaNativeScan.scala` ~319 | `taskList.unsupported_features` is non-empty. As of this branch the gate is empty by construction (`scan.rs` ~152), but the plumbing is preserved for future per-snapshot capability checks. |
| Plain Parquet scan via DataFusion path doesn't support `input_file_name` | `CometScanRule.scala` ~451 | Same as above but for the non-Delta DataFusion scan. |

## Cases where Comet *does* accept the scan but takes a slow-ish path

  - **Pushdown of nested-access filters** is dropped (logged as
    `skipping pushdown of nested-access filter ...`) when the filter
    references a `GetArrayItem` / `GetArrayStructFields` / `GetMapValue`
    expression. Spark still evaluates the filter post-scan, so results
    are correct — only row-group-level pruning is lost.
  - **DV-in-use scans** put one file per `FileGroup` so per-partition DV
    bookkeeping stays 1:1 with one physical file. Non-DV files are
    bin-packed normally. The choice intentionally trades scheduling
    granularity for a much simpler running-row-offset DV applicator.

## Observability

`CometDeltaNativeScanExec.metrics` registers, in addition to the
standard `output_rows` / `numOutputRows` (the latter aliased so Spark's
streaming `ProgressReporter` finds it):

  - `total_files` and `numFiles` (the latter aliased to match
    `FileSourceScanExec` — Delta tests check it for partition pruning).
  - `dv_files` — count of files with deletion vectors attached.
  - `num_splits` — how many byte-range / file-group slices the scheduler
    actually produced.

`spark.comet.explainFallback.enabled=true` surfaces every `withInfo`
decline annotation in `EXPLAIN EXTENDED`, which is the primary way to
find out *why* a particular Delta scan didn't get accelerated.

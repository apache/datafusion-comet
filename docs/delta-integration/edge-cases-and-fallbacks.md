# Edge cases & correctness traps

This file collects the long tail of subtle issues the branch addresses.
Most of them are individually small, but together they're the bulk of
the hard-won correctness work — each one is a fall-back trigger or a
specific behavior that exists *because* of a real Delta-side test or
runtime quirk. Most have a one-line note in the source pointing back
to the offending suite or scenario.

## Deletion vectors — three guises, three handlers

DVs reach Comet from three places:

  1. **Plan-level `Project + Filter(__delta_internal_is_row_deleted = 0)`**
     wrappers that Delta's `PreprocessTableWithDVsStrategy` injects
     during logical-to-physical conversion. Triggered when
     `useMetadataRowIndex=false`.
  2. **`DeletionVectorBoundFileFormat`** wrapping the underlying
     `FileFormat` opaquely. Triggered when `useMetadataRowIndex=true`
     (Delta's default) — this is the one Comet *cannot* intercept,
     which is why `CometDeltaDvConfigRule` flips the conf to false.
  3. **AddFile-with-DeletionVectorDescriptor** travelling on a
     `TahoeBatchFileIndex` for MERGE / UPDATE / DELETE post-join scans.

Comet's handling:

  - Strategy 1 + the scan is a regular `TahoeLogFileIndex` /
    `PreparedDeltaFileIndex`: `stripDeltaDvWrappers` removes the
    Project+Filter, the kernel path materialises DVs into per-file
    `Vec<u64>`, and `DeltaDvFilterExec` applies them per batch on the
    executor.
  - Strategy 1 + the scan is a `TahoeBatchFileIndex` with DVs:
    `scanBelowFallsBackForDvs` returns true, the wrapper stays, the
    inner scan is added to `dvProtectedScans`, and Comet declines —
    Delta's reader supplies the synthetic column and applies the DV.
  - Strategy 1 + a `PreparedDeltaFileIndex` whose required schema
    *already* contains `__delta_internal_is_row_deleted`: same — Delta's
    `PreprocessTableWithDVs` has already injected the column, Comet
    can't synthesise it, so the wrapper stays and Comet declines.
  - Strategy 2 (metadata-row-index): Comet shouldn't be reaching this
    path — `CometDeltaDvConfigRule` flips the conf off pre-strategy.
    The `synthetic columns in scan output` decline check is a
    second-line defense.
  - Strategy 3: `CometDeltaNativeScan.convert` declines outright if any
    AddFile carries a DV (Phase-1 limitation).

## Column mapping — three places it has to be respected

Delta's column mapping (`id` or `name` mode) renames every column in
the parquet files to a UUID-derived "physical" name. The logical
schema the user sees is mapped via per-StructField metadata:
`delta.columnMapping.physicalName = "col-<uuid>-<short>"`.

Three places this metadata has to flow through Comet correctly:

  1. **Top-level data columns**: native side substitutes physical
     names into `data_schema` so `ParquetSource` projects by physical
     name; a `ProjectionExec` aliases physical → logical at the end so
     upstream operators see logical names.
  2. **Nested fields** (struct inner fields, array elements, map
     keys/values): `physicaliseStructField` / `physicaliseDataType`
     recurses through the schema during serde, applying physical names
     at every level. `required_schema` keeps logical names; only
     `data_schema` goes physical.
  3. **Filter expressions**: `ColumnMappingFilterRewriter` walks each
     `PhysicalExpr` filter and renames `Column(logical)` to
     `Column(physical)`. Without this, the row-group filter would look
     up logical names against the physical-named batch and either
     return nulls or trigger "Unable to get field named ..." errors.

A subtle gotcha: `HadoopFsRelation` strips the `physicalName` metadata
from `dataSchema` on construction. The branch fixes this two ways:
  - On the kernel path: kernel's `Snapshot.schema()` is the
    authoritative source, and `column_mappings` flows through the proto.
  - On the pre-materialised path: `withDeltaColumnMappingMetadata` in
    `CometScanRule` reattaches metadata onto the scan's relation by
    joining against the snapshot schema fetched via reflection.

Another gotcha: some Delta tests (`DeltaSourceSuiteBase.withMetadata`)
attach `physicalName` metadata even to non-column-mapped tables. The
serde guards against this by only honoring physical names when the
authoritative `delta.columnMapping.mode` configuration is `id` or
`name` — otherwise Comet would chase non-existent physical names and
produce empty rows.

The hardest case is "drop column with constraints" on a column-mapped
table with complex columns. The fallback non-native path through
`CometScanExec` doesn't consistently rewrite nested physical names,
producing `Utf8 <= Int32` errors downstream. The branch declines
acceleration for column-mapped tables with complex required-schema
columns when the route would go through the fallback path
(`CometScanRule.scala` ~627). For pure scalar column-mapped tables
the native serde handles it correctly.

## Row tracking — synthesis vs. materialisation

Delta's row tracking exposes per-row `_metadata.row_id` and
`_metadata.row_commit_version` columns. Their values come from one of
two places:

  - **Materialised** in a physical column. Tables whose row tracking
    was enabled at create time (or that have been backfilled) carry
    the values inline; the materialised physical column name is in
    `Metadata.configuration` under the
    `materializedRowIdColumnName` / `materializedRowCommitVersionColumnName`
    properties.
  - **Synthesised** as `baseRowId + parquet_row_index` (for `row_id`)
    or `defaultRowCommitVersion` (for `row_commit_version`). These
    per-file constants live on the `AddFile` action in the log.

The branch implements both, in the simplest order: read the
materialised column when present, fall back to synthesis when null.

The `applyRowTrackingRewrite` function (`CometScanRule.scala` ~742):

  1. Detect any `row_id` / `row_commit_version` field in the required
     schema.
  2. If `delta.enableRowTracking=false` ⇒ decline.
  3. If neither materialised name is in metadata ⇒ decline (pure
     synthesis without a coalesce target isn't yet wired up).
  4. Rewrite the schemas to read the materialised physical column.
  5. Wrap the underlying `FileIndex` in a
     `RowTrackingAugmentedFileIndex` so `AddFile.baseRowId` and
     `defaultRowCommitVersion` arrive as per-file constant partition
     columns.
  6. Wrap the scan in a `Project(coalesce(materialised, baseRowIdCol +
     _tmp_metadata_row_index))`. The `_tmp_metadata_row_index` field is
     pulled from the parquet reader using Spark's
     `FileSourceGeneratedMetadataStructField` mechanism that Comet
     already handles.

## Time travel — pinning the snapshot

Delta's analyzer pins the resolved snapshot version into the
`PreparedDeltaFileIndex` at analysis time. By the time Comet sees
the `FileSourceScanExec`, `relation.location.toString` looks like:

```
Delta[version=42, file:/path/to/table]
```

`DeltaReflection.extractSnapshotVersion` parses the version from the
`toString` rather than calling into Delta's APIs. This is deliberate:
calling `TahoeLogFileIndex.cachedState` (the official accessor) flips
`stateReconstructionTriggered = true` on the snapshot, which breaks
Delta's own `ChecksumSuite` "post commit snapshot should have a
checksum without triggering state reconstruction" assertion.

Same reasoning applies to `inputFiles`: the branch validates filesystem
schemes by reading `rootPaths` rather than enumerating files, again to
avoid triggering state reconstruction.

## Shallow clone path quirks

Shallow-cloned Delta tables store their AddFile paths as absolute
URLs pointing at the *source* table's filesystem. Two complications:

  1. **Hadoop `Path.toUri.toString` form**: produces `file:/abs/path`
     (single slash) instead of the spec-compliant `file:///abs/path`.
     Comet's `resolve_file_path` accepts both via `has_uri_scheme` and
     passes them through verbatim — joining onto `table_root` would
     double-prefix. The native scan layer (`object_store::path::Path
     ::from_url_path`) opens both forms.
  2. **Cross-FS clone test mocks**: Delta's test harness occasionally
     emits malformed test-only URIs like `s3:/path` (single slash, real
     S3 URLs are `s3://bucket/key`). For `PreparedDeltaFileIndex` Comet
     samples the first couple of input files and declines on detection
     so the native reader doesn't fail at object-store open time.

## Streaming and CDF

  - **Streaming source progress**: Delta streaming relies on Spark's
    `ProgressReporter` which extracts per-source input row counts from
    the metric named `numOutputRows`. Comet's native side emits
    `output_rows`. The branch aliases the same `SQLMetric` instance
    under both names so streaming progress reads the right value.
  - **Pre-materialised file indexes for streaming micro-batches**: a
    streaming source surfaces a `TahoeLogFileIndex` whose `addFiles`
    has been narrowed to one micro-batch. Re-running kernel log replay
    against the same snapshot would return *all* active files (much
    more than the batch); the pre-materialised path uses the
    `addFiles` list directly to honor the batch's exact file set.
  - **CDF (`option("readChangeFeed", "true")`)**: Delta's CDC indexes
    (`CdcAddFileIndex`, `TahoeRemoveFileIndex`, `TahoeChangeFileIndex`)
    stash the CDC metadata columns (`_change_type`, `_commit_version`,
    `_commit_timestamp`) into `AddFile.partitionValues` and add them
    to `partitionSchema`. Comet treats them as ordinary partition
    columns — they materialise via the standard partition-value path.
    No CDC-specific wiring needed beyond classifying these indexes as
    "batch-style" so `extractBatchAddFiles` covers them.

## MERGE / UPDATE / DELETE post-join scans

After analyzing a MERGE, UPDATE, or DELETE, Delta produces an internal
read of *only the touched files* via a `TahoeBatchFileIndex` with a
pre-narrowed `addFiles` list. Important constraints:

  - File set must be honored exactly. Re-running log replay would
    return the full snapshot.
  - `input_file_name()` must work — Delta's UPDATE traverses
    `select(input_file_name()).distinct()` to identify what files to
    rewrite. Comet's `setInputFileForDeltaScan` populates
    `InputFileBlockHolder` per-partition for embedded Delta scans so
    these expressions evaluate correctly. The branch also declines the
    optimised path for any plan that references `input_file_name()`
    inside expressions (a defense-in-depth measure).
  - DV-bearing AddFiles still fall back (Phase-1 limit).

## DPP / AQE interaction

Dynamic Partition Pruning (`DynamicPruningExpression`) can land on a
Delta scan in two states:

  - **Resolved** to an `InSubqueryExec` whose values are populated.
    Standard `InterpretedPredicate` evaluation in
    `applyDppFilters` handles this.
  - **Unresolved** as a `SubqueryAdaptiveBroadcastExec` placeholder
    that AQE will replace with the real broadcast plan once the
    upstream stage runs.

Two careful behaviors:

  1. `prunePartitions` (planning-time) excludes DPP filters entirely
     — they're not resolved yet. Static partition filters still apply.
  2. `buildPerPartitionBytes` is *not* memoised — it recomputes at
     every `doExecuteColumnar` so AQE-resolved values are picked up.
     If the broadcast still hasn't materialised, `applyDppFilters`
     skips pruning (correct but slower) rather than calling
     `doExecute` on the placeholder (which throws).

A related fix outside the Delta module: Comet's other
`CometNativeExec` wrappers preserve the logical link on Comet
exchange wrappers so AQE can correctly correlate stages. Without
this, AQE-driven shuffles re-introduced after the Comet rewrite
would lose their original logical anchor and produce wrong DPP
broadcasts. (Two commits in the branch — `7d8c3b0b` and `7d52de7b`
— land the fix.)

## Encryption

Mirrors `CometNativeScanExec`'s wiring:
  - At planning time: validate the encryption hadoop conf is
    supported via `isEncryptionConfigSupported`. Decline otherwise.
  - At execution time: when `encryptionEnabled(hadoopConf)`,
    `broadcast` the conf and gather every input file path so the
    parquet reader can decrypt per file.

## ignoreMissingFiles

Forwarded from `SQLConf.ignoreMissingFiles` (or the
`ignoremissingfiles` data-source option). Plumbed into the proto's
`ignore_missing_files` flag, then into
`init_datasource_exec`, where the resulting `FileSource` is decorated
with `IgnoreMissingFileSource`. Per-file `NotFound` becomes an empty
stream for that file; everything else fails normally.

## Misc smaller traps

  - **Time-travel partition values**: `TIMESTAMP` partition values are
    stored in the writer's JVM default timezone, not UTC. The native
    side's `build_delta_partitioned_files` takes
    `common.session_timezone` and uses it for parsing. The
    `castPartitionString` helper in `DeltaReflection` does the same on
    the Scala side for partition-pruning evaluation.
  - **`TIMESTAMP_NTZ` partition values**: NTZ values must NOT shift by
    the session TZ. A separate code path in the partition-string
    parser handles this — without it the read-back values are off by
    the local UTC offset.
  - **Self-join across snapshot versions**: `sourceKey` includes
    `snapshot_version` so `ReuseExchangeAndSubquery` doesn't collapse
    the two scans into one shuffle output.
  - **Streaming `numOutputRows` alias**: see Streaming above.
  - **`numFiles` metric alias**: Delta's own `DeltaSuite` partition-
    pruning tests look up `metrics.get("numFiles")` (the
    `FileSourceScanExec` name) on the executed plan. The branch
    aliases `total_files` to `numFiles` so these assertions pass on
    Comet's exec.
  - **`synthesizedFilePartitions` accessor**: tests like
    `DeltaSinkSuite` introspect `inputRDDs.head.asInstanceOf
    [FileScanRDD].filePartitions` to check partition pruning. The
    `dev/diffs/delta/<version>.diff` patches the helper in Delta to
    call this accessor as a backstop, so the same assertions pass.
  - **Plain Parquet read of `_delta_log` files**: Delta's own error
    contracts (specific substrings expected in errors) are tested
    against checkpoint failures. Comet wraps reads in a generic "Data
    read failed", losing the substring. The branch declines acceleration
    for any parquet scan rooted under `_delta_log/`.
  - **`DebugFilesystem` and Delta test prefixes**: Delta's tests inject
    `test%file%prefix-` and `test%dv%prefix-` into file names, but
    delta-kernel-rs reads files by the *real* names recorded in the
    transaction log, which don't include the prefixes. The
    `dev/diffs/delta/<version>.diff` disables these prefixes for the
    regression run; production users are unaffected.

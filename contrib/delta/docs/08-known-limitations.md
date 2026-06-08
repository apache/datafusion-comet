<!--
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

# Known limitations & deliberate tradeoffs

This document tracks deliberate limitations, workarounds, and known-failing
behaviors in the contrib-delta native scan, so they can be opened as GitHub
issues once the work merges. Each entry notes: the behavior, why it's that way,
the correctness impact, the guarding test (if any), and the work needed to close
it.

Two kinds of entries:

- **Tradeoffs** — places where we deliberately accept reduced acceleration (or
  decline to native-scan) to preserve correctness. These are stable and
  intentional; the "fix" is a future enhancement.
- **Pending regression failures** — Delta own-suite tests that still fail under
  Comet and are not yet fixed. These are bugs to close, grouped by root cause.

The Delta own-suite regression is run via
`contrib/delta/dev/run-regression.sh 4.1.0 full` (see
`.github/workflows/delta_regression_test.yml`).

---

## Part A — Deliberate tradeoffs (open as enhancement issues)

### A1. DPP on a partitioned Delta scan — FIXED

- **History:** A dynamic-partition-pruning (DPP) broadcast join / MERGE over a
  partitioned Delta table originally crashed
  (`CometSubqueryAdaptiveBroadcastExec ... does not support the execute() code
  path`), then (intermediate fix) ran correctly but unpruned in the
  native-block case. Now it engages the native scan, returns correct results,
  AND prunes to the required partitions in both the standalone and the
  parent-block (MERGE/join) cases.
- **Two root causes that had to be solved:**
  1. **Orphaned rewrite (`#3510`).** The AQE DPP subquery arrives as an
     unexecutable `CometSubqueryAdaptiveBroadcastExec`.
     `CometPlanAdaptiveDynamicPruningFilters` rewrites it to an executable
     `CometSubqueryBroadcastExec` (with broadcast reuse), but the rewritten
     scan *copy* was dropped when `transformUp` rebuilt the enclosing native
     block (`TreeNode.makeCopy` can't carry `@transient` fields). **Fix:** the
     scan implements `withDynamicPruningFilters` to install the rewrite IN PLACE
     via a transient side-channel (`dppFiltersOverride`) and return `this`, so
     it lands on the same instance that executes. `dppFilters` (the case-class
     field) is left untouched so node equality/canonicalization is unaffected.
  2. **Fixed partition count vs runtime pruning.** The native scan's partition
     count is pinned at planning. **Fix:** group ALL tasks once
     (`taskGroups = packTasks(allTasks)`) so the count is stable, then prune
     tasks WITHIN each group at execution (a fully-pruned group becomes an empty
     DeltaScan = 0 rows, but the partition slot remains). `perPartitionData` is
     recomputed (not memoized) so a parent block's `findAllPlanData` sees the
     pruned task lists after the broadcast is materialized.
- **Guard:** `CometDeltaDppReproSuite` asserts native engagement, correct
  results, AND that the fact scan reads ~120 of 2000 rows (real pruning) for a
  SELECT broadcast join; plus a MERGE-into-partitioned guard.
- **Residual (MERGE / re-planned plans):** the in-place rewrite lives in a
  transient `dppFiltersOverride` (not a constructor field), so it is LOST
  whenever the plan is copied after the optimizer rule runs (e.g. MERGE
  re-plans internally). In that case `effectiveDppFilters` reverts to the
  placeholder and the scan reads ALL partitions (correct, unpruned) rather
  than pruning. To stay crash-safe, BOTH subquery-resolution paths skip the
  unexecutable placeholder: the fused-block path (`ensureSubqueriesResolved`,
  via `findAllPlanData`) and the standard-lifecycle path (`waitForSubqueries`,
  used when the scan is a native-block root, e.g. a MERGE target read under
  `CometNativeColumnarToRowExec`). `applyDppFilters` enforces the same skip.
- **Commits:** `64cd878a` (no-crash) → in-place rewrite + stable-group pruning.

### A2. Cloud credential plumbing — brought to core parity

The full credential audit (task #72) aligned the Delta engine with core Comet's
`parquet_support::prepare_object_store_with_configs`: **S3** bridges Hadoop
`fs.s3a.*` keys explicitly (object_store can't read them); **Azure / GCS / other**
schemes are built through `object_store::parse_url`, which sources credentials from
the ambient environment (`AZURE_*` / `GOOGLE_*` / ADC / instance metadata). A2a–A2d
are now closed; A2e is the remaining residual.

- **A2a. GCS (`gs://`) — FIXED.** `create_object_store` now has a `gs`/`gcs` arm
  (and `az`/`azure`/`abfs`/`abfss`/`wasb`/`wasbs`) that builds via
  `object_store::parse_url`, exactly as core does for non-S3 schemes. GCS reads
  resolve credentials from ADC / `GOOGLE_*` env. The `gcp` object_store feature
  is enabled in the contrib crate (matching core's feature set). No bespoke
  `fs.gs.*` bridging — core doesn't bridge them either.
- **A2b. Per-bucket S3 keys — FIXED.** `delta_storage_config_from_map` takes the
  table's S3 bucket (parsed from the table-root URL in `jni.rs` /
  `delta_scan.rs`) and prefers `fs.s3a.bucket.<bucket>.<suffix>` over the global
  `fs.s3a.<suffix>`, matching Hadoop S3A / core semantics. Guarded by
  `jni::tests::extract_storage_config_matrix`.
- **A2c. Azure (`abfs`/`abfss`/...) — FIXED via parity.** Azure is built through
  `object_store::parse_url` + ambient credentials (env / managed identity), the
  same mechanism core uses. Hadoop `fs.azure.*` config bridging is intentionally
  not done (core doesn't do it either), so this matches core exactly rather than
  exceeding it.
- **A2d. DV-read + synthetic-columns paths ignored `storage_config` — FIXED.**
  `dv_reader::read_dv_indexes` now takes a `&DeltaStorageConfig`;
  `DeltaKernelScanExec` passes `&self.storage_config` and `DeltaSyntheticColumnsExec`
  gained a `storage_config` field threaded from the core planner. DV reads against a
  credentialed store now reuse the data read's engine instead of building a
  credential-less one. (Was a real correctness bug for credentialed tables with
  deletion vectors.)
- **A2e. RESIDUAL — Hadoop explicit S3 credential-provider classes.**
  `fs.s3a.aws.credentials.provider` (AssumedRole / WebIdentity / custom provider
  classes) is not honored: when no static keys are present, object_store's own
  default chain is used instead (static keys → IMDS / ECS task role / env). This
  covers instance-profile and IRSA-via-IMDS, but not an explicitly-configured
  assumed-role / web-identity provider. Closing it means exposing core's
  `objectstore::s3::create_store` through `comet-contrib-spi` (a core change beyond
  this contrib PR). A secondary residual: Layer-2
  `augmentWithResolvedAwsCredentials` resolves only global provider-chain keys, not
  per-bucket ones (static per-bucket is already handled by A2b).
- **Correctness:** Where unsupported, reads fail (no creds) rather than producing
  wrong data.
- **Guard:** `CometDeltaCredentialAuditSuite`,
  `jni::tests::{extract_storage_config_matrix, azure_and_gcs_keys_do_not_leak_into_s3_config}`,
  `engine::tests::create_object_store_{azure,gcs}_via_parse_url`.

### A3. Path-based CDF reads decline to native (`DeltaCDFRelation`)

- **Behavior:** Table-API CDC reads engage native (`CometDeltaCdcSuite`).
  **Path-based** `readChangeFeed` (`spark.read.format("delta").option("readChangeFeed", true).load(path)`)
  routes through `DeltaCDFRelation`, which `DeltaScanRule` (matching
  `CometScanExec` over `HadoopFsRelation`) does not intercept — so it runs on
  Spark's reader.
- **Correctness:** Correct (Spark handles it); no acceleration.
- **To close:** Teach the rule to handle `DeltaCDFRelation`.
- **Guard:** `CometDeltaScanConfAuditSuite` ("GAP CDF: path-based readChangeFeed
  does not engage native").

### A4. VARIANT type

- **Behavior:** Tolerant guard — if native engages on a VARIANT column it must
  match vanilla; today it may decline. Documented so a future silent-corruption
  regression is caught.
- **Guard:** `CometDeltaTypeRoundTripAuditSuite` ("GAP: VARIANT").

### A5. Decline gates (intentional fallbacks)

`DeltaScanRule` deliberately declines to native-scan in these cases (correct via
Spark's reader; no acceleration). These are not bugs but are worth an issue if we
want to accelerate them:

- CDC delete/insert event reads with inverted DV semantics
  (`hasInvertedRowIndexFilters`).
- DV-bearing reads when
  `spark.databricks.delta.deletionVectors.useMetadataRowIndex=false`.
- `TahoeLogFileIndexWithCloudFetch` (Databricks-proprietary, no OSS reproducer).
- Tables/queries that fail the schema/encryption compatibility checks.

### A6. INT96 timestamps read as nanoseconds (far-future overflow) — kernel gap

The kernel-read path reads each file through delta-kernel's `read_parquet_files`, which
uses kernel's fixed `reader_options()` (`ArrowReaderOptions::new().with_skip_arrow_metadata(true)`,
`pub(crate)` — no engine-facing knob). arrow-rs always decodes a parquet INT96 timestamp
as `Timestamp(Nanosecond)`, which kernel then casts to the table's declared unit. Values
after ~year 2262 overflow `i64` nanoseconds **during the read**, before any cast — so they
can read back wrong. Spark/Comet's own parquet path avoids this via `coerce_int96="us"`,
but kernel exposes no equivalent.

- **Impact:** narrow — only INT96-physical timestamp columns with dates beyond ~2262.
  Modern Delta writers use INT64 logical timestamps; INT96 is the legacy encoding.
- **Surfaced by:** `CometDeltaTypeRoundTripAuditSuite` "date / timestamp / timestamp_ntz
  round-trip" when it exercises far-future INT96 values.
- **Options:** (a) upstream delta-kernel fix exposing an INT96 coercion hook — filed as
  [delta-io/delta-kernel-rs#2709](https://github.com/delta-io/delta-kernel-rs/issues/2709);
  (b) a Comet-side custom `ParquetHandler`; (c) accept the limitation. Currently (c) + (a).
  Referenced from the `CAVEAT` comment in `read_file_via_kernel` (`kernel_scan.rs`).

### A7. Manual field-id repoint under column-mapping `id` mode — kernel id/name-fallback gap

- **Behavior:** Delta's `DeltaColumnMappingSuite` "explicit id matching" test
  manually rewrites a field's `delta.columnMapping.id` via a raw `DeltaLog`
  transaction (its `updateFieldIdFor` helper) to (case 1) a **non-existent** id,
  and (case 2) **another column's** id, then asserts strict id-*only* matching:
  case 1 must read back `NULL` (no parquet column has that id), case 2 must read
  the column that physically carries the repointed id. The kernel-read path
  diverges on both, so the test is **ignored** under Comet (see the
  `ignore("explicit id matching")` hunk in `dev/diffs/4.1.0.diff`).
- **Root cause:** the read goes through delta-kernel-rs's field matcher
  (`match_parquet_fields` / `get_indices` in kernel's
  `engine/arrow_utils/mod.rs`). Kernel matches each requested field to a parquet
  column **by field-id, then FALLS BACK to name** when the requested id is not
  present in the file. That name fallback is correct for normal column-mapping
  reads (physicalName and id are always consistent there, so id matches first and
  the fallback never fires). But after a manual repoint the field's id and its
  physicalName disagree with the file:
  - **Case 1 (id → non-existent):** the requested id isn't in the file, so kernel
    falls back to the physicalName and reads the **stale column's data** instead
    of NULL. This is a *silent wrong value*, not an error.
  - **Case 2 (id → another column's id):** the field matches the intended parquet
    column by id, but the now-orphaned physical column ALSO matches the requested
    field by name — two parquet columns bind to one requested field, and the read
    fails with `FAILED_READ_FILE`.
- **Why it's not declined Comet-side:** the repoint is only detectable by reading
  the parquet file's *actual* field-ids — which is exactly where kernel then
  mis-matches. There is **no plan-time schema signal** that distinguishes a
  repointed table from a normal column-mapping-`id` table (both have well-formed
  physicalName + id per field), so a precise decline has nothing to fire on. A
  broad "decline all CM-id reads" gate would over-decline the large body of
  legitimate column-mapping reads that work correctly today (the CM suites pass).
  And case 1's failure mode (wrong value, no error) can't be caught without doing
  the read. So neither a plan-time gate nor a runtime fallback is viable.
- **Real-user impact: nil.** Repointing a field's `delta.columnMapping.id` onto a
  different id requires hand-writing a `DeltaLog` transaction. No normal Delta
  operation — `ALTER`, `RENAME`, schema evolution, OPTIMIZE — ever does this. The
  test deliberately corrupts the id mapping to probe an internal strict-id
  invariant; it is not a code path real tables reach.
- **Correct fix (upstream kernel):** make the name fallback in
  `match_parquet_fields` fire only for parquet columns that carry **no** field-id,
  i.e. never let a name match override an explicit-but-absent id under CM-`id`
  mode. To be filed against delta-kernel-rs (same component as the INT96 gap
  A6 / [#2709]). Once kernel is strict-id under CM, flip the diff hunk back to
  `test(...)` and delete this entry.
- **Guard:** the ignore lives in `dev/diffs/4.1.0.diff` with the full rationale
  inline; this entry is the tracking record. Internal task #79.

---

## Part B — Pending regression failures (open as bug issues)

From a full Delta 4.1 own-suite run (70 distinct failing tests at the time of
that run). Some families below have since been fixed on this branch; status
noted per family. Re-run the suite after the in-flight fixes to get the current
list before opening issues.

### B1. Time travel / snapshot version — FIXED (commit `ccde0058`)

Root cause: `DeltaReflection` refreshed the snapshot to **head** even for
`versionAsOf` / `timestampAsOf` reads, returning current data for a historical
query. Fixed by reading `preparedScan.scannedSnapshot` (what vanilla reads).
Tests: "Time travel with schema changes", "time travel with partition changes",
"don't time travel a valid delta path with @ syntax", "scans on different
versions of same table", "SPARK-41154 ... time travel spec", "Dataframe-based
time travel ... timestamp precisions", "clone a time traveled source
(version/timestamp)", "cloneAtTimestamp/cloneAtVersion API", "vacuumed version",
"cold snapshot initialization", "snapshot is updated properly when owner
changes". Guard: `CometDeltaTimeTravelReproSuite`. **Re-run to confirm all clear.**

### B2. DPP MERGE crash on partitioned tables — FIXED (commit `64cd878a`)

Root cause + fix: see A1. Tests (isPartitioned: true): "basic case - local
predicates - ... updates and inserts", "extended syntax - ...", "unlimited
clauses - ...". Guard: `CometDeltaDppReproSuite`. Pruning in the join case
remains (A1 / #198).

### B3. Row tracking — materialized row-id / row-commit-version columns — FIXED

- **Tests (~18):** "z-order {un,}partitioned table with {fresh,stable} row IDs
  (+ filter)", "z-order preserves row tracking on backfill enabled tables",
  "auto-compact ..." (same matrix), "write and read table with {no-nulls,mixed}
  materialized columns", "read mixed materialized columns with filter", "write
  and read with column names similar to row tracking columns", "write and read
  with conflicting columns".
- **Symptom:** Projecting `_metadata.row_id` / `_metadata.row_commit_version`
  returns wrong values (e.g. expected `[0,0,1]` got `[0,500,4]`).
- **Root cause (from plan):** After z-order/compaction/explicit materialization,
  Delta persists stable row IDs into real parquet columns
  `_row-id-col-<uuid>` / `_row-commit-version-col-<uuid>`. The downstream
  projection is `coalesce(_metadata.row_id, base_row_id + row_index)`. The native
  scan classifies those names as synthetic (`isExtraSyntheticName`) and
  synthesizes from `base_row_id + row_index` instead of reading the persisted
  values, so the coalesce falls back to the wrong synthesized id.
- **Fix:** `CometDeltaNativeScan.convert` now treats `_row-id-col-*` /
  `_row-commit-version-col-*` as REAL parquet columns: they are added to the
  file data schema (`materializedRowTrackingFields`) and read by name (null for
  files that don't carry them), and removed from every synthetic classification
  (`isSynthetic`, `isExtraSyntheticName`, `metadataColumnNamesEmitted`, the
  projection-vector `isSyntheticFieldName`). The downstream `coalesce` then uses
  the persisted stable value when present and falls back to base+index only when
  null. `base_row_id` / `row_index` / `default_row_commit_version` remain
  synthesised. Filter pushdown on these columns stays conservatively disabled.
- **Guard:** `CometDeltaRowTrackingMaterializedSuite` (row IDs stable across
  OPTIMIZE / UPDATE; materialised row_commit_version matches vanilla). Verified
  no regression across 55 contrib tests. Full Delta-suite verification of the
  RowTracking{Merge,Delete,Compaction,ReadWrite} families pending the next
  full regression re-run.
- **Tracking:** internal task #197 (F3).

### B4. Triage of remaining failures

The originally-"untriaged" failures resolve into:

- **Mostly B3 (row tracking).** "DELETE/UPDATE MATCHED only MERGE", "... WHEN NOT
  MATCHED BY SOURCE", "{DELETE,UPDATE} only with source rows matching multiple
  target rows", "Multiple merges into the same table", "Source and target
  referencing to the same table", "Target is accessed through a view", "MERGE
  preserves Row Tracking on tables enabled using backfill", "Optimized writes
  (partitioned/unpartitioned/disabled)", "DELETE with persistent DVs disabled"
  all live in `rowid/RowTracking{Merge,Delete,...}Suite` and fail the same way:
  a stable row ID changes across a rewrite (e.g. "Row ID has change for row with
  stored_id = 412"). Same root cause as B3 — the native scan synthesizes row IDs
  instead of reading the materialized columns. **Addressed by the B3/F3 fix**;
  pending confirmation in the next full regression re-run.
- **F1 (already fixed).** "data skipping shouldn't use expressions involving a
  subquery" is the same `CometSubqueryAdaptiveBroadcastExec` DPP crash as B2 —
  fixed by `64cd878a`. Confirm via re-run.

### B5. Deeply-nested data-skipping expression — protobuf recursion limit — FIXED

- **Test:** "remove redundant stats column references in data skipping
  expression" (+ "old behavior with DataFrame schema" variant), from
  `DataSkippingDeltaTests`.
- **Symptom:** A WHERE with ~101 AND'd predicates produces a very deep boolean
  expression; serializing it to Comet's native proto exceeds protobuf's default
  recursion limit (100): `InvalidProtocolBufferException: Protocol message had
  too many levels of nesting` from `ExprOuterClass$BinaryExpr.mergeFrom`.
- **Parse site:** `CometNativeExec.findShuffleScanIndices`
  (`OperatorOuterClass.Operator.parseFrom`) re-parses the serialized plan; a
  `CometFilter` carrying the 202-conjunct left-deep `And` is a deep `BinaryExpr`.
- **Fix (base Comet):** balance associative `And`/`Or` chains at serialization so
  the proto is O(log n) deep instead of O(n) -- `CometAnd`/`CometOr` flatten the
  chain (`flattenAssociative`) and emit a balanced `BinaryExpr` tree
  (`QueryPlanSerde.createBalancedBinaryExpr`). Comet evaluates And/Or
  vectorially (both sides always evaluated), so rebalancing is semantically
  identical. This is a CORE Comet change (not contrib-only) -- flag in the PR.
- **Guard:** `CometDeltaEdgeCaseRegressionSuite` ("F4: deeply-nested data-skipping
  filter ...", now a passing `test`). Verified no regression in base
  `CometExpressionSuite` (123 tests).

### B6. Corrupted-file read error compatibility (SC-8810) — FIXED

- **Test:** "SC-8810: skipping deleted file still throws on corrupted file"
  (`DeltaSuite`).
- **Symptom:** With one data file truncated to 0 bytes, vanilla Spark+Delta
  throws `[FAILED_READ_FILE.NO_HINT]`; Comet's native reader throws
  `CometNativeException: External: Generic LocalFileSystem error: Requested range
  was invalid` instead, so the test's message assertion fails.
- **Fix (CORE Comet):** `CometExecIterator.isFileReadError` now also recognises
  object-store read failures (truncated/empty file: "Requested range was
  invalid"; "Object at location ... not found"; "Generic <Store> error: ..."),
  not just "Parquet error: ...". These wrap to `FAILED_READ_FILE.NO_HINT` via
  `ShimSparkErrorConverter.wrapNativeParquetError`, matching Spark's own
  file-read error. Flag in the PR (affects all Comet native scans).
- **Guard:** `CometDeltaEdgeCaseRegressionSuite` ("F6: reading a corrupted file
  ..."). `SparkErrorConverterSuite` (base) unaffected (covers cast overflow).

### B7. Row-tracking MERGE drops rows (materialized col absent from some files) — FIXED

- **Tests:** `RowTrackingMergeCommonNameBasedCDCOnSuite` "INSERT NOT MATCHED only
  MERGE", "UPDATE only with source rows matching multiple target rows", "DELETE
  only with source rows matching multiple target rows" (and the same family in
  other generated `RowTrackingMerge*` suites). Surfaced by the full 4.1 run; not
  in the original baseline because those suites weren't reached before the run was
  stopped. Regression introduced by B3 (F3).
- **Symptom:** After an INSERT/MERGE on a row-tracking table, reading it back with
  `_metadata.row_id` non-deterministically returned far fewer rows than written
  (e.g. 1600–4800 of 6000 across runs). The CDF test's fullouter join then flagged
  the missing rows as "deleted" and failed asserting a CDF delete entry exists.
- **Root cause:** B3 reads the materialized `_row-id-col-<uuid>` as a real parquet
  data column. That column is physically present only in files rewritten by a
  row-id-preserving op — ABSENT from freshly appended/inserted files (often absent
  from *every* file). When one Spark partition packs several such files, the native
  scan emits one parquet file-group per file (needed for per-file `row_index`), and
  reading a column physically absent from some files **across the
  concurrently-executed file-groups** non-deterministically drops whole
  file-groups' rows. Forcing one file per Spark partition reads the full row set
  correctly — confirming it's the cross-file-group concurrency, not the null-fill
  value. (Underlying: a latent core issue null-filling a projected-but-absent
  column across concurrent file-groups in one `DataSourceExec`.)
- **Fix:** `CometDeltaNativeScan.createExec` sets `oneTaskPerPartition = true` when
  the scan reads materialized row-tracking columns, so each such file is its own
  Spark partition → each native plan is single-file-group → the absent-column
  null-fill runs without cross-file-group concurrency. Same mechanism already used
  for `input_file_name()`.
- **Guard:** `CometDeltaRowTrackingMergeReproSuite` (INSERT-only MERGE on a
  row-tracking table; native key set == vanilla, full row count). Verified against
  the failing `RowTrackingMergeCommonNameBasedCDCOnSuite` (17/17 pass).
- **Tracking:** internal task #204.

### B8. Nested array-of-struct / map schema evolution — native panic — FIXED

- **Tests (~95, the dominant family in the 4.1 run):** "schema evolution - array of
  struct / nested struct / map of struct ..." in `MergeIntoSchemaEvolutionSuite` +
  `RowTrackingMergeSuite` (e.g. "array of struct with same columns but in different
  order ... by name/position", "extra nested column in source - update - array of
  struct", "struct in different order - nested array of struct", "... map struct
  value ...").
- **Symptom:** native error/panic reading an `array<struct>` / nested-struct / map
  column via the kernel-read path: `Invalid argument error: column types must match
  schema types, expected List(Struct(...)) but found List(Struct(...), field:
  'element')` (and an equivalent `assert_eq!` / `arrow-select coalesce.rs:539` panic
  on the unpartitioned pass-through). A *plain* read of a consistent `array<struct>`
  table is fine; the trigger is the schema-evolution / MERGE read.
- **Root cause:** the kernel-read batch names nested Arrow fields by Spark convention
  (list element `"element"`), but `DeltaKernelScanExec.output_schema` -- derived from
  the proto `required_schema` via `convert_spark_types_to_arrow_schema` -- carries
  empty nested field names. The emitted batch schema therefore != `output_schema`,
  which DataFusion rejects. The old ParquetSource path normalized nested names via
  Comet's `SparkSchemaAdapter`.
- **Fix:** `DeltaKernelScanExec::append_partition_columns` now reconciles every
  emitted column to `output_schema`'s exact field type (incl. nested list / struct /
  map field names) via `arrow_cast` -- a metadata-only relabel, since the data is
  already in logical (output) order from the kernel transform; applied on both the
  partitioned and the (previously pass-through) unpartitioned path.
- **Guard:** `CometDeltaNestedArrayStructReproSuite` (MERGE `array<struct>` with
  reordered nested fields; proven red→green).
- **Tracking:** internal task #73.

### B9. Nested schema evolution + nested column mapping — native wrong/error — FIXED

- **Tests (large family, ~35 in the 4.1 run):** nested-struct field ADDITION /
  reorder (`MergeIntoNestedStructEvolution*` / `...InMapEvolution*`, `SchemaUtilsSuite`
  "normalize column names - e2e nested struct") AND nested column-mapped RENAME
  (`CometDeltaNativeSuite` #47). The schema-evolution family first surfaced as
  `[FAILED_READ_FILE.NO_HINT]` on `test%file%prefix-...parquet` files — but the `%` was a
  red herring (Delta's harness prefixes every data file with `test%file%prefix-`; the
  kernel-read resolves `%` paths fine, see `CometDeltaPercentFileNameReproSuite` /
  `CometParquetPercentPathSuite`). The real fault was nested schema reconciliation.
- **Root cause:** the read previously re-implemented schema reconciliation Comet-side
  (a hand-rolled `align_batch_to_schema` / `reconcile_array`, and a `physicalise_field`
  walk over the proto `column_mappings`). That handled top-level cases but got nested
  column mapping wrong: the physical read schema physicalised only the TOP level, leaving
  nested field names logical with no field-ids. Kernel's reader matches file columns by
  field-id (then name), so nested columns went unmatched and were NULL-filled — #47 read
  back `(id, null, null)`.
- **Fix (use kernel as intended — see `10-iceberg-style-kernel-read.md`):** the read now
  uses delta-kernel's own primitives end-to-end.
  - `read_file_via_kernel` reads each file with `engine.parquet_handler().read_parquet_files(physical_schema)`
    then `transform_to_logical(physical_schema, logical_schema, transform)` — kernel's
    `fixup_parquet_read` does the reorder / null-fill / cast (added & reordered nested
    fields), exactly as `Scan::execute` does. The hand-rolled `align_batch_to_schema` /
    `reconcile_array` / `coerce_int96_to_micros` are deleted.
  - The schemas are produced by kernel's `Scan`, not reconstructed Comet-side: the driver
    projects the snapshot schema to the read columns (`snapshot.schema().project(cols)`,
    which keeps the `delta.columnMapping.physicalName` annotations `with_schema` needs),
    builds the scan, and ships `scan.physical_schema()` / `scan.logical_schema()` (Arrow
    IPC, field-ids preserved at EVERY nesting level) to the executor via
    `DeltaScanCommon.kernel_physical_schema` / `kernel_logical_schema`. The kernel-driver
    `planDeltaScan` path returns them inline; the batch-file-index path
    (`PreparedDeltaFileIndex`, file list from AddFiles) fetches them via the schema-only
    `planDeltaReadSchemas`. The executor reads with the physical schema (so kernel matches
    by field-id at every level) and relabels via the schema pair. `physicalise_field`
    remains only as a fallback when no kernel schemas were shipped.
- **Guards (red→green, all assert Comet matches Spark):**
  `CometDeltaNestedArrayStructReproSuite` "read after ALTER ADD nested struct field
  (schema-evolution null-fill)" + the MERGE reorder cases; `CometDeltaNativeSuite` #47
  "nested column-mapped table with a nested rename".
- **Tracking:** internal task #74 (the column-mapping slice of the broader kernel
  alignment, task #76).

---

## How to use this doc

1. Before merge, re-run the full regression to refresh Part B (B1/B2 should be
   clear; confirm B3/B4 status).
2. For each remaining Part A and Part B entry, open a GitHub issue with the
   description here and a link to the guarding test.
3. As entries are closed, flip the corresponding GAP-marker test to a positive
   assertion and remove the entry.

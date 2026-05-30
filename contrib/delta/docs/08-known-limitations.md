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

### A2. Cloud credential plumbing gaps

Surfaced by the credential audit (`CometDeltaCredentialAuditSuite`,
`jni::tests::extract_storage_config_known_gaps`). Each is asserted as a gap today
and flips to a failure when closed.

- **A2a. GCS (`gs://`) not supported native-side.** `NativeConfig` extracts
  `fs.gs.*` keys, but `DeltaStorageConfig` (native `jni.rs`) has no `gcp_*`
  fields and `create_object_store` has no `gs`/`gcs` arm, so the keys are
  dropped. GCS-backed Delta tables can't be natively read with credentials.
- **A2b. Per-bucket S3 keys not bridged.** `NativeConfig` extracts
  `fs.s3a.bucket.<name>.*`, but native only maps the global `fs.s3a.access.key` /
  `fs.s3a.secret.key`; per-bucket creds silently fall back to global.
- **A2c. `abfs` / `abfss` drop `fs.azure.*`.**
  `NativeConfig.objectStoreConfigPrefixes` registers only `fs.abfs.` /
  `fs.abfss.` for those schemes, not `fs.azure.` — so Hadoop-style Azure account
  keys / OAuth / MSI creds (historically under `fs.azure.*`) are not extracted
  for ADLS Gen2 URIs.
- **Correctness:** Reads fail (no creds) rather than producing wrong data.
- **Guard:** `CometDeltaCredentialAuditSuite`, `jni::tests`.

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

---

## How to use this doc

1. Before merge, re-run the full regression to refresh Part B (B1/B2 should be
   clear; confirm B3/B4 status).
2. For each remaining Part A and Part B entry, open a GitHub issue with the
   description here and a link to the guarding test.
3. As entries are closed, flip the corresponding GAP-marker test to a positive
   assertion and remove the entry.

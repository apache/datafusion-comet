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

This document tracks the **deliberate limitations and tradeoffs** that remain in
the contrib-delta native scan, so they can be opened as GitHub issues once the
work merges. Each entry notes: the behavior, why it's that way, the correctness
impact, the guarding test (if any), and the work needed to close it. Every entry
here is a place where we deliberately accept reduced acceleration (or decline to
native-scan) to preserve correctness, or an upstream-kernel gap — they are
stable and intentional, so the "fix" is a future enhancement, not an open bug.

This doc lists only what is **still** a limitation. The development-time Delta
own-suite regression failures that were once tracked here — time travel /
snapshot version, DPP MERGE crashes on partitioned tables, materialized
row-tracking columns, nested array-of-struct / nested-column-mapping schema
evolution, corrupted-file read-error parity, and deeply-nested data-skipping
filters — have all been fixed on this branch and are guarded by the
`CometDelta*ReproSuite` / `*RegressionSuite` / `*AuditSuite` tests, so they are
no longer listed below. The full Delta own-suite regression runs via
`contrib/delta/dev/run-regression.sh 4.1.0 full` (see
`.github/workflows/delta_regression_test.yml`).

---

## Deliberate tradeoffs & upstream gaps (open as enhancement issues)

### A1. DPP pruning not preserved on MERGE / re-planned plans (residual)

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
     scan _copy_ was dropped when `transformUp` rebuilt the enclosing native
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
`fs.s3a.*` keys explicitly (`object_store` can't read them); **Azure / GCS / other**
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
- **A2d. DV-read path ignored `storage_config` — FIXED.**
  `dv_reader::read_dv_indexes` takes a `&DeltaStorageConfig` and
  `DeltaKernelScanExec` passes `&self.storage_config`. DV reads against a
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
- CDF (`readChangeFeed`) reads of a **deletion-vector** table — kernel's
  `TableChanges` can't resolve the persistent `deletion_vector_*.bin`, so
  `convertCdf` declines via `DeltaReflection.cdfHasUnsupportedTableFeatures` and
  Spark's CDF reader serves it. Guard: `CometDeltaCdcSuite` "CDC read of a
  deletion-vector table declines to Spark". (Catalog-managed CDF declines for a
  different reason — see A8.)

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
  and (case 2) **another column's** id, then asserts strict id-_only_ matching:
  case 1 must read back `NULL` (no parquet column has that id), case 2 must read
  the column that physically carries the repointed id. The kernel-read path
  diverges on both, so the test is **ignored** under Comet (see the
  `ignore("explicit id matching")` hunk in `contrib/delta/dev/diffs/4.1.0.diff`).
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
    of NULL. This is a _silent wrong value_, not an error.
  - **Case 2 (id → another column's id):** the field matches the intended parquet
    column by id, but the now-orphaned physical column ALSO matches the requested
    field by name — two parquet columns bind to one requested field, and the read
    fails with `FAILED_READ_FILE`.
- **Why it's not declined Comet-side:** the repoint is only detectable by reading
  the parquet file's _actual_ field-ids — which is exactly where kernel then
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
- **Guard:** the ignore lives in `contrib/delta/dev/diffs/4.1.0.diff` with the full rationale
  inline; this entry is the tracking record. Internal task #79.

### A8. Catalog-managed (coordinated-commits) tables — kernel snapshot needs `max_catalog_version`

- **Behavior:** a catalog-managed table (Delta `catalogOwned` / coordinated-commits
  feature) is **not** read through the native delta-kernel path, for either read
  shape — but the two shapes degrade differently:
  - **Regular `SELECT` (pre-existing):** `DeltaScanRule` plants a
    `CometDeltaScanMarker` wrapping the original Delta `FileSourceScanExec`.
    `CometExecRule` tries to convert it to a native `CometDeltaNativeScanExec`, but
    the conversion runs `planDeltaScan`, which builds a kernel snapshot on the driver
    and throws `Catalog-managed table requires max_catalog_version`; the conversion
    declines (`return None`). The marker is left in the plan and, being a
    `LeafExecNode`, **delegates execution to its wrapped vanilla `originalScan`** — a
    plain Spark Delta read. So a catalog-managed regular scan returns **correct
    results but runs on vanilla Spark with NO Comet scan acceleration**: not the
    kernel-native path, and not Comet's regular Parquet scan either (the marker hides
    `originalScan` from `CometScanRule`). Partition pruning / limit pushdown still
    work, because Delta's planning applies them to the wrapped scan (verified: the
    `DeltaWithCatalogOwned*` / `DeltaLimitPushDownWithCatalogOwned*` suites read 1
    file for a one-partition predicate and produce correct `ScanReport`s once the
    marker is unwrapped). _Possible enhancement:_ on decline, let the marker fall
    through to `CometScanExec` (Comet's Parquet scan) so catalog-managed users get at
    least baseline Comet acceleration — correct because catalog-management is a
    commit-coordination feature, not a data-layout one, so the Parquet files read
    fine.
  - **CDF (`readChangeFeed`):** kernel's `TableChanges` raises the same requirement,
    but at EXECUTION time where the planning-time catch can't see it (it would crash
    mid-query). `convertCdf` therefore declines up front via
    `DeltaReflection.cdfHasUnsupportedTableFeatures` (matches `catalogOwned` /
    coordinated-commits / commit-coordinator config keys or protocol features), and
    Spark's CDF reader serves it (no Comet for that read).
- **Why not just supply `max_catalog_version`?** We have the driver-resolved snapshot
  version, so we _could_ call kernel's `with_max_catalog_version()`. But
  catalog-managed = coordinated commits, where the commit coordinator can hold
  _ratified-but-unbackfilled_ commits that aren't in `_delta_log`. Our native engine
  reads the log through `object_store` only (no commit-coordinator client), so
  supplying just the version risks clearing the error while **silently reading stale /
  incomplete change data** — strictly worse than declining. Spark's reader has the
  coordinator client, so the fallback is guaranteed correct.
- **Correctness:** preserved — both shapes produce correct results via their fallback;
  no wrong data.
- **Guard:** the Delta own-suite regression `*WithCatalogOwned*Suite` family runs green
  via these fallbacks — `DeltaCDCScalaWithCatalogOwnedBatch{1,2}Suite` (CDF decline),
  `DeltaTimeTravelWithCatalogOwned*` / `DeltaLimitPushDownWithCatalogOwned*` etc.
  (regular-scan marker→vanilla fallback). CDF decline detection lives in
  `DeltaReflection.cdfHasUnsupportedTableFeatures`. The marker-bearing reads are
  invisible to Delta's scan-finding test helpers (a declined `CometDeltaScanMarker`
  is neither `FileSourceScanExec` nor `CometDeltaNativeScanExec`), so `4.1.0.diff`
  unwraps `CometDeltaScanMarker => originalScan` in `DeltaSuite`'s partition-skip
  `fileScans` collect and in `ScanReportHelper.collectScans` (mirroring the existing
  `TestsStatistics` unwrap). Without it, `DeltaWithCatalogOwnedBatch1Suite`'s "query
  with predicates should skip partitions" and the `DeltaLimitPushDownWithCatalogOwned*`
  `getScanReport` tests fail (`MatchError: List()` / `size 0`) even though the reads
  are correct. `4.0.0.diff` / `3.3.2.diff` do NOT need the same unwrap: their
  scan-inspecting suites (`DeltaLimitPushDownSuite`, `DataSkippingDeltaTests`,
  `MergeIntoSuiteBase`, `DeltaSuite`) have no `WithCoordinatedCommits` variant, and
  kernel gates the `max_catalog_version` decline on `CatalogManaged` /
  `CatalogOwnedPreview` (4.1's `catalogOwned`), not the older `coordinatedCommits`
  feature — so no catalog-managed marker arises there (3.3.2 also runs `lite`, which
  drops the `WithCoordinatedCommits` families entirely).
- **Work to close (future enhancement):** thread the driver-resolved
  `max_catalog_version` through the scan proto into kernel's `Snapshot` /
  `TableChanges` builder, AND either (a) implement a commit-coordinator read path in
  the native engine, or (b) prove a version-only read matches vanilla for the
  unbackfilled-commit case before enabling it. Until then decline is the
  conservative-correct choice. Internal task #20.

---

## How to use this doc

1. For each entry above, open a GitHub issue with the description here and a link
   to the guarding test.
2. As an entry is closed, flip its GAP / decline guard test to a positive
   assertion (native now engages and matches Spark) and delete the entry — the
   way A3 (path-based CDF) was retired once kernel-native CDC (#84) landed.

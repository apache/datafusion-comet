<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with this
  work for additional information regarding copyright ownership. The ASF
  licenses this file to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations
  under the License.
-->

# Iceberg-style Delta contrib — read _through_ delta-kernel-rs

> **Status: IMPLEMENTED and default.** This started as a plan; it is now the only
> read path. `DeltaKernelScanExec` reads each Delta file through delta-kernel-rs
> **0.24** (which shares Comet's **arrow-58**, so there is no arrow bridge), handling
> plain tables, column mapping (name + id, including nested), partitions, deletion
> vectors, row-tracking, `_metadata`, schema evolution, zero-data-column reads, and
> INT96 timestamps (coerced to micros on read). The legacy ParquetSource path has
> been removed (#50), and `delta_scan.rs` in core is now a ~72-line shim that
> delegates to `comet_contrib_delta::planner::plan_delta_scan` (#77).
>
> Since the original plan, the per-file pipeline collapsed further than §2 below
> envisaged: column-mapping **physicalisation** was dropped — kernel ships its own
> `physical_schema()`/`logical_schema()` and `transform_to_logical` does the relabel
> (#76, nested CM #47); the separate `DeltaSyntheticColumnsExec` is **deleted** —
> `DeltaKernelScanExec` now synthesizes ALL output columns in-worker, by name (#82);
> and CDF (`readChangeFeed`) is kernel-native via `TableChanges` →
> `CometDeltaCdfScanExec`, split multi-partition (#84/#2) — not a synthetic-columns
> fallback. The custom pieces that **remain** are the INT96-driven custom read +
> `align_batch_to_schema` and the DV decode (`dv_reader`); they exist because we
> reimplement kernel's per-file pipeline rather than call `Scan::execute` (kernel's
> per-file scan data and INT96 hook aren't serializable/exposed — see
> `12-elimination-evaluation.md`). The original plan follows for the historical record.

## 1. Why

Comet's **Iceberg** contrib delegates the whole query lifecycle to `iceberg-rust`. The
driver plans (`TableScan` → `FileScanTask`s), the executor reads through
`iceberg::arrow::ArrowReaderBuilder` (which applies MOR positional/equality deletes and
predicate row-selection), and Comet adds only a thin Spark-parity adapter
(`SparkPhysicalExprAdapterFactory` in `iceberg_scan.rs`). The Comet-side surface is tiny.

Comet's **Delta** contrib does the opposite. It uses `delta-kernel-rs` to **plan only**
(`Snapshot::scan` → `scan_metadata` → `visit_scan_files`), then **bypasses kernel's reader**
and re-implements the entire read pipeline on Comet's `ParquetSource`:

- deletion-vector application (`dv_reader.rs` + the unified DV-sweep exec in
  `synthetic_columns.rs`),
- synthetic columns (`row_index` / `is_row_deleted` / `row_id` / `row_commit_version` /
  `_metadata.*`),
- column-mapping physicalisation (logical→physical nested names and
  `delta.columnMapping.id`→`parquet.field.id`) in `CometDeltaNativeScan.scala`,
- the wiring that stitches all of that onto the parquet scan in
  `native/core/src/execution/planner/delta_scan.rs`.

That re-implementation is the bulk of the contrib's reviewable surface, and it includes the
**historically buggy** column-mapping code (its own comments cite repeated data-corruption
fixes; a spike confirmed it is load-bearing for NAME-mode nested types).

**Goal of this plan:** move the read onto kernel's `Scan` path, the way Iceberg uses
`iceberg-rust`, deleting the re-implementation while keeping **every existing test** as the
behavioural safety net.

## 2. What gets deleted vs kept

**Deleted (the win):**

| Surface                                                                                                                  | ~LOC  |
| ------------------------------------------------------------------------------------------------------------------------ | ----- |
| `contrib/delta/native/src/synthetic_columns.rs` (DV-sweep exec + synthetic columns)                                      | ~1218 |
| `contrib/delta/native/src/dv_reader.rs` (DV decode)                                                                      | ~259  |
| `delta_scan.rs` `after_synthetics` + column-mapping-rename wiring                                                        | ~205  |
| `CometDeltaNativeScan.scala` physicalisation (`extractSnapshotSchema`, `physicalise*`, `translateDeltaFieldIdToParquet`) | ~161  |
| Proto fields for DV descriptors / synthetic-emit flags / column mappings + their Scala wiring                            | ~200  |
| `DeltaScanRule.withDeltaColumnMappingMetadata` and related                                                               | ~100  |

Order **~2000+ lines** removed, plus the elimination of the highest-risk hand-rolled logic
(column mapping, DV offset accounting).

**Kept (deliberately):**

- **Every contrib test** — `CometDelta*Suite`, the regression harness, and the
  `dev/diffs/*.diff`. They assert end-to-end behaviour that the kernel-read path must
  reproduce. They are the proof the deletion is safe.
- The **JNI planning bridge** (`Java_..._planDeltaScan` → kernel `scan_metadata`). Kept,
  extended to carry the per-file transform (see §4).
- The **Scala dispatch** (`CometDeltaScanMarker`, `DeltaScanRule`, `CometDeltaNativeScan`),
  minus the physicalisation. The marker/fallback logic stays.
- The **engine / object-store config** (`DeltaStorageConfig`, the cached kernel engine).
- The **decline-to-vanilla fallback** (when kernel can't handle a feature, fall back to
  Spark's reader). The set of declined cases may _shrink_ (kernel handles more natively).
- The **build gate** (`dev/verify-contrib-delta-gate.sh`).

## 3. Target architecture (mirrors `iceberg_scan.rs`)

```
DRIVER (JVM → JNI → kernel, once per query):
  Snapshot::scan(predicate) → scan_metadata → visit_scan_files
    → per-file ScanTask { path, size, partition_values, dv_descriptor,
                          transform_expr, physical_read_schema }
    → serialize to proto (one task per file group)

EXECUTOR (new DeltaKernelScanExec, analog of IcebergScanExec):
  for each assigned ScanTask:
    1. kernel engine reads the parquet file (physical schema)
    2. apply the kernel transform expression  → column mapping + partition injection
                                               + row-index/row-id/commit-version materialise
    3. apply the deletion-vector mask          → drop deleted rows
    → Arrow RecordBatch in the Delta *logical* schema
  thin Spark-parity adapter (timestamp units, decimal, etc.) → Comet downstream plan
```

The executor stage replaces today's `init_datasource_exec` + DV-sweep exec + rename
projection with a single kernel-backed scan exec plus a thin adapter, exactly the shape of
`IcebergScanExec::execute_with_tasks`.

## 3a. Spike 0 result (GATE — resolved: GO, with a refined design)

Examined `delta_kernel` 0.19.2 source (`src/scan/{mod,state,state_info}.rs`,
`src/expressions/mod.rs`, `src/transforms.rs`). Verdict: **feasible now, no upstream
dependency required**, but **not** the pure "ship the scan task" model Iceberg uses.

What's public and usable on an executor without the `Snapshot`:

- `scan::state::transform_to_logical(engine, physical_data, physical_schema, logical_schema,
transform)` — applies the transform via kernel's expression evaluator. This is what does
  column mapping (**including nested**), partition injection, and row-tracking. Handing this to
  kernel removes our buggy parquet-schema-adapter physicalisation entirely.
- `scan::state::DvInfo::get_selection_vector(engine, table_root)` — the DV mask.
- `engine.parquet_handler().read_parquet_files(...)` — the physical read.
- `scan::state::ScanFile` is `pub` and its serializable fields (`path`, `size`, `dv_info`,
  `partition_values: HashMap<String,String>`) are exactly what the driver already pulls out via
  `visit_scan_files`.
- `expressions::Transform` builder (`with_replaced_field`, `with_inserted_field`,
  `with_dropped_field`) + `Expression`/`Scalar` for **building** a transform.

The one real gap: `ScanFile.transform` is an `ExpressionRef` that is **not serde-serializable**,
and kernel's internal transform-spec builder (`TransformSpec` / `get_state_info` in
`state_info.rs`) is `pub(crate)`. So we cannot ship kernel's _own_ computed transform across the
JNI/proto boundary, the way Iceberg ships a `FileScanTask`.

**Design implication (revised from §4):** the executor **rebuilds an equivalent transform** from
serializable inputs (logical + physical schema, the column-mapping metadata the contrib already
extracts, per-file `partition_values`, and `base_row_id`) using the public `Transform`/`Expression`
API, then applies it via the public `transform_to_logical` + `get_selection_vector`. The
transform _application_ is kernel's (correct nested handling); only the small, declarative
transform _spec_ is ours.

**Net effect on the deletion:**

- Deleted as planned: `dv_reader.rs` (→ `get_selection_vector`), `synthetic_columns.rs` (the
  DV-sweep + all synthetic columns → kernel's `GenerateRowId` / `MetadataDerivedColumn` transform
  - DV mask), and the historically-buggy nested-name physicalisation (→ `transform_to_logical`).
- Added (smaller, safer): `DeltaKernelScanExec` (~200-300 LOC, like `iceberg_scan.rs`), a
  transform-spec builder over the public `Transform` API (~200-400 LOC, declarative expression
  construction rather than schema-tree rewriting with pruning), and a Spark-parity adapter
  (~100-200 LOC, like `IcebergStreamWrapper`).
- Still a large net reduction (~−1900 deleted vs ~−500 to −900 added), and the deleted code is
  the riskiest part (DV offset accounting + the buggy physicalisation), replaced by kernel's
  correct application.

**Parallel (non-blocking) upstream ask:** request that kernel-rs make the transform spec
serializable/public (or `Expression` serde). The JVM `delta-kernel-spark` connector already
distributes the read this way; the Rust kernel lacking it is the only reason we rebuild the
transform instead of shipping kernel's. If that lands, the transform-spec builder we add now
deletes too, reaching the full Iceberg-clean shape.

## 4. The crux (original framing): distributing kernel's read

`Scan::execute(engine)` is a **single-node** iterator over _all_ files. Spark needs the read
distributed across executors. This is the one genuinely hard part, and it is the gate for the
whole effort.

The model that works (and the one the JVM `delta-kernel-spark` connector already uses):

- The driver runs log replay **once** (`scan_metadata`), and for each selected file extracts
  the **transform expression** and **DV** kernel computed (these already live in
  `ScanMetadata.scan_files`; today the contrib only pulls `path` + `dv_descriptor` +
  `base_row_id` out of it via `visit_scan_files`).
- Those per-file tasks are serialized and distributed.
- Each executor applies kernel's **per-file read + transform + DV** using the engine handlers
  (`parquet_handler.read_parquet_files` + `expression_handler.evaluate(transform)` + DV mask)
  **without reconstructing the Snapshot** (no per-executor log replay).

> **Spike 0 (GATE — do this before anything else):** confirm `delta-kernel-rs` (currently
> 0.19.2) exposes per-file read + transform + DV on an executor without re-replaying the log,
> and that the transform expression is serializable across our JNI/proto boundary. The JVM
> kernel supports this cleanly; the Rust kernel's surface for it is the open question. If it
> does not, this refactor is blocked on an upstream kernel ask and should not start.

Two fallbacks if Spike 0 is partly negative:

- **4a.** Kernel reads + transforms, but we still distribute by file group exactly as today
  (the existing per-file-group partitioning already gives one file per DV'd partition).
- **4b.** If kernel can't evaluate the transform on the executor, keep a _minimal_ native
  translation for the column-mapping rename only (the spike showed nested field-id matching
  already works natively for ID mode), and still delete the DV/synthetic re-implementation.
  This is a partial win.

## 5. Migration strategy (incremental, never red)

The user's constraint is "keep all the tests." The strategy keeps them green at every step by
running the **new path behind a flag against the existing suites before deleting anything**.

- **Phase 0 — Spike 0 (gate).** Prove distributed per-file kernel read+transform+DV. Output:
  go / no-go + the concrete kernel API to use.
- **Phase 1 — build alongside.** Add `DeltaKernelScanExec` + the new proto task, behind
  `spark.comet.delta.kernelRead.enabled` (default **off**). The old path stays the default.
  No tests change.
- **Phase 2 — parity.** Flip the flag **on in the contrib test runner** and run the full
  contrib suite + `dev/verify-contrib-delta-gate.sh`. Close the Spark-parity gaps in the
  adapter (timestamp units, decimal, row-tracking column types). Then a **3.5 lite
  regression** with the flag on. Iterate until green.
- **Phase 3 — flip default.** Default the flag **on**; keep the old path reachable as a
  fallback for one release. Run **4.0 / 4.1 full** regressions (column mapping is
  version-sensitive).
- **Phase 4 — delete.** Remove the old read pipeline (the table in §2), the dead proto
  fields, and the flag. The kept tests now run against the kernel-read path only.

Each phase is independently revertible. Nothing is deleted until the kept tests pass against
the new path.

## 6. Performance plan

The read source changes; the **downstream native execution** (Comet's main speedup) does not.
Filters/aggregations/joins still run natively on the Arrow batches kernel produces. Both
kernel's and `iceberg-rust`'s readers are `arrow-rs`-vectorised, so raw scan throughput should
be in the same ballpark as Iceberg's (which ships).

The one real risk is **dynamic pushdown**. Comet's current `ParquetSource` path threads
runtime DPP filters (from joins) into the parquet read for row-group/page skipping. A
pre-planned kernel read applies the static predicate + partition pruning but generally will
not push a runtime filter into the read. This is the **same trade-off Iceberg already
accepts** (its `FileScanTask`s are planned up front).

Benchmark, before flipping the default, on three shapes:

1. **scan-heavy** (full-table projection) — expect parity.
2. **selective join with DPP** — the at-risk case; measure the regression.
3. **DV-heavy delete-read** — kernel applies the DV during read; compare to the DV-sweep exec.

Mitigations if (2) regresses: keep a Comet downstream `Filter` (correctness is never at risk),
and only if needed push the dynamic filter into the per-file kernel read. Also confirm there
is **no per-executor log replay** (would be a duplicate-snapshot-read perf bug).

> Honest note: this section is directional, reasoned from architecture, not measured. The
> benchmark in Phase 2/3 is what decides whether the default flips.

## 7. Correctness / parity risks

- **Spark type parity of kernel output** — the adapter must reconcile kernel's Arrow types
  with what Comet expects (timestamp unit/zone, decimal precision, the materialised
  row-tracking column types). This is the bulk of Phase 2.
- **Column mapping** — kernel resolves it per the Delta spec, which should be _more_ correct
  than the hand-rolled physicalisation. This removes the historically buggy area rather than
  porting it. Validate with `CometDeltaColumnMappingSuite` /
  `CometDeltaColumnMappingPhysicalNameReproSuite` / `CometDeltaTypeRoundTripAuditSuite`.
- **Deletion vectors** — kernel applies them; validated by `CometDeltaCoverageSuite`,
  `CometDeltaDeleteWithDVReproSuite`, the base `DeletionVectorsSuite`. The
  `useMetadataRowIndex=false` slow-path decline may change or disappear (kernel materialises
  row-index itself).
- **Row tracking** — kernel materialises `row_id` / `row_commit_version`; validated by
  `CometDeltaRowTracking*`.
- **Predicate pushdown** — `scan_builder().with_predicate(...)` already pushes the static
  predicate for file skipping. Confirm parity.
- **Fallback parity** — the existing decline cases (unsupported features, schemes) must still
  fall back identically.

## 8. Verification (gate every phase)

1. Native: `cargo build -j 4 --features contrib-delta` + the native unit tests.
2. JVM compile: `./mvnw -Pspark-4.1,contrib-delta install -DskipTests`.
3. Full contrib suite with the flag on:
   `wildcardSuites='org.apache.comet.contrib.delta,org.apache.spark.sql.delta.CometDeltaCheckpointFilterReproSuite'`.
4. `dev/verify-contrib-delta-gate.sh` (default build carries zero Delta surface).
5. 3.5 lite regression (Phase 2), then 4.0 / 4.1 full (Phase 3).
6. The benchmark suite (§6) before flipping the default.

## 9. Open questions / honest risks

1. **Spike 0** — does kernel-rs 0.19.2 support distributed per-file read+transform+DV without
   Snapshot reconstruction? **Primary risk; gates everything.**
2. **Transform serialization** — can kernel's transform expression cross the JNI/proto
   boundary, or must the executor re-derive it from the (serialized) column-mapping metadata?
3. **DPP pushdown loss** — magnitude is workload-dependent and unmeasured.
4. **Kernel-rs maturity** — the read+transform path is newer than Iceberg-rust's reader.
5. **Duplicate snapshot read** — must confirm the executor read does not re-replay the log.

## 10. Sequencing / effort (rough)

- Spike 0: a few days, and it can fail fast.
- Phase 1 (build behind flag): ~1–2 weeks.
- Phase 2 (parity + 3.5 lite green): ~1–2 weeks, mostly the adapter.
- Phase 3–4 (flip + delete + full regressions): after parity holds.

The deletion (Phase 4) is the easy part. The value is front-loaded in Spike 0 and Phase 2.
If Spike 0 is green and Phase 2 reaches parity, this is the right long-term shape for the
contrib: Iceberg-sized surface, the buggy hand-rolled logic gone, and the perf trade-off the
same one Iceberg already lives with.

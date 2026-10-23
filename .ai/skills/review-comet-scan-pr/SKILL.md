---
name: review-comet-scan-pr
description: Use when reviewing a DataFusion Comet pull request that touches how Comet reads data files, including CometScanRule and its fallback gates, CometScanExec, CometNativeScanExec, the native scan serde, the Parquet schema adapter, filter pushdown into the scan, object store and URL scheme handling, Parquet encryption, or the Iceberg and CSV scan paths. Load alongside review-comet-pr.
argument-hint: <pr-number>
---

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

Scan-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers reading data files.

The scan is where Comet reads bytes that Spark wrote and has to agree with Spark about what they
mean. Almost every bug in this area is a silent wrong answer: a column that reads as NULL, a value
that overflows where Spark would have raised, a file that resolves to the wrong field. Very little
of it crashes. Review it expecting the failure mode to be quiet.

## Read the Contributor Guide First

| Doc                                                    | What you need from it                                                                 |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| `docs/source/contributor-guide/scan.md`                | Which path runs, the fallback gate model, the schema triple, the adapter, the gotchas |
| `docs/source/user-guide/latest/datasources.md`         | The user-facing contract for formats, storage, schemes, and credentials               |
| `docs/source/contributor-guide/plugin_overview.md`     | Where `CometScanRule` sits relative to `CometExecRule`                                |
| `docs/source/user-guide/latest/iceberg.md`             | Iceberg PRs only                                                                      |
| `docs/source/contributor-guide/iceberg-spark-tests.md` | Iceberg PRs only. Those suites report in the merge queue, not on the PR               |

Read `scan.md` before the diff. The single most common review finding here is a change that is
locally correct but inconsistent with how the other gates, or the other two case-folding call
sites, already solve the same problem.

## 1. Which Path Does the PR Touch

| The diff touches                                                                                 | Path                                    |
| ------------------------------------------------------------------------------------------------ | --------------------------------------- |
| `transformV1Scan`, `nativeScan`, `CometScanExec`, `CometNativeScanExec`, `CometNativeScan.scala` | V1 Parquet, the main native reader      |
| `native/core/src/parquet/`                                                                       | The native reader, shared by every path |
| `transformV2Scan` Iceberg arm, `CometIcebergNativeScan*`, `IcebergReflection`                    | Iceberg                                 |
| `transformV2Scan` CSV arm, `CometCsvNativeScanExec`, `csv_scan.rs`                               | Experimental CSV V2                     |
| `CometScanContrib`                                                                               | The out-of-tree contrib SPI             |

There is no dedicated Iceberg review skill yet, so an Iceberg scan PR uses this one plus the two
Iceberg docs above.

**`CometSparkToColumnarExec` is not a scan.** It converts Spark's own reader output to Arrow after a
fallback, so the Spark-parity questions below do not apply to it. Review it with
`review-comet-ffi-pr` for the conversion boundary and `review-comet-memory-pr` for the allocator it
feeds.

**Watch for a PR that assumes V2 Parquet is native.** It is not. `transformV2Scan` has no
`ParquetScan` arm, so a V2 Parquet scan falls through to the catch-all. A test written without
pinning `spark.sql.sources.useV1SourceList` may be passing for the wrong reason, or not exercising
the native reader at all.

## 2. Fallback Gates

Most scan PRs add, move, widen, or remove a gate in `CometScanRule`. Treat the direction of the
change as the first question, because the two directions carry completely different risk.

**Narrowing (a new decline) is cheap to get wrong safely.** The cost is lost performance. The
review question is whether the gate is too broad, and whether the reason text tells a user what to
do.

**Widening (removing or loosening a decline) is the dangerous direction.** Comet is now claiming
reads it previously refused. "The tests pass" is not evidence, because the tests were written
against the narrower behavior. Ask what Spark does with the newly claimed input and where the
answer came from. Reading `ParquetReadSupport`, `ParquetVectorUpdaterFactory`, or
`VectorizedColumnReader` in the Spark source is an answer. Inference from the Parquet spec is not,
because Spark's reader diverges from it in documented places.

Checklist for a new or moved gate:

- [ ] **It fails closed.** Every `catch` around reflection, metadata access, or a native probe must
      add a fallback reason, not assume the scan is safe. A gate that swallows an exception and
      continues turns an unverifiable table into a wrong answer or a native crash.
- [ ] **It tags a reason**, so `EXPLAIN EXTENDED` and `spark.comet.explain.fallback.log.enabled` can
      show it. This is easy to miss in review because the query still returns the right answer, just
      slowly, and the user has nothing to read.
- [ ] **It matches the local return convention.** Some gates `return withFallbackReason(...)`
      immediately. Others (notably `CometNativeScan.isSupported` and the Iceberg arm) tag and
      continue so that every applicable reason accumulates before the single decision at the end.
      A gate that returns early where the surrounding code accumulates hides the other reasons.
- [ ] **It is in the right function.** A guard hoisted above the contrib hook declines scans Comet
      does not own. A guard specific to the native Parquet reader belongs in `nativeScan`, not
      `transformV1Scan`, so contrib scans are unaffected. `scan.md` § The Fallback Gate Model has
      the placement rule.
- [ ] **A gate that duplicates an existing one is drift waiting to happen.** The AQE DPP check is
      deliberately in two places with a comment saying the second is a safety net. A new duplicate
      without that rationale should be one gate.
- [ ] **A gate that asks the native layer beats a hardcoded list.** Scheme support is answered by
      `NativeBase.isObjectStoreSchemeSupported` so the planner cannot drift from `object_store`. A
      new hardcoded list needs a reason why it cannot ask, of the kind `icebergReadableSchemes` has.

## 3. Schemas and Projection

`data_schema`, `required_schema`, `partition_schema`, and a `projection_vector` indexing into
`data_schema ++ partition_schema`. A PR that changes one has to change the others consistently, and
the failure mode is columns silently swapping values. `scan.md` § Schemas and Projection has the
layout.

- [ ] **Constant metadata columns keep their `_comet_metadata_` rename and uniquification.**
      DataFusion substitutes partition constants by name, so dropping either lets a user column of
      the same name receive the metadata value instead of its own.
- [ ] **Index arithmetic survives the change.** `dataSchemaIndexes`, `partitionSchemaIndexes`, and
      the `partitionSchema.length` assert are load-bearing, as is the native side binding
      `data_filters` against `required_schema ++ partition_schema`.
- [ ] **Variant stays out of the native data schema.** A PR that changes how the data schema is
      built needs to preserve the pruning, or an unsupported type reaches the reader for a query
      that never mentions it.
- [ ] **A new `NativeScanCommon` field is set on every path that builds one.** Proto defaults are
      silent: an unset `bool` reads as `false` natively, which may be a valid-looking but wrong
      Spark semantic.

## 4. Spark Parity in the Schema Adapter

`schema_adapter.rs` is not a cast layer. It is a reimplementation of what Spark's vectorized
Parquet reader produces for a given file and requested schema, **including the errors**. Review a
change here against Spark's source, not against what looks reasonable.

- [ ] **The rejection matrix still mirrors `ParquetVectorUpdaterFactory.getUpdater`**, at the same
      Spark versions, with the same leaf-by-leaf walk and the same first-verdict-wins ordering. A
      change that short-circuits differently changes which error a user sees.
- [ ] **Errors stay deferred.** A rejection becomes a `RejectOnNonEmpty` that fires only on a
      non-empty batch, so a file whose offending row groups are all pruned still reads, as it does
      in Spark. A PR that raises at plan time breaks queries Spark accepts, and a test that always
      reads the offending row group will not notice.
- [ ] **Case folding goes through `name_fold.rs`.** A new `to_lowercase()` or
      `eq_ignore_ascii_case()` anywhere in the scan path is a finding. Three call sites share the
      policy, and those drifting apart is what produced #5495.
- [ ] **Field-id matching keeps Spark's gate**, which uses ids only when the conf is on _and_ the
      requested schema actually carries them. Removing either half changes behavior on schemas
      Spark reads by name.
- [ ] **`is_pure_structural_narrowing` is an allow list on purpose.** Any PR touching
      `parquet_convert_array` should say whether that predicate needs a matching exclusion, because
      a case it does not know to exclude silently starts producing wrong results.
- [ ] **A new `SparkParquetOptions` field is plumbed end to end.** Proto field, serde, planner,
      `get_options`, and **both** constructors, `new` and `new_without_timezone`. A field missing
      from one constructor gets a default that is wrong for one caller.

## 5. Filter Pushdown

The organizing distinction: pushdown is an **optimization** for per-row `RowFilter` evaluation,
because Spark's `Filter` above the scan re-evaluates every data filter anyway. It is a
**correctness surface** for row-group and page-index pruning, because pruning drops rows before
that `Filter` ever sees them. The three checks below follow from which side a change lands on.

- [ ] **Filters go through `try_pushdown_filters`, not `with_predicate`.** A PR that starts
      trusting the discarded parent pushdown result needs to say what now guarantees the filter is
      applied. See `scan.md` § Filter Pushdown.
- [ ] **`has_data_filters` is not `!data_filters.is_empty()`.** It looks like dead redundancy and is
      not: it drives `checked_timestamp_overflow`. Collapsing the two makes a filtered scan raise on
      values Spark would have pruned away before converting them.
- [ ] **A new pushed-down expression is Spark-compatible under pruning.** An expression whose native
      semantics differ from Spark's only in an edge case still silently drops rows once statistics
      pruning uses it.

## 6. Object Stores and Schemes

- [ ] **One object store is registered per `FilePartition`, keyed on the first file.** Everything
      about multi-bucket handling follows from this. A PR that widens which schemes are claimed
      needs to say what happens to a partition spanning two buckets. The existing V1-versus-Iceberg
      asymmetry is deliberate, so do not let a PR "fix" it silently.
- [ ] **A new scheme needs both sides.** The JVM gate and the native reader must agree. The `hdfs`
      default is the cautionary tale: the JVM default is `Set("hdfs")` specifically to mirror
      `is_hdfs_scheme`, because an empty default would silently fall back every plain `hdfs://` scan.
- [ ] **Iceberg's scheme list tracks `storage_factory_for`.** `icebergReadableSchemes` must stay in
      lockstep with the match arms in `iceberg_common.rs`. Admitting a scheme iceberg-rust cannot
      build turns a clean JVM fallback into a native runtime error.
- [ ] **A new Hadoop-surface key is read from the Hadoop config, not SQLConf**, or it will not work
      for users who configure through `core-site.xml`.
- [ ] **New credential or endpoint handling is documented** in `datasources.md`, which carries the
      hand-maintained S3 and Azure key tables.

## 7. Version-Dependent Behavior

Scan semantics differ by Spark version more than most areas. The project's answer is a per-version
constant in `ShimCometConf`, or a Spark conf read with a version-dependent default, never a new
`spark.comet.*` config. `scan.md`'s `SparkParquetOptions` table lists the current set.

- [ ] A PR that adds a version-dependent scan behavior uses one of those two shapes. A
      `spark.comet.*` config for something Spark already decides by version is wrong, and
      `spark.comet.schemaEvolution.enabled` was removed for exactly that reason (#4298).
- [ ] A PR that changes one of them checks all supported versions, not just the default profile.
      The default Maven profile is Spark 4.1, so a 3.x behavior change can compile and pass locally
      while being wrong on 3.4.

## 8. Reader Options and DataFusion Upgrades

This is the fail-closed rule applied to upstream defaults.

- [ ] **`get_options` stays an allow list.** A PR that replaces it with a bulk copy of the session
      options makes every future DataFusion option silently active.
- [ ] **`coerce_int96` and `coerce_int96_tz` stay hardcoded.** Making them session-overridable
      breaks the adapter's ability to distinguish an INT96-derived TimestampLTZ.
- [ ] **A DataFusion version bump is a scan review.** The allow list, the
      `EagerPageIndexReaderFactory` workaround and its stated exit condition, and the schema
      adapter's assumptions about `nested_struct::cast_column` all track upstream.

## 9. Tests

Ask what level the change is at and whether the test matches it.

Match the change to the suite that covers it. `scan.md`'s Testing table is the inventory. The rules
that inventory does not carry:

- [ ] **Test at the level the change lives at.** A gate in `CometScanRule` needs no execution at
      all: apply the rule to a plan and assert on the resulting node type, as `CometScanRuleSuite`
      does. A conversion rule wants both a Rust unit test for the decision and a Scala test that
      compares against Spark on a real file.
- [ ] **A fallback test asserts the node type, not just the answer.** A query that falls back still
      returns the right answer, so a test that only checks results passes whether or not the gate
      fires. Assert that `CometNativeScanExec` is absent, or that the expected fallback reason is
      present.
- [ ] **A conversion test uses a real file with the offending physical type.** A test that writes
      through Spark and reads it back cannot produce a physical/logical mismatch, so it does not
      exercise the rejection matrix. The checked-in fixtures under
      `spark/src/test/resources/test-data/` exist for this.
- [ ] **A parity claim is tested against Spark, not against an expectation.** `checkSparkAnswer`
      and friends compare the two engines. A hardcoded expected value encodes the author's belief
      about Spark rather than Spark's behavior.
- [ ] **The Spark SQL suites are the real check**, and they report in the merge queue rather than on
      the PR. For a change to the reader or a gate, ask for the `run-spark-*-tests` label matching
      the Spark version whose behavior changes, or `run-iceberg-tests` for the Iceberg path, or a
      local `dev/local-ci.sh` run. `docs/source/contributor-guide/ci.md` has the current label list.

## 10. Does the PR Make `scan.md` Stale?

Every table in `scan.md` is a hand-maintained enumeration, so check each one the PR could
invalidate. Two need naming individually:

- The **Which Scan Path Runs** table. The claim that V2 Parquet is not read natively is the single
  statement in the doc most likely to become wrong.
- The **Page Index and Metadata Caching** section, which describes a workaround with a stated exit
  condition. A DataFusion bump that fixes `apache/datafusion#23978` should delete it rather than
  edit it.

Check also whether `datasources.md` (user-facing formats, schemes, credentials) or the
`### CometScanRule` section of `plugin_overview.md` still holds.

## Common Scan Review Findings

In rough order of how often they are worth raising. Each points back at the section that explains
it:

1. A widened gate with no evidence of what Spark does with the newly claimed input (§2).
2. A decline that does not tag a fallback reason (§2).
3. A new case-insensitive comparison that bypasses `name_fold.rs` (§4).
4. A fallback test that asserts the query result rather than the plan shape (§9).
5. A gate placed above the contrib hook (§2).
6. A reflection `catch` that continues rather than declining (§2).
7. A new `NativeScanCommon` field that one serde path forgets to set (§3).
8. A conversion accepted natively that Spark's `getUpdater` rejects, or vice versa (§4).
9. Version-dependent behavior expressed as a `spark.comet.*` config (§7).
10. `scan.md` left describing the old gate list (§10).

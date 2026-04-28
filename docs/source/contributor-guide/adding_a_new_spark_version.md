<!---
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

# Adding Support for a New Spark Version

This guide describes how to bring up support for a new Apache Spark release in
Comet. Past examples include the work to add Spark 4.0, Spark 4.1, and the
Spark 4.2 preview profile. The goal is a repeatable recipe that keeps each
pull request small, reviewable, and easy to revert if a problem is discovered
later.

## Why Stage the Work

Adding a new Spark version touches the build, the shim layer, CI, and three
different test suites (Comet's own JVM tests, Spark's SQL tests, and the
plan stability golden files). Bundling everything into one pull request
produces a diff that is hard to review and almost impossible to bisect. A
staged approach instead introduces one capability at a time, with CI proving
each stage green before the next one lands.

A typical bring-up uses several focused PRs:

1. **PR 1: Maven profile, shims, and a compile-only CI job.**
2. **PR 2: Enable Comet's own JVM test suite under the new profile.**
3. **PR 3: Enable Spark's SQL tests under the new profile, skipping the
   failing ones with linked issues.**
4. **PR 4: Add the version to the experimental tier in the user guide.**
5. **Follow-up PRs: Fix one skipped test (or one related cluster of tests)
   per PR, removing the skip as part of the fix.**
6. **Eventual promotion PR: Move the version from experimental to
   supported in the user guide once the criteria in stage 5 are met.**

The sections below describe each stage in detail.

## Stage 1: Maven Profile, Shims, and Compile-Only CI

The first PR should produce a configuration where `./mvnw -Pspark-X.Y compile`
succeeds, but no tests are required to pass yet. Keeping this PR
compilation-only avoids mixing build issues with test failures.

### Add the Maven Profile

Add a new `<profile>` block to the top-level `pom.xml`. Copy the most recent
existing profile (for example `spark-4.1`) and update the version
properties:

- `spark.version`: the full upstream version, including any qualifier
  (for example `4.2.0-preview4`).
- `spark.version.short`: the major.minor (for example `4.2`).
- `parquet.version`, `slf4j.version`, `scala.version`,
  `scala.binary.version`, `java.version`: align with what the new Spark
  release actually publishes. A wrong Scala version is a common source of
  `NoSuchMethodError` at runtime, so prefer the exact patch version Spark
  uses rather than a looser pin.
- `shims.majorVerSrc` and `shims.minorVerSrc`: the directory names the
  build helper plugin will add to the source path. By convention the
  major-version directory groups shims that are identical across the family
  (for example `spark-4.x`), and the minor-version directory holds
  per-release overrides (for example `spark-4.2`).

### Lay Out the Shim Directories

The build helper plugin in `spark/pom.xml` adds `src/main/${shims.majorVerSrc}`
and `src/main/${shims.minorVerSrc}` to the compile source roots. Files in the
minor directory shadow files in the major directory, so the typical pattern
is:

- `spark/src/main/spark-4.x/org/apache/comet/shims/` for shims that work for
  the whole 4.x family.
- `spark/src/main/spark-X.Y/org/apache/comet/shims/` for files that have to
  diverge for one specific release.

When Spark X.Y is brand new and you do not yet know which shims will need to
diverge, start by setting `shims.majorVerSrc` to an existing major directory
(for example `spark-4.x`) and `shims.minorVerSrc` to a new empty
`spark-X.Y` directory. Compile the project; the compiler will tell you which
shims need a per-version override. Add only those, and leave the rest in the
shared major directory. Commit `13e5f8cf5` ("refactor: consolidate identical
spark-4.0 and spark-4.1 shims into spark-4.x") shows the cleanup that
follows when shims that previously diverged turn out to be identical.

The same layering applies to `spark/src/test/spark-X.Y/` and
`common/src/main/spark-X.Y/`.

### Add a Version-Detection Helper

`CometSparkSessionExtensions.scala` exposes helpers like `isSpark40Plus` and
`isSpark41Plus` that the rest of the codebase uses to gate version-specific
logic and to skip tests. Add the matching helper for the new version
(`isSpark42Plus`, etc.) in this PR so that later stages can use it.

### Add a Compile-Only CI Job

Edit `.github/workflows/pr_build_linux.yml` and `pr_build_macos.yml` to add
the new Spark version to the `build-spark` (or equivalent compile-only) job
matrix. Do not add it to the heavier test matrices yet. A compile-only job
keeps the CI cost of stage 1 small and prevents test failures on the new
version from blocking unrelated PRs.

When CI capacity is constrained (the macOS runners in particular), it is
acceptable to drop an older minor version from the macOS PR matrix while a
preview version is being stabilized. PR #4104 ("ci: reduce macOS PR matrix
to single Spark 4.0 profile") is a precedent for this kind of trim.

### What to Avoid in Stage 1

- Do not enable any test job for the new version yet.
- Do not regenerate golden files yet. Plan stability output is sensitive to
  shim correctness, and regenerating before the shims are stable produces
  noisy diffs that get overwritten in stage 2.
- Do not modify `dev/generate-versions.py` or other release-doc scripts in
  this PR. Those are owned by the release process and have their own update
  cadence.

## Stage 2: Enable Comet's JVM Tests

Once the new profile compiles cleanly in CI, the next PR turns on Comet's
own test suites under the new profile.

### Add the New Profile to the Test Matrix

Promote the new Spark version from the compile-only job to the main test
jobs in `.github/workflows/pr_build_linux.yml` (and `pr_build_macos.yml` if
capacity allows). Use `scan_impl: "auto"` so both `native_datafusion` and
`native_iceberg_compat` get exercised, matching how earlier versions are
configured.

### Run the Suite Locally First

Run the JVM test suite locally against the new profile before pushing, since
CI iterations are slow:

```sh
make
./mvnw -Pspark-X.Y test
```

Expect failures. Triage them into three buckets:

1. **Real shim gaps**: a Spark API changed and the shim still calls the old
   signature. Fix these in this PR. Per-version overrides go under
   `spark/src/main/spark-X.Y/`; if the change applies to the whole family,
   put the fix in the shared major-version directory.
2. **Behavioral differences that need a code change**: for example a new
   Spark error class, a renamed config, or a new `OneRowRelation` planning
   path. Fix the small ones in this PR. Larger ones should be split into
   their own PRs and the affected tests skipped.
3. **Things that are clearly broken and need real investigation**: skip with
   a linked issue (see the next section) and fix in a follow-up.

### Skip Failing Tests with Linked Issues

For tests that cannot be fixed in this PR, use ScalaTest's `assume()` with a
GitHub issue link as the message:

```scala
assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/NNNN")
```

Open one issue per test or per cluster of related tests, and reference the
issue from the `assume()` call. The link is what makes the skip
recoverable: a contributor can grep for it later when the underlying problem
is fixed.

Resist the temptation to disable a whole test class. Per-test skips keep the
coverage loss visible and minimize the risk of silently dropping a real
regression.

### Update Fallback Reason Strings If Needed

Some Comet rules (notably in `CometScanRule.scala`) match on Spark error
messages or class names. Spark releases occasionally rename these. Update
matchers to use a common substring that works across all supported versions
rather than branching on `isSparkXYPlus`, so the matcher stays compact.

### What to Avoid in Stage 2

- Do not regenerate plan stability golden files yet. That belongs to
  stage 3 once Comet's own suite is green.
- Do not enable Spark's SQL tests yet. They are larger, noisier, and
  benefit from landing on a known-good Comet test baseline.

## Stage 3: Enable Spark SQL Tests and Plan Stability

The third PR turns on the externally-driven test suites: Spark's own SQL
tests run through Comet, and the TPC-DS plan stability golden files.

### Plan Stability Golden Files

Plan stability tests live under
`spark/src/test/resources/tpcds-plan-stability/approved-plans-{v1_4,v2_7}-sparkX_Y/`.
The suite (`CometPlanStabilitySuite`) falls back through earlier versions
when no version-specific approved plan exists, so most queries do not need
their own copy. Wire the new version into the fallback chain:

```scala
private val planName = if (isSpark42Plus) {
  "approved-plans-v1_4-spark4_2"
} else if (isSpark41Plus) {
  "approved-plans-v1_4-spark4_1"
} else if (isSpark40Plus) {
  "approved-plans-v1_4-spark4_0"
} else {
  ...
}
```

Update the version regex in `dev/regenerate-golden-files.sh` to allow the
new version, then regenerate:

```sh
./dev/regenerate-golden-files.sh --spark-version X.Y
```

The script automatically deduplicates: if a regenerated plan matches the
fallback chain it is removed from the version-specific directory. Only the
queries whose plans actually differ on the new version end up under
`approved-plans-*-sparkX_Y/`. Inspect each surviving diff: a small,
explainable difference is fine, but a large or mysterious diff is usually a
sign of a shim bug worth investigating before approving the plan.

### Spark SQL Test Overrides

Spark SQL tests run against patched Spark sources under `dev/diffs/`. Each
supported Spark version has its own diff file. The mechanics of starting
from the closest existing diff, applying it with `--reject`, resolving the
rejects (often with `wiggle`), and regenerating the new diff file are
described in detail in [Spark SQL Tests](spark-sql-tests.md). Follow that
page for the diff workflow itself; the additional points specific to a
new-version bring-up are:

- When Spark introduces new error classes (Spark 4.1 changed
  `DIVIDE_BY_ZERO` to `REMAINDER_BY_ZERO` for modulo, for example), prefer
  matchers that work across versions, like matching on the substring
  `BY_ZERO`, rather than branching by version.
- The same skip-with-linked-issue rule applies as in stage 2: one issue per
  test or cluster, and do not disable whole suites.

### CI for the Spark SQL Tests

Spark SQL tests do not run from the main PR build workflows. They have
their own dedicated workflow files:

- `.github/workflows/spark_sql_test.yml`
- `.github/workflows/spark_sql_test_native_iceberg_compat.yml`

Add the new version to the matrix in each of these files (`spark-short`,
`spark-full`, `java`, `scan-impl`). Use the closest existing entry as a
template.

### Final Sweep Before Merging Stage 3

- Run `make format` to apply scalafix and spotless.
- Run clippy (`cd native && cargo clippy --all-targets --workspace -- -D warnings`).
- Confirm that every skip introduced in this PR has a linked GitHub issue.

## Stage 4: Announce the Version in the User Guide

Once stage 3 is merged and CI is green, advertise the version to users.

The single source of truth for which Spark versions Comet works with is the
`### Supported Spark Versions` section in
`docs/source/user-guide/latest/installation.md`. It contains two tables and a
list of per-version jar download links. Update each:

- Add a row to the **experimental** table (the one introduced by the
  sentence "Experimental support is provided for the following versions
  ..."). Include the Java version, Scala version, and the `Yes`/`No`
  values for "Comet Tests in CI" and "Spark SQL Tests in CI" that match
  what stage 2 and stage 3 actually enabled.
- Add a `(Experimental)` jar download link below the existing entries.

Do not add the new version to the main "Supported Spark Versions" table
yet. That table is reserved for versions that have completed the promotion
criteria described in the next section.

Other user-guide pages (`operators.md`, `datatypes.md`,
`understanding-comet-plans.md`, etc.) generally do not mention specific
Spark versions and do not need editing for a new bring-up. The exception is
text that calls out a specific version's behavior, for example
`understanding-comet-plans.md` mentions `Spark 4.0 and newer`. Search the
user guide for the previous version string when adding a new one and
extend any such phrases that should now apply.

`docs/generate-versions.py` is about Comet release branches, not Spark
versions, and does not need editing.

## Stage 5 (Eventually): Promote from Experimental to Supported

The user guide currently uses two tiers, "Supported" and "Experimental".
"Experimental" is the term already established in `installation.md`,
`operators.md`, and `datasources.md`, and is broadly understood, so keep
using it rather than inventing a new label. It is intentionally distinct
from Spark's own "preview" terminology, which refers to upstream Spark
release qualifiers like `4.2.0-preview4` rather than to Comet's confidence
in its integration.

A version starts experimental and is promoted later. Promotion is its own
small PR, gated by these criteria:

- The Spark version is a final upstream release, not a preview, snapshot,
  or release candidate.
- Both "Comet Tests in CI" and "Spark SQL Tests in CI" are `Yes` for the
  version, and have been `Yes` continuously for at least one Comet release
  cycle.
- No `assume(!isSparkXYPlus, ...)` skip remains for a known correctness
  issue. Skips for unrelated, infrastructural, or environment-specific
  reasons are acceptable; correctness skips are not.
- Plan stability output for the version is approved (either a clean
  fallback chain or explicit per-version golden files).
- No open `Critical` or `Blocker`-tagged issue references the version.

When the criteria are met, the promotion PR moves the version's row from
the experimental table into the main "Supported Spark Versions" table and
removes the `(Experimental)` qualifier from the jar download link. No
shim, code, or test changes should be bundled with this promotion. Keeping
it as a doc-only PR makes it easy to revert if a problem shows up after
the promotion.

## Follow-Up PRs: Fix the Skipped Tests

Each follow-up PR should target one issue (or one cluster of related issues)
opened during stages 2 and 3. The pattern is:

1. Reproduce the failure under `-Pspark-X.Y`.
2. Identify the root cause: shim gap, expression behavior change, planner
   change, or genuine Comet bug exposed on the new version.
3. Implement the fix.
4. Remove the corresponding `assume()` skip and rerun the suite.
5. Reference the issue in the PR title or description so it auto-closes on
   merge.

Avoid bundling unrelated skip removals into one PR. A targeted PR per
issue keeps the diff small and makes regressions easy to bisect.

## Related Documentation

- [Development Guide](development.md): build prerequisites, test commands,
  and the canonical Maven invocations.
- [Comet Plugin Overview](plugin_overview.md): how the planner rules and
  shims fit together, useful when diagnosing version-specific failures.
- [Spark SQL Tests](spark-sql-tests.md): mechanics of running Spark's own
  SQL tests through Comet.
- [Bug Triage](bug_triage.md): conventions for opening and labeling issues
  for the skipped tests.

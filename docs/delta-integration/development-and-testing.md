# Development & testing

## Test surface added by this branch

```
spark/src/test/scala/org/apache/comet/
├── CometDeltaTestBase.scala            — shared session config + test helpers (UTC tz, disable DebugFilesystem, point Delta at our path-prefix-free fileNamePrefix)
├── CometDeltaNativeSuite.scala         — primary suite (~34 cases): basic / partitioned / projection / filter / DV / column mapping / time travel / aggregation / joins / window / NULL / case-insensitivity / ORDER BY+LIMIT
├── CometDeltaAdvancedSuite.scala       — schema evolution, mergeSchema, complex partition combinations, edge-case partitions (NTZ, decimals, dates)
├── CometDeltaColumnMappingSuite.scala  — id mode / name mode / RENAME COLUMN / DROP COLUMN with constraints / nested physical names
├── CometDeltaRowTrackingSuite.scala    — materialised + synthesised _row_id_ / _row_commit_version_; baseRowId fallback
├── CometDeltaRoundTripSuite.scala      — write-Delta-read-Delta loops to validate full type coverage end-to-end
├── CometDeltaBenchmarkTest.scala       — micro-benchmarks driven by the same harness as CometDeltaReadBenchmark
├── DeltaReadFromS3Suite.scala          — MinIO/S3 round-trip suite (skipped unless S3 endpoint is configured)
├── CometFuzzDeltaBase.scala            — fuzz harness that generates random schemas + tables, reads with both Comet-Delta and vanilla Delta, asserts equality
├── CometFuzzDeltaSuite.scala           — entry point for the fuzz harness; runs N iterations of random schemas + queries
└── CometArrayElementFilterRepro.scala  — minimal repro for the historical Utf8 <= Int32 nested-filter bug; documents the boundary
```

The `CometCastSuite`, `CometExpressionSuite`, and
`CometTemporalExpressionSuite` got incremental additions for the
expression-level fixes that supported Delta correctness (NTZ hour
extraction, partition-string casting, etc.).

## Dev scripts (under `dev/`)

  - **`build-comet-delta.sh`** — `mvn install -Pspark-3.5 -Prelease
    -DskipTests` plus `cargo build` invoked in the right order. Used
    by every other dev script as the build step.
  - **`run-delta-test.sh`** — run a single Comet test class quickly.
    No regression harness, no Delta clone — just `./mvnw test
    -Dsuites=<className>`.
  - **`run-delta-regression.sh [VERSION] [FILTER]`** — full Delta
    regression: clone Delta at the right tag, apply
    `dev/diffs/delta/<VERSION>.diff`, run `build/sbt "spark/test"` (or
    a targeted suite). Mirrors the GitHub Actions workflow exactly.
    Honors `DELTA_WORKDIR=<path>` to reuse a checkout, `DELTA_JAVA_HOME`
    to point Delta's SBT at JDK 17 (Delta's `icebergShaded/assembly`
    invokes Gradle 7.5.1 internally, which doesn't support JDK 19+),
    and `FAST=1` to skip the Comet rebuild between runs.
  - **`run-delta-regression-targeted.sh`** — like the regression script
    but takes an explicit list of test classes from a file. Used to
    triage the long tail of failing tests one by one.
  - **`run-delta-all-failures.sh`** — runs each previously-failing
    test in its own SBT invocation so a single failure doesn't abort
    the rest. Slow (~40s/test startup) but produces a clean per-test
    pass/fail matrix.
  - **`iterate-delta-fix.sh`** — one-shot iteration loop. Has hardcoded
    `SELECTOR` and `GREP_PATTERN` strings the developer edits between
    iterations. Builds Comet, runs the test, greps the log. The
    `SKIP_BUILD=1` mode reuses the existing JAR for native-only
    iteration.
  - **`inspect-targeted-failure.sh`** — focused debugging script that
    captures stack traces, plan dumps, and metric values from a single
    failing test invocation.

## Per-version diffs (`dev/diffs/delta/`)

Each `<version>.diff` is generated against the upstream Delta source at
that version's tag. Three files in Delta are patched:

  1. `build.sbt` — adds Comet as a test-scope dependency.
  2. `DeltaSQLCommandTest.scala` — injects Comet's plugin and shuffle
     manager into the test session's `sparkConf`.
  3. `DeltaHiveTest.scala` — same injection for Hive-based tests.

Plus a few targeted helper-file edits (e.g. patches to
`DeltaSinkSuite`'s partition-pruning helper to fall through to
`CometDeltaNativeScanExec.synthesizedFilePartitions`, the merge-metrics
shim accommodating Comet's two-file output, and disabling test-only
`test%file%prefix-` / `test%dv%prefix-` injections).

To regenerate a diff:

```bash
cd delta-lake          # checkout at the right tag
git checkout -- .
git apply ../datafusion-comet/dev/diffs/delta/3.3.2.diff
# … make changes …
git diff > ../datafusion-comet/dev/diffs/delta/3.3.2.diff
```

Each diff must be regenerated against its own tag (`v2.4.0`, `v3.3.2`,
`v4.0.0`).

## CI workflows

  - **`delta_spark_test.yml`** — `Delta Lake Native Scan Tests`. Runs
    Comet's own Delta suites (`CometDeltaNativeSuite` etc.) on every
    PR. Fast (~10–15 min). Built on `pr_build_linux.yml`'s
    infrastructure.
  - **`delta_regression_test.yml`** — `Delta Lake Regression Tests`.
    Matrix over (Delta 2.4.0 + Spark 3.4, Delta 3.3.2 + Spark 3.5,
    Delta 4.0.0 + Spark 4.0), all on Java 17. Clones Delta, applies
    the appropriate diff, runs `spark/test`. Slow (~hours).
  - **`pr_build_linux.yml` / `pr_build_macos.yml`** — got Delta-related
    additions to make sure the existing matrix still builds cleanly
    with the new Delta module compiled in (it doesn't get exercised
    here unless the user opts in).

The custom action `setup-delta-builder/action.yaml` (in
`.github/actions/`) wraps the Delta clone + JDK17 setup so both
workflows can reuse it.

## Benchmarks

  - **`benchmarks/tpc/create-delta-tables.py`** — converts an existing
    Parquet TPC dataset (TPC-DS or TPC-H) into a Delta-formatted
    warehouse for benchmarking.
  - **`benchmarks/tpc/engines/comet-delta.toml`** /
    `comet-delta-hashjoin.toml` — engine descriptors for the
    `tpcbench.py` harness, so the same TPC suite that runs against
    plain Comet, vanilla Spark, etc. can also run against
    Comet+Delta.
  - **`spark/src/test/scala/org/apache/spark/sql/benchmark/
    CometDeltaReadBenchmark.scala`** — micro-benchmark suite using
    Spark's `SqlBasedBenchmark` framework. Output goes to
    `spark/benchmarks/CometDeltaReadBenchmark-results.txt`.
  - **`benchmarks/tpc/`** got a small extension to `CometBenchmarkBase`
    so Delta tables show up alongside Parquet/Iceberg.

To regenerate the TPC-DS plan-stability fixtures with the
`native_delta_compat` scan implementation, see the existing
contributor docs at
`docs/source/contributor-guide/delta-spark-tests.md` § *TPC-DS Plan
Stability Fixtures*.

## How the fuzz harness works

`CometFuzzDeltaSuite` extends `CometFuzzDeltaBase` which:

  1. Generates a random Spark schema (combinations of primitives +
     nested types up to a configurable depth).
  2. Writes a small Delta table with random data.
  3. Runs a list of randomly-generated queries (projection, filter,
     aggregation, join) twice — once with Comet's Delta path enabled,
     once with it disabled.
  4. Asserts the two result sets are equal (after order normalisation).

This is the broadest correctness check in the branch. Failures here
become reproducer test cases in the more targeted suites.

## Local quickstart

```bash
# Build native + Spark side
cd native && cargo build && cd ..
./mvnw install -Prelease -DskipTests -Pspark-3.5

# Run Comet's own Delta suite
./mvnw -Pspark-3.5 -pl spark -am test \
  -Dsuites=org.apache.comet.CometDeltaNativeSuite \
  -Dmaven.gitcommitid.skip

# Smoke-test the regression suite (just confirm Comet is wired in)
dev/run-delta-regression.sh 3.3.2

# Run one Delta test class against the regression harness
dev/run-delta-regression.sh 3.3.2 DeltaTimeTravelSuite

# Iterate on a single failing test (edit SELECTOR + GREP_PATTERN in the script)
dev/iterate-delta-fix.sh
```

## Reading EXPLAIN

When a query *should* be accelerated but isn't, the first stop is:

```
SET spark.comet.explainFallback.enabled=true;
EXPLAIN EXTENDED <your query>;
```

Every `withInfo(scanExec, "...")` decline annotation surfaces in the
plan output, telling you exactly why Comet declined. The phrasing
matches the strings in `CometScanRule.scala` and
`CometDeltaNativeScan.scala` so you can search the source for the
message.

When the scan *is* accelerated, look for `CometDeltaNativeScan
<table-root> (N files[, dpp=[...]])` in the plan tree, with metrics
for `total_files` / `numFiles`, `dv_files`, `output_rows`, and
`num_splits`.

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

# Running Delta Lake Tests with Comet

## Comet's own Delta test suite

The primary test suite is `CometDeltaNativeSuite` with 34 test cases covering:

- Basic reads (unpartitioned, multi-file, partitioned, multi-column partition)
- Projection pushdown, filter pushdown, predicate variety
- Complex types (arrays, maps, structs, deeply nested)
- Schema evolution, time travel (version + timestamp)
- Deletion vectors (pre-DELETE acceleration + post-DELETE native DV filter)
- Column mapping (id mode, name mode, rename)
- Aggregation, joins, window functions, UNION, DISTINCT
- NULL handling, case insensitivity, ORDER BY + LIMIT

### Running the suite

```bash
# Build native library first
cd native && cargo build && cd ..

# Run the Delta suite
./mvnw -Pspark-3.5 -pl spark -am test \
    -Dsuites=org.apache.comet.CometDeltaNativeSuite \
    -Dmaven.gitcommitid.skip
```

### Test dependencies

Delta tests require `io.delta:delta-spark_2.12:3.3.2` which is added as a
test-scope dependency in `spark/pom.xml` under the `spark-3.5` profile. The
dependency excludes Spark and Hadoop transitives so Comet's pinned versions
stay authoritative.

### Test harness notes

The Delta test suite disables Spark's `DebugFilesystem` and Delta's test-only
filename prefixes (`test%file%prefix-`, `test%dv%prefix-`) because
delta-kernel-rs reads files by the names recorded in the transaction log, which
don't include Spark/Delta's test prefixes. Production users are unaffected.

The suite also sets `spark.databricks.delta.deletionVectors.useMetadataRowIndex=false`
to use Delta's older DV read strategy that inserts a `Project -> Filter` subtree
(which Comet's plan rewrite can detect and strip), rather than the default
metadata-row-index strategy that's opaque to plan-level rewriting.

## Delta Lake Regression Suite

In addition to Comet's own Delta test suites, we run Delta Lake's own Spark test suite with Comet
enabled as a regression check. This is analogous to the [Iceberg regression suite](iceberg-spark-tests.md)
and ensures that Comet's Spark plugin and shuffle manager don't break Delta Lake's existing
functionality.

The diffs patch three files in the Delta Lake source:

- `build.sbt` — adds Comet as a test dependency
- `DeltaSQLCommandTest` — injects Comet plugin and shuffle manager into `sparkConf`
- `DeltaHiveTest` — injects the same config for Hive-based tests

### Running locally

The easiest way is the helper script, which builds Comet, clones Delta at the right tag, applies the
diff, and runs tests in one command:

```shell
# smoke test only (fast — verifies Comet is wired in)
dev/run-delta-regression.sh 3.3.2

# a specific test class
dev/run-delta-regression.sh 3.3.2 DeltaTimeTravelSuite

# full Delta test suite (slow)
dev/run-delta-regression.sh 3.3.2 full
```

Set `DELTA_WORKDIR=/path/to/checkout` to reuse an existing Delta clone between runs.

Set `DELTA_JAVA_HOME=$(/usr/libexec/java_home -v 17)` to run Delta's SBT on JDK 17
while Comet was built on a newer JDK. Needed because Delta's `spark/test` triggers
`icebergShaded/assembly`, which runs Gradle 7.5.1 (no JDK 19+ support) internally.

To do the steps manually:

1. Build and install Comet to the local Maven repository:

```shell
./mvnw install -Prelease -DskipTests -Pspark-3.5
```

2. Clone Delta Lake and apply the diff:

```shell
git clone --depth 1 --branch v3.3.2 https://github.com/delta-io/delta.git delta-lake
cd delta-lake
git apply ../datafusion-comet/dev/diffs/delta/3.3.2.diff
```

3. Run the Delta Spark tests:

```shell
SPARK_LOCAL_IP=localhost build/sbt "spark/test"
```

### Updating diffs

To update a diff (e.g. after modifying test configuration), apply the existing diff, make your
changes, then regenerate:

```shell
cd delta-lake
git checkout -- .
git apply ../datafusion-comet/dev/diffs/delta/3.3.2.diff

# Make changes ...

git diff > ../datafusion-comet/dev/diffs/delta/3.3.2.diff
```

Repeat for each Delta version (2.4.0, 3.3.2, 4.0.0). Each diff must be generated against its own
tag.

### Running in CI

The `delta_regression_test.yml` workflow applies these diffs and runs Delta's test suite with Comet
enabled. The test matrix covers Delta 2.4.0 (Spark 3.4), 3.3.2 (Spark 3.5), and 4.0.0 (Spark 4.0)
with Java 17.

## Benchmarks

### Micro-benchmark

```bash
SPARK_GENERATE_BENCHMARK_FILES=1 make \
    benchmark-org.apache.spark.sql.benchmark.CometDeltaReadBenchmark
```

Results are written to `spark/benchmarks/CometDeltaReadBenchmark-results.txt`.

### TPC-DS / TPC-H

1. Convert Parquet data to Delta:

```bash
spark-submit \
    --packages io.delta:delta-spark_2.12:3.3.2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    benchmarks/tpc/create-delta-tables.py \
    --benchmark tpcds \
    --parquet-path /data/tpcds \
    --warehouse /data/delta-tpcds
```

2. Run the benchmark:

```bash
DELTA_JAR=/path/to/delta-spark.jar \
COMET_JAR=/path/to/comet-spark.jar \
DELTA_WAREHOUSE=/data/delta-tpcds \
python benchmarks/tpc/tpcbench.py \
    --engine comet-delta \
    --benchmark tpcds
```

Engine configs are in `benchmarks/tpc/engines/comet-delta.toml` and
`benchmarks/tpc/engines/comet-delta-hashjoin.toml`.

## TPC-DS Plan Stability Fixtures

To generate the `q*.native_delta_compat/` plan stability golden files:

1. Add `CometConf.SCAN_NATIVE_DELTA_COMPAT` to the `scanImpls` list in
   `CometPlanStabilitySuite.scala` (line 66).

2. Ensure TPC-DS data exists as Delta tables (see `create-delta-tables.py`
   in the Benchmarks section above).

3. Generate golden files:

```bash
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark \
    -Dsuites=org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite \
    test -Pspark-3.5 -Dmaven.gitcommitid.skip
```

4. Commit the generated `q*.native_delta_compat/extended.txt` files under
   `spark/src/test/resources/tpcds-plan-stability/approved-plans-*`.

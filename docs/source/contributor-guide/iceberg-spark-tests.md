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

# Running Iceberg Spark Tests

Running Apache Iceberg's Spark tests with Comet enabled is a good way to ensure that Comet produces the same
results as Spark when reading Iceberg tables. To enable this, we apply diff files to the Apache Iceberg source
code so that Comet is loaded when we run the tests.

Here is an overview of the changes that the diffs make to Iceberg:

- Configure Comet as a dependency and set the correct version in `libs.versions.toml` and `build.gradle`
- Delete upstream Comet reader classes that reference legacy Comet APIs removed in [#3739]. These classes were
  added upstream in [apache/iceberg#15674] and depend on Comet's old Iceberg Java integration. Since Comet now
  uses a native Iceberg scan, these classes fail to compile and must be removed.
- Configure test base classes (`TestBase`, `ExtensionsTestBase`, `ScanTestBase`, etc.) to load the Comet Spark
  plugin and shuffle manager
- Enable the Iceberg write split-operator plan (`spark.comet.write.iceberg.splitOperator.enabled`) alongside the
  native scan in every Comet-configured session. The flag is off by default for users, so Iceberg's own suites
  are the only place the split plan (`IcebergCommit -> IcebergWrite`) is exercised against Iceberg's write,
  commit, and row-level-operation tests. See [#5259]
- Enable Comet's native (iceberg-rust) Parquet writer (`spark.comet.iceberg.write.enabled`) in the same sessions.
  The native writer is experimental and off by default for users, so this is where it runs against Iceberg's
  write, commit, and row-level-operation tests.
- Enable `spark.comet.exec.localTableScan.enabled` in the same sessions. `CometIcebergNativeWrite` sets
  `requiresNativeChildren`, so without this flag a write fed by an inline `VALUES` list keeps Spark's row-based
  `LocalTableScanExec`, the conversion is declined, and the write silently runs on the JVM writer. Many Iceberg
  suites seed their data that way, so leaving it off hides the native writer from most of the write surface.
- Enable fallback logging (`spark.comet.explainFallback.enabled`) so that every operator Comet declines is
  reported in the test output together with the reason it was declined. The output goes to the JUnit XML
  reports rather than the CI job log; see [Which writer ran each Iceberg write](#which-writer-ran-each-iceberg-write)
  for how CI reports native write coverage.

[#3739]: https://github.com/apache/datafusion-comet/pull/3739
[#5259]: https://github.com/apache/datafusion-comet/issues/5259
[apache/iceberg#15674]: https://github.com/apache/iceberg/pull/15674

`dev/local-ci.sh` runs all of the steps below the way CI runs them:

```shell
dev/local-ci.sh iceberg              # every target the workflow runs
dev/local-ci.sh iceberg shard-2      # one shard of the core test job
dev/local-ci.sh iceberg 1.9 shard-2  # a non-default Iceberg version
```

See [Continuous Integration](ci.md#reproducing-a-suite-failure-locally). The manual steps below
are still the reference, and are what you want when updating a diff.

## 1. Install Comet

Run `make release` in Comet to install the Comet JAR into the local Maven repository, specifying the Spark version.

```shell
PROFILES="-Pspark-4.1" make release
```

## 2. Clone Iceberg and Apply Diff

Clone Apache Iceberg locally and apply the diff file from Comet against the matching tag.

```shell
git clone git@github.com:apache/iceberg.git apache-iceberg
cd apache-iceberg
git checkout apache-iceberg-1.8.1
git apply ../datafusion-comet/dev/diffs/iceberg/1.8.1.diff
```

## 3. Run Iceberg Spark Tests

```shell
ENABLE_COMET=true ./gradlew -DsparkVersions=3.5 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
  :iceberg-spark:iceberg-spark-3.5_2.13:test \
  -Pquick=true -x javadoc
```

The three Gradle targets tested in CI are:

| Gradle Target                                 | What It Covers                                                                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `iceberg-spark-<ver>:test`                    | Core read/write paths (Parquet, Avro, ORC, vectorized), scan operations, filtering, bloom filters, runtime filtering, deletion handling, structured streaming, DDL/DML (create/alter/drop, writes, deletes), filter and aggregate pushdown, actions (snapshot expiration, file rewriting, orphan cleanup, table migration), serialization, and data format conversions. |
| `iceberg-spark-extensions-<ver>:test`         | SQL extensions: stored procedures (migrate, snapshot, cherrypick, rollback, rewrite-data-files, rewrite-manifests, expire-snapshots, remove-orphan-files, etc.), row-level operations (copy-on-write and merge-on-read update/delete/merge), DDL extensions (branches, tags, alter schema, partition fields), changelog tables/views, metadata tables, and views.       |
| `iceberg-spark-runtime-<ver>:integrationTest` | A single smoke test (`SmokeTest.java`) that validates the shaded runtime JAR. The `spark-runtime` module has no main source — it packages Iceberg and all dependencies into a shaded uber-JAR. The smoke test exercises basic create, insert, merge, query, partition field, and sort order operations to confirm the shaded JAR works end-to-end.                      |

## Updating Diffs

To update a diff (e.g. after modifying test configuration), apply the existing diff, make changes, then
regenerate:

```shell
cd apache-iceberg
git reset --hard apache-iceberg-1.8.1 && git clean -fd
git apply ../datafusion-comet/dev/diffs/iceberg/1.8.1.diff

# Make changes, then run spotless to fix formatting
./gradlew spotlessApply

# Stage any new or deleted files, then generate the diff
git add -A
git diff apache-iceberg-1.8.1 > ../datafusion-comet/dev/diffs/iceberg/1.8.1.diff
```

Repeat for each Iceberg version (1.8.1, 1.9.1, 1.10.0, 1.11.0). The file contents differ between versions, so each
diff must be generated against its own tag.

## Running Tests in CI

The `iceberg_spark_test_<version>.yml` workflows apply these diffs and run the three Gradle targets above
against each Iceberg version, all with Java 17. Iceberg 1.8.1 runs against Spark 3.4.3; Iceberg 1.9.1 and 1.10.0
run against Spark 3.5.9; Iceberg 1.11.0 runs against Spark 4.1.3. Iceberg 1.11.0 runs in the
merge queue; 1.8.1, 1.9.1 and 1.10.0 run once a night against `main`. All four run earlier on a
pull request labeled `run-iceberg-tests`; none runs on an unlabeled pull request. All caller
workflows delegate to `iceberg_spark_test_reusable.yml`, which holds the build and test job logic. See
[.github/workflows/README.md](https://github.com/apache/datafusion-comet/blob/main/.github/workflows/README.md)
for how the pull-request, merge-queue and nightly tiers differ.

The core Spark test target runs in four independent workers. The workflow passes
`dev/ci/iceberg-test-shards.gradle` as a Gradle init script: one worker runs the long
`TestStructuredStreamingRead` family, and the others hash the remaining class names into three
buckets. New tests are assigned automatically. Nested classes and all parameterized cases stay
with their enclosing class; Gradle's existing includes, exclusions, and JUnit configuration are
unchanged. The extensions and shaded-runtime targets remain unsharded.

The matrix and partition count come from the same definition in `dev/ci/check-iceberg-shards.py`;
adding another matrix dimension does not change the partition count. Each worker records its
unsharded candidate set with only the Comet shard predicate disabled, then restores the predicate
before recording its selected set and executing tests. Both inventories and the JUnit XML reports
are uploaded. A dependent coverage job requires all shard indices, matching unsharded inventories,
and selected sets whose disjoint union equals that inventory. It downloads only artifacts for the
same Iceberg/Spark/Scala/JDK configuration in the current workflow run and uses the latest available
attempt per shard, so rerunning only failed jobs can reuse earlier successful shards' inventories.

These candidate inventories include classes that JUnit may not execute, so the runtime job also
runs `dev/ci/check-iceberg-shards.py`, a small Gradle/JUnit fixture that checks the four shards'
combined candidate classes and executed test cases equal an unsharded run exactly once. It also
checks nested, parameterized, inherited, and dynamically generated tests, existing exclusions,
and failure propagation. The fixture does not compile Spark or Iceberg.

Apply the `run-iceberg-tests` label to a pull request whenever it touches the Iceberg scan or write
path, reflection code (`org.apache.comet.iceberg.IcebergReflection`), or other logic whose behavior
can differ across Iceberg versions. The Comet test suites in the Linux build do not exercise Iceberg's
own Spark tests, so without the label the first Iceberg 1.11 verdict is the merge queue's, and the
first verdict on the older Iceberg versions is the nightly run's, after the change has landed.

### Which writer ran each Iceberg write

A passing Iceberg job does not show that Comet's native writer ran. `CometIcebergNativeWrite` falls back
to Iceberg's JVM writer without failing the write, no upstream test asserts which writer ran, and Gradle
does not copy the fallback warnings into the job log. So the core and extensions jobs set
`COMET_ICEBERG_WRITE_REPORT_DIR`, the environment variable behind the test-only config
`spark.comet.testing.icebergWriteReport.dir`. When it is set, the Comet driver plugin registers
`IcebergWriteReportListener`, which writes one JSON line for each Iceberg write the tests run. Each line
records one of three writers:

- `native`: Comet's native writer (`CometIcebergWriteExec`).
- `jvm`: Comet's split operator planned the write but kept Iceberg's JVM writer (`IcebergWriteExec`).
  The line includes the reasons Comet recorded for not converting it.
- `spark`: Spark's own V2 write operator ran the write, so Comet's split operator never saw it. Examples
  are `WriteDelta` for merge-on-read, `WriteToDataSourceV2` for a streaming micro-batch, and on Spark
  3.4 the CTAS and RTAS execs, which write the table themselves.

`dev/ci/summarize-iceberg-writes.py` turns these records into a table on the job's summary page. It
shows the count and share of each writer, the most common fallback reasons, and the Spark write
operators. Each shard and the extensions job gets its own table. The shard coverage job adds one for
all shards together, counting only the latest attempt of each shard. A shard whose latest attempt
recorded no writes is named above the table rather than counted from an earlier attempt. The raw
records are uploaded with the job's other reports. The summary never fails a job.
`dev/local-ci.sh iceberg` prints the same summary after each shard and after the extensions target.

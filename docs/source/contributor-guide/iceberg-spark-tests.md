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

[#3739]: https://github.com/apache/datafusion-comet/pull/3739
[apache/iceberg#15674]: https://github.com/apache/iceberg/pull/15674

## 1. Install Comet

Run `make release` in Comet to install the Comet JAR into the local Maven repository, specifying the Spark version.

```shell
PROFILES="-Pspark-3.5" make release
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

Repeat for each Iceberg version (1.8.1, 1.9.1, 1.10.0). The file contents differ between versions, so each
diff must be generated against its own tag.

## Running Tests in CI

The `iceberg_spark_test.yml` workflow applies these diffs and runs the three Gradle targets above against
each Iceberg version. The test matrix covers Spark 3.4 and 3.5 across Iceberg 1.8.1, 1.9.1, and 1.10.0
with Java 11 and 17. The workflow runs on all pull requests and pushes to the main branch.

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

# Local Spark SQL Tests

These scripts reproduce the `spark_sql_test.yml` GitHub Actions workflow on a
developer machine. They run Spark's own SQL test suites with Comet enabled,
which is useful for debugging a Spark SQL test failure locally instead of
waiting on CI.

The Spark version is selected with `SPARK_VERSION` and defaults to `4.1.1`.
Supported versions, each mirroring a CI matrix config:

| `SPARK_VERSION` | JDK used by CI |
|-----------------|----------------|
| `3.4.3`         | 11             |
| `3.5.8`         | 11             |
| `4.0.2`         | 21             |
| `4.1.1`         | 17             |

## Prerequisites

- A JDK with `JAVA_HOME` set, matching the Spark version under test (see the
  table above). `run.sh` warns if the active JDK differs from the one CI uses.
- A Rust toolchain, plus `protobuf-compiler` and `clang`, for the Comet native build.
- Git, and enough disk space for an `apache/spark` checkout and its build output.

## Usage

Run from anywhere inside the repository:

```sh
dev/ci/spark-sql-tests/run.sh [module]
```

`module` is one of the seven CI shards, or `all` (the default):

| Module       | Spark suites |
|--------------|--------------|
| `catalyst`   | `catalyst/test` |
| `sql_core-1` | `sql` suites excluding `ExtendedSQLTest` / `SlowSQLTest` |
| `sql_core-2` | `sql` `ExtendedSQLTest` suites |
| `sql_core-3` | `sql` `SlowSQLTest` suites |
| `sql_hive-1` | `hive` suites excluding `ExtendedHiveTest` / `SlowHiveTest` |
| `sql_hive-2` | `hive` `ExtendedHiveTest` suites |
| `sql_hive-3` | `hive` `SlowHiveTest` suites |

Examples:

```sh
# Run a single shard
dev/ci/spark-sql-tests/run.sh sql_core-1

# Run all seven shards sequentially
dev/ci/spark-sql-tests/run.sh

# Re-run a shard without rebuilding Comet or re-applying the Spark diff
SKIP_BUILD=1 SKIP_SPARK_SETUP=1 dev/ci/spark-sql-tests/run.sh sql_core-1

# Test a different Spark version
SPARK_VERSION=4.0.2 dev/ci/spark-sql-tests/run.sh sql_core-1
```

The first run clones `apache/spark` and builds both Comet and Spark, which
takes a while. A full `all` run takes several hours, the same as CI. Per-module
output is written to `dev/ci/spark-sql-tests/logs/<version>/<module>.log`, and a
PASS/FAIL summary is printed at the end.

## Environment variables

| Variable           | Default                                   | Effect |
|--------------------|-------------------------------------------|--------|
| `SPARK_VERSION`    | `4.1.1`                                   | Spark version to test: `3.4.3`, `3.5.8`, `4.0.2`, or `4.1.1`. |
| `SKIP_BUILD`       | unset                                     | `1` skips the Comet build and reuses existing artifacts. |
| `SKIP_SPARK_SETUP` | unset                                     | `1` skips the Spark clone/reset/diff step. |
| `COMET_SPARK_DIR`  | `~/.cache/datafusion-comet/apache-spark-<version>` | Persistent Spark checkout location, namespaced by version. |
| `SPARK_REF`        | `v<version>`                              | Git ref checked out for the Spark sources. |
| `SBT_MEM`          | `4096`                                    | sbt heap size in MB. |
| `LC_ALL`           | `C.UTF-8`                                 | Locale for the sbt run. Use `en_US.UTF-8` on macOS if `C.UTF-8` is unavailable. |
| `PYSPARK_PYTHON`   | a nonexistent path                        | Python interpreter for Spark. The default skips Spark 4.x's Python data source probe, which can hang on machines that have `python3`. Export a real interpreter to run the Python-dependent suites. |

> **Note on Python:** Spark 4.x probes for Python data sources during query
> analysis by spawning a Python worker. The CI `amd64/rust` container has no
> `python3`, so the probe is skipped. On a developer machine that has `python3`
> the worker can hang indefinitely (the JVM-side read has no idle timeout),
> stalling suites such as `GlobalTempViewSuite`. `run.sh` therefore points
> `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` at a nonexistent path by default so
> the probe is skipped, matching CI.

## How it works

1. `run.sh` builds Comet with `PROFILES=-Pspark-<short> make release` (unless
   `SKIP_BUILD=1`), then purges partial Maven cache entries so sbt's resolver
   does not choke on POM-only artifacts.
2. `setup-spark.sh` maintains a persistent `apache/spark` checkout per version:
   it clones the `v<version>` tag on first use, and on every run resets it to a
   clean state and applies `dev/diffs/<version>.diff`. Spark's compiled
   `target/` artifacts are preserved across runs so rebuilds are incremental.
3. `run.sh` runs the selected module shard(s) with `build/sbt`, using the same
   environment and arguments as the `spark_sql_test.yml` workflow, including the
   per-version test-group isolation (Spark 4.0 forks a dedicated JVM per
   leak-prone Parquet/Orc suite; other versions run serially).

The CI workflow's optional Comet fallback-reason log collection
(`workflow_dispatch`) is not reproduced.

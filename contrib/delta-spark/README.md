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

# Comet Delta Lake Contrib (experimental)

Native Delta Lake reads for Comet. Delta tables are scanned through Comet's
existing native Parquet reader, so they get row-group pruning, page-index
pruning, and filter pushdown, with deletion vectors applied inside the scan.

The jar also gives Comet a Rust implementation of Delta's `InterleaveBits`
(the Z-order key) for one or two integer inputs. It is on whenever the jar is
present; `spark.comet.expression.InterleaveBits.enabled=false` turns it off.

Scan support is experimental and explicitly opt-in. Two things are required:

1. This module's jar (`comet-contrib-delta-spark`) on the classpath, alongside
   `delta-spark`. It is never bundled into `comet-spark`; without it, Comet
   has no Delta surface at all.
2. `spark.comet.scan.delta.enabled=true`. The default is `false`, so the jar
   alone leaves Delta table scans to Spark.

Unsupported tables and features fall back to Spark's reader. See the
[user guide](https://datafusion.apache.org/comet/user-guide/latest/delta.html)
for configuration details.

## Supported versions

| Spark | Delta          | Status                                        |
| ----- | -------------- | --------------------------------------------- |
| 3.5   | 3.3.x          | supported                                     |
| 4.0   | 4.0.x          | supported                                     |
| 4.1   | 4.3.x          | supported                                     |
| 3.4   | delta-core 2.4 | not supported (older Delta, would need shims) |
| 4.2   | none released  | inert until Delta ships a Spark 4.2 release   |

## Building and testing

The module builds under the `delta` Maven profile. It resolves `comet-spark`
from the local Maven repository, so install `common` and `spark` from the same
checkout immediately before, as CI does, with the `delta` profile active so that
install produces the spark test-jar the contrib suites depend on; a stale sibling
install is the trap the contributor guide warns about:

```shell
./mvnw -Pspark-3.5,delta install -pl common,spark -DskipTests
./mvnw -Pspark-3.5,delta install -pl contrib/delta-spark
```

Run the test suites the same way (`test` instead of `install` on the second
line). CI runs them via `.github/workflows/delta_contrib_test.yml`: nightly on
Spark 3.5, 4.0 and 4.1, or on a pull request carrying the `run-delta-tests`
label.
`CometDeltaS3Suite` starts MinIO through Testcontainers and cancels itself when
no Docker daemon is reachable; setting `COMET_DELTA_S3_REQUIRED=1`, as the
`contrib-delta-s3` CI job does, turns that cancel into a suite failure.

`dev/` contains a benchmark script (`bench_delta_comet.py`) and a harness for
running Delta's own test suites against Comet (`run-delta-regression.sh`).

### InterleaveBits benchmark

Run from the repository root with JDK 17 and the Rust toolchain available:

```shell
(cd native && cargo build --release)
./mvnw -Pspark-4.1,delta -Prelease install -DskipTests
COMET_BENCHMARK_JVM_ARGS=$(./mvnw -q -Pspark-4.1 help:evaluate -DforceStdout -Dexpression=extraJavaTestArgs)
COMET_CONF_DIR="$PWD/conf" \
  ./mvnw -f contrib/delta-spark/pom.xml -Pspark-4.1,delta -Prelease \
  exec:exec -Dexec.executable=java -Dexec.classpathScope=test \
  "-Dexec.args=-Xmx6g $COMET_BENCHMARK_JVM_ARGS -Djava.library.path=$PWD/native/target/release -classpath %classpath org.apache.spark.sql.benchmark.CometDeltaInterleaveBitsBenchmark"
```

This compares one and two integer inputs over one million Parquet rows, including
nulls in the second input, with Delta's fast implementation enabled and disabled.
Both engines consume the projected keys. Check the output for fallback warnings
before interpreting timings. Results include scanning and projection; they do
not measure `OPTIMIZE` or the kernel in isolation.

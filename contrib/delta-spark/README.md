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

Support is experimental and explicitly opt-in. Two things are required:

1. This module's jar (`comet-contrib-delta-spark`) on the classpath, alongside
   `delta-spark`. It is never bundled into `comet-spark`; without it, Comet
   has no Delta surface at all.
2. `spark.comet.scan.delta.enabled=true`. The default is `false`, so the jar
   alone does nothing.

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
| 4.2   | 4.4.x          | supported                                     |

Spark 4.2 uses the `delta-spark_4.2_2.13` artifact, pinned to Delta 4.4.1 by
the `spark-4.2` Maven profile. Earlier Spark profiles keep `delta-spark` with
their existing version pins.

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
Spark 3.5, 4.0, 4.1 and 4.2, or on a pull request carrying the `run-delta-tests`
label.
`CometDeltaS3Suite` starts MinIO through Testcontainers and cancels itself when
no Docker daemon is reachable; setting `COMET_DELTA_S3_REQUIRED=1`, as the
`contrib-delta-s3` CI job does, turns that cancel into a suite failure.

`dev/` contains a benchmark script (`bench_delta_comet.py`) and a harness for
running Delta's own test suites against Comet (`run-delta-regression.sh`).

### S3 encryption context

A nonempty effective `fs.s3a.encryption.context` makes the Delta scan fall back to Spark. The fallback also applies on older Hadoop versions that ignore the key. The context is attached to KMS write requests, but Hadoop 3.5 also parses it while initializing an S3 filesystem for reads. A local Delta log or shallow clone can reference S3 data files without initializing that data bucket during metadata reads. Comet therefore declines configured contexts, including valid ones, so Spark performs the same initialization checks before opening those files. Bucket aliases and credential-provider values are included; decline reasons do not print context values. Existing SSE-C and client-side encryption declines still apply.

The MinIO CI jobs run on Spark 3.5 and 4.2. The Spark 4.2 case covers a local shallow clone whose files remain in S3, with valid and malformed global, short-bucket and long-bucket context settings. The unconfigured control must use the native Delta scan; configured reads must fall back, returning rows for valid context and failing like Spark for malformed context.

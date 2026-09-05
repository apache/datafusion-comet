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
[user guide](https://datafusion.apache.org/comet/user-guide/delta.html)
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

The module builds under the `delta` Maven profile:

```shell
./mvnw -Pspark-3.5,delta install -pl contrib/delta-spark
```

Run the test suites the same way (`test` instead of `install`). CI runs them
on Spark 3.5, 4.0, and 4.1 via `.github/workflows/delta_contrib_test.yml`.

`dev/` contains a benchmark script (`bench_delta_comet.py`) and a harness for
running Delta's own test suites against Comet (`run-delta-regression.sh`).

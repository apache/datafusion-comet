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

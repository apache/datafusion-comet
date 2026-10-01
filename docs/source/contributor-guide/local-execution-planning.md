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

# Local planning diagnosis checkpoint

Stage 5c corrects the interpretation of the stage 5b planning result. The old
`planning_ms` includes DataFrame construction, Parquet schema inference, analysis,
optimization and physical preparation. It does not measure local admission alone.
There is no production planner optimization in this checkpoint: profiling did
not justify changing the admission rules or introducing plan/schema caches.

## Evidence and changes

An initial JFR recording of 110 local executions (including warmup) contained
197 execution samples, only three with local admission planner frames. Main-thread
park events included 1.575 seconds in Parquet schema inference / Spark schema
merge jobs, out of 2.874 seconds in Spark jobs overall. These diagnostic totals
include warmup and startup; sparse samples cannot quantify admission CPU precisely.
The large digest-validation workload is also visible in CPU samples and is outside
the query timing interval. The JFR binary is intentionally not checked in.

To check admission directly, a temporary wrapper timed CometLocalRule.apply.
After ten warmup calls, 50 calls had median 1.822 ms, nearest-rank p95 4.380 ms,
and maximum 4.607 ms. The first cold call took 227.435 ms, so startup is a separate
concern. Timers were read before logger output. This instrumented run is not used
as a baseline. Its log and instrumented rule source are saved with the results; the wrapper
was removed before the controlled comparisons and is absent from production.

The manual benchmark now records `dataframe_ms` and `physical_planning_ms`, whose
sum preserves `planning_ms`. The latter includes lazy optimization, physical
preparation and path assertions, not just local admission. `--schema-mode explicit`
provides the known schemas of the generated fixtures to all three modes, eliminating
repeated schema inference without caching plans or skipping file discovery.
The default remains `infer`; these are two different application input patterns,
not a production speedup. `--jfr` records diagnostic profiles separately. Child JVM
logs now go to each run's log instead of the shared unit-test log.

## Controlled comparison

Same host, JDK 17, release library, data and resource settings as stage 5b. For
each schema mode, three separate JVMs run Spark, existing Comet and local, followed
by another pass in reverse order. Each case warms up twice and measures five times.
The complete order was infer/forward, explicit/forward, explicit/reverse,
infer/reverse. No compilation ran concurrently. All **420** executions agreed on
row counts and digests across both schema modes and passes; path assertions and
local handle cleanup checks passed. Five-sample medians and OS cache/JIT effects
remain limitations; no significance or general workload claims are made.

Each table cell is **forward / reverse** in milliseconds. Independent medians
need not add up. Raw samples, metadata and plans are saved under
`benchmarks/results/local-execution/2026-09-30-planning/` with the existing raw
benchmark `.txt` suffix convention.

### Infer schema

| Query | Mode | DataFrame construction | Physical preparation | Execution + collect | Total |
|---|---|---:|---:|---:|---:|
| scan-filter-project | spark | 35.1 / 32.2 | 3.0 / 2.9 | 22.5 / 33.3 | 60.9 / 69.0 |
| scan-filter-project | comet | 33.4 / 75.1 | 6.4 / 5.2 | 16.5 / 17.0 | 57.1 / 98.3 |
| scan-filter-project | local | 49.8 / 50.6 | 5.5 / 5.8 | 16.9 / 15.6 | 76.3 / 77.8 |
| grouped-count-min-max | spark | 29.3 / 34.5 | 2.6 / 2.3 | 66.8 / 54.9 | 97.2 / 108.6 |
| grouped-count-min-max | comet | 30.2 / 51.3 | 4.7 / 4.8 | 37.4 / 28.0 | 72.4 / 88.2 |
| grouped-count-min-max | local | 32.0 / 79.4 | 3.8 / 4.2 | 17.6 / 18.7 | 54.7 / 100.6 |
| partitioned-join | spark | 67.8 / 68.6 | 4.8 / 4.7 | 73.3 / 88.3 | 154.4 / 163.5 |
| partitioned-join | comet | 86.3 / 111.2 | 8.0 / 7.5 | 36.7 / 39.5 | 128.7 / 163.1 |
| partitioned-join | local | 103.5 / 93.3 | 8.1 / 7.8 | 13.0 / 13.4 | 122.6 / 114.9 |
| top-k | spark | 32.5 / 30.8 | 2.6 / 2.5 | 26.1 / 24.9 | 60.2 / 66.2 |
| top-k | comet | 40.2 / 43.2 | 4.1 / 4.4 | 25.5 / 26.7 | 73.2 / 76.4 |
| top-k | local | 34.8 / 51.0 | 4.4 / 4.5 | 14.6 / 13.8 | 54.1 / 67.7 |
| full-sort | spark | 38.7 / 34.0 | 2.5 / 3.0 | 110.9 / 114.2 | 154.5 / 151.4 |
| full-sort | comet | 31.3 / 56.8 | 4.5 / 4.2 | 82.2 / 91.5 | 119.3 / 145.8 |
| full-sort | local | 32.9 / 62.4 | 3.7 / 4.2 | 73.6 / 68.4 | 112.5 / 134.0 |

### Explicit schema

| Query | Mode | DataFrame construction | Physical preparation | Execution + collect | Total |
|---|---|---:|---:|---:|---:|
| scan-filter-project | spark | 6.7 / 7.0 | 3.0 / 2.9 | 23.8 / 23.5 | 33.2 / 32.7 |
| scan-filter-project | comet | 7.1 / 6.9 | 5.4 / 5.5 | 17.3 / 17.2 | 32.6 / 28.9 |
| scan-filter-project | local | 7.2 / 7.4 | 6.1 / 5.6 | 18.3 / 17.8 | 31.5 / 31.8 |
| grouped-count-min-max | spark | 6.4 / 6.6 | 2.6 / 2.3 | 54.9 / 53.8 | 64.1 / 61.2 |
| grouped-count-min-max | comet | 6.2 / 5.9 | 5.0 / 4.8 | 31.2 / 35.2 | 43.1 / 49.4 |
| grouped-count-min-max | local | 6.3 / 6.3 | 4.5 / 4.3 | 19.2 / 18.7 | 29.3 / 29.3 |
| partitioned-join | spark | 10.2 / 8.7 | 4.7 / 4.5 | 93.6 / 85.6 | 110.4 / 98.6 |
| partitioned-join | comet | 11.1 / 11.3 | 7.9 / 7.7 | 37.9 / 48.5 | 59.5 / 66.6 |
| partitioned-join | local | 11.5 / 10.0 | 8.2 / 8.1 | 14.9 / 14.4 | 32.8 / 33.5 |
| top-k | spark | 6.9 / 7.1 | 2.5 / 2.5 | 24.4 / 26.2 | 34.2 / 41.6 |
| top-k | comet | 7.1 / 7.4 | 4.4 / 4.7 | 26.7 / 26.9 | 39.7 / 39.9 |
| top-k | local | 7.2 / 6.8 | 4.8 / 4.6 | 14.8 / 15.3 | 27.2 / 27.9 |
| full-sort | spark | 6.5 / 6.5 | 2.6 / 2.6 | 105.9 / 109.8 | 115.3 / 119.9 |
| full-sort | comet | 7.2 / 6.6 | 4.9 / 4.9 | 81.1 / 88.5 | 93.5 / 100.6 |
| full-sort | local | 7.2 / 7.0 | 4.1 / 4.3 | 70.2 / 69.9 | 82.6 / 81.5 |

With explicit schemas, local aggregate, join, Top-K and full-sort end-to-end
medians beat existing Comet in both passes. Scan/filter/project is mixed. Local
physical preparation is of the same order as existing Comet, while DataFrame
construction with inferred schemas dominates much of the old planning interval.
This evidence does not support attributing the previous planning gap to a costly
local admission implementation. Inference is still real work for applications
that do not supply a schema; the inferred results remain part of the report.

## Reproduce and verification

Use the stage 5b build/data preparation instructions, then run in fresh directories:

```shell
for schema in infer explicit; do
  for mode in spark comet local; do
    python3 dev/bench-local-execution.py "$mode" --schema-mode "$schema" \
      --data /tmp/comet-local-data --output "/tmp/comet-planning-$schema-forward" || exit 1
  done
done
for schema in explicit infer; do
  for mode in local comet spark; do
    python3 dev/bench-local-execution.py "$mode" --schema-mode "$schema" \
      --data /tmp/comet-local-data --output "/tmp/comet-planning-$schema-reverse" || exit 1
  done
done
# Diagnostic run: keep it separate from unprofiled comparisons.
python3 dev/bench-local-execution.py local --jfr --repetitions 20 \
  --data /tmp/comet-local-data --output /tmp/comet-planning-profile
"$JAVA_HOME/bin/jfr" print --stack-depth 256 \
  --events jdk.ExecutionSample,jdk.ThreadPark /tmp/comet-planning-profile/local.jfr
```

The schemas are specific to `prepare`'s synthetic data, not an override for arbitrary
user data. Keep schema modes in separate output directories and compare signatures
across directories as well as modes. Supplying a schema does not cache a native
graph: every action still constructs its own query-owned graph.

Spark 4.1 compilation, Spark 3.5 strict-warning main/test compilation, formatting,
Python syntax checks and the JFR launcher smoke run passed. Production sources and
native library are unchanged, so native lifecycle tests were not rerun. Spark SQL
suite remains deferred by the user; no PR, CI label or push was made.

Stop at this checkpoint. The next proposed stage is a larger-data, explicit-schema
scaling and memory-pressure checkpoint using the existing operators, to determine
whether native execution or resource limits warrant optimization before extending
TPC coverage. No new operator admission is implied by these results.

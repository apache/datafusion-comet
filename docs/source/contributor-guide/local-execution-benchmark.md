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

# Local execution performance checkpoint

This is a development checkpoint for the experimental single-process mode, not a
TPC benchmark or a general performance claim. Production code was unchanged in
this checkpoint; the native library was built from `63bd47c3a` in release mode.
The manual harness and raw results are committed alongside this report.

## Coverage before timing

Planning the repository TPC SQL over one-row Parquet fixtures admitted **zero**
complete queries: TPC-H has 22 fallbacks; TPC-DS has 102 fallbacks and one planning
error across 103 SELECT instances in 99 files. Four TPC-DS files contain two
SELECT statements. TPC-DS q30 references `c_last_review_date_sk`, while the Spark
schema helper used by the fixture declares `c_last_review_date`; this is recorded
as an unresolved planning error, not as fallback. CHAR/VARCHAR fixture fields use
physical StringType. TPCH q15 creates and drops its temporary view; other SELECTs
are planned without execution.

This is fixture-based admission evidence, not TPC correctness validation. No
TPC timings are reported. The current operator surface excludes, among other
things, SUM/AVG, nested joins, many expressions and subqueries. Broadening that
surface remains separate work.

## Method

Apple M4 Max (16 logical CPUs), 64 GiB RAM, macOS 26.6.2 arm64; Spark 4.1.3,
Scala 2.13.17, Zulu OpenJDK 17.0.16. The coverage-only process used JDK 21; all
six timed JVMs used JDK 17. The same optimized native library was used throughout
(SHA-256 recorded in metadata). No compilation ran concurrently with timing.

Each mode uses a fresh JVM with 512 MiB initial / 2 GiB maximum heap, local[4],
AQE disabled, eight shuffle partitions, broadcast disabled, UTC, batch size 8192,
and four Comet Tokio workers. Spark off-heap and local native reservation limits
are both configured to 512 MiB. Existing Comet uses fair_unified and native
shuffle. These settings have different accounting scopes and do not establish
equal total RSS limits. Local execution produces one Spark result partition.

A fixed 500,000-row fact dataset (eight files) and 5,000-row dimension (four files)
are reused in every process. Cases cover scan/filter/project, grouped
COUNT/MIN/MAX, a shuffled hash equijoin, Top-K, and full sort. There are two warmup
and five measured iterations per case, with rotated case order. The forward pass
runs Spark, Comet, local; the second pass reverses mode order. Filesystem caches
are not cleared. These small warm-cache workloads are preliminary evidence.

Planning includes DataFrame construction, file/schema discovery and executed-plan
creation. Execution includes collect and native graph creation; total is their
sum. Process CPU includes all JVM/native threads during this interval. Digest
calculation is outside timing, but its allocations and JIT/GC effects may affect
subsequent iterations. Sorted cases compare ordered rows; others compare a sorted
multiset of row strings. All 210 executions, including warmups across both passes,
agreed on row counts and SHA-256 digests. Every local query had exactly one
CometLocal node and zero live native handles after collect; every Comet baseline
had Comet operators. Saved physical plans confirm native shuffle in the join
baseline. This validates these concrete inputs, not arbitrary SQL semantics.

## Results

Each cell is the median of five samples, **forward / reverse**, in milliseconds.
Columns are independent medians, so planning plus execution medians need not sum
to the total median.

| Query | Mode | Planning | Execution + collect | Total | Process CPU |
|---|---|---:|---:|---:|---:|
| scan-filter-project | spark | 44.9 / 36.1 | 22.4 / 23.7 | 84.2 / 60.5 | 273.7 / 133.3 |
| scan-filter-project | comet | 43.1 / 45.4 | 15.8 / 16.1 | 58.9 / 64.1 | 170.0 / 147.8 |
| scan-filter-project | local | 49.1 / 50.8 | 15.2 / 15.5 | 65.2 / 66.3 | 184.4 / 165.3 |
| grouped-count-min-max | spark | 40.1 / 33.4 | 49.4 / 52.8 | 83.6 / 86.2 | 336.0 / 325.1 |
| grouped-count-min-max | comet | 30.5 / 36.5 | 30.5 / 31.5 | 63.6 / 67.4 | 221.4 / 224.8 |
| grouped-count-min-max | local | 53.6 / 63.6 | 16.4 / 17.1 | 71.5 / 81.0 | 147.8 / 127.5 |
| partitioned-join | spark | 64.3 / 69.0 | 78.8 / 76.2 | 151.0 / 171.6 | 560.2 / 510.0 |
| partitioned-join | comet | 68.4 / 108.4 | 37.1 / 38.0 | 105.5 / 146.9 | 390.7 / 376.3 |
| partitioned-join | local | 103.3 / 128.6 | 12.6 / 12.4 | 115.9 / 140.4 | 268.8 / 289.5 |
| top-k | spark | 30.8 / 33.5 | 21.8 / 26.0 | 53.9 / 70.5 | 151.1 / 201.8 |
| top-k | comet | 32.8 / 37.9 | 22.7 / 33.6 | 56.0 / 75.5 | 194.8 / 295.9 |
| top-k | local | 39.9 / 43.9 | 12.4 / 13.3 | 52.8 / 56.6 | 146.5 / 142.1 |
| full-sort | spark | 33.6 / 37.7 | 106.7 / 106.6 | 134.5 / 149.3 | 514.3 / 477.8 |
| full-sort | comet | 34.3 / 45.5 | 82.8 / 82.8 | 118.8 / 128.3 | 463.9 / 492.1 |
| full-sort | local | 58.7 / 39.5 | 67.3 / 65.0 | 126.0 / 111.4 | 372.3 / 387.1 |

The local execution median was lower than existing Comet in all five cases in
both passes, but planning was higher in nine of ten case/pass comparisons. End-to-end local totals did
not consistently beat existing Comet. Top-K improved in both passes; scan and
aggregate were slower in both; join and full sort changed relative order between
passes. These results justify profiling planning and admission before expanding
scope or claiming an overall speedup. They do not identify the exact planning
bottleneck: repeated file discovery, Comet conversion and local admission still
need separate profiling.

## Resource samples

The launcher samples the whole Java process and temporary directory occupancy
at approximately 100 ms intervals. Values below are **forward / reverse**, MiB.
Peak RSS includes startup, warmup, collection and digest validation. Retained RSS
is the median during the one-second idle period after all cases, without forced
GC; it is not proof of a leak or the amount of live native memory.

| Mode | Peak process RSS | Idle RSS | Peak native temp | Peak Spark temp |
|---|---:|---:|---:|---:|
| spark | 1546.4 / 1783.4 | 1315.8 / 1672.3 | 0.0 / 0.0 | 11.4 / 11.1 |
| comet | 1754.4 / 1596.1 | 1754.4 / 1596.0 | 0.0 / 0.0 | 8.0 / 10.1 |
| local | 1598.1 / 1667.4 | 1269.1 / 1667.4 | 0.0 / 0.0 | 0.0 / 0.0 |

No native spill files were observed under the configured native temp directory.
Spark-directory occupancy includes shuffle files and possibly native shuffle
spill. Occupancy is not cumulative spill bytes; short-lived files can be missed.
The benchmark does not expose DataFusion operator peak reservation or cumulative
spill metrics and does not exercise memory pressure. Stage 5a's deterministic
spill/budget tests provide separate correctness coverage. RSS does not show a
consistent local-mode advantage over both baselines.

## Reproduce

Run from the repository root. Follow the development guide for toolchains and
build native before the JVM suite. Do not use Maven `-pl`. For example:

```shell
export JAVA_HOME=$(/usr/libexec/java_home -v 17) # macOS; select JDK 17 on other OSes
(cd native && cargo build --release -p datafusion-comet --locked)
./mvnw test -Pspark-4.1 -Dsuites=org.apache.comet.local.CometLocalExecutionSuite
./mvnw test-compile -Pspark-4.1 -DskipTests
python3 dev/bench-local-execution.py prepare --data /tmp/comet-local-data --output /tmp/comet-local-prepare
python3 dev/bench-local-execution.py coverage --data /tmp/comet-local-coverage-data --output /tmp/comet-local-coverage
for mode in spark comet local; do
  python3 dev/bench-local-execution.py "$mode" --data /tmp/comet-local-data --output /tmp/comet-local-forward || exit 1
done
for mode in local comet spark; do
  python3 dev/bench-local-execution.py "$mode" --data /tmp/comet-local-data --output /tmp/comet-local-reverse || exit 1
done
```

The launcher obtains the JVM classpath from the Spark 4.1 local suite's XML report;
compile the benchmark test object before running it. It explicitly loads the
release library for timing. Use fresh output directories and fixture data paths;
input creation never overwrites existing data. Default row count is 500,000 and
repetitions five. Timing mode rejects debug libraries. This is a manual object,
not a CI timing suite. Mode logs, plans, CSV, metadata and samples are written to
the output directory. Compare both passes' digests before interpreting timings.

Raw results are under `benchmarks/results/local-execution/2026-09-30/`, including
per-iteration timing, process samples, path-verification plans and the coverage
CSV/error. Stored CSV and JSON files have a `.txt` suffix, matching the repository
convention for raw benchmark output and its existing license-check exclusion.
Remove that suffix when loading them with tools that infer format from extensions.
An earlier interrupted run was discarded: the sampler raced Spark's
deleted temporary directories. Directory walking now tolerates those removals;
the interruption was not an engine failure.

Spark 4.1 main/test compilation, Spark 3.5 strict-warning main/test compilation,
Python syntax validation and formatting checks passed. Spark SQL suite validation
remains explicitly deferred by the user. No CI label,
PR update or push was performed. Stop at this checkpoint; the proposed next stage
is to profile planning/admission overhead and verify a narrowly scoped remedy
with this same harness before adding more operators.

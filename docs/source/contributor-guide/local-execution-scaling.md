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

# Larger-data and memory-pressure checkpoint

Stage 5d found a reproducible full-sort memory limit. At 512 MiB, all three modes
complete the five cases on five million fact rows. At 64 MiB, both local mode and
existing Comet fail with an ExternalSorterMerge reservation error; Spark completes.
Local also fails at 128 MiB. This checkpoint records the limitation and recovery
behavior; it does not change production execution or claim a spill fix.

## Setup and checks

Same M4 Max host, JDK 17, Spark 4.1.3, optimized native library and explicit schemas
as stage 5c. Fact rows increase from 500,000 to **5,000,000**, dimension rows from
5,000 to **50,000**; the generated Parquet data occupies about 68 MiB. Generation
remains eight fact files and four dimension files. Group cardinality stays 4,096,
the join has one matching dimension row per matching fact row, and Top-K returns
1,000 rows. This does not cover skew, high-cardinality aggregation or a large join
build side. Full sort collects five million rows into the driver, so JVM output
materialization is a substantial part of time and RSS.

All runs use local[4], four native workers, eight shuffle partitions, batch size
8,192, AQE/broadcast off and a 2 GiB heap. The new `--memory-mib` option changes
Spark off-heap and local per-query reservation settings together. Their accounting
scopes differ; neither is a total process/RSS cap. Native spilling remains enabled.
At 512 MiB, mode order is Spark/Comet/local then local/Comet/Spark. Each process
warms each case twice and measures three times, rotating case order. Pressure runs
are separate, not pooled into the healthy comparison. No compilation overlaps
with measured runs. These are small-sample warm-cache observations, not general
performance or linear scaling claims.

The **150** complete 512 MiB case executions agreed on row counts and digests and
passed path assertions. Spark's 25 executions at 64 MiB also matched, as did all
completed rows from the interrupted Comet/local pressure runs (199 completed CSV
rows altogether). Sorted cases validate order; other cases validate a multiset.
Failed runs are recorded explicitly and excluded from timing summaries.

## 512 MiB results

Total time includes DataFrame construction, physical preparation and collect;
digest validation is excluded. Values are three-sample medians in milliseconds,
**forward / reverse**.

| Query | Spark | Existing Comet | Local |
|---|---:|---:|---:|
| scan-filter-project | 68.8 / 69.9 | 62.3 / 60.6 | 64.3 / 66.7 |
| grouped-count-min-max | 134.4 / 126.5 | 73.4 / 73.2 | 82.9 / 86.2 |
| partitioned-join | 267.5 / 237.6 | 105.3 / 106.1 | 60.0 / 61.4 |
| top-k | 66.4 / 70.7 | 52.3 / 49.2 | 47.6 / 47.6 |
| full-sort | 784.4 / 774.0 | 622.1 / 610.5 | 723.8 / 717.3 |

Local join and Top-K totals are lower than existing Comet in both passes. Local
scan, grouped aggregate and full sort are slower in both. The larger dataset does
not preserve every advantage seen at 500,000 rows. Those earlier results used
five measured repetitions and were collected in separate processes/at a different
time; they are context, not a controlled linear-scaling estimate.

Whole-process RSS includes startup, warmup, collect and digest allocations. Values
are MiB, **forward / reverse**. Idle RSS is the median during one second after all
cases without forced GC, not a live-native-memory or leak measurement. Temporary
occupancy is sampled about every 100 ms; it is not cumulative spill bytes.

| Mode | Peak RSS | Idle RSS | Peak native temp | Peak Spark temp |
|---|---:|---:|---:|---:|
| spark | 2684.9 / 2893.1 | 1579.6 / 1573.1 | 0.0 / 0.0 | 43.4 / 43.4 |
| comet | 2683.0 / 2598.5 | 1672.2 / 2581.2 | 0.0 / 0.0 | 58.7 / 39.2 |
| local | 2961.0 / 2809.4 | 2938.7 / 2809.1 | 0.0 / 0.0 | 0.0 / 0.0 |

## Pressure failure and recovery

| Budget | Mode | Observed outcome |
|---|---|---|
| 64 MiB | Spark | All 25 executions complete and match the baseline |
| 64 MiB | Comet | Full-sort fails during measured iteration 2; 22 earlier executions completed |
| 64 MiB | Local | First warmup full-sort fails after Top-K succeeds |
| 128 MiB | Local | First warmup full-sort fails after Top-K succeeds |

Local 64 MiB reports an additional 208.1 KiB request with only 328 bytes available.
At 128 MiB it reports an additional 128 KiB request, 48.6 MiB already held by the
merge reservation and 69.8 KiB available. Comet's 64 MiB error also identifies
ExternalSorterMerge as a non-spillable consumer. These are reservation failures,
not JVM OutOfMemoryError. They are not evidence that more disk space would help.

No native temporary bytes were observed by the sampler, and those directories
were empty after each process exited. Short-lived files may be missed. In
particular, neither the exception name nor `spill=true` proves that spill files
were successfully produced or that out-of-core sort succeeded. Spark temporary
files include shuffle and possibly spill; their occupancy cannot identify native
operator spill volume.

The new manual `pressure` mode reproduces the local 64 MiB failure three times
in one JVM. It requires the specific native ExternalSorterMerge allocation error,
checks zero active native handles and zero imported Arrow bytes after each failed
collect, then executes a new local Top-K and compares all 1,000 ordered rows with
Spark. All three cycles pass, including the post-recovery handle/Arrow checks.
This demonstrates recovery and those ownership checks, not complete native RSS
reclamation. The pressure mode intentionally fails if sort unexpectedly succeeds
or returns any unrelated error; it is not a performance benchmark or a general
exception-tolerant runner.

Inspection of the pinned DataFusion 55.1.0 sort implementation shows that the
ExternalSorterMerge reservation participates in both in-memory and spilled merge
paths. Its in-memory run-coalescing branch currently requires one sort expression;
this fixture sorts by rank and id. That is a candidate for focused investigation,
not a proven root cause or an implemented remedy.

## Reproduce

Follow stage 5b's native/JVM build instructions, including generating the Spark
4.1 suite classpath. The launcher defaults to 512 MiB and rejects budgets below
16 MiB. Use new directories:

```shell
python3 dev/bench-local-execution.py prepare --rows 5000000 \
  --data /tmp/comet-large-data --output /tmp/comet-large-prepare
for mode in spark comet local; do
  python3 dev/bench-local-execution.py "$mode" --rows 5000000 --repetitions 3 \
    --schema-mode explicit --memory-mib 512 \
    --data /tmp/comet-large-data --output /tmp/comet-large-forward || exit 1
done
# Repeat with modes local/comet/spark into a new reverse-order output directory.
# Each pressure timing run is separate; Comet/local currently exit nonzero.
python3 dev/bench-local-execution.py local --rows 5000000 --repetitions 3 \
  --schema-mode explicit --memory-mib 64 \
  --data /tmp/comet-large-data --output /tmp/comet-large-pressure
# Validate expected failure and subsequent recovery in the same JVM:
python3 dev/bench-local-execution.py pressure --rows 5000000 \
  --schema-mode explicit --memory-mib 64 \
  --data /tmp/comet-large-data --output /tmp/comet-large-recovery
```

Raw timing, process samples, plans, metadata, failure excerpts and recovery output
are stored in `benchmarks/results/local-execution/2026-09-30-scaling/`. CSV and JSON
carry a `.txt` suffix under the existing raw-result convention. Pressure directories
contain partial CSVs and must not be treated as completed benchmark runs.

Spark 4.1 compilation, Spark 3.5 strict-warning main/test compilation, formatting
and Python syntax validation passed. No production/native code changed. Spark SQL
suite remains deferred; no PR, CI label or push was made.

Stop here. Next checkpoint: isolate the two-key sort merge reservation failure in
a native regression, distinguish in-memory merge from spill merge, and validate
a bounded fix before continuing performance or operator expansion.

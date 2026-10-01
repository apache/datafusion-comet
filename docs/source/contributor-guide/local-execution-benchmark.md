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

# Local Execution Benchmark

`dev/bench-local-execution.py` runs a manual benchmark that compares Spark local
mode, existing Comet with native shuffle, and [local execution](local-execution.md)
in separate JVMs. It is not a TPC benchmark or a CI timing suite, and the results
below are small-sample, warm-cache observations on one machine, not a general
performance claim.

## Workload

`prepare` writes a synthetic fact table (`id`, `k = id % 4096`, a decimal amount and
a nullable string; eight Parquet files) and a dimension table whose keys match a
contiguous one percent of the fact ids (four files). The timed cases are:

| Case                  | Query                                                |
| --------------------- | ---------------------------------------------------- |
| scan-filter-project   | `id % 100 = 0` filter and three projected columns    |
| grouped-count-min-max | `COUNT`, `MIN`, `MAX` grouped by `k` (4,096 groups)  |
| partitioned-join      | fact joined to dimension with a `SHUFFLE_HASH` hint  |
| top-k                 | order by a computed rank and `id`, limit 1,000       |
| full-sort             | the same order without a limit, collecting every row |

Each mode runs in a fresh JVM with a 2 GiB heap, `local[4]`, AQE and broadcast joins
disabled, eight shuffle partitions, UTC, batch size 8,192 and four Comet Tokio
workers. `--memory-mib` sets both Spark's off-heap size (Comet uses `fair_unified`)
and `spark.comet.exec.local.memoryLimit`. These have different accounting scopes,
and neither limits process RSS. Each case is warmed up twice, then measured with the
case order rotated between iterations.

Timing starts before DataFrame construction and ends when `collect` returns, so it
includes planning, native graph creation, execution, row conversion and driver
result collection. Result digests are computed outside the timed interval. Sorted
cases compare rows in order; the others compare a sorted multiset. Every execution,
including warmups, must match across modes, and the plan must contain a local node
(local mode) or Comet operators (Comet mode), so fallback cannot be timed as local
execution. Process CPU covers all JVM and native threads during the interval.

`--schema-mode explicit` supplies the fixture schemas; the default `infer` adds
Parquet schema inference, which costs every mode alike and can dominate small
queries.

## Results

Apple M4 Max (16 logical CPUs), 64 GiB RAM, macOS 26.6.2, Spark 4.1.3, Zulu OpenJDK
17.0.16, release native library. Five million fact rows (67 MiB of Parquet), 50,000
dimension rows, explicit schemas and a 512 MiB budget. Values are medians of five
measured iterations in milliseconds, **forward / reverse** mode order. All 210
executions agreed on row counts and digests.

| Case                  |         Spark |         Comet |         Local |
| --------------------- | ------------: | ------------: | ------------: |
| scan-filter-project   |   76.2 / 83.2 |   65.5 / 70.7 |   68.1 / 65.9 |
| grouped-count-min-max | 129.4 / 140.8 |   79.4 / 83.1 |   66.2 / 63.6 |
| partitioned-join      | 264.4 / 287.1 | 110.3 / 115.3 |   56.5 / 57.7 |
| top-k                 |   84.3 / 81.2 |   67.1 / 56.6 |   38.0 / 39.3 |
| full-sort             | 869.4 / 978.9 | 710.2 / 754.6 | 366.3 / 359.1 |

Median process CPU time in milliseconds:

| Case                  |       Spark |       Comet |       Local |
| --------------------- | ----------: | ----------: | ----------: |
| scan-filter-project   |   314 / 390 |   238 / 258 |   287 / 248 |
| grouped-count-min-max |   578 / 671 |   301 / 361 |   336 / 239 |
| partitioned-join      | 1146 / 1298 |   470 / 523 |   244 / 234 |
| top-k                 |   332 / 390 |   345 / 252 |   145 / 158 |
| full-sort             | 4000 / 4445 | 3549 / 3598 | 1667 / 1815 |

Planning (DataFrame construction plus physical preparation) takes about 9 to 20 ms
in every mode and case, and local physical preparation is within a millisecond of
existing Comet's.

Local execution is faster than Comet for aggregation, join, Top-K and full sort. For
the join and aggregation it replaces Comet's shuffle files with in-memory DataFusion
exchanges. For full sort, a separate measurement (not part of the raw results) found that pulling the sorted rows
out of the native graph takes a fraction of the total, and most of the remaining
time is Spark's result path; local mode avoids the single-threaded encode/decode
part of it through [same-JVM result delivery](local-execution.md#result-delivery),
while driver deserialization of five million rows into `Row` objects is common to
every mode. The scan case has no exchange to remove and uses the same Parquet
reader, so the modes are within noise.

Peak process RSS was about 2.6 to 2.7 GiB for every mode. Peak RSS includes startup,
warmup, collection and digest validation, so it does not distinguish the modes.
No native spill files were observed at 512 MiB.

## Memory pressure

`pressure` mode repeats three cycles in one JVM: full sort under the given budget
must spill, observed by polling the native temporary directory, and match Spark row
by row; with `spark.comet.exec.local.spill.enabled=false` the same sort must fail on
a native resource error; then a local Top-K must match Spark. After each step,
native query handles and imported Arrow memory must return to zero.

At 64 MiB and 128 MiB all three cycles passed. Each full sort returned five million
rows with a peak of about 78 MiB of spill files. With spill disabled, the 64 MiB
sort failed with an `ExternalSorterMerge` reservation error and the 128 MiB sort
with `Memory Exhausted while Sorting (DiskManager is disabled)`.

## Admission coverage

`coverage` plans the repository's TPC-H and TPC-DS queries over one-row Parquet
fixtures without executing them. No complete query is admitted: TPC-H has 22
fallbacks, TPC-DS has 102 fallbacks and one planning error, because TPC-DS q30
references `c_last_review_date_sk` while the fixture schema declares
`c_last_review_date`. Missing aggregate functions, nested joins, subqueries and
many expressions keep these queries outside admission.

## Reproduce

Run from the repository root. Build native code first and never use Maven `-pl`:

```shell
export JAVA_HOME=$(/usr/libexec/java_home -v 17) # macOS; select JDK 17 elsewhere
(cd native && cargo build --release -p datafusion-comet --locked)
./mvnw test -Pspark-4.1 -Dsuites=org.apache.comet.local.CometLocalExecutionSuite
./mvnw test-compile -Pspark-4.1 -DskipTests

python3 dev/bench-local-execution.py prepare --rows 5000000 \
  --data /tmp/comet-local-data --output /tmp/comet-local-prepare
python3 dev/bench-local-execution.py coverage \
  --data /tmp/comet-local-coverage-data --output /tmp/comet-local-coverage
for mode in spark comet local; do
  python3 dev/bench-local-execution.py "$mode" --rows 5000000 --repetitions 5 \
    --schema-mode explicit --memory-mib 512 \
    --data /tmp/comet-local-data --output /tmp/comet-local-forward || exit 1
done
for mode in local comet spark; do
  python3 dev/bench-local-execution.py "$mode" --rows 5000000 --repetitions 5 \
    --schema-mode explicit --memory-mib 512 \
    --data /tmp/comet-local-data --output /tmp/comet-local-reverse || exit 1
done
for budget in 64 128; do
  python3 dev/bench-local-execution.py pressure --rows 5000000 \
    --schema-mode explicit --memory-mib "$budget" \
    --data /tmp/comet-local-data --output "/tmp/comet-local-pressure-$budget" || exit 1
done
```

The launcher reads the JVM classpath from the Spark 4.1 local suite's report, so run
that suite and compile the benchmark first; switch back to the Spark 4.1 profile if
another profile was built last. Timing modes require the release library. Input
creation never overwrites data, and each mode needs a fresh output directory (or one
where that mode has not run). Each output directory receives the mode's log, CSV,
process samples, metadata and the executed plan of each case. The launcher compares
digests across all modes present in the output directory. `--jfr` records a
diagnostic profile; do not compare its timings.

Raw results for the tables above are under `benchmarks/results/local-execution/`.
CSV and JSON files carry a `.txt` suffix, following the repository's convention for
raw benchmark output; remove it when loading them with tools that infer the format
from the extension. Local paths in the metadata are replaced by `<output-root>`.

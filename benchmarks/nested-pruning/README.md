<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Nested struct statistics pruning

Comet needs native expression plumbing to use
[peterxcli/datafusion#2](https://github.com/peterxcli/datafusion/pull/2).
Spark already serializes the nested predicates, but Comet's custom `GetStructField`
physical expression does not expose DataFusion's scalar-UDF field-access capability.
The adapter declares the selected field while retaining Comet's parent-null handling.
It resolves the field by name after schema adaptation and retains ordinal access when
field names are duplicated.

The dependency is pinned to `0b0506a9acab9d5892ecf7e89243c3b34664bcc6`, which includes
the field-access capability, nested input requirements, and row-group pruning.
The baseline is unchanged Comet `bb9e74020adc228e486f6f4d0fa68292b30bff31`, with its
checked-in DataFusion 55.0.0 dependencies. The comparison therefore includes the
DataFusion fork update and the Comet adapter. A pruning-disabled candidate control
isolates the effect of statistics pruning.

This change does not resolve [#5739](https://github.com/apache/datafusion-comet/issues/5739).
Timestamp conversion and Spark's other filtering paths still need separate parity work.
The optimization applies to supported struct-leaf predicates with useful statistics;
it does not imply equivalent improvements for arrays, maps, Variant, or unfiltered scans.

## Results (2026-09-08)

On `chia-ping-aws1` (AMD EPYC 7282, 16 vCPUs, 31 GiB RAM), using Rust 1.98.0,
OpenJDK 17.0.20, Spark 4.1.3, NumPy 2.5.3, PyArrow 25.0.1, and the settings below:

| Query                         | Baseline median | Candidate median | Baseline / candidate |
| ----------------------------- | --------------: | ---------------: | -------------------: |
| Nested filter, sorted keys    |        642.3 ms |         182.6 ms |                3.52x |
| Top-level filter, sorted keys |        138.8 ms |         138.3 ms |                1.00x |
| Projection only               |        530.2 ms |         537.4 ms |                0.99x |
| Nested filter, shuffled keys  |        585.4 ms |         577.1 ms |                1.01x |

The selective nested query skips **63 of 64 row groups**. Reader bytes decrease from
**182,481,422 to 2,851,301 (98.4% less)**, and scan output decreases from 4,194,304 to
65,536 rows. The final aggregate checksums are identical. Disabling pruning in the
candidate restores 0 pruned groups, 182,481,422 reader bytes, and a **638.0 ms** median
(five control measurements), close to the baseline. This supports attributing the
selective-query improvement to statistics pruning.

The three other comparisons differ by less than 2%, with overlapping interquartile
ranges. They show no consistent speedup. The selective query has interquartile ranges
of 629.3–647.1 ms for baseline and 168.8–199.3 ms for candidate.

All measured queries used `CometNativeScan`; the plans, metrics, checksums, individual
timings, and summary statistics are retained in [results.json](results.json).
Additional candidate controls with decoder filtering enabled passed checksum and
pruning-counter checks. These controls are not a matched comparison of decoder-filter
performance. Pilot timings were excluded from the reported medians.

Validation passed: four Rust expression tests; five Spark tests covering the new
pruning regression, nested schema evolution, field-ID mapping, and nullable struct
access (one pre-existing Spark test remains ignored); native release builds; Maven
packaging; Rust formatting; and the Spark style/format checks. The Spark regression
exercises the real native scan with statistics pruning both enabled and disabled.

Remote checkouts are `~/oss/comet-nested-pruning` and `~/oss/comet-nested-baseline`.
Data is in `~/oss/comet-nested-data`; libraries, raw logs, samples, the implementation
patch, and [binary/data hashes](hashes.sha256) are retained in
`~/oss/comet-nested-results`. The Python environment is `~/oss/comet-nested-venv`.
The host-side `~/oss/comet-nested-run-rounds.py` records the alternating run order.

## Reproduce

Install `numpy==2.5.3`, `pyarrow==25.0.1`, and `pyspark==4.1.3` in a dedicated Python environment.
Generate the files once and reuse them for every revision:

```sh
python benchmark.py --generate --data /path/to/data
```

Each file contains 4,194,304 rows and 64 row groups of 65,536 rows. The schema has
`k: int64` and `s: struct<inner: struct<k: int64>, p0: int64, ..., p7: int64>`.
The payload values are seeded random integers. One file has sorted keys; the other
has shuffled keys. Both use Snappy compression, no dictionary encoding, and row-group
statistics. The selective predicate retains 65,536 rows (1.5625%). Every query sums
all eight payload fields and checks the answer against sums computed during generation.

Build both native revisions with the same settings, from their respective `native`
directories, and retain each resulting `libcomet.so` in a separate directory:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_RELEASE_LTO=false \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release --no-default-features --locked
```

Build the Spark jar from the Comet root after the native build:

```sh
./mvnw package -DskipTests -Djni.dir=/path/to/candidate-library-directory
```

Use the same jar, data, JVM, and CPU affinity for both native libraries:

```sh
SPARK_LOCAL_IP=127.0.0.1 COMET_WORKER_THREADS=8 taskset -c 0-7 \
  python benchmark.py --data /path/to/data --jar /path/to/comet.jar \
  --library-dir /path/to/candidate-library-directory --label candidate-0 \
  --runs 5 --warmup 5 --expect-nested-pruned 63 --output candidate-0.json
```

Use `--expect-nested-pruned 0` for the baseline or the candidate with `--pruning-off`.
The harness explicitly selects Comet's default of disabled decoder row filtering.
`--row-filter` exercises decoder filtering as an additional control. Page-index and
Bloom-filter pruning are disabled. Spark uses `local[1]`, one shuffle partition,
and disabled AQE. Timings include SQL planning, execution, and collecting the aggregate
result; they exclude Spark startup, schema inference, and data generation.

The measured comparison uses four rounds, alternating baseline/candidate order,
with five warmups followed by five measured executions per query in each process.
Report medians over the 20 measurements per revision, alongside the scan metrics.
`bytes_scanned` excludes metadata reads, as documented by Comet's reader factory.
These are warm-cache measurements on synthetic local files, using matching release
builds with LTO disabled; they do not establish production or object-storage speedups.

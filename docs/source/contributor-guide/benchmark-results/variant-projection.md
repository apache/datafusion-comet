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

# Whole Variant projection

Local measurements of the Spark byte compatibility changes in
[PR #5868](https://github.com/apache/datafusion-comet/pull/5868), September 16, 2026.
These fixtures establish no performance benefit: canonical reads are close, while Comet takes
about 1.3–1.6 times as long for ordinary shredding and about twice as long for empty keys.

Environment: Apple M4, 24 GiB RAM, macOS 26.6.2, JDK 21.0.6, Spark 4.0.4, Rust 1.96.0,
DataFusion 55.0.0, Arrow/Parquet 59.3.0. Native code used the optimized `ci` profile with jemalloc
(no LTO, debug assertions enabled). The JVM heap was 4 GiB. Results are specific to this local,
warm filesystem workload; CPU placement and thermal state were not controlled.

## Matched scans

`CometVariantReadBenchmark` writes one Parquet file per fixture with 100,000 repeated objects,
a 1,024-byte string payload, and Parquet dictionary encoding enabled. Both readers use the same
file and hash every returned Variant's value and metadata bytes through a Dataset action.
Planning, scanning, row conversion, and consumption are included; file creation is excluded.
The benchmark checks byte equality and native scan engagement before timing. Ordinary shredded
fixtures include all schema keys in metadata; the empty-key fixture exercises metadata repair.

Two runs reverse the reader order. Each case has 7–17 measured iterations after warmup.
Cells show average ± standard deviation in milliseconds, with Spark-first / Comet-first runs.

| Fixture | Spark (ms) | Comet (ms) |
| --- | --- | --- |
| Canonical | 132 ± 8 / 128 ± 4 | 125 ± 4 / 136 ± 5 |
| Partially shredded | 153 ± 4 / 152 ± 1 | 242 ± 2 / 242 ± 5 |
| Fully shredded | 161 ± 4 / 147 ± 1 | 207 ± 3 / 209 ± 4 |
| Empty key | 148 ± 3 / 150 ± 2 | 310 ± 2 / 311 ± 3 |

After building and installing the `ci` library and Spark 4.0 artifacts:

```shell
SPARK_LOCAL_IP=127.0.0.1 make -o release \
  benchmark-org.apache.spark.sql.benchmark.CometVariantReadBenchmark \
  PROFILES=-Pspark-4.0 BENCH_HEAP=4g -- 100000 1024
# Repeat with --reverse-cases appended.
```

## Native normalization allocations

The ignored `benchmark_variant_buffer_reuse` test isolates normalization with 4,096 rows,
4,096-byte payloads and repeated metadata dictionaries. It constructs Arrow inputs before
timing, warms up three batches, then normalizes 30 batches. Jemalloc's thread counter measures
cumulative allocated bytes, including temporary and output buffers. This measures allocation
traffic, not retained memory or allocation counts, and excludes Parquet decoding and JVM work.
These larger Arrow fixtures are separate from the scan fixtures above.

| Fixture | Allocated bytes per row | Mean ms per batch |
| --- | --- | --- |
| Canonical | 10,332 | 3.762 |
| Partially shredded | 57,402 | 15.925 |
| Fully shredded | 52,320 | 11.796 |
| Empty key | 83,399 | 22.268 |

```shell
cd native
cargo test -p datafusion-comet --profile ci --features jemalloc \
  benchmark_variant_buffer_reuse --lib -- --ignored --nocapture
```

Shredded reconstruction currently pays for Arrow unshredding plus Spark byte reconstruction.
These measurements leave reducing that allocation traffic as follow-up work.

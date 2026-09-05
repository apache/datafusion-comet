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

# Shuffle IPC context reuse (#5446)

The existing local shuffle encoder reuses Arrow's `IpcWriteContext`, including its
FlatBufferBuilder. This change adds measurement and regression coverage; it does
not change production context lifetimes or compression settings.

## Method

- Base commit: `75fdddc92` (DataFusion upgrade #5262).
- Locked dependencies: DataFusion 55.0.0, Arrow/Parquet 59.3.0. The manifest's
  `59.2.0` requirement permits this locked Arrow version.
- Host: Apple M4, macOS arm64; rustc 1.96.0; Cargo release profile.
- Run date: 2026-09-06 (Asia/Taipei).
- Compare a fresh context on every block with one warmed context retained across
  blocks. Both arms retain the same writer and destination buffer capacity.
- Allocation regression: 100 blocks per arm, three repetitions, 128 rows,
  4/50 primitive columns, None/LZ4/Snappy/Zstd(1). The existing test allocator
  counts successful Rust allocation/reallocation requests and requested bytes.
  These are allocation traffic, not peak or retained memory; Zstd C allocations
  are excluded equally in both arms. Byte equality and IPC round-trips are checked
  outside allocation measurement.
- Criterion: mixed four-column, flat 50-column, and nested schemas at 128/8192
  rows, with the same four codecs. No allocation instrumentation runs in this
  benchmark executable.

The measured entry point is production `ShuffleBlockWriter::write_batch`, shared
by `BufBatchWriter` and JVM sorted-row partition serialization. This isolates
encoding, excluding row conversion, partitioning, filesystem I/O, and Spark.
The JVM row path also reconstructs its block writer per batch, which this
benchmark excludes. First-use context allocation is excluded from the warmed
reuse arm; short-lived writers with only one block cannot amortize that cost.

RSS and dictionary schemas use a per-block `StreamWriter`, which owns a separate
context. The allocation regression includes both as negative controls: retaining
the caller's context cannot save allocations in those paths. Outer compression
encoders are still created per block. Arrow IPC buffer compression is disabled,
so the observed delta isolates metadata-builder reuse, not compressor reuse.

## Reproduce

Run from `native/`:

```sh
cargo test --locked --release -p datafusion-comet-shuffle --lib ipc_context_reuse_allocations -- --nocapture
cargo test --locked --release -p datafusion-comet-shuffle --lib
for run in 1 2 3; do
  cargo bench --locked -p datafusion-comet-shuffle --bench shuffle_writer -- \
    shuffle_ipc_context --sample-size 10 --warm-up-time 0.5 \
    --measurement-time 1 --noplot --save-baseline "run${run}"
done
```

The loop runs three passes on the same host. On macOS, ensure the
JDK's `lib/server` directory is available to the dynamic linker if required.

## Validation

- Focused allocation regression: passed; every repetition returned identical
  counts and both context lifetimes produced byte-identical, readable blocks.
- Full native shuffle library suite (release): **119 passed, 0 failed**.
- Full native/JVM build: `JAVA_HOME=/path/to/jdk17 make FEATURES_ARG=--release PROFILES=-Prelease` passed (Spark 4.1, Scala 2.13). JVM tests are skipped by this package build; the shuffle test count above is native.
- `cargo fmt --all --check` and `git diff --check`: passed.

The local fast path saves 9 allocation/reallocation calls and 1,064 requested
bytes per block for four primitive columns, and 12 calls and 8,232 bytes for
50 columns. The absolute savings are identical under all four codecs. RSS and
dictionary fallback paths save zero allocations with their current lifetimes.

## Allocation results

All three repetitions produced identical counts. Calls include reallocations; bytes are requested bytes per block.

| Path       | Columns | Codec    | Calls fresh → reused | Bytes fresh → reused |
| ---------- | ------: | -------- | -------------------: | -------------------: |
| local      |       4 | None     |              40 → 31 |          6640 → 5576 |
| local      |       4 | Lz4Frame |              43 → 34 |      160669 → 159605 |
| local      |       4 | Snappy   |              43 → 34 |      181434 → 180370 |
| local      |       4 | Zstd(1)  |              41 → 32 |        39408 → 38344 |
| rss        |       4 | None     |              47 → 47 |          4128 → 4128 |
| rss        |       4 | Lz4Frame |              50 → 50 |      158157 → 158157 |
| rss        |       4 | Snappy   |              50 → 50 |      178922 → 178922 |
| rss        |       4 | Zstd(1)  |              48 → 48 |        36896 → 36896 |
| dictionary |       1 | None     |              59 → 59 |          4436 → 4436 |
| dictionary |       1 | Lz4Frame |              62 → 62 |      158465 → 158465 |
| dictionary |       1 | Snappy   |              62 → 62 |      179230 → 179230 |
| dictionary |       1 | Zstd(1)  |              60 → 60 |        37204 → 37204 |
| local      |      50 | None     |            289 → 277 |       102160 → 93928 |
| local      |      50 | Lz4Frame |            292 → 280 |      256189 → 247957 |
| local      |      50 | Snappy   |            292 → 280 |      276954 → 268722 |
| local      |      50 | Zstd(1)  |            290 → 278 |      134928 → 126696 |
| rss        |      50 | None     |            295 → 295 |        43032 → 43032 |
| rss        |      50 | Lz4Frame |            298 → 298 |      197061 → 197061 |
| rss        |      50 | Snappy   |            298 → 298 |      217826 → 217826 |
| rss        |      50 | Zstd(1)  |            296 → 296 |        75800 → 75800 |

## Timing results (exploratory; not accepted speedup evidence)

The host had substantial concurrent Java/Rust activity: load averages were
75.22 / 77.04 / 53.99 at benchmark start. All 48 cases completed in each of
three passes, but the large variation and reversals below make elapsed-time
comparisons unreliable. These runs demonstrate why allocation savings should
not be translated into a claimed runtime speedup. These earlier runs are superseded by the scheduled quieter-host rerun below;
no end-to-end Spark speedup was measured.

Criterion retains each pass under `native/shuffle/target/criterion/` in the
`run1`, `run2`, and `run3` baseline directories.

Microseconds per block: median of the three run medians. The range is the per-run reduction in median time (negative means slower).

| Schema | Rows | Codec    | Fresh µs | Reused µs | Median reduction |    Per-run range |
| ------ | ---: | -------- | -------: | --------: | ---------------: | ---------------: |
| flat   |  128 | Lz4Frame |   18.189 |    17.505 |             3.8% |     3.0% to 9.7% |
| flat   |  128 | None     |   12.884 |    10.173 |            21.0% |   10.5% to 28.3% |
| flat   |  128 | Snappy   |   35.433 |    32.156 |             9.2% |   -0.2% to 27.2% |
| flat   |  128 | Zstd(1)  |   30.956 |    30.059 |             2.9% |    2.9% to 24.2% |
| flat   | 8192 | Lz4Frame |  293.300 |   309.182 |            -5.4% |   -12.7% to 7.0% |
| flat   | 8192 | None     |  158.900 |   154.418 |             2.8% |   -12.4% to 2.8% |
| flat   | 8192 | Snappy   |  747.928 |   576.878 |            22.9% |   -9.8% to 33.3% |
| flat   | 8192 | Zstd(1)  |  295.079 |   363.506 |           -23.2% |  -23.2% to 16.2% |
| mixed  |  128 | Lz4Frame |    6.720 |     6.458 |             3.9% |   -41.7% to 5.8% |
| mixed  |  128 | None     |    2.995 |     1.659 |            44.6% |    9.6% to 44.6% |
| mixed  |  128 | Snappy   |   11.660 |     9.267 |            20.5% |    6.1% to 40.1% |
| mixed  |  128 | Zstd(1)  |   17.156 |    18.370 |            -7.1% |   -36.4% to 5.2% |
| mixed  | 8192 | Lz4Frame |  228.346 |   247.321 |            -8.3% |  -24.3% to -6.7% |
| mixed  | 8192 | None     |   57.858 |    21.462 |            62.9% |    2.0% to 69.2% |
| mixed  | 8192 | Snappy   |  265.283 |   248.265 |             6.4% |   -14.2% to 6.4% |
| mixed  | 8192 | Zstd(1)  |  371.345 |   379.277 |            -2.1% |  -11.2% to -2.1% |
| nested |  128 | Lz4Frame |   21.829 |    19.755 |             9.5% |   -16.1% to 9.5% |
| nested |  128 | None     |   13.157 |    11.177 |            15.0% |    4.9% to 21.9% |
| nested |  128 | Snappy   |   39.269 |    45.618 |           -16.2% |  -44.5% to 16.6% |
| nested |  128 | Zstd(1)  |   53.670 |    42.028 |            21.7% |  -62.8% to 38.5% |
| nested | 8192 | Lz4Frame |   97.012 |    95.462 |             1.6% |  -48.2% to 11.6% |
| nested | 8192 | None     |   56.446 |    48.942 |            13.3% |   -4.2% to 13.3% |
| nested | 8192 | Snappy   |  259.343 |   249.919 |             3.6% |    -1.2% to 3.6% |
| nested | 8192 | Zstd(1)  |  196.375 |   145.598 |            25.9% | -109.1% to 40.6% |

## Scheduled rerun: quieter host

At 06:34 Taipei time on 2026-09-06 the host load averages were
1.61 / 2.09 / 4.73, compared with 75.22 / 77.04 / 53.99 during the original run.
At completion they were 3.96 / 2.92 / 4.24. The same locked dependencies,
executable, release profile, and Criterion settings were used for three passes;
all 48 cases completed in every pass. Baselines are named
`scheduled-20260906-1`, `scheduled-20260906-2`, and `scheduled-20260906-3`,
leaving the original baselines intact. Reproduce with the loop above, replacing
`run${run}` with `scheduled-20260906-${run}` (use a new prefix for later runs).

Small batches show repeatable benefits in every schema/codec combination:
median time reductions range from 2.0% to 16.8%. Mixed 128-row batches without
compression decrease from 1.644 to 1.368 microseconds, with a 15.7–17.4% reduction
across the three passes. This is substantially smaller and more stable than the
original noisy 44.6% headline for that case.

For 8,192-row batches, uncompressed gains range from 0.5% to 3.1%; compressed
cases are mostly unchanged (median reductions from -0.2% to 0.6%). The apparent
0.2% slowdown for wide LZ4 batches changes direction between passes (-0.7% to
+0.8%), so this does not establish a repeatable regression. Similarly, mixed
Zstd batches range from -0.4% to +0.1%. These sub-percent results should be
regarded as approximately neutral, not reliable gains or regressions.

This supports metadata reuse for small blocks, not a broad end-to-end shuffle
speedup. The allocation regression and full native shuffle library suite were
rerun successfully: 119 passed, 0 failed.

Microseconds per block: median of the three run medians. The range is the per-run reduction in median time (negative means slower).

| Schema | Rows | Codec    | Fresh µs | Reused µs | Median reduction |  Per-run range |
| ------ | ---: | -------- | -------: | --------: | ---------------: | -------------: |
| flat   |  128 | Lz4Frame |   15.704 |    14.876 |             5.3% |   4.3% to 6.5% |
| flat   |  128 | None     |    9.885 |     9.301 |             5.9% |   5.8% to 6.1% |
| flat   |  128 | Snappy   |   24.739 |    23.988 |             3.0% |   2.4% to 3.1% |
| flat   |  128 | Zstd(1)  |   25.004 |    24.378 |             2.5% |   2.5% to 2.9% |
| flat   | 8192 | Lz4Frame |  249.845 |   250.248 |            -0.2% |  -0.7% to 0.8% |
| flat   | 8192 | None     |  107.407 |   106.882 |             0.5% |  -0.0% to 0.9% |
| flat   | 8192 | Snappy   |  518.848 |   516.717 |             0.4% |  -0.0% to 0.4% |
| flat   | 8192 | Zstd(1)  |  240.051 |   238.635 |             0.6% |   0.3% to 0.6% |
| mixed  |  128 | Lz4Frame |    5.714 |     5.382 |             5.8% |   5.8% to 6.7% |
| mixed  |  128 | None     |    1.644 |     1.368 |            16.8% | 15.7% to 17.4% |
| mixed  |  128 | Snappy   |    8.129 |     7.756 |             4.6% |   4.5% to 6.3% |
| mixed  |  128 | Zstd(1)  |   15.225 |    14.918 |             2.0% |   1.8% to 2.4% |
| mixed  | 8192 | Lz4Frame |  217.285 |   216.996 |             0.1% |  -0.0% to 0.3% |
| mixed  | 8192 | None     |   17.049 |    16.520 |             3.1% |   2.0% to 3.1% |
| mixed  | 8192 | Snappy   |  235.768 |   235.139 |             0.3% |   0.2% to 0.4% |
| mixed  | 8192 | Zstd(1)  |  349.038 |   348.874 |             0.0% |  -0.4% to 0.1% |
| nested |  128 | Lz4Frame |   14.380 |    13.825 |             3.9% |   2.8% to 3.9% |
| nested |  128 | None     |   10.374 |     9.812 |             5.4% |   4.1% to 5.4% |
| nested |  128 | Snappy   |   19.911 |    19.399 |             2.6% |   2.2% to 2.9% |
| nested |  128 | Zstd(1)  |   23.123 |    22.587 |             2.3% |   2.3% to 2.4% |
| nested | 8192 | Lz4Frame |   78.985 |    78.855 |             0.2% |   0.0% to 0.9% |
| nested | 8192 | None     |   42.475 |    41.910 |             1.3% |   1.3% to 2.4% |
| nested | 8192 | Snappy   |  217.298 |   216.970 |             0.2% |   0.1% to 0.4% |
| nested | 8192 | Zstd(1)  |  117.770 |   117.365 |             0.3% |   0.1% to 0.7% |

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

# datafusion-comet-shuffle: Shuffle Writer and Reader

This crate provides the shuffle writer and reader implementation for Apache DataFusion Comet and is maintained as part
of the [Apache DataFusion Comet] subproject.

[Apache DataFusion Comet]: https://github.com/apache/datafusion-comet/

## Shuffle Benchmark Tool

A standalone benchmark binary (`shuffle_bench`) is included for profiling shuffle write
performance outside of Spark. It streams input data directly from Parquet files.

### Basic usage

```sh
cargo run --release --features shuffle-bench --bin shuffle_bench -- \
  --input /data/tpch-sf100/lineitem/ \
  --partitions 200 \
  --codec lz4 \
  --hash-columns 0,3
```

### Options

| Option                | Default                    | Description                                            |
| --------------------- | -------------------------- | ------------------------------------------------------ |
| `--input`             | _(required)_               | Path to a Parquet file or directory of Parquet files   |
| `--partitions`        | `200`                      | Number of output shuffle partitions                    |
| `--partitioning`      | `hash`                     | Partitioning scheme: `hash`, `single`, `round-robin`   |
| `--hash-columns`      | `0`                        | Comma-separated column indices to hash on (e.g. `0,3`) |
| `--codec`             | `lz4`                      | Compression codec: `none`, `lz4`, `zstd`, `snappy`     |
| `--zstd-level`        | `1`                        | Zstd compression level (1–22)                          |
| `--batch-size`        | `8192`                     | Batch size for reading Parquet data                    |
| `--memory-limit`      | _(none)_                   | Memory limit in bytes; triggers spilling when exceeded |
| `--write-buffer-size` | `1048576`                  | Write buffer size in bytes                             |
| `--limit`             | `0`                        | Limit rows processed per iteration (0 = no limit)      |
| `--iterations`        | `1`                        | Number of timed iterations                             |
| `--warmup`            | `0`                        | Number of warmup iterations before timing              |
| `--output-dir`        | `/tmp/comet_shuffle_bench` | Directory for temporary shuffle output files           |

### Profiling with flamegraph

```sh
cargo flamegraph --release --features shuffle-bench --bin shuffle_bench -- \
  --input /data/tpch-sf100/lineitem/ \
  --partitions 200 --codec lz4
```

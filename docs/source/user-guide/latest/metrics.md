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

# Comet Metrics

## Spark SQL Metrics

Set `spark.comet.metrics.detailed=true` to see all available Comet metrics.

### CometScanExec

| Metric      | Description                                                                                                                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scan time` | Total time to scan a Parquet file. This is not comparable to the same metric in Spark because Comet's scan metric is more accurate. Although both Comet and Spark measure the time in nanoseconds, Spark rounds this time to the nearest millisecond per batch and Comet does not. |

### Exchange

Comet adds some additional metrics:

| Metric                          | Description                                                   |
| ------------------------------- | ------------------------------------------------------------- |
| `native shuffle time`           | Total time in native code excluding any child operators.      |
| `repartition time`              | Time to repartition batches.                                  |
| `memory pool time`              | Time interacting with memory pool.                            |
| `encoding and compression time` | Time to encode batches in IPC format and compress using ZSTD. |

## Native Metrics

Setting `spark.comet.explain.native.enabled=true` will cause native plans to be logged in each executor. Metrics are
logged for each native plan (and there is one plan per task, so this is very verbose).

Here is a guide to some of the native metrics.

### ScanExec

| Metric            | Description                                                                                         |
| ----------------- | --------------------------------------------------------------------------------------------------- |
| `elapsed_compute` | Total time spent in this operator, fetching batches from a JVM iterator.                            |
| `jvm_fetch_time`  | Time spent in the JVM fetching input batches to be read by this `ScanExec` instance.                |
| `arrow_ffi_time`  | Time spent using Arrow FFI to create Arrow batches from the memory addresses returned from the JVM. |

### ShuffleWriterExec

| Metric            | Description                                                   |
| ----------------- | ------------------------------------------------------------- |
| `elapsed_compute` | Total time excluding any child operators.                     |
| `repart_time`     | Time to repartition batches.                                  |
| `ipc_time`        | Time to encode batches in IPC format and compress using ZSTD. |
| `mempool_time`    | Time interacting with memory pool.                            |
| `write_time`      | Time spent writing bytes to disk.                             |

## Task-Level Input Metrics on Spark 4.1+

Comet's native scans set `inputMetrics.bytesRead` to the actual file IO performed by the
DataFusion parquet reader (`bytes_scanned`). This is the truthful number you would see at the
filesystem layer.

Spark 4.1 changed its own parquet reader to pre-open the `SeekableInputStream` and read the file
footer outside the `FileScanRDD.compute()` thread. Spark's `inputMetrics.bytesRead` is updated
from a Hadoop FileSystem thread-local byte counter that only captures reads on the
`compute()` thread, so reads serviced by the pre-opened stream's internal buffer go uncounted.
The under-count is largest when the file fits in the pre-fetched buffer (tiny files, unit test
sizes) and shrinks as files grow large enough that subsequent row-group reads cross the buffer
and trigger fresh FS reads on the `compute()` thread.

This is purely an observability difference: `inputMetrics.bytesRead` is reported to listeners
and the Spark UI but is not consumed by the planner, the optimizer, or AQE, so the discrepancy
does not affect query plans, partitioning, or correctness. Records read (`recordsRead`) is
unaffected and remains exactly equal between Comet and Spark.

If you compare Comet's `bytesRead` against vanilla Spark's on Spark 4.1+ (via the Spark UI or
the REST API), expect Comet's number to be substantially larger for small files, and closer to
Spark's for large files. Comet's value reflects what the storage layer actually delivered.

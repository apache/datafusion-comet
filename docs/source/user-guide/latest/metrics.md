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

Comet operators report the following metrics in the Spark SQL UI.

### CometScanExec

| Metric      | Description                                                                                                                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scan time` | Total time to scan a Parquet file. This is not comparable to the same metric in Spark because Comet's scan metric is more accurate. Although both Comet and Spark measure the time in nanoseconds, Spark rounds this time to the nearest millisecond per batch and Comet does not. |

### Exchange

Comet adds some additional metrics:

| Metric                          | Description                                                                 |
| ------------------------------- | --------------------------------------------------------------------------- |
| `native shuffle time`           | Total time in native code excluding any child operators.                    |
| `repartition time`              | Time to repartition batches.                                                |
| `partition interleaving time`   | Time to interleave partitioned batches before writing them.                 |
| `memory pool time`              | Time interacting with memory pool.                                          |
| `encoding and compression time` | Time to encode batches in IPC format and compress using ZSTD.               |
| `disk spilled bytes`            | Actual bytes written to native shuffle spill files on disk.                 |
| `memory spilled bytes`          | Uncompressed Arrow backing-buffer and partition-index data before spilling. |

Disk and memory spilled bytes measure different representations of the same native shuffle spill.
Disk spill bytes count the actual bytes written to disk: compressed when shuffle compression is
enabled and uncompressed when `spark.shuffle.compress=false`. Memory spill bytes follow Spark's
`memoryBytesSpilled` semantics and include uncompressed in-memory Arrow backing-buffer and
partition-index data rather than their on-disk size. These values also appear in Spark's task
metrics and Spark UI as `diskBytesSpilled` and `memoryBytesSpilled`, respectively.

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

| Metric                 | Description                                                           |
| ---------------------- | --------------------------------------------------------------------- |
| `elapsed_compute`      | Total time excluding any child operators.                             |
| `repart_time`          | Time to repartition batches.                                          |
| `interleave_time`      | Time to interleave partitioned batches before writing them.           |
| `ipc_time`             | Time to encode batches in IPC format and compress using ZSTD.         |
| `mempool_time`         | Time interacting with memory pool.                                    |
| `write_time`           | Time spent writing bytes to disk.                                     |
| `spill_count`          | Number of native shuffle spills.                                      |
| `spilled_bytes`        | Actual bytes written to native shuffle spill files on disk.           |
| `memory_spilled_bytes` | Uncompressed Arrow backing-buffer and partition-index memory spilled. |

### Native Parquet scans

Native Parquet scans expose these counters in the Spark SQL metric map as well as native
execution metrics. Counters accumulate per scan operator; they do not instrument individual rows.

| Metric                                     | Description                                                                                                                                                                                                                  |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scan_io_data_bytes`                       | Bytes returned to the Parquet reader for projected data-page ranges.                                                                                                                                                         |
| `scan_io_metadata_bytes`                   | Bytes returned for footer prefetches, page indexes, and Bloom filters. A footer prefetch can also contain unused data bytes.                                                                                                 |
| `scan_io_footer_reads`                     | Storage reads of complete serialized footer payloads, counted once per metadata open. Plaintext payloads must decode successfully; encrypted payloads are counted before key retrieval/decryption, even if those later fail. |
| `scan_io_footer_bytes`                     | Serialized footer payload bytes, excluding the final eight-byte trailer. These bytes are already included in `scan_io_metadata_bytes`.                                                                                       |
| `scan_io_object_store_get_calls`           | Nonempty GET operations at the native remote `ObjectStore` API, after range coalescing. Does not count transport retries or HEAD requests.                                                                                   |
| `scan_io_object_store_get_requested_bytes` | Requested coalesced range bytes at that API. A failed or partly consumed request can contribute requested bytes without the same number of response bytes.                                                                   |
| `scan_io_object_store_response_bytes_read` | Response bytes actually consumed at that API, including bytes fetched between coalesced ranges. Not HTTP wire bytes.                                                                                                         |
| `scan_io_metadata_cache_hits`              | Successful, cache-eligible metadata opens requiring no storage reads.                                                                                                                                                        |
| `scan_io_metadata_cache_misses`            | Successful, cache-eligible metadata opens requiring storage reads. Failed opens and encrypted opens, which bypass this shared cache, increment neither cache counter.                                                        |

Reader-level and object-store bytes are two views of the same reads; do not add them together.
Likewise, footer bytes are a subset of metadata bytes, not a third reader-level category. A warm
metadata-cache hit contributes no new metadata or footer I/O. A valid plaintext footer followed by
a page-index failure still contributes footer bytes; an invalid plaintext footer does not.

Remote counters follow the backend selected during object-store construction, including native
S3 (`s3`/`s3a`), GCS (`gs`), Azure (`az`, `adl`, `azure`, `abfs`, `abfss`), and HTTP(S) stores.
Local files, in-memory stores, and HDFS/custom backends (including cloud-looking schemes selected
through `fs.comet.libhdfs.schemes`) retain reader-level counters but have zero remote counters.
The native cloud wrapper observes default range coalescing; custom `get_ranges` implementations
require an explicit accounting contract before being composed with that wrapper.

For data-bearing scans, comparing remote response bytes with reader data bytes can reveal
coalescing and metadata overhead. For a metadata-only scan, data bytes are zero: report the
metadata and remote totals instead of dividing by zero. Cancellation can leave late asynchronous
work outside the final metric snapshot; these counters are not a guarantee of complete network
traffic accounting after cancellation.

## Task-Level Input Metrics on Spark 4.1+

Comet's native scans populate `inputMetrics.bytesRead` from the existing `bytes_scanned`
counter. It counts requested data/Bloom-filter ranges through the Parquet reader's byte-read
methods, not all filesystem I/O. Footer and page-index reads through metadata loading bypass
this counter, and range coalescing can fetch more bytes than the logical ranges request. The
additional scan I/O metrics above expose those differences without changing `bytes_scanned`.
The native `scan_efficiency_ratio` still uses `bytes_scanned` as its numerator and has the same
blind spots; it is not the remote read-amplification ratio described above.

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
Spark's for large files in that workload. Neither metric should be interpreted as complete
filesystem or network traffic accounting.

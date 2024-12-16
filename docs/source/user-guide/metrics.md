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

`CometScanExec` uses nanoseconds for total scan time. Spark also measures scan time in nanoseconds but converts to
milliseconds _per batch_ which can result in a large loss of precision, making it difficult to compare scan times
between Spark and Comet.

### Exchange

Comet adds some additional metrics:

| Metric                          | Description                                                                               |
|---------------------------------|-------------------------------------------------------------------------------------------|
| `ipc time`                      | Time to encode batches in IPC format                                                      |
| `native shuffle time`           | Total time spent in native shuffle writer, excluding the execution time of the input plan |
| `native shuffle input time`     | Time spend executing the shuffle input plan and fetching batches.                         |
| `shuffle wall time (inclusive)` | Total time executing the shuffle write, inclusive of executing the input plan.            |

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

| Metric            | Description                                           |
| ----------------- | ----------------------------------------------------- |
| `elapsed_compute` | Total time excluding any child operators.             |
| `input_time`      | Time spent executing input plan and fetching batches. |
| `write_time`      | Time spent writing bytes to disk.                     |

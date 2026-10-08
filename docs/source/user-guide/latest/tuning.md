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

# Comet Tuning Guide

Comet provides some tuning options to help you get the best performance from your queries. Every
deployment needs to configure how much memory Comet can use, so start with memory tuning. The
guide is split into the following pages:

- [Memory Tuning](tuning/memory.md): configuring Comet's off-heap memory pool and the executor
  memory overhead, choosing a memory pool, batch size, and limiting spill disk usage.
- [Shuffle Tuning](tuning/shuffle.md): enabling Comet shuffle, the native and columnar shuffle
  implementations, and shuffle compression.
- [Remote Shuffle with Celeborn](tuning/celeborn.md): bounding frame sizes and memory admission
  for native remote shuffle.
- [Scan Tuning](tuning/scans.md): Parquet filter pushdown, Parquet split sizing, and Iceberg data
  file concurrency.
- [Operator Tuning](tuning/operators.md): joins, adaptive partial aggregation, and sorting on
  floating-point values.
- [Reducing Row/Columnar Conversion Overhead](tuning/transitions.md): stages in which many
  operators fall back to Spark.

## Nested and Wide Data

Rows with many columns, long strings, or nested columns such as arrays of structs make every batch
larger, so queries over such data spill more. For these workloads:

- Choose the codec for spill files from native sorts, aggregations, and joins, which Comet
  compresses with `lz4` by default. See
  [Compressing Spill Files](tuning/memory.md#compressing-spill-files).
- Lower the batch size if wide batches cause spilling or out-of-memory errors. See
  [Batch Size](tuning/memory.md#batch-size).
- Check which shuffles can run natively when partitioning keys are nested. See
  [Native Shuffle](tuning/shuffle.md#native-shuffle).
- Watch for row/columnar conversions, which are expensive for such schemas. See
  [Wide or Deeply Nested Schemas](tuning/transitions.md#wide-or-deeply-nested-schemas).

## Configuring Tokio Runtime

Comet uses a global tokio runtime per executor process. By default it starts one worker thread per executor core
(`spark.executor.cores`, or the thread count of `local[N]` and `local[*]` masters) and allows up to 512 blocking
threads, which is tokio's default. If `spark.executor.cores` is not set outside local mode, Comet starts a single
worker thread. These values can be overridden using the environment variables `COMET_WORKER_THREADS` and
`COMET_MAX_BLOCKING_THREADS`.

## Metrics Overhead

The SQL metrics described in [Metrics](metrics.md) are always collected. Setting `spark.comet.metrics.enabled=true`
additionally publishes plan-coverage counters (`operators.native`, `operators.spark`, `queries.planned`,
`transitions`, and `acceleration.ratio`) through Spark's metrics system under the `comet` source. It is disabled by
default because it walks every executed plan on the driver after each query, and the counters are only useful with an
external sink (for example Prometheus) configured. This setting must be applied before the `SparkSession` is created.

## Explain Plan

For an explanation of Comet plan output, the configs that control it, and how
fallback to Spark works, see [Understanding Comet Plans](understanding-comet-plans.md).

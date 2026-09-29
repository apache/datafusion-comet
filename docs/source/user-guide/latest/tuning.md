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
- [Remote Shuffle with Celeborn](tuning/celeborn.md): using Comet native shuffle with Apache
  Celeborn.
- [Scan Tuning](tuning/scans.md): Parquet filter pushdown, Parquet split sizing, and Iceberg data
  file concurrency.
- [Operator Tuning](tuning/operators.md): joins, adaptive partial aggregation, and sorting on
  floating-point values.
- [Reducing Row/Columnar Conversion Overhead](tuning/transitions.md): stages in which many
  operators fall back to Spark.

## Configuring Tokio Runtime

Comet uses a global tokio runtime per executor process. By default it starts one worker thread per core that the
executor runs tasks on, and allows up to 512 blocking threads, which is tokio's default. Comet takes the number of
cores from the first of these that applies:

- the thread count of a `local`, `local[N]` or `local[*]` master, in local mode
- `spark.executor.cores`, when it is set
- the cores per worker `C` of a `local-cluster[N, C, M]` master
- the number of processors available to the executor's JVM on a standalone (`spark://`) cluster, since a standalone
  executor without `spark.executor.cores` takes every core its worker offers, and a worker offers all of its
  machine's cores unless it is started with a different number (`SPARK_WORKER_CORES`)
- one on YARN and Kubernetes, which is their default for `spark.executor.cores`

On any other cluster manager, Comet starts a single worker thread when `spark.executor.cores` is not set, and logs a
warning. Native plans that read no input from the JVM, such as a native Parquet scan feeding a sort, run entirely on
the worker threads, so tasks share them when an executor runs more tasks at once than it has worker threads. These
values can be overridden using the environment variables `COMET_WORKER_THREADS` and `COMET_MAX_BLOCKING_THREADS`.

## Metrics Overhead

The SQL metrics described in [Metrics](metrics.md) are always collected. Setting `spark.comet.metrics.enabled=true`
additionally publishes plan-coverage counters (`operators.native`, `operators.spark`, `queries.planned`,
`transitions`, and `acceleration.ratio`) through Spark's metrics system under the `comet` source. It is disabled by
default because it walks every executed plan on the driver after each query, and the counters are only useful with an
external sink (for example Prometheus) configured. This setting must be applied before the `SparkSession` is created.

## Explain Plan

For an explanation of Comet plan output, the configs that control it, and how
fallback to Spark works, see [Understanding Comet Plans](understanding-comet-plans.md).

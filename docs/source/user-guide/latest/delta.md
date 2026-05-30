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

# Accelerating Delta Lake Parquet Scans using Comet

## Native Reader

Comet can read [Delta Lake](https://delta.io/) tables natively, bypassing Spark's
`DeltaParquetFileFormat` reader and scanning the underlying Parquet through Comet's
native engine. Snapshot resolution, file listing, deletion-vector handling, column
mapping, and row tracking are driven by [`delta-kernel-rs`](https://crates.io/crates/delta_kernel),
Delta's official Rust integration library, so the integration tracks the Delta
transaction protocol rather than re-implementing log parsing.

The integration uses a reflection bridge (no compile-time dependency on `delta-spark`)
and recognizes Delta scans at the same planning layer as Comet's Parquet rule, replacing
the whole scan node. The design mirrors the existing
[Iceberg integration](iceberg.md); see the in-repo design docs under
[`contrib/delta/docs/`](https://github.com/apache/datafusion-comet/tree/main/contrib/delta/docs)
for details.

## Building with Delta support

Delta acceleration is a **contrib feature** and is opt-in at build time (it carries
heavy transitive dependencies — `delta-kernel-rs`, `object_store`, Arrow — that are
intentionally kept out of default builds). Build Comet from source with **both** the
Cargo feature (links the Delta crate into `libcomet`) and the Maven profile (compiles
and packages the contrib into `comet-spark` and registers the Spark extension):

```shell
# Native: link the Delta crate into libcomet
cd native && cargo build --release --features contrib-delta && cd ..
# JVM: compile + package the Delta contrib into comet-spark
./mvnw -Pscala-2.13 -Pspark-4.0 -Pcontrib-delta install -DskipTests
```

A default build (without these flags) carries **zero** Delta surface area — the
reflection bridge returns no handler and the native dispatch arm is compiled out.

### Supported Spark / Delta versions

| Spark | Delta | Scala |
|-------|-------|-------|
| 3.5   | 3.3.2 | 2.12 / 2.13 |
| 4.0   | 4.0.0 | 2.13  |
| 4.1   | 4.1.0 | 2.13  |

Delta 4.1 (Spark 4.1) requires Java 17.

## Usage

With a Delta-enabled Comet build on the classpath, native Delta scans are **enabled by
default** (`spark.comet.scan.deltaNative.enabled=true`). The example below uses the
Spark 4.0 / Scala 2.13 build:

```shell
$SPARK_HOME/bin/spark-shell \
    --jars comet-spark-spark4.0_2.13-$COMET_VERSION.jar \
    --packages io.delta:delta-spark_2.13:4.0.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

```scala
scala> spark.sql("SELECT * FROM delta.`/tmp/my_delta_table` WHERE id > 10").show()
```

To disable native Delta scans (fall back to Spark's Delta reader), set
`spark.comet.scan.deltaNative.enabled=false`.

### Tuning

| Config | Default | Description |
|--------|---------|-------------|
| `spark.comet.scan.deltaNative.enabled` | `true` | Enable native Delta table scans. |
| `spark.comet.scan.deltaNative.dataFileConcurrencyLimit` | `1` | Per-task concurrency when reading data files. Raise to 2–8 on tables with many small files to hide I/O latency (uses more memory). |
| `spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature` | `true` | When `true`, fall back to Spark's Delta reader on any unsupported Delta protocol feature. Set `false` to error instead (useful in tests asserting the native path is taken). |

## Supported features

- Standard Delta reads with projection and filter pushdown
- **Column mapping** (`id` and `name` modes)
- **Deletion vectors** (rows masked by DVs are filtered natively)
- **Row tracking** (`row_id` / `row_commit_version` synthesized or read from materialized columns)
- **Time travel** (`VERSION AS OF` / `TIMESTAMP AS OF`)
- **Partition pruning**, including Dynamic Partition Pruning (e.g. `MERGE` over partitioned tables)
- **Generated / partition columns**
- Storage: local filesystem, HDFS, and S3-compatible object stores

## Current limitations

When the contrib encounters a Delta feature it does not yet accelerate, it declines that
scan and Spark's own Delta reader handles it (correctness is always preserved; the reason
is visible in `EXPLAIN EXTENDED` when `spark.comet.explainFallback.enabled=true`). Notable
fallbacks:

- Path-based Change Data Feed reads (`DeltaCDFRelation`); table-API CDF still uses Spark
- `VARIANT`-typed columns
- Some cloud credential configurations are not bridged to the native reader (e.g. GCS,
  certain per-bucket S3 credential chains)
- Writes are not accelerated (reads only)

See
[`contrib/delta/docs/08-known-limitations.md`](https://github.com/apache/datafusion-comet/blob/main/contrib/delta/docs/08-known-limitations.md)
for the authoritative, up-to-date catalog of deliberate tradeoffs and tracked issues.

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

# Accelerating Delta Lake Parquet Scans using Comet (Experimental)

**Note: Delta Lake integration is a work-in-progress.**

## Native Reader

Comet's fully-native Delta Lake integration reads the Delta transaction log via
[delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) and reads parquet files
through Comet's own tuned ParquetSource. No compile-time dependency on `spark-delta`
is required -- Comet detects Delta tables at runtime via class-name reflection.

The key configuration to enable native Delta is `spark.comet.scan.deltaNative.enabled=true`.

```shell
$SPARK_HOME/bin/spark-shell \
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:0.15.0,io.delta:delta-spark_2.12:3.3.2 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.scan.deltaNative.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

### Tuning

Comet's native Delta reader supports per-file parallelism with the config
`spark.comet.scan.deltaNative.dataFileConcurrencyLimit`. This value defaults to 1.
Increasing it to values between 2 and 8 can improve throughput for tables with many
small files by overlapping I/O latency.

### Supported features

The native Delta reader supports the following features:

**Protocol versions:**

- Reader version 1, 2, and 3
- Writer version up to 7 (for DV-capable tables)

**Schema and data types:**

- All primitive types (boolean, byte, short, int, long, float, double, string,
  binary, date, timestamp, decimal)
- Complex types: arrays, maps, and structs (including deeply nested)
- Schema evolution (adding columns via `mergeSchema`)

**Column mapping:**

- Column mapping mode `none` (default)
- Column mapping mode `id` (field-ID-based)
- Column mapping mode `name` (physical-name-based)
- Column rename via `ALTER TABLE RENAME COLUMN` (metadata-only operation)

**Time travel:**

- `VERSION AS OF` queries to read historical snapshots
- `TIMESTAMP AS OF` queries

**Deletion vectors:**

- Tables with `delta.enableDeletionVectors=true`
- Reads after `DELETE`, `UPDATE`, and `MERGE` operations that produce DVs
- Both inline and on-disk deletion vectors
- DV replacement across multiple commits

**Filter pushdown (two-level):**

1. **File-level (kernel stats):** Per-file column statistics (min/max/null_count) are
   used by delta-kernel-rs to skip entire files that cannot match the predicate.
2. **Row-group-level (ParquetSource):** Comet's ParquetSource applies predicate pushdown
   within the files that kernel kept, using Parquet page-index and row-group stats.

Supported predicates: `=`, `!=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `NOT`,
`IS NULL`, `IS NOT NULL`.

**Partitioning:**

- Standard partitioning with partition pruning
- Multiple partition columns
- Typed partition columns (int, long, date, string, etc.)
- Combined partition + data-column filter predicates

**Storage:**

- Local filesystem
- S3-compatible storage (AWS S3, MinIO) via object_store credentials
- Azure Blob Storage (ABFS/ABFSS) via Azure credentials

### Configuration reference

| Config | Default | Description |
|--------|---------|-------------|
| `spark.comet.scan.deltaNative.enabled` | `false` | Enable native Delta Lake scan |
| `spark.comet.scan.deltaNative.dataFileConcurrencyLimit` | `1` | Per-task file read parallelism |
| `spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature` | `true` | Fall back to Spark for unsupported reader features |

### Current limitations

The following scenarios will fall back to Spark's native Delta reader:

- Delta writes (reads are accelerated, writes use Spark)
- Tables with `typeWidening` enabled
- Tables with `rowTracking` enabled
- Change Data Feed (`readChangeFeed`) queries
- The `_metadata.row_index` virtual column
- Spark 3.4 uses Delta 2.4.x (DVs not supported in Delta 2.x; simpler feature set)
- Spark 4.0 uses Delta 4.0.x (experimental)

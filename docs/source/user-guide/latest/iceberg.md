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

# Accelerating Apache Iceberg Parquet Scans using Comet

## Native Reader

Comet's native Iceberg reader relies on reflection to extract `FileScanTask`s from Iceberg, which are
then serialized to Comet's native execution engine (see
[PR #2528](https://github.com/apache/datafusion-comet/pull/2528)).

The example below uses Spark's package downloader to retrieve Comet 0.14.0 and Iceberg
1.8.1, but Comet has been tested with Iceberg 1.5, 1.7, 1.8, 1.9, and 1.10. The native Iceberg
reader is enabled by default. To disable it, set `spark.comet.scan.icebergNative.enabled=false`.

```shell
$SPARK_HOME/bin/spark-shell \
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:0.14.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-core:1.8.1 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

### Tuning

Comet’s native Iceberg reader supports fetching multiple files in parallel to hide I/O latency with the
config `spark.comet.scan.icebergNative.dataFileConcurrencyLimit`. This value defaults to 1 to
maintain test behavior on Iceberg Java tests without `ORDER BY` clauses, but we suggest increasing it to
values between 2 and 8 based on your workload.

### Supported features

The native Iceberg reader supports the following features:

**Table specifications:**

- Iceberg table spec v1 and v2 (v3 will fall back to Spark)

**Schema and data types:**

- All primitive types including UUID
- Complex types: arrays, maps, and structs
- Schema evolution (adding and dropping columns)

**Time travel and branching:**

- `VERSION AS OF` queries to read historical snapshots
- Branch reads for accessing named branches

**Delete handling (Merge-On-Read tables):**

- Positional deletes
- Equality deletes
- Mixed delete types

**Filter pushdown:**

- Equality and comparison predicates (`=`, `!=`, `>`, `>=`, `<`, `<=`)
- Logical operators (`AND`, `OR`)
- NULL checks (`IS NULL`, `IS NOT NULL`)
- `IN` and `NOT IN` list operations
- `BETWEEN` operations

**Partitioning:**

- Standard partitioning with partition pruning
- Date partitioning with `days()` transform
- Bucket partitioning
- Truncate transform
- Hour transform

**Storage:**

- Local filesystem
- Hadoop Distributed File System (HDFS)
- S3-compatible storage (AWS S3, MinIO)

### REST Catalog

Comet's native Iceberg reader also supports REST catalogs. The following example shows how to
configure Spark to use a REST catalog with Comet's native Iceberg scan:

```shell
$SPARK_HOME/bin/spark-shell \
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:0.14.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-core:1.8.1 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.rest_cat=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.rest_cat.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.rest_cat.uri=http://localhost:8181 \
    --conf spark.sql.catalog.rest_cat.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

Note that REST catalogs require explicit namespace creation before creating tables:

```scala
scala> spark.sql("CREATE NAMESPACE rest_cat.db")
scala> spark.sql("CREATE TABLE rest_cat.db.test_table (id INT, name STRING) USING iceberg")
scala> spark.sql("INSERT INTO rest_cat.db.test_table VALUES (1, 'Alice'), (2, 'Bob')")
scala> spark.sql("SELECT * FROM rest_cat.db.test_table").show()
```

### Current limitations

The following scenarios will fall back to Spark's native Iceberg reader:

- Iceberg table spec v3 scans
- Iceberg writes (reads are accelerated, writes use Spark)
- Tables backed by Avro or ORC data files (only Parquet is accelerated)
- Tables partitioned on `BINARY` or `DECIMAL` (with precision >28) columns
- Scans with residual filters using `truncate`, `bucket`, `year`, `month`, `day`, or `hour`
  transform functions (partition pruning still works, but row-level filtering of these
  transforms falls back)

### Task input metrics

The native Iceberg reader populates Spark's task-level `inputMetrics.bytesRead` (visible in the Spark UI Stages tab) using the `bytes_read` counter from iceberg-rust's `ScanMetrics`. This counter includes bytes read from both data files and delete files.

Iceberg Java does not explicitly report `bytesRead` to Spark's task input metrics. On the iceberg Java path, any `bytesRead` value comes from Hadoop's filesystem-level I/O counters, not from Iceberg itself. Because Comet's native reader and the Hadoop filesystem use different counting mechanisms, the exact byte counts will differ between the two paths.

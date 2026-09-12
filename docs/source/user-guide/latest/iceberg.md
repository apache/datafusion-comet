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

The example below uses Spark's package downloader to retrieve Comet $COMET_VERSION and Iceberg
1.8.1, but Comet has been tested with Iceberg 1.5, 1.7, 1.8, 1.9, 1.10, and 1.11. The native Iceberg
reader is enabled by default. To disable it, set `spark.comet.scan.icebergNative.enabled=false`.

The example uses the Spark 3.5 / Scala 2.12 build of Comet; substitute the Comet artifact
matching your Spark and Scala versions (Comet also ships Spark 3.5 / Scala 2.13 and Spark
4.0/4.1 / Scala 2.13 jars; see the [installation guide](installation.md) for the full list).

```shell
$SPARK_HOME/bin/spark-shell \
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:$COMET_VERSION,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-core:1.8.1 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explain.fallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

Catalog configuration is standard Iceberg-on-Spark and independent of Comet. The native reader has been tested with Hadoop, Hive, and REST catalogs. The example above uses a Hadoop catalog. For the full catalog configuration reference, see Iceberg's [Spark catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/#catalogs).

### Micro-batch streaming reads

On Spark 4.x, native Iceberg micro-batch source reads can be enabled experimentally with
`spark.comet.scan.icebergNative.streaming.enabled=true`. Native scans and native execution must
also be enabled. Spark 3.x retains its Spark reader because its streaming progress reporter does
not support a replacement source node.

```python
spark.conf.set("spark.comet.scan.icebergNative.streaming.enabled", "true")
# Allow the foreachBatch DataFrame's RDDScan to enter native batch operators.
spark.conf.set("spark.comet.sparkToColumnar.enabled", "true")

query = (
    spark.readStream
    .option("streaming-max-files-per-micro-batch", "1000")
    .table("catalog.db.events")
    .writeStream
    .option("checkpointLocation", "/checkpoints/iceberg-events")
    .trigger(availableNow=True)
    .foreachBatch(process_batch)
    .start()
)
query.awaitTermination()
```

Comet reads the file tasks that Iceberg planned for each batch's start and end offsets. Spark
continues to manage admission limits, checkpoints, and sink commits. Existing
Iceberg scan compatibility checks also apply to streaming. Unsupported sources retain their Spark
reader; continuous processing is not accelerated.

Trigger support follows the installed Iceberg runtime. Iceberg 1.10 falls back from `AvailableNow`
to a single batch, ignoring its file admission limit. Iceberg 1.11 supports `AvailableNow` with
admission limits.

Inside `foreachBatch`, use `batch_df.sparkSession` for reference-table reads and temporary views.
Enable `spark.comet.sparkToColumnar.enabled` with `RDDScan` in its supported operator list (the
default) so the callback's DataFrame can feed native operators. These batch queries can use Comet's
existing native joins, aggregations, and Iceberg reference scans.
An Iceberg output table can be appended with `violations.writeTo("catalog.db.violations").append()`.
Spark can retry a callback, so writes still need an idempotency strategy using the batch ID.
Use a new checkpoint when migrating a source from Delta to Iceberg.

#### Streaming execution and state

Enable `spark.comet.exec.streaming.enabled=true` to use native operators in Spark 4.x
micro-batches. Supported streaming aggregates perform partial aggregation, merge restored state,
and compute results in Comet. Spark's state store retains its checkpoint format, commit protocol,
watermark tracking, and state eviction. The boundary converts Spark state rows to Arrow batches.
A stateful `foreachBatch` callback must consume the complete batch so Spark can commit every
state partition; returning after only `head()` or another partial action is insufficient.

Only aggregates with compatible state buffers can use this path. Counts, ordinary numeric sums,
non-decimal averages, and supported min/max expressions are eligible. Aggregates with incompatible
buffers, such as `collect_set`, retain Spark execution. Stream-stream joins, streaming
deduplication, session-window state, and arbitrary user-defined state functions also retain Spark
execution. This option does not provide a native replacement for every Spark stateful operator.

#### Append streams and change data capture

The native source accelerates Iceberg's standard `readStream` path. It reads append snapshots
and does not generate change types or update/delete images. Overwrite and delete snapshots fail
by default. Iceberg's `streaming-skip-overwrite-snapshots` and `streaming-skip-delete-snapshots`
options ignore those snapshots; they do not turn the append reader into a change data feed.
See Iceberg's [streaming reads](https://iceberg.apache.org/docs/latest/spark-structured-streaming/#streaming-reads).

For an integrity pipeline that consumes an append-only event table, the producer must supply
operation types and images. Polaris and Lakekeeper provide the REST catalog without changing
these source semantics.

### Batch change data capture

Enable `spark.comet.scan.icebergNative.changelog.enabled=true`, together with the native Iceberg
reader and native execution, to accelerate Iceberg's `.changes` table and
[`create_changelog_view`](https://iceberg.apache.org/docs/latest/spark-procedures/#create_changelog_view).
This experimental path reads the added and removed data-file tasks planned by Iceberg. It uses
the existing native reader and performs carry-over removal, update-image pairing, or net-change
calculation in Rust. The procedure and temporary-view registration remain in Spark.

```sql
CALL catalog.system.create_changelog_view(
  table => 'db.profiles',
  changelog_view => 'profile_changes',
  options => map('start-snapshot-id', '123', 'end-snapshot-id', '456'),
  identifier_columns => array('tenant', 'id'),
  compute_updates => true
);
SELECT * FROM profile_changes
WHERE _change_type IN ('INSERT', 'UPDATE_AFTER');
```

Replace the example snapshot IDs with retained snapshots of the source table. The start bound is
exclusive and the end bound inclusive; timestamp bounds follow the installed Iceberg runtime.
The procedure always removes unchanged rows carried over by copy-on-write rewrites. Explicit
`identifier_columns` enable update images by default. With `compute_updates=true`, omitted
identifier columns come from the table schema. `net_changes=true` cancels matching inserts and
deletes across the range; Iceberg rejects combining net changes with update images.

Results preserve `_change_type`, `_change_ordinal`, and `_commit_snapshot_id`. Binary values and
nested floating-point values retain Iceberg's JVM iterator because their external-row equality
semantics differ from Arrow value equality. Unknown procedure closure layouts also retain the
JVM iterator. The normal native-reader format and type restrictions still apply.

This is a bounded batch API. It does not supply streaming offsets, checkpoint persistence, or an
exactly-once sink for a polling CDC job. In Iceberg 1.11, changelog planning rejects snapshots with
delete manifests, so merge-on-read changes involving delete files are unsupported before Comet
executes the scan. Snapshot history and removed data files must remain available for the range.

### Tuning

Comet’s native Iceberg reader supports fetching multiple files in parallel to hide I/O latency with the
config `spark.comet.scan.icebergNative.dataFileConcurrencyLimit`. This value defaults to 1 to
maintain test behavior on Iceberg Java tests without `ORDER BY` clauses, but we suggest increasing it to
values between 2 and 8 based on your workload.

### Supported features

The native Iceberg reader supports the following features:

**Table specifications:**

- Iceberg table spec v1, v2, and v3

**Encryption:**

- Encrypted v3 tables using 128-bit or 256-bit AES-GCM data keys (requires Iceberg 1.11 or newer).
  Iceberg-Java unwraps the key envelope on the driver during planning and stores the plaintext data
  key in each file's `key_metadata`, which the native reader uses directly, so no KMS integration is
  needed on the native side. Iceberg-Java also permits 192-bit data keys; those tables fall back to
  Spark (no AES-192-GCM in the underlying crypto).

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
- Deletion vectors (v3)

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
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:$COMET_VERSION,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-core:1.8.1 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.rest_cat=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.rest_cat.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.rest_cat.uri=http://localhost:8181 \
    --conf spark.sql.catalog.rest_cat.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explain.fallback.enabled=true \
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

### Object store configuration (S3)

The native reader has its own Rust object store client and does not go through Iceberg's JVM FileIO, neither `S3FileIO` nor the older Hadoop S3A filesystem. It configures that client from the catalog's `s3.*` properties (the same keys `S3FileIO` reads), from `spark.hadoop.fs.s3a.*` settings, or, for a scheme opted into `spark.hadoop.fs.comet.s3Compliant.schemes`, from vendor-style `fs.<scheme>.<authority>.*` keys (see [S3-Compliant Filesystem Schemes](datasources.md#s3-compliant-filesystem-schemes)). That third source is translated into the same `fs.s3a.*` shape as the second before it reaches the reader. S3 configuration therefore reaches the native reader through one of these three channels.

For a custom S3-compatible endpoint, configure the catalog with the endpoint, path-style access, region, and credentials (Hive shown):

```shell
    --conf spark.sql.catalog.s3_cat=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.s3_cat.type=hive \
    --conf spark.sql.catalog.s3_cat.uri=thrift://metastore:9083 \
    --conf spark.sql.catalog.s3_cat.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.s3_cat.s3.endpoint=https://s3.example.com:9000 \
    --conf spark.sql.catalog.s3_cat.s3.path-style-access=true \
    --conf spark.sql.catalog.s3_cat.client.region=us-east-1 \
    --conf spark.sql.catalog.s3_cat.s3.access-key-id=... \
    --conf spark.sql.catalog.s3_cat.s3.secret-access-key=...
```

These `s3.*` storage properties are not specific to the Hive catalog shown here. When `s3.access-key-id` / `s3.secret-access-key` are omitted, credentials come from the standard AWS chain (environment variables, instance profiles, and so on). `client.region` is auto-detected for AWS but should be set for non-AWS endpoints. If your REST catalog vends temporary credentials, the native reader does not consume them automatically, and wiring that requires the credential provider bridge. See Iceberg's [S3 FileIO](https://iceberg.apache.org/docs/latest/aws/#s3-fileio) docs for the full property list, and [S3 Credential Providers](s3-credential-providers.md) for vended or per-request credentials.

### Current limitations

The following scenarios will fall back to the JVM Iceberg reader:

- Iceberg table spec v4 or newer
- v3 tables with columns that declare an initial default value
- v3 column types the native reader cannot read (`variant`, `geometry`, `geography`, `unknown`)
- Encrypted tables with 192-bit data keys (no AES-192-GCM in the underlying crypto)
- Delete files in a format other than Parquet or Puffin (Avro or ORC positional/equality deletes)
- Iceberg writes (reads are accelerated, writes use Spark)
- Tables backed by Avro or ORC data files (only Parquet is accelerated)
- Tables partitioned on `BINARY` or `DECIMAL` (with precision >28) columns
- Scans with residual filters using `truncate`, `bucket`, `year`, `month`, `day`, or `hour`
  transform functions (partition pruning still works, but row-level filtering of these
  transforms falls back)

### Iceberg UDFs

Iceberg ships several `ScalaUDF`s that surface in user queries and maintenance actions:

- `IcebergSpark.registerBucketUDF` and `registerTruncateUDF` register `bucket(N, col)` and
  `truncate(W, col)` for use in `SELECT` / `JOIN` / `WHERE` predicates that align with hidden
  partitioning.
- `RewriteDataFiles` with `sort-strategy=zorder` builds a tree of per-type ordered-bytes UDFs
  (`INT_ORDERED_BYTES`, `LONG_ORDERED_BYTES`, ..., `INTERLEAVE_BYTES`) over the sort key columns
  during compaction.

[Scala UDF and Java UDF Support](scala_java_udfs.md) is enabled by default
(`spark.comet.exec.scalaUDF.codegen.enabled=true`), so these UDFs run through native execution and
the project, exchange, and sort operators around them stay on the Comet path end-to-end. Setting
`spark.comet.exec.scalaUDF.codegen.enabled=false` causes the enclosing operator to fall back to
Spark, which forces a columnar-to-row roundtrip and demotes the surrounding shuffle from
`CometExchange` to `CometColumnarExchange`.

### Iceberg system functions

Iceberg's system functions `bucket`, `truncate`, `years`, `months`, `days`, and `hours` (the SQL
form of its partition transforms, for example `SELECT system.bucket(16, id) FROM t`) run natively.
Spark binds them as static invocations of Iceberg's per-type implementations under
`org.apache.iceberg.spark.functions`, and Comet recognizes those classes wherever the expression
appears: in a projection, a filter, a sort key, or the hash partitioning of a shuffle.

The native kernels reproduce Iceberg's Java semantics exactly rather than approximately:

- `bucket` hashes the spec's byte encoding of each value (8-byte little-endian for integers, dates,
  and timestamps; UTF-8 for strings; raw bytes for binary; the minimal big-endian two's complement
  of the unscaled value for decimals) with 32-bit Murmur3 and masks the sign bit before taking the
  modulus.
- `truncate` uses Java's wrapping integer arithmetic and counts code points (not bytes) for
  strings. Decimal inputs are the one case that stays with Spark, see below.
- `years`, `months`, `days`, and `hours` are evaluated in UTC regardless of the session timezone
  and go negative before the epoch; `days` returns a date, the other three an int. They cover the
  whole `DATE` and `TIMESTAMP` domain, as Iceberg's `DateTimeUtil` does.

`truncate` on a `decimal` column falls back to Spark. Truncating a negative decimal grows its
magnitude, so the result can need one more digit than the column's precision allows:
`truncate(10, v)` on a `decimal(18,4)` value of `-99999999999999.9999` is
`-100000000000000.0000`, which has 19 digits. Iceberg's `TruncateDecimal` hands that oversized
value back unchanged and Spark turns it into null only when the row is materialized. An Arrow
`Decimal128(precision, scale)` array has no encoding for that intermediate, so a native kernel
would have to null it during evaluation, which changes what an enclosing predicate or hash sees.
Every other `truncate` input type, and `bucket` on decimals, runs natively.

This matters most for writes. A partitioned table with the default `write.distribution-mode`
(`hash`) is planned with a shuffle and a local sort keyed on the partition transforms, and with
these functions native the whole sub-plan feeding the [native Iceberg writer](iceberg-writes.md)
stays in Comet. A `numBuckets` or `width` argument that is not a positive integer literal makes
the expression fall back to Spark.

### Task input metrics

The native Iceberg reader populates Spark's task-level `inputMetrics.bytesRead` (visible in the Spark UI Stages tab) using the `bytes_read` counter from iceberg-rust's `ScanMetrics`. This counter includes bytes read from both data files and delete files.

Iceberg Java does not explicitly report `bytesRead` to Spark's task input metrics. On the iceberg Java path, any `bytesRead` value comes from Hadoop's filesystem-level I/O counters, not from Iceberg itself. Because Comet's native reader and the Hadoop filesystem use different counting mechanisms, the exact byte counts will differ between the two paths.

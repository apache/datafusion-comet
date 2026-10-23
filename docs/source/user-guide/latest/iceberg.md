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

# Apache Iceberg

Comet accelerates Iceberg tables whose data files are Parquet, in two ways:

- **[Reading](#native-reader)**: the native Iceberg reader scans the table's data and delete
  files in Rust. It is enabled by default.
- **[Writing](#writes)**: with `spark.comet.write.iceberg.enabled=true`, Comet writes the data
  files of eligible writes natively. It is disabled by default.

This page covers both, followed by the [known issues](#known-issues). Behavior that applies to
Parquet files in general is documented on the [Parquet](parquet.md) page.

## Native Reader

Comet's native Iceberg reader relies on reflection to extract `FileScanTask`s from Iceberg, which are
then serialized to Comet's native execution engine (see
[PR #2528](https://github.com/apache/datafusion-comet/pull/2528)).

The example below uses Spark's package downloader to retrieve Comet $COMET_VERSION and Iceberg
1.8.1, but Comet has been tested with Iceberg 1.5, 1.8, 1.9, 1.10, and 1.11. The native Iceberg
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
    --conf spark.memory.offHeap.size=2g \
    --conf spark.executor.memoryOverhead=2g
```

Catalog configuration is standard Iceberg-on-Spark and independent of Comet. The native reader has been tested with Hadoop, Hive, and REST catalogs. The example above uses a Hadoop catalog. For the full catalog configuration reference, see Iceberg's [Spark catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/#catalogs).

### Tuning

Comet’s native Iceberg reader fetches multiple data files in parallel to hide I/O latency. The config
`spark.comet.scan.icebergNative.dataFileConcurrencyLimit` sets how many files one task reads at a time.
It defaults to 2, which helps most for tables with many small files or high-latency storage such as S3.
Values between 2 and 8 are reasonable; raise it based on your workload, at the cost of extra memory,
because each file in flight holds its own buffers.

When the limit is above 1, batches from different files are returned in no particular order, so a
query without an `ORDER BY` can return rows in a different order than with a limit of 1. Spark does
not guarantee that order, but some tests and applications depend on it. Set the limit to `1` to read
the files one after another, in the order Spark assigned them.

The Iceberg Spark tests are an example. They expect file order from queries without an `ORDER BY`, so
the Iceberg test jobs set the environment variable `COMET_ICEBERG_DATA_FILE_CONCURRENCY_LIMIT=1`,
which overrides the default. See [Iceberg Spark tests](../../contributor-guide/iceberg-spark-tests.md).

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
- NULL checks (`IS NULL`, `IS NOT NULL`) on primitive columns
- `IN` list operations (`NOT IN` is applied after the scan)
- `BETWEEN` operations

NULL checks on struct, array, and map columns still use native scans and return correct
results, but are not pushed into iceberg-rust, which binds accessors only for primitive fields.
These null checks are not pushed, but the other predicates in a positive conjunction still are.
iceberg-rust uses a pushed predicate to skip row groups and pages and to filter rows during
decode, and the filter above the scan evaluates the complete condition. `OR` is pushed only when
both branches can be, and `NOT` only when its whole child can be.

Some predicates are left to the filter above the scan because iceberg-rust would return
different rows than Spark:

- Range comparisons on `FLOAT` and `DOUBLE` columns, and comparisons with `0.0`, `-0.0` or NaN.
- `NOT` over equality on a nullable column.
- Comparisons other than `=` on a column whose type was promoted. This applies to every `BIGINT`
  and `DOUBLE` column when only part of a condition is pushed, or when expiring snapshots removed
  older schemas.

Tables with a `schema.name-mapping.default` property push a condition only when all of it can be
pushed.

**Partitioning:**

- Standard partitioning with partition pruning
- Date partitioning with `days()` transform
- Bucket partitioning
- Truncate transform
- Hour transform

**Storage:**

- Local filesystem
- S3-compatible storage (AWS S3, MinIO)
- Google Cloud Storage (`gs`) and Alibaba Cloud OSS (`oss`)

HDFS-backed tables are not supported by the native Iceberg reader and fall back to Spark.

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
    --conf spark.memory.offHeap.size=2g \
    --conf spark.executor.memoryOverhead=2g
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

Per-bucket `fs.s3a.bucket.<bucket>.*` settings apply to the bucket that holds the table's data and delete files. The native reader uses one object-store configuration per scan, so a scan whose data or delete files span more than one S3 bucket falls back to Spark.

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

These `s3.*` storage properties are not specific to the Hive catalog shown here. When `s3.access-key-id` / `s3.secret-access-key` are omitted, credentials come from the standard AWS chain (environment variables, instance profiles, and so on). The region is not auto-detected: when neither the catalog (`client.region` or `s3.region`) nor the executor environment (`AWS_REGION` or `AWS_DEFAULT_REGION`) supplies one, Comet uses `us-east-1`, so set it for AWS buckets in any other region. If your REST catalog vends temporary credentials, the native reader does not consume them automatically, and wiring that requires the credential provider bridge. See Iceberg's [S3 FileIO](https://iceberg.apache.org/docs/latest/aws/#s3-fileio) docs for the full property list, and [S3 Credential Providers](s3-credential-providers.md) for vended or per-request credentials.

### Current limitations

The following scenarios will fall back to the JVM Iceberg reader:

- Iceberg table spec v4 or newer
- v3 tables with columns that declare an initial default value
- v3 column types the native reader cannot read (`geometry`, `geography`, `unknown`), and
  `variant` columns the query reads (on Spark 4.0+, a table whose `variant` columns are not
  projected is read natively)
- Encrypted tables with 192-bit data keys (no AES-192-GCM in the underlying crypto)
- Delete files in a format other than Parquet or Puffin (Avro or ORC positional/equality deletes)
- Equality deletes keyed on a field nested in a struct
- Tables backed by Avro or ORC data files (only Parquet is accelerated)
- Queries that use `input_file_name()`, `input_file_block_start()` or `input_file_block_length()`
- Scans whose data or delete files span more than one S3 bucket (the native reader uses one
  object-store configuration per scan)
- Scans where Iceberg reports an output ordering (for example with
  `spark.sql.iceberg.planning.preserve-data-ordering=true`). Spark may then skip the sort above the
  scan, and the native reader does not preserve file order
- Tables partitioned on `BINARY` or `DECIMAL` (with precision >28) columns
- Scans with residual filters using `truncate`, `bucket`, `year`, `month`, `day`, or `hour`
  transform functions (partition pruning still works, but row-level filtering of these
  transforms falls back)
- Scans that read a struct, array, or map column with a nested field that schema evolution added
  or renamed. The native reader cannot yet match such a field to data files written before the
  change. The check uses the table's schema history, so the fallback stays after those files are
  rewritten
- Scans where no schema in the table's history gives a partition source or equality-delete key a
  name that the scan's other columns don't use (every task of a native scan must give such a
  column the same name)

The cases in which the native reader returns different results from Spark, or fails where Spark
succeeds, without falling back are listed under [Known Issues](#known-issues).

Writes are not covered by this list. By default Spark plans Iceberg writes with its own V2 write
operator, and iceberg-java writes the data files. With `spark.comet.write.iceberg.enabled=true`,
Comet plans an Iceberg write with its split-operator plan and writes eligible data files natively;
see [Writes](#writes) for the plan, the native writer, and when each applies.

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
appears: in a projection, a filter, a sort key, or the hash partitioning of a shuffle. The
exception is a call nested inside an expression that Comet runs through the JVM codegen
dispatcher, such as `map(...)`. That makes the operator fall back to Spark.

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
these functions native the whole sub-plan feeding the [native Iceberg writer](#writes)
stays in Comet. A `numBuckets` or `width` argument that is not a positive integer literal makes
the expression fall back to Spark.

### Task input metrics

The native Iceberg reader populates Spark's task-level `inputMetrics.bytesRead` (visible in the Spark UI Stages tab) using the `bytes_read` counter from iceberg-rust's `ScanMetrics`. This counter includes bytes read from both data files and delete files. The scan's SQL metrics, including Iceberg's planning counters, are listed in the [Metrics Guide](metrics.md#cometicebergnativescan).

Iceberg Java does not explicitly report `bytesRead` to Spark's task input metrics. On the iceberg Java path, any `bytesRead` value comes from Hadoop's filesystem-level I/O counters, not from Iceberg itself. Because Comet's native reader and the Hadoop filesystem use different counting mechanisms, the exact byte counts will differ between the two paths.

## Writes

With `spark.comet.write.iceberg.enabled=true`, Comet plans Iceberg writes with the split-operator
plan described below, and writes the data files of each eligible write natively with iceberg-rust.
A write that is not eligible falls back to iceberg-java's writer. Natively written files differ
from iceberg-java's in the ways listed under [Accepted divergences](#accepted-divergences).

The setting is off by default, so Spark plans its own write operator and iceberg-java writes every
data file. Validate the native writer against your own workloads before enabling it, including
the costs described under [Native Parquet write eligibility](#native-parquet-write-eligibility)
and [Failure handling](#failure-handling).

### Overview

Spark writes an Iceberg table through a single physical operator that combines data-file
writing with metadata writing, committing, and catalog validation. Spark's Adaptive Query
Execution (AQE) already re-plans the sub-query feeding that operator — the scans, projects,
sorts, and exchanges producing the rows — but the operator itself sits outside AQE, so the
data-file writing cannot be re-planned in response to how its input ran. And because data-file
writing is bundled with the metadata and commit steps, there is no separate step for Comet to
replace.

When `spark.comet.write.iceberg.enabled=true`, Comet rewrites eligible Iceberg writes into two
operators:

1. **`IcebergWrite`** — writes the data files on the executors, exactly as iceberg-java does
   today, and returns each task's serialized commit message. This operator and the sub-query
   feeding it run inside AQE.
2. **`IcebergCommit`** — collects the commit messages on the driver and performs the normal
   Iceberg commit (including commit-time validation), outside AQE, exactly once.

The split moves data-file writing inside AQE and separates it from the commit, which gives Comet
a step to replace: when the write passes the eligibility check below, the `IcebergWrite`
operator's per-task Parquet write is delegated to
[iceberg-rust](https://github.com/apache/iceberg-rust) via Comet's native execution pipeline
([#5361](https://github.com/apache/datafusion-comet/pull/5361)). A write that does not pass keeps
`IcebergWrite`, and iceberg-java writes its data files; only the plan shape changes.

### How the native write works

The JVM-side planner marshals everything iceberg-rust needs — the write schema and partition
spec as JSON, the data location, the resolved parquet writer settings, the writer mode
(unpartitioned / fanout / clustered, mirroring `SparkWrite`'s own choice), object-store
configuration (the table's `FileIO` properties, e.g. REST-vended credentials, merged over
`fs.s3a.*` settings translated from the effective Hadoop configuration carried by the table's
`FileIO`, including catalog-specific `hadoop.*` overrides, since `HadoopFileIO` carries its S3
configuration in the Hadoop Configuration rather than in `FileIO` properties), and per-task IDs —
into the serialized native plan. On each task, iceberg-rust writes the Parquet files and
returns its `DataFile` metadata packed as a single in-memory Iceberg V2 data manifest; the JVM
decodes those bytes with Iceberg's own `ManifestFiles.read`, re-derives each file's manifest
metrics from the written Parquet footer with Iceberg's `MetricsConfig` logic (so metrics modes,
truncation, and bounds decisions are iceberg-java's by construction), and wraps the result in
the same `TaskCommit` message the JVM writer would have produced. Everything iceberg-java does
post-write — snapshot assignment, manifest-list aggregation, commit validation and retries —
is untouched: `IcebergCommit` performs the normal `BatchWrite.commit`.

For `ResolvingFileIO`, Hadoop settings are taken from the delegate opening the data location.
An S3 location handled by `S3FileIO` uses its initialized FileIO properties, so Hadoop options
on the wrapper do not change the native endpoint or encryption settings. If that delegate cannot
be resolved, the write falls back to iceberg-java.

Before a task opens a partition's first file, it holds the partition's initial rows in memory
to choose which columns to dictionary-encode using parquet-mr's size accounting (see the
accepted divergences below). It normally waits for at least `write.parquet.page-row-limit`
rows (at least 100 when the configured limit is lower), or until the partition ends. The
buffering threshold is `write.parquet.row-group-size-bytes`, shared across all partitions in
a fanout write. Reaching it makes each partition still holding rows choose from the rows it
already has, even if they do not fill its first page.

### Configuration

Standard Comet + Iceberg setup (see [Native Reader](#native-reader)) plus the write setting, which is
off by default:

```
# Standard Comet / Iceberg wiring
spark.plugins=org.apache.spark.CometPlugin
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<name>.type=hadoop                          # or hive / glue / rest / ...
spark.sql.catalog.<name>.warehouse=...

# Split-operator plan and native Parquet writer
spark.comet.write.iceberg.enabled=true

# Lets writes whose input is a local relation (INSERT ... VALUES, a local DataFrame) use the
# native writer; see "Native Parquet write eligibility" below
spark.comet.exec.localTableScan.enabled=true
```

### Supported operations

The split-operator plan supports the following operations on every Spark version Comet
supports:

- `INSERT INTO` / DataFrame `append` (`AppendData`)
- `INSERT OVERWRITE`, static and dynamic (`OverwriteByExpression`,
  `OverwritePartitionsDynamic`)
- Copy-on-write `DELETE` / `UPDATE` / `MERGE` (`ReplaceData`)
- Iceberg's `rewrite_data_files` procedure, which writes each rewritten file group as an append

For an unpartitioned copy-on-write `MERGE`, the native Iceberg writer is reachable on Spark
3.5+ when `spark.comet.exec.mergeRows.enabled=true`. With the flag disabled, the JVM
`MergeRowsExec` breaks the fully-native child chain required by `CometIcebergWriteExec`.
On Spark 4.1+, the native MergeRows path also preserves the semantic counters required by the
summary-aware writer commit contract.

For copy-on-write row-level DML, the mechanism differs by Spark version: on Spark 4.0+ the
analyzer emits operation-coded rows that Comet's writer dispatches through `ReplaceData`'s
projections, while on Spark 3.4/3.5 the rewritten rows are written as a plain row stream. The
supported set of operations is the same either way.

Merge-on-read uses Spark's `WriteDelta`, and Iceberg's JVM `DeltaWriter` writes its rows under
either plan, so it keeps Spark's stock write plan. On Spark 3.5+ the testing setting
`spark.comet.write.iceberg.splitOperator.enabled` plans it as the split plan, with the same driver
commit and reporting path, but `CometIcebergWriteExec` is never used for position-delta rows.
Spark 3.4 always leaves `WriteDelta` on Spark's stock write plan.

On Spark 4.1+ the split plan matches two further stock-Spark behaviours: MERGE metrics are
forwarded to the writer's commit (Iceberg 1.11+ records them in the snapshot summary), and
cached catalog tables are recached by name after a write so cache entries survive schema
changes.

### When Comet falls back to Spark's write operator

The rewrite is skipped — and the write runs through Spark's stock combined operator — when:

- `spark.comet.write.iceberg.enabled` is `false` (the default) and the testing setting
  `spark.comet.write.iceberg.splitOperator.enabled`, which Comet 1.1.0 documented as a user setting,
  is not on;
- Comet is disabled (`spark.comet.enabled=false`), or its native execution is
  (`spark.comet.exec.enabled=false`);
- Comet is in plan-only mode (`spark.comet.explain.planOnly.enabled=true`);
- the write is neither an Iceberg `SparkWrite` nor a supported Iceberg position-delta write;
- the table uses merge-on-read (`WriteDelta`), unless the testing setting
  `spark.comet.write.iceberg.splitOperator.enabled` is on with Spark 3.5+;
- the statement is CTAS / RTAS on Spark 3.4, where the staged exec writes inline; on Spark
  3.5+ those statements re-plan their inner append, which is intercepted normally;
- the write is a streaming write (`writeStream` into an Iceberg table);
- the write requires Spark's commit coordinator, which Comet's per-task commit protocol does
  not use;
- Comet cannot reflect the Iceberg internals needed to build the two-operator plan (for
  example an unrecognised write class, or an Iceberg 1.5 `ReplaceIcebergData` plan whose fields
  it cannot read).

In every fallback case the write is planned as if Comet were absent; there is no correctness
trade-off, only no plan change.

### Native Parquet write eligibility

When `spark.comet.write.iceberg.enabled=true`
([#5361](https://github.com/apache/datafusion-comet/pull/5361)), the `IcebergWrite` operator's
per-task Parquet write is delegated to [iceberg-rust](https://github.com/apache/iceberg-rust).
The native writer must produce the same outcome as iceberg-java — the same Parquet features,
statistics, and manifest metadata — so a write is only eligible when every table property it
depends on is one the native path reproduces exactly, and additionally only when the plan
feeding the write is fully Comet-native. For a partitioned table that plan includes the hash
distribution and local sort Iceberg requests on its partition transforms; those stay native
because the transforms themselves have native implementations (see
[Iceberg system functions](#iceberg-system-functions)). Ineligible writes run through
iceberg-java unchanged, with the reason reported as a fall-back reason in Comet's extended EXPLAIN
output. A write that runs natively shows `CometIcebergWrite` under `IcebergCommit` in the physical
plan; an ineligible write keeps `IcebergWrite`.

The native writer reads its input as Arrow batches from a Comet operator, so the write's input
must itself run in Comet. A write whose input is a local relation, such as `INSERT ... VALUES` or
`df.writeTo(...).append()` on a DataFrame built from local data, is fed by Spark's
`LocalTableScanExec`, which Comet only converts when `spark.comet.exec.localTableScan.enabled=true`
(off by default). Without that setting such writes keep `IcebergWrite` and run through
iceberg-java.

**Most Iceberg write settings are not supported.** Detection is an allowlist: a write is
eligible only when its entire effective configuration matches the table below, and anything
else — any other write-affecting property, any key added by a future Iceberg version, any
value outside the supported set, any reflection failure while inspecting the write — falls
back to iceberg-java with a reason reported in extended EXPLAIN. Checks run on the effective
configuration: table properties overlaid with `SparkWrite.writeProperties`, which is where
iceberg-java resolves per-write options and `spark.sql.iceberg.*` session overrides.

A write is eligible only when ALL of the following hold:

| Setting                                                                                                                                     | Supported values                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| resolved write format (`write-format` option overlaid on `write.format.default`)                                                            | `parquet`                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `format-version`                                                                                                                            | `1`, `2` or `3` (on `3`, writes that carry row lineage fall back; see below)                                                                                                                                                                                                                                                                                                                                                                                                    |
| `write.parquet.compression-codec` / `compression-level` / `row-group-size-bytes` / `page-size-bytes` / `page-row-limit` / `dict-size-bytes` | the sizes/limit must be positive Java ints (`Integer.parseInt` semantics — no trimming, no values past `Int.MaxValue` — matching iceberg-java, whose writer fails on anything else); `compression-level` must be a Java int within the native writer's per-codec range (zstd 1–22, gzip 0–9, brotli 0–11; ignored by both writers for snappy/lz4/none) — iceberg-java never validates the level, so an out-of-range value falls back rather than becoming a native task failure |
| `write.parquet.row-group-check-min-record-count`                                                                                            | unset or `100` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `write.parquet.row-group-check-max-record-count`                                                                                            | unset or `10000` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `write.parquet.page-version`                                                                                                                | unset or `v1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `write.parquet.shred-variants`                                                                                                              | unset or `false` (Spark 4.x / Iceberg 1.11 resolve this into every parquet write)                                                                                                                                                                                                                                                                                                                                                                                               |
| `write.parquet.variant-inference-buffer-size`                                                                                               | any value (only meaningful when shredding, which is gated)                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `write.parquet.bloom-filter-enabled.column.<col>`                                                                                           | unset or `false`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `write.metadata.metrics.*`                                                                                                                  | any value (manifest metrics are re-derived on the JVM with Iceberg's own logic)                                                                                                                                                                                                                                                                                                                                                                                                 |
| `write.spark.fanout.enabled`                                                                                                                | any value (the native writer implements both clustered and fanout modes)                                                                                                                                                                                                                                                                                                                                                                                                        |
| `write.target-file-size-bytes`                                                                                                              | any value (the two writers can choose different roll points; see accepted divergences)                                                                                                                                                                                                                                                                                                                                                                                          |
| data location URI scheme                                                                                                                    | `file`, `memory`, `s3`, `s3a`, `gs`, matched case-sensitively (`S3://` falls back). `s3`, `s3a` and `gs` need a bucket in the authority (`s3://bucket/...`), so a hostless form such as `s3:/bucket/key` falls back. `gs` only when the `FileIO` opening the data location is a `GCSFileIO`; see below                                                                                                                                                                          |
| resolved `table.locationProvider()`                                                                                                         | Iceberg's built-in `DefaultLocationProvider`                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Hadoop S3A settings for an `s3` / `s3a` data location                                                                                       | only `fs.s3a.access.key`, `secret.key`, `session.token`, `endpoint`, `endpoint.region`, and `path.style.access`, including their `fs.s3a.bucket.<data-bucket>.*` forms; any other effective `fs.s3a.*` setting falls back, apart from the ignored settings listed below                                                                                                                                                                                                         |
| Iceberg `FileIO` S3 settings for an `s3` / `s3a` data location                                                                              | the S3 endpoint, region, static/session credentials, path-style, SSE (`none`, `s3`, `kms`, or `custom`; not `dsse-kms`), assume-role, anonymous/config-chain settings parsed by the pinned iceberg-rust version, plus Comet's credential-provider class and built-in web-identity properties. When a custom provider is configured, its vendor-owned `s3.*` / `client.*` properties are also forwarded; unsupported Iceberg-defined S3 settings still fall back                 |
| partition spec                                                                                                                              | any, except a `void` field whose source column was dropped beside a live field (see below)                                                                                                                                                                                                                                                                                                                                                                                      |
| column types                                                                                                                                | any except `uuid` (Spark plans it as a string; no Arrow cast reaches `fixed(16)`) and the v3 types `variant`, `unknown`, `timestamp_ns`, `geometry` and `geography`                                                                                                                                                                                                                                                                                                             |

Within the namespaces that shape data-file bytes — `write.parquet.*` and `parquet.*` —
everything not listed above must be absent: unvetted `write.parquet.*` keys (e.g.
`bloom-filter-max-bytes`, `stats-enabled.column.*`, keys added by future Iceberg versions),
any `parquet.*` table property (including `parquet.enable.dictionary`), and any `parquet.*`
key in the session Hadoop configuration other than the reader-only
`parquet.hadoop.vectored.io.enabled` (with `HadoopFileIO`-backed output those reach
iceberg-java's writer but not the native one). Also gated explicitly: any `encryption.*` key,
`write.object-storage.enabled=true`, `write.location-provider.impl`, and `io-impl`.

Three checks look past properties at the table's instantiated state, because they can be
configured at the catalog level (or by a custom `TableOperations`) without any table or write
property changing: `table.locationProvider()` must be Iceberg's `DefaultLocationProvider`;
`table.io()` must be a recognized `FileIO` (the same allowlist the native scan uses, minus the
`EncryptingFileIO` family — the native writer produces plaintext files, so an encrypting `FileIO`
is rejected on the write side); and `table.encryption()` must be Iceberg's
`PlaintextEncryptionManager`. Anything else falls back.

A `gs` data location additionally requires that the `FileIO` actually opening it is a
`GCSFileIO` (for a `ResolvingFileIO`, the delegate it instantiates for that location,
which is a `HadoopFileIO` when the GCS `FileIO` cannot be loaded or initialized). A
`HadoopFileIO` takes its GCS credentials, endpoint and project from `fs.gs.*` in the Hadoop
Configuration, and only `fs.s3a.*` is translated into the native `FileIO`, so the native writer
could resolve a different storage identity or endpoint than the JVM writer would. That
combination falls back; a `GCSFileIO` carries its `gcs.*` settings in `FileIO.properties()`,
which are forwarded.

For an `s3` or `s3a` data location, the gate also inspects both the table FileIO's effective Hadoop
configuration and `table.io().properties()`. These are separate allowlists because Hadoop S3A
keys are translated before they reach iceberg-rust, while Iceberg `FileIO` keys are forwarded
directly. When the FileIO exposes a Hadoop configuration, its initialized values govern both the
gate and native translation, including after session or catalog options change. FileIO
implementations without a Hadoop configuration, such as `S3FileIO`, use only their initialized
properties; session and catalog Hadoop options are neither checked nor forwarded. Hadoop's built-in
`core-default.xml` values are not treated as explicit settings, but
programmatic settings and values from site or custom `*-default.xml` resources are. Spark's
session-wide S3A vectored-read and `downgrade.syncable.exceptions` compatibility settings are also
ignored, as are the S3A committer (`committer.name`, `committer.magic.enabled`,
`committer.threads`), `bulk.delete.page.size` and `experimental.input.fadvise` settings, because
none of them can alter an Iceberg data-file write request. Spark 4.1 sets `committer.name` and
`committer.magic.enabled` itself when `spark-hadoop-cloud` is on the classpath. Ignored settings are
matched by their plain `fs.s3a.<name>` keys, so a per-bucket `fs.s3a.bucket.<data-bucket>.<name>`
form still falls back. Unknown explicit
`fs.s3a.*`, `s3.*`, or `client.*` settings therefore fall back at planning time instead of being
silently ignored by the native storage backend. The exception is a vendor-owned `s3.*` /
`client.*` property when
`s3.comet.credential.provider.class` is configured: the provider receives the unfiltered FileIO
bag and can consume that property. Iceberg-defined settings that the native storage path cannot
honour still fall back even with a provider. A per-bucket Hadoop setting counts only for the exact
data-bucket name, so configuration for a longer dotted bucket does not by itself disable the
native write. If Iceberg's AWS property classes cannot be loaded, vendor `s3.*` / `client.*` keys
fall back too and planning still completes. The fall-back reason reports only sorted property
names, never their values, so credentials and tokens do not enter EXPLAIN or plan logs.

On a format-version 3 table with Iceberg 1.10 or newer, a write that rewrites existing rows falls
back. Copy-on-write `DELETE`, `UPDATE` and `MERGE` and `rewrite_data_files` write the row lineage
columns `_row_id` and `_last_updated_sequence_number` into the new data files, so that rewritten
rows keep their ids, and the native writer does not write those columns. Appends, overwrites,
`CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` write the data columns only, and
Iceberg assigns the new rows their ids when it commits, so those run natively. Earlier Iceberg
versions write no lineage columns, so the rule never applies to them.

A format-version-1 table keeps a dropped partition field as a `void` field, and its source column
can be dropped afterwards. A spec that mixes such a field with a live one falls back: iceberg-java
cannot write through it either, so the write fails with iceberg-java's own error rather than in
the native writer ([#6141](https://github.com/apache/datafusion-comet/issues/6141)). A spec whose
fields are all `void` writes unpartitioned and stays eligible.

Other `write.*` properties are intentionally not gated because they cannot make the native
writer produce different data files: distribution and ordering settings shape the Spark plan
identically on both paths, WAP / branch / snapshot properties act on the JVM committer,
`write.avro.*` / `write.orc.*` apply only to formats already excluded, and merge-on-read
settings route the write through `WriteDelta`, whose task writer remains iceberg-java rather
than the native writer. Most native eligibility rules are pinned by
`CometIcebergWriteDetectionSuite`.

Manifest `DataFile` metrics are assembled on the JVM before commit: each written file's
metrics are re-derived from its parquet footer through the version-matched
`ParquetUtil.footerMetrics` and `MetricsConfig.forTable`, with float/double NaN counts and
bounds carried over from the native writer's tracked state. iceberg-java's metadata decisions
— metrics modes, the inferred-column cap
(`write.metadata.metrics.max-inferred-column-defaults`), bound truncation, and list/map bounds
suppression — are therefore applied by iceberg-java's own code regardless of what the native
writer reports.

iceberg-java's writer takes these metrics from the footer it still holds in memory, but the native
path reads each footer back from storage: two small reads per written file, one for the footer's
length and one for the footer itself, made one file after another before the task finishes. On S3
or GCS each read is a GET request, so the task waits two request round trips per file. That is
small next to uploading a large file, but a write that produces many small files, such as a fanout
write over many partitions, pays it for every one of them.

### Failure handling

Eligibility is decided entirely at plan time. That includes the reflection surface: every
iceberg-java class, method, and constructor the executor-side commit-message assembly uses is
eagerly resolved by the eligibility gate on the driver, so an Iceberg release that moves any
of them declines the native path with a fall-back reason instead of failing tasks mid-write.
Once planned, the physical plan is fixed — there is no per-task re-decision or runtime switch
back to the JVM writer.

When a native write fails partway through a task (an object-store error, a data-dependent cast
failure), the error propagates as an ordinary Spark task failure and Spark's task retry
re-executes it — through the native writer again. Retries cannot collide: each attempt's task
attempt id is embedded in its data file names.

The native writer's buffers are charged to Comet's memory pool, the off-heap budget Comet's other
native operators draw on, where iceberg-java's buffers sit on the JVM heap. A fanout write keeps a
data file open for every partition a task writes to. Each open file holds the row group it is
writing in memory, up to `write.parquet.row-group-size-bytes`, and on S3 or GCS also the last row
group it flushed, which is uploaded once the next one is complete or the file closes. Each
partition also holds the rows that have not reached its file yet. So a task writing to many
partitions needs memory in proportion to them. When the pool cannot grant it, the task writes out
and closes the partitions holding the most memory until what is left fits, and a closed
partition's next rows open a new file. The write then finishes with more, smaller files than
iceberg-java's would. When every partition keeps getting rows, a task given an eighth of the memory
its partitions need writes four to five files per partition instead of one, however many partitions
it writes to. The `files closed early to free memory` metric of its `CometIcebergWrite` operator
counts the files it closed early. Disabling the fanout writer (`write.spark.fanout.enabled=false`)
avoids the extra files: Spark then sorts each task's rows by partition, and the task keeps one file
open at a time. A larger `spark.memory.offHeap.size` also helps, and Iceberg's `rewrite_data_files`
procedure compacts small files once they are written.

A write that keeps one file open, unpartitioned or clustered, has no partition to close. When that
file outgrows the pool, the task fails with a `CometNativeException` reading
`Additional allocation failed for IcebergWriteExec` instead of exceeding the executor's memory, and
Spark retries it like any other task failure. A smaller `write.parquet.row-group-size-bytes` or a
larger `spark.memory.offHeap.size` lets such a write fit.

Partial results are never committed. The commit set is exactly the commit messages returned by
successful tasks — a failed task contributes none — and if the job fails, the driver-side
commit operator aborts without committing anything. A failed task attempt also deletes the
data files it created, as iceberg-java's writer abort does. The native writer records every
location it hands to a file writer, and cleanup has no ownership gap. Native keeps its cleanup
guard armed after yielding the output batch. The JVM reads those locations into a task failure
listener first, then polls the native output to EOF; that EOF is the acknowledgement that lets
native disarm. A failure in this narrow handoff window can therefore trigger best-effort deletion
on both sides, which is harmless. The native guard still covers a failed write, a task torn down
before the write completed — for example because the operator feeding it threw — and a failure
encoding the manifest or building the output batch. The handoff does not depend on decoding the
manifest: the locations are read before the manifest is decoded, so a failure in that decode still
cleans up. Cleanup never masks the original failure; anything it misses is invisible to every
reader, since readers resolve files through committed manifests only, and is reclaimed by Iceberg's
normal `remove_orphan_files` maintenance.

When one task fails, the tasks that had already completed leave committed-nothing data files
too. The committer collects each task's commit message as that task finishes, so on a job
failure it aborts with the completed messages and deletes their data files through the table
`FileIO`. (From Iceberg 1.6 on, Iceberg's own `SparkWrite.abort` skips cleanup unless a commit
failed with a cleanable error, so on the stock path those files are left for
`remove_orphan_files`. Iceberg 1.5, which the Spark 3.4 profile pins, deletes them.) A
failure during the driver-side commit itself behaves exactly as on the stock path: the commit
messages carry genuine `SparkWrite$TaskCommit` objects, so Iceberg's own `SparkWrite.abort`
cleanup (which deletes the files listed in the commit messages for cleanable failures) applies
unchanged.

### Accepted divergences

Some differences between parquet-mr and the pinned parquet-rs / iceberg-rust are unconditional —
they apply to every native write and cannot be configured away. Enabling
`spark.comet.write.iceberg.enabled` accepts them. They fall into two classes with very
different blast radius: differences confined to the physical bytes of a data file (cosmetic —
no reader decision is based on them), and differences visible in manifest metadata (these
outlive the write and feed later readers' pruning decisions, so each one is analyzed
individually below).

#### Physical file layout only (cosmetic)

No Iceberg reader bases a planning or correctness decision on these; they change the bytes of
a data file but not what any reader computes from it:

- Footer key-value metadata differs: native files carry an `ARROW:schema` entry and no
  `iceberg.schema` entry; iceberg-java files are the opposite.
- The Parquet root schema element is named `arrow_schema` (iceberg-java: `table`).
- `created_by` reads `Apache Iceberg <version> (Comet)`, where iceberg-java's files carry
  parquet-mr's version string.
- No page CRC checksums: iceberg-java writes one in every page header, and parquet-rs writes none.
  Neither writer puts statistics in page headers; page pruning uses the column index, which both
  writers produce.
- Dictionary-encoded pages are labeled `RLE_DICTIONARY` (parquet-mr v1 files: `PLAIN_DICTIONARY`).
- The native writer uses parquet-mr's size accounting to choose dictionary encoding: a column
  whose sampled rows show the dictionary saving no space is written plain, with no dictionary page,
  instead of carrying a dictionary page that every selective read of it would have to fetch
  ([#6114](https://github.com/apache/datafusion-comet/issues/6114)). The native writer decides
  once per partition, normally from that partition's first page of rows in the task, and keeps the
  decision for every file and row group it writes for the partition; parquet-mr decides again
  for every row group. If buffered rows reach `write.parquet.row-group-size-bytes`, the choice
  uses the rows collected so far. In a fanout write, all partitions share this threshold, so
  each partition still buffering can make its choice before it has a full first page. Later
  rows could have changed parquet-mr's decision, so this difference is not limited to columns
  close to the size cut-off. Where the page size rather than `write.parquet.page-row-limit` ends a
  column's first page, the native page ends at the first row past parquet-mr's size threshold,
  while parquet-mr only ends it at its next periodic size check, so a column close to the
  cut-off can be decided the other way. Close to the cut-off both encodings take about the same
  space. A column that keeps its dictionary and later fills it falls back to plain on both
  writers, but parquet-rs's dictionary page then holds every entry, where parquet-mr's holds
  only the entries earlier pages used.
- Row-group boundaries: both writers close a row group by size, but parquet-mr checks its buffered
  size only at record-count checkpoints, while parquet-rs checks its own estimate as it writes each
  batch, so the boundaries land on different rows.
- File names: both writers name files `<partition>-<task attempt>-<operation>-<count>`, counting
  per task. The native writer zero-pads the task attempt id and counts from `00000`; iceberg-java
  leaves the id unpadded and counts from `00001`.
- Partition directory names match iceberg-java 1.8+'s `PartitionSpec.partitionToPath` for every
  partition type the native writer accepts. On Iceberg 1.5.x,
  which the Spark 3.4 profile pins, iceberg-java itself spelled `timestamp` and `timestamptz`
  directories with `LocalDateTime.toString()` / `OffsetDateTime.toString()`
  (`ts=1969-12-31T23:59:58.500Z`) and left the partition field name unescaped; Comet uses the
  1.8+ spelling on every profile. Distinct partition values still get distinct directories in all
  cases, and no reader parses these names: files are resolved through committed manifests.
- File rolling lands on the same row grid as iceberg-java but not necessarily on the same row.
  Both writers re-check the current file's size against `write.target-file-size-bytes` once
  every 1000 rows of that file (iceberg-java's `RollingFileWriter.ROWS_DIVISOR`; Comet hands the
  iceberg-rust writer rows in 1000-row units, per partition file, to get the same grid), so each
  writer rolls only on a 1000-row boundary of its own file.
  The shared grid is all that is shared. What each writer compares against the target differs —
  flushed bytes plus parquet-rs's estimate of the open row group, versus parquet-mr's file
  position plus its buffered size — and the two use different threshold comparisons. These are
  independent size estimates, so nothing bounds how far apart the two writers' roll points are:
  they may cross the target several grid steps apart, and the resulting files can differ in row
  count by an arbitrary number of 1000-row blocks. Do not rely on file-layout parity between the
  two writers; rely only on each file rolling on its own 1000-row boundary.
- A fanout write that the memory pool cannot hold closes partitions before the task ends (see
  [Failure handling](#failure-handling)), where iceberg-java's fanout writer keeps every file open
  until then. So a partition can have more, smaller files than iceberg-java writes, and a file
  closed early ends off the 1000-row grid, as the last file of a task does. A partition closed
  before its rows filled a first page makes its dictionary choice from the rows it has.
- A fanout write lists a task's data files in file-path order, where iceberg-java lists them in
  its own `StructLikeMap` iteration order. Both are stable across runs, and neither is a
  documented ordering, but the manifest entry order becomes the scan-task order and so the row
  order of an unordered `SELECT *`. On a format-version 3 table it also decides which row ids the
  commit gives each file's rows, so the same rows can get different `_row_id` values from the two
  writers. The ids are unique either way, and across tasks iceberg-java's own assignment already
  depends on the order in which the tasks finish. Only the sorted order is reproducible on the
  native path: the order the native writer closes its files in depends on the order rows arrive in
  and on which partitions it closes early to free memory. Clustered and unpartitioned writes append
  in creation order on both paths and are unaffected.
- Compressed page bytes are implementation-defined: the codec and any explicit level are
  translated, and a `zstd` table with no level is written at parquet-mr's default level of 3, but
  parquet-rs and parquet-mr embed different encoder implementations (LZ4 framing, for one), and
  their pages and row groups end at different rows, so byte-identical output is not achievable even
  for a default `zstd` table. The decompressed data is identical. For the same reason,
  codec-level side channels (`zlib.compress.level`, `compression.brotli.quality`,
  `io.compression.codec.zstd.level` — the last is present in every Hadoop configuration by
  default) are not gated: they can only shift compressed bytes, which are already accepted as
  divergent.

#### Manifest metadata visible to later readers

Manifest metrics drive partition- and file-level pruning for every future reader of the table,
so a divergence here would outlive the write. This class is deliberately kept almost empty:
`DataFile` metrics are not taken from the native writer's manifest but re-derived on the JVM
from each written file's parquet footer through iceberg-java's own `ParquetUtil.footerMetrics`
and `MetricsConfig.forTable` (see above). Metrics modes, lower/upper bound truncation, the
null-count conventions, and list/map bounds suppression are therefore iceberg-java's code
making iceberg-java's decisions. The parity tests write the same rows through both writers and
compare the committed `readable_metrics` (value, null and NaN counts, and lower and upper bounds)
for the column types they cover. Two footer-derived values can still differ from what
iceberg-java's _writer-tracked_ state would have recorded, and both are analyzed safe:

- Float/double bounds involving zero may differ in sign: parquet-rs normalises footer
  statistics to min `-0.0` / max `+0.0` (the parquet-format recommendation), while
  iceberg-java's writer-tracked bounds preserve the exact sign it saw. The native path's
  manifest bounds inherit the normalised values — a strictly conservative widening that cannot
  change pruning decisions.
- On Iceberg 1.9+, manifest `value_counts` / `null_value_counts` for float/double columns
  nested under a nullable struct count rows whose parent struct is null (they come from the
  parquet footer), while iceberg-java's writer-tracked counts do not. The native counts are the
  correct ones. iceberg-java's counts leave out rows whose struct is null. When every non-null
  struct in a file holds a value, iceberg-java records a null count of 0, and `IS NULL` prunes a
  file that contains matching rows
  ([apache/iceberg#17561](https://github.com/apache/iceberg/issues/17561)). The native counts
  never prune such a file. `IS NOT NULL` pruning is the same for both.

All content not listed above — the logical data, statistics values, and manifest metadata —
must match iceberg-java exactly, or the write falls back. The open bugs in which a native write
still differs are listed in
[Known Issues](#writes-that-differ-from-spark).

## Known Issues

This section lists open issues in which Comet's Iceberg reads and writes give a different result
from Spark or iceberg-java, or fail where they succeed, without falling back. Each entry links to
its tracking issue. The cases where Comet falls back to Spark are listed under
[Current limitations](#current-limitations) and [When Comet falls back to Spark's write
operator](#when-comet-falls-back-to-sparks-write-operator). Storage-specific issues are listed under
the [S3](datasources.md#s3-limitations) and [Azure](datasources.md#azure-limitations) limitations.

Which entries apply depends on which native paths are enabled:

| Native path    | Setting                                  | Default |
| -------------- | ---------------------------------------- | ------- |
| Iceberg scan   | `spark.comet.scan.icebergNative.enabled` | `true`  |
| Iceberg writer | `spark.comet.write.iceberg.enabled`      | `false` |

### Reads that differ from Spark

- **Migrated tables that use a name mapping.** The `migrate`, `snapshot` and `add_files`
  procedures set `schema.name-mapping.default` for files without Iceberg field ids. When a query
  also reads a top-level primitive column, the native scan returns NULL for nested fields (struct
  fields, list elements and map values). Filters on those fields then select the wrong rows
  ([#6790](https://github.com/apache/datafusion-comet/issues/6790)). The fix needs an iceberg-rust
  upgrade ([#6934](https://github.com/apache/datafusion-comet/issues/6934)).
- **Data files with neither field ids nor a name mapping.** Spark reads every nested field of such
  a file as NULL, and the native scan returns the values stored in the file
  ([#6794](https://github.com/apache/datafusion-comet/issues/6794)).
- **Data files that store a narrower type than their column.** Files written before a type
  promotion, such as `INT` to `BIGINT`, keep the older type, and files imported by `add_files`,
  `migrate` or `snapshot` can store any narrower type. iceberg-rust compares such a file with a
  filter literal incorrectly, so comparisons other than `=` pushed to the native scan can drop
  matching rows. Comet avoids this for promotions that the schema history records. When only part
  of a condition is pushed, it also avoids this for promotions made before Iceberg 0.12 and for
  tables with a name mapping, which those procedures set. Other cases can still drop rows
  ([#7138](https://github.com/apache/datafusion-comet/issues/7138)).
- **`VERSION AS OF` reads of a column renamed after the snapshot.** The renamed column reads as
  NULL, and two columns that swapped names return each other's values
  ([#6822](https://github.com/apache/datafusion-comet/issues/6822)).
- **Reflection failures while the scan is planned.** If Comet fails to read the table's schema or
  metadata through reflection, it continues with an empty or partial result instead of falling
  back, which can resolve a column to a different field id
  ([#5257](https://github.com/apache/datafusion-comet/issues/5257)).

### Writes that differ from Spark

The native Iceberg writer is disabled by default. With `spark.comet.write.iceberg.enabled=true`:

- **Brotli tables.** The native writer commits brotli-compressed files. JVM Parquet readers can
  read them only if Hadoop's `BrotliCodec` is on the classpath. iceberg-java fails the write
  instead ([#6960](https://github.com/apache/datafusion-comet/issues/6960)).
- **`write.parquet.compression-codec=none`.** The native writer writes the table uncompressed, and
  iceberg-java rejects the value. The native writer also trims or lowercases a few other settings
  that iceberg-java reads as written
  ([#7008](https://github.com/apache/datafusion-comet/issues/7008)).

The physical differences that every native data file has, and why they are safe, are listed in
[Accepted divergences](#accepted-divergences).

### Reads that fail where Spark succeeds

- **A data file that omits an optional nested field.** The native scan fails on a data file that
  lacks an optional struct field, which a write with `write.spark.accept-any-schema=true` can
  produce without any schema change
  ([#7033](https://github.com/apache/datafusion-comet/issues/7033)).
- **`VERSION AS OF` reads of a column whose type was promoted after the snapshot**
  ([#6993](https://github.com/apache/datafusion-comet/issues/6993)).
- **Column names that only newer Unicode case data folds together**, such as a pair of Vithkuqi
  letters, which Unicode 14 added. Queries on such a table fail with a duplicate-field error
  ([#5540](https://github.com/apache/datafusion-comet/issues/5540)).
- **Iceberg tables whose metadata location has no host.** The native Iceberg scan skips the
  credential provider configured with `s3.comet.credential.provider.class`
  ([#6536](https://github.com/apache/datafusion-comet/issues/6536)).

### Writes that fail where Spark succeeds

With `spark.comet.write.iceberg.enabled=true`:

- **A write that keeps one file open and outgrows the off-heap pool.** An unpartitioned or
  clustered write fails its task when its open file outgrows the pool, where iceberg-java writes
  it on the JVM heap. [Failure handling](#failure-handling) describes the
  error and the settings that let such a write fit.

### Differences that are not Comet bugs

- **iceberg-java 1.9 and later: `IS NULL` on a float or double field under a NULL struct.**
  iceberg-java leaves the rows where the struct is NULL out of the field's null count, so a filter
  such as `WHERE s.x IS NULL` can skip data files that hold matching rows, with or without Comet
  ([apache/iceberg#17561](https://github.com/apache/iceberg/issues/17561)). Files written by Comet's
  native Iceberg writer are not affected.
- **iceberg-java before 1.12: a NULL struct whose first field is an identity partition source.**
  iceberg-java reads the struct as a struct of NULLs, and Comet returns NULL, as iceberg-java 1.12
  does. [apache/iceberg#17320](https://github.com/apache/iceberg/pull/17320) is titled for nested
  initial defaults, but it fixes this case too: it keeps one file column in the read, so the
  struct's definition level survives when every projected field is a constant, and an identity
  partition value is such a constant.

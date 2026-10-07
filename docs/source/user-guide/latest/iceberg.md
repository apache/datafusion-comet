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
- NULL checks (`IS NULL`, `IS NOT NULL`) on primitive columns
- `IN` list operations (`NOT IN` is applied after the scan)
- `BETWEEN` operations

NULL checks on struct, array, and map columns still use native scans and return correct
results, but are not pushed into iceberg-rust, which binds accessors only for primitive fields.
These residuals provide no native row-group pruning, nor do conjunctions containing them;
safe partial pruning is tracked in [#5883](https://github.com/apache/datafusion-comet/issues/5883).

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
- HDFS (`hdfs`), through iceberg-rust's own HDFS client. See
  [Object store configuration (HDFS)](#object-store-configuration-hdfs) for NameNode
  configuration and the cases that fall back to Spark

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

### Object store configuration (HDFS)

`hdfs://` tables are read and written through iceberg-rust's `hdfs-native` backend, a pure-Rust HDFS RPC client. This is **not** the libhdfs/JNI client that the plain-Parquet native scan uses for `spark.hadoop.fs.comet.libhdfs.schemes`: the two clients live in the same process but connect independently, so an Iceberg table and a plain Parquet file on the same cluster each open their own connections. The Rust client still reads `core-site.xml` / `hdfs-site.xml` from `$HADOOP_CONF_DIR` (or `$HADOOP_HOME/etc/hadoop`) on each executor. It does not reuse the JVM's `UserGroupInformation` or the delegation tokens Spark obtains: it authenticates as the executor process, through the system `libgssapi_krb5` and the default Kerberos ticket cache, or otherwise as `HADOOP_USER_NAME`. It does read a token file named by `HADOOP_TOKEN_FILE_LOCATION`, but it looks HDFS delegation tokens up under the service `ha-hdfs:nameservice` of its own synthetic nameservice (see below), so a token issued for the cluster is not expected to match. Comet's tests do not cover a secured cluster.

The Rust client finds the NameNode from the authority of each location. An authority with a port (`hdfs://nn.example.com:8020/...`) is dialed as is, so a single-NameNode cluster addressed that way needs no configuration. An authority without a port (`hdfs://nameservice1/...`) is a logical nameservice, which the client resolves only through a declaration in the properties it is given: `hdfs.name-node.<nameservice>`, a comma-separated `host:port` list, or Hadoop's `dfs.ha.namenodes.<nameservice>` and `dfs.namenode.rpc-address.<nameservice>.<nn>` keys passed as `hadoop.*` properties. It does not look the nameservice up in `$HADOOP_CONF_DIR`, and it has no default port. The global `hdfs.name-node` property serves only locations with no authority, so it does not apply to a location that names a NameNode or nameservice.

Comet writes this declaration for you, as `hdfs.name-node.<authority>`, on the driver, from the Hadoop configuration of the table's `FileIO` (which carries the catalog's `hadoop.*` overrides, as the JVM reader sees them), or from the session Hadoop configuration when the `FileIO` has none. The authority is that of the table's data files, which Iceberg allows to differ from the metadata location, or that of the data location for a write. For an HA nameservice, Comet reads `dfs.ha.namenodes.<nameservice>` and each `dfs.namenode.rpc-address.<nameservice>.<nn>` and declares the same failover list the JVM client would use. Each `rpc-address` must be `host:port`; the JVM client rejects one without a port as well. The list is all-or-nothing: if any NameNode named in `dfs.ha.namenodes.<nameservice>` has no `host:port` `rpc-address`, Comet declares nothing rather than a partial failover list, and the scan or write falls back (see below). A portless authority that the Hadoop configuration does not name as a nameservice (through `dfs.nameservices` or `dfs.ha.namenodes.<authority>`) is a plain host, which Comet declares on port 8020, Hadoop's default NameNode RPC port, as the JVM client dials it. Nothing needs to be set as long as the standard HDFS client configuration is on the driver's classpath. To override the derived list, or to declare a nameservice the Hadoop configuration does not resolve (a federated nameservice without HA, for example), set the property on the catalog:

```shell
    --conf spark.sql.catalog.hdfs_cat=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.hdfs_cat.type=hadoop \
    --conf spark.sql.catalog.hdfs_cat.warehouse=hdfs://nameservice1/warehouse \
    --conf spark.sql.catalog.hdfs_cat.hdfs.name-node.nameservice1=nn1.example.com:8020,nn2.example.com:8020
```

An explicit catalog value always wins over the one derived from the Hadoop configuration. It reaches Comet through the table's `FileIO` properties, so it takes effect only with a catalog that initializes its `FileIO` with the catalog properties: the Hadoop, JDBC and REST catalogs do, and the Hive catalog does only when `io-impl` is set. Every entry must be `host:port` (an IPv6 literal in brackets, such as `[::1]:8020`). The `hdfs://` prefix is optional, and spaces around an entry and a trailing `/` are tolerated. Comet does not add a default port to an entry you write yourself. Hadoop's own keys can be set the same way, as `hadoop.dfs.ha.namenodes.<nameservice>` and `hadoop.dfs.namenode.rpc-address.<nameservice>.<nn>`, and they win over `hdfs.name-node.<nameservice>`.

Other HDFS client settings can be forwarded with `hadoop.`-prefixed catalog properties (for example `spark.sql.catalog.hdfs_cat.hadoop.dfs.client.use.datanode.hostname=true`), which reach Comet through the same catalogs and override the values loaded from `$HADOOP_CONF_DIR`. The client connects through a nameservice of its own rather than the cluster's, so settings keyed by the cluster's nameservice name, such as `dfs.client.failover.proxy.provider.<nameservice>` or `dfs.client.failover.random.order.<nameservice>`, do not apply to it.

The following cases fall back to Spark at planning, instead of failing on an executor:

- A location with no authority (`hdfs:///warehouse/...`) falls back for both reads and writes, even when `hdfs.name-node`, `hdfs.host`, `hdfs.port` or `hadoop.fs.defaultFS` is set. Comet requires the NameNode or nameservice in the location itself.
- A catalog `hdfs.name-node` or `hdfs.name-node.<nameservice>` entry that is not `host:port`, such as one without a port, falls back, and the reason names the property and the entry. This holds for every such property the catalog sets, including one for another nameservice, because iceberg-rust parses them all when it opens the storage.
- A portless authority that neither the Hadoop configuration nor the catalog declares falls back. Typically the Hadoop configuration names the nameservice, but a `dfs.namenode.rpc-address.<nameservice>.<nn>` entry is missing or has no port, or the nameservice has no `dfs.ha.namenodes.<nameservice>`: fix the Hadoop configuration, or set `hdfs.name-node.<nameservice>` on the catalog.
- An authority with userinfo (`hdfs://user@nn.example.com:8020/...`), or with a port that is not a number from 1 to 65535, falls back.
- A read whose data and delete files carry more than one `hdfs://` authority falls back, because Comet declares the NameNodes of a single authority per scan. Authorities are compared as written, so one cluster under two spellings counts as two.
- A nameservice whose NameNodes are found through DNS (`dfs.client.failover.resolve-needed.<nameservice>=true`) falls back: the native client dials each `rpc-address` once and does not expand a name to its addresses, so it would keep a single failover target.
- A portless authority that is not a plain ASCII host or nameservice name (an IPv6 literal without a port, or a non-ASCII name) falls back, because iceberg-rust looks such names up in a rewritten form.
- An `hdfs.port` that is not a port, an `hdfs.host` that does not form `host:port` with it, or a bare `hadoop.` property falls back, because iceberg-rust rejects them for every path when it opens the storage.
- A catalog that declares a nameservice named `nameservice` (as `hdfs.name-node.nameservice` or `hadoop.dfs.ha.namenodes.nameservice`) falls back for any other location, because that is the name of the client's own synthetic nameservice and the declaration would replace its NameNodes.

A host, with or without a port, is not validated, so a mistyped host still fails when a task first opens a file.

Failover follows a NameNode that refuses connections or answers as a standby, which the HA test covers before and after a failover and with a NameNode shut down. Two limits of the native client remain. It has no NameNode connect or RPC timeout, so a NameNode that stops answering without refusing connections can stall a read or write rather than fail over. And it counts every failover against `dfs.client.failover.max.attempts` but sleeps only once per round over the list, so with more than two NameNodes (observers included) it waits less time than the JVM client for a new active. To see why a table fell back, set `spark.comet.explain.fallback.enabled=true`: the driver log then lists the reason each stage could not run in Comet (see [Understanding Comet Plans](understanding-comet-plans.md)).

### Current limitations

The following scenarios will fall back to the JVM Iceberg reader:

- Iceberg table spec v4 or newer
- v3 tables with columns that declare an initial default value
- v3 column types the native reader cannot read (`geometry`, `geography`, `unknown`), and
  `variant` columns the query reads (on Spark 4.0+, a table whose `variant` columns are not
  projected is read natively)
- Encrypted tables with 192-bit data keys (no AES-192-GCM in the underlying crypto)
- Delete files in a format other than Parquet or Puffin (Avro or ORC positional/equality deletes)
- Tables backed by Avro or ORC data files (only Parquet is accelerated)
- Scans whose data or delete files span more than one S3 bucket (the native reader uses one
  object-store configuration per scan)
- Scans whose data or delete files span more than one HDFS authority (Comet declares the
  NameNodes of one authority per scan)
- HDFS locations the native client could not reach: no authority (`hdfs:///...`), an
  `hdfs.name-node` or `hdfs.name-node.<nameservice>` entry that is not `host:port`, or a
  nameservice that neither the Hadoop configuration nor the catalog declares with `host:port`
  NameNodes, and the other HDFS cases listed in
  [Object store configuration (HDFS)](#object-store-configuration-hdfs)
- Tables partitioned on `BINARY` or `DECIMAL` (with precision >28) columns
- Scans with residual filters using `truncate`, `bucket`, `year`, `month`, `day`, or `hour`
  transform functions (partition pruning still works, but row-level filtering of these
  transforms falls back)
- Scans that read a struct, array, or map column with a nested field that schema evolution added
  or renamed. The native reader cannot yet match such a field to data files written before the
  change. The check uses the table's schema history, so the fallback stays after those files are
  rewritten

Writes are not covered by this list. By default Iceberg writes use Spark's own writer; see
[Iceberg Writes](iceberg-writes.md) for the experimental native writer and when it applies.

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
these functions native the whole sub-plan feeding the [native Iceberg writer](iceberg-writes.md)
stays in Comet. A `numBuckets` or `width` argument that is not a positive integer literal makes
the expression fall back to Spark.

### Task input metrics

The native Iceberg reader populates Spark's task-level `inputMetrics.bytesRead` (visible in the Spark UI Stages tab) using the `bytes_read` counter from iceberg-rust's `ScanMetrics`. This counter includes bytes read from both data files and delete files. The scan's SQL metrics, including Iceberg's planning counters, are listed in the [Metrics Guide](metrics.md#cometicebergnativescan).

Iceberg Java does not explicitly report `bytesRead` to Spark's task input metrics. On the iceberg Java path, any `bytesRead` value comes from Hadoop's filesystem-level I/O counters, not from Iceberg itself. Because Comet's native reader and the Hadoop filesystem use different counting mechanisms, the exact byte counts will differ between the two paths.

The task-level `inputMetrics.recordsRead` is the scan's `number of output rows`. iceberg-rust applies the residual predicate Comet hands it as a row filter inside the scan, so both count the rows that pass it. They can therefore be lower than the `BatchScan` figures on the Iceberg Java path, where every row leaves the scan and is filtered by the `Filter` above it.

Iceberg's `number of row deletes applied` is not reported for the native scan. It counts deletes applied by Iceberg Java's reader, and iceberg-rust's `ScanMetrics` exposes bytes read only, so Comet omits it instead of showing a constant 0.

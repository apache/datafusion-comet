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

# Supported Spark Data Sources

## File Formats

### Parquet

Parquet scans run in Rust via DataFusion if all data types in the schema are supported. When the scan
falls back to Spark, enabling `spark.comet.convert.parquet.enabled` will immediately convert the data into
Arrow format, allowing the Comet pipeline to take over after that, but the process may not be efficient.

### Apache Iceberg

Comet accelerates Iceberg scans of Parquet files and has an experimental, opt-in native Iceberg writer.
See the [Iceberg Guide] and [Iceberg Writes](iceberg-writes.md) for more information.

[iceberg guide]: iceberg.md

### CSV

Comet provides experimental Rust-based CSV scan support. When `spark.comet.scan.csv.v2.enabled` is enabled, CSV files
are read in Rust for improved performance. This feature is experimental and performance benefits are
workload-dependent. Only Spark's DataSource V2 CSV scan is accelerated, and Spark reads CSV through the V1 API by
default, so also remove `csv` from `spark.sql.sources.useV1SourceList`.

Alternatively, when `spark.comet.convert.csv.enabled` is enabled, data from Spark's CSV reader is immediately
converted into Arrow format, allowing the Comet pipeline to take over after that.

### JSON

Comet does not provide a Rust-based JSON scan, but when `spark.comet.convert.json.enabled` is enabled, data is immediately
converted into Arrow format, allowing the Comet pipeline to take over after that.

### Other Spark inputs

Comet can also convert the output of these Spark inputs to Arrow format, so that the operators
above them can run in Comet. Only `spark.comet.convert.oneRowRelation.enabled` is on by default,
because the row it converts has no columns.

- `spark.comet.convert.range.enabled`: `spark.range` and SQL `range()`, for ranges that Comet does
  not generate natively with `spark.comet.exec.range.enabled`.
- `spark.comet.convert.inMemoryCache.enabled`: in-memory cached tables that Comet's native cache
  scan does not read, such as tables cached in Spark's default format.
- `spark.comet.convert.rdd.enabled`: a DataFrame created from an RDD of rows, for example with
  `spark.createDataFrame(rdd, schema)`.
- `spark.comet.convert.oneRowRelation.enabled`: the single row that a query without a `FROM`
  clause, such as `SELECT 1`, reads.
- `spark.comet.convert.rowDataSource.enabled`: Data Source V1 relations that are not file-based,
  such as JDBC tables, which Spark scans with `RowDataSourceScanExec`.

To convert any other leaf operator, such as the scan of a Data Source V2 connector or of a file
format other than Parquet, JSON and CSV, set `spark.comet.sparkToColumnar.enabled=true` and name the
operator in `spark.comet.sparkToColumnar.supportedOperatorList` by its Spark class name without the
`Exec` suffix, such as `BatchScan` or `FileSourceScan`.

### Spark-to-Comet conversion types

Spark-to-Comet conversion supports `ARRAY<STRING>` and `MAP<STRING,STRING>` with binary
string semantics, both as top-level fields and inside supported structs. Arrays and maps may
be null; array elements and map values may also be null. Map keys must be non-null.
This applies to Spark row and columnar inputs when conversion is enabled for the source.
Other array element types, other map key/value types, nested collections, and non-binary
string collations remain unsupported at this conversion boundary. Source defaults are unchanged.

This includes row-backed `ExistingRDD` inputs when `spark.comet.convert.rdd.enabled=true`. Spark
still produces the RDD rows; conversion lets eligible downstream operators execute in Comet.

The same types apply to the output of typed `Dataset` operations, such as `map`, which Comet
converts when `spark.comet.convert.typedDataset.enabled=true`. A column of any other type keeps
the operators above the typed operation on Spark.

## Data Catalogs

### Apache Iceberg

See the dedicated [Comet and Iceberg Guide](iceberg.md).

## Supported Storages

Comet supports most standard storage systems, such as local file system and object storage.

### HDFS

The Apache DataFusion Comet Rust-based reader seamlessly scans files from remote HDFS for [supported formats](#supported-spark-data-sources)

```{warning}
HDFS support is experimental and is not covered by continuous integration. Comet reads HDFS through
`libhdfs`, which registers a thread-local destructor that detaches the calling thread from the JVM
regardless of which component attached it
([HDFS-16021](https://issues.apache.org/jira/browse/HDFS-16021), still open upstream). Comet
attaches its own worker threads, so a worker that has read from HDFS can crash the JVM with a
`SIGSEGV` when it later exits
([#5023](https://github.com/apache/datafusion-comet/issues/5023)). The crash surfaces well after
the HDFS read itself, typically while an unrelated query is running.
```

Native Iceberg scans do not support HDFS-backed tables; those scans fall back to Spark. See the
[Comet and Iceberg Guide](iceberg.md).

### Building Comet with HDFS support

To build Comet with remote HDFS support it is required to have a JDK installed.

Example:
Build a Comet for `spark-4.1` provide a JDK path in `JAVA_HOME`
Provide the JRE linker path in `RUSTFLAGS`, the path can vary depending on the system. Typically JRE linker is a part of installed JDK

```shell
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
make release PROFILES="-Pspark-4.1" RUSTFLAGS="-L $JAVA_HOME/libexec/openjdk.jdk/Contents/Home/lib/server"
```

Start Comet with HDFS support as [described](installation.md/#run-spark-shell-with-comet-enabled)
and add additional parameters

```shell
--conf spark.hadoop.fs.defaultFS="hdfs://namenode:9000" \
--conf spark.hadoop.dfs.client.use.datanode.hostname = true \
--conf dfs.client.use.datanode.hostname = true
```

Query a struct type from Remote HDFS

```shell
spark.read.parquet("hdfs://namenode:9000/user/data").show(false)

root
 |-- id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- personal_info: struct (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- lastName: string (nullable = true)
 |    |-- ageInYears: integer (nullable = true)

25/01/30 16:50:43 INFO core/src/lib.rs: Comet native library version $COMET_VERSION initialized
== Physical Plan ==
* CometColumnarToRow (2)
+- CometNativeScan:  (1)


(1) CometNativeScan:
Output [3]: [id#0, first_name#1, personal_info#4]
Arguments: [id#0, first_name#1, personal_info#4]

(2) CometColumnarToRow [codegen id : 1]
Input [3]: [id#0, first_name#1, personal_info#4]


25/01/30 16:50:44 INFO opendal::services::hdfs: Connecting to Namenode (hdfs://namenode:9000)
+---+----------+-----------------+
|id |first_name|personal_info    |
+---+----------+-----------------+
|2  |Jane      |{Jane, Smith, 34}|
|1  |John      |{John, Doe, 28}  |
+---+----------+-----------------+



```

Verify the scan type should be `CometNativeScan`.

### Local HDFS development

- Configure local machine network. Add hostname to `/etc/hosts`

```shell
127.0.0.1	localhost   namenode datanode1 datanode2 datanode3
::1             localhost namenode datanode1 datanode2 datanode3
```

- Start local HDFS cluster, 3 datanodes, namenode url is `namenode:9000`

```shell
docker compose -f kube/local/hdfs-docker-compose.yml up
```

- Check the local namenode is up and running on `http://localhost:9870/dfshealth.html#tab-overview`
- Build a project with HDFS support

```shell
JAVA_HOME="/opt/homebrew/opt/openjdk@17" make release PROFILES="-Pspark-4.1" RUSTFLAGS="-L /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home/lib/server"
```

- Run local test

```scala

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      "fs.defaultFS" -> "hdfs://namenode:9000",
      "dfs.client.use.datanode.hostname" -> "true") {
      val df = spark.read.parquet("/tmp/2")
      df.show(false)
      df.explain("extended")
    }
  }
```

Or use `spark-shell` with HDFS support as described [above](#building-comet-with-hdfs-support)

Comet also has a test suite that exercises a native scan through `libhdfs` against a fake Hadoop
filesystem, so it needs no cluster. Because of the crash described above it is excluded from CI and
run by hand:

```shell
./mvnw test -Dtest=none -Dsuites="org.apache.comet.parquet.ParquetReadFromFakeHadoopFsSuite"
```

## S3

Comet's Parquet scan completely offloads data loading to Rust. It uses the
[`object_store` crate](https://crates.io/crates/object_store) to read data from S3 and supports
configuring S3 access using standard
[Hadoop S3A configurations](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration)
by translating them to the `object_store` crate's format.

This implementation maintains compatibility with existing Hadoop S3A configurations, so existing code will
continue to work as long as the configurations are supported and can be translated without loss of functionality.

### Root CA Certificates

One major difference between Spark and Comet is the mechanism for discovering Root
CA Certificates. Spark uses the JVM to read CA Certificates from the Java Trust Store, but Comet's
Rust-based scans use system Root CA Certificates (typically stored
in `/etc/ssl/certs` on Linux). These scans will not be able to interact with S3 if the Root CA Certificates are not
installed.

### Supported Credential Providers

AWS credential providers can be configured using the `fs.s3a.aws.credentials.provider` configuration. The following table shows the supported credential providers and their configuration options:

| Credential provider                                                                                                                                                                          | Description                                                                                                     | Supported Options                                                                                                               |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`                                                                                                                                      | Access S3 using access key and secret key                                                                       | `fs.s3a.access.key`, `fs.s3a.secret.key`                                                                                        |
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`                                                                                                                                   | Access S3 using temporary credentials                                                                           | `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.session.token`                                                                |
| `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider`                                                                                                                                | Access S3 using AWS STS assume role                                                                             | `fs.s3a.assumed.role.arn`, `fs.s3a.assumed.role.session.name` (optional), `fs.s3a.assumed.role.credentials.provider` (optional) |
| `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`                                                                                                                               | Access S3 using EC2 instance profile or ECS task credentials (tries ECS first, then IMDS)                       | None (auto-detected)                                                                                                            |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`<br/>`com.amazonaws.auth.AnonymousAWSCredentials`<br/>`software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider`       | Access S3 without authentication (public buckets only)                                                          | None                                                                                                                            |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`<br/>`software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider`                                             | Load credentials from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) | None                                                                                                                            |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider`<br/>`software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider`                                                     | Access S3 using EC2 instance metadata service (IMDS)                                                            | None                                                                                                                            |
| `com.amazonaws.auth.ContainerCredentialsProvider`<br/>`software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider`<br/>`com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper` | Access S3 using ECS task credentials                                                                            | None                                                                                                                            |
| `com.amazonaws.auth.WebIdentityTokenCredentialsProvider`<br/>`software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`                                               | Authenticate using web identity token file                                                                      | None                                                                                                                            |
| `com.amazonaws.auth.profile.ProfileCredentialsProvider`<br/>`software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider`                                                             | Authenticate using a named profile from the local AWS credentials file                                          | None                                                                                                                            |

Multiple credential providers can be specified in a comma-separated list using the `fs.s3a.aws.credentials.provider` configuration, just as Hadoop AWS supports. If `fs.s3a.aws.credentials.provider` is not configured, Hadoop S3A's default credential provider chain will be used. All configuration options also support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

### Additional S3 Configuration Options

Beyond credential providers, Comet's Parquet scan supports additional S3 configuration options:

| Option                          | Description                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| `fs.s3a.endpoint`               | The endpoint of the S3 service                                                                     |
| `fs.s3a.endpoint.region`        | The AWS region for the S3 service. If not specified, the region will be auto-detected.             |
| `fs.s3a.path.style.access`      | Whether to use path style access for the S3 service (true/false, defaults to virtual hosted style) |
| `fs.s3a.requester.pays.enabled` | Whether to enable requester pays for S3 requests (true/false)                                      |

All configuration options support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

### S3-Compliant Filesystem Schemes

Some environments front an S3-compatible service (MinIO, Ceph RGW, Cloudflare R2, Wasabi, and
similar) with a vendor-branded Hadoop filesystem client that registers its own URL scheme, for
example `blob://`, instead of `s3://` or `s3a://`. Comet can treat such schemes as aliases for
`s3://` so the native Parquet and Iceberg scans read them directly, without the caller rewriting
URLs.

This is opt-in and disabled by default. Enable it by listing the schemes to treat as S3-compliant
aliases in `spark.hadoop.fs.comet.s3Compliant.schemes` (Hadoop key
`fs.comet.s3Compliant.schemes`), a comma-separated, case-insensitive list. This mirrors the
existing `fs.comet.libhdfs.schemes` config.

```shell
--conf spark.hadoop.fs.comet.s3Compliant.schemes=blob
```

Multiple schemes can be listed together:

```shell
--conf spark.hadoop.fs.comet.s3Compliant.schemes=blob,minio,r2
```

With no configuration, Comet claims none of these aliases, so for example a `blob://` path falls
back to Spark unchanged. The empty default is deliberate: short scheme names like `blob` are not
unique to S3-compatible storage. Azure Blob Storage is the clearest example, so claiming `blob://`
unconditionally would risk misrouting paths that were never meant for Comet's S3 client. Only add a
scheme here if you intend Comet to treat it as S3-compatible.

For each scheme `<s>` listed in `fs.comet.s3Compliant.schemes`, Comet also reads vendor-style,
per-authority Hadoop keys of the form `fs.<s>.<authority>.<property>` (the authority is typically
the bucket or account name from the URL) and translates them into the `fs.s3a.*` surface described
above. The recognized vendor-style properties and their `fs.s3a.*` targets are:

| Vendor property (`fs.<s>.<authority>.<property>`) | Translated `fs.s3a.*` suffix |
| ------------------------------------------------- | ---------------------------- |
| `awsAccessKeyId`                                  | `access.key`                 |
| `awsSecretAccessKey`                              | `secret.key`                 |
| `awsSessionToken`                                 | `session.token`              |
| `endpoint`                                        | `endpoint`                   |
| `region`                                          | `endpoint.region`            |
| `pathStyleAccess`                                 | `path.style.access`          |

Unrecognized `fs.<s>.<authority>.*` properties are ignored.

A URL with no authority, such as the triple-slash `blob:///bucket/key` form, reports its authority
as the literal string `default`. For that case, list the keys under `fs.<s>.default.<property>`.
Comet resolves `default` to the bucket taken from the URL path (`bucket` here) and applies the
translated settings at that bucket's scope, exactly as if they had been written under
`fs.<s>.bucket.<property>`.

When `fs.<s>.<authority>.endpoint` is set, Comet defaults path-style access to enabled for that
bucket, since most non-AWS S3-compatible services require it. This is only a default: set
`fs.s3a.bucket.<bucket>.path.style.access` (or the equivalent per-scheme key) explicitly to
override it.

Once translated, these values are applied at the same `fs.s3a.bucket.{bucket-name}.*` scope
described in [Additional S3 Configuration Options](#additional-s3-configuration-options), so the
credential providers and options documented above also apply to alias-scheme URLs. The same
translation feeds the native Iceberg scan; see
[Object store configuration (S3)](iceberg.md#object-store-configuration-s3) in the Iceberg guide.

A native Parquet scan whose alias-scheme paths span more than one bucket falls back to Spark. Alias
schemes apply to native scans only: a native Iceberg write to an alias-scheme location falls back to
iceberg-java.

### Examples

The following examples demonstrate how to configure S3 access using different authentication methods.

**Example 1: Simple Credentials**

This example shows how to access a private S3 bucket using an access key and secret key. The `fs.s3a.aws.credentials.provider` configuration can be omitted since `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` is included in Hadoop S3A's default credential provider chain.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.s3a.access.key=my-access-key \
--conf spark.hadoop.fs.s3a.secret.key=my-secret-key
...
```

**Example 2: Assume Role with Web Identity Token**

This example demonstrates using an assumed role credential to access a private S3 bucket, where the base credential for assuming the role is provided by a web identity token credentials provider.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider \
--conf spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam::123456789012:role/my-role \
--conf spark.hadoop.fs.s3a.assumed.role.session.name=my-session \
--conf spark.hadoop.fs.s3a.assumed.role.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
...
```

### Limitations

Comet's S3 support has the following limitations:

1. **Partial Hadoop S3A configuration support**: Not all Hadoop S3A configurations are currently supported. Only the configurations listed in the tables above are translated and applied to the underlying `object_store` crate.

2. **Custom credential providers**: Custom credential provider classes named in `fs.s3a.aws.credentials.provider` are not supported; only the standard providers listed in the table above are. To route credential requests through your own Java code, implement Comet's `CometS3CredentialProvider` SPI; see [S3 Credential Providers](s3-credential-providers.md). Broader Hadoop S3A integration is tracked in [#1829](https://github.com/apache/datafusion-comet/issues/1829).

## Azure

Comet's Parquet scan reads Azure Data Lake Storage Gen2 (ADLS Gen2) through the
[`object_store` crate](https://crates.io/crates/object_store), using the `abfs` and `abfss` URL schemes.
The Hadoop ABFS configuration you already have (the `fs.azure.*` keys in `core-site.xml` or under `spark.hadoop.*`) authenticates the native scan as well, so an existing setup keeps working.

URLs use the shape Spark and Hadoop emit: `abfss://<container>@<account>.dfs.core.windows.net/<path>`. The `wasb`, `wasbs`, `az`, `azure`, and `adl` schemes are not supported by the native scan.

### Root CA Certificates

Azure scans discover Root CA Certificates the same way S3 scans do. See [Root CA Certificates](#root-ca-certificates) above. The Rust-based scan uses system Root CA Certificates rather than the Java Trust Store.

### Authentication is resolved by Hadoop

For each `abfs` or `abfss` path, the Spark driver asks the hadoop-azure library on its classpath which authentication mechanism applies to that container and account, and which values it needs. The question goes to the same `AbfsConfiguration` the ABFS `FileSystem` uses, so every Hadoop rule holds: account-scoped keys (`<key>.<account host>`), container-scoped keys (`<key>.<container>.<account host>`, hadoop-azure 3.4.2 and later), credential providers behind `hadoop.security.credential.provider.path` (a JCEKS keystore, for example) and `${...}` substitution behave exactly as they do for Spark's own reads. This holds for the hadoop-azure matching each Spark line's Hadoop: 3.3.4 with Spark 3.4 and 3.5, 3.4.1 with 4.0, 3.4.2 with 4.1, 3.5.0 with 4.2.

The native scan receives the resolved values and builds the `object_store` client from them. It reads no `fs.azure.*` key of its own. A scan whose files span more than one ABFS container or account, or that mixes `abfs`/`abfss` paths with other schemes, falls back to Spark's own scan.

hadoop-azure must be on the driver classpath. It already is whenever Spark can list the path. When it is missing, the scan fails with an error that says so.

#### Mechanisms

| Hadoop mechanism                                                                                                               | Native scan   | Keys the native scan receives                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------ | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SharedKey` (Hadoop's default when `fs.azure.account.auth.type` is unset), with any configured `fs.azure.account.keyprovider`  | Supported     | `fs.azure.account.key`. Hadoop's key provider runs on the driver and the key it returns is forwarded                                                                                                                                                                         |
| `OAuth` with `ClientCredsTokenProvider`                                                                                        | Supported     | `fs.azure.account.oauth2.client.id`, `fs.azure.account.oauth2.client.secret`, `fs.azure.account.oauth2.client.endpoint` (the tenant and authority host come from the endpoint URL)                                                                                           |
| `OAuth` with `MsiTokenProvider`                                                                                                | Supported     | `fs.azure.account.oauth2.msi.endpoint`, plus `fs.azure.account.oauth2.client.id` when set. `fs.azure.account.oauth2.msi.tenant` and `fs.azure.account.oauth2.msi.authority` are accepted but unused: `object_store`'s IMDS provider sends only the client id to the endpoint |
| `OAuth` with `WorkloadIdentityTokenProvider` (hadoop-azure 3.4.1 and later)                                                    | Supported     | `fs.azure.account.oauth2.client.id`, `fs.azure.account.oauth2.msi.tenant`, `fs.azure.account.oauth2.token.file`, `fs.azure.account.oauth2.msi.authority` (the last two take Hadoop's defaults when unset)                                                                    |
| `SAS` with the fixed token (hadoop-azure 3.4.1 and later)                                                                      | Supported     | `fs.azure.sas.fixed.token`                                                                                                                                                                                                                                                   |
| `OAuth` with `RefreshTokenBasedTokenProvider` or `UserPasswordTokenProvider`                                                   | Not supported |                                                                                                                                                                                                                                                                              |
| `SAS` with a `fs.azure.sas.token.provider.type` class                                                                          | Not supported |                                                                                                                                                                                                                                                                              |
| `fs.azure.account.auth.type=Custom`                                                                                            | Not supported |                                                                                                                                                                                                                                                                              |
| `WorkloadIdentityTokenProvider` with a `fs.azure.account.oauth2.client.assertion.provider.type` (hadoop-azure 3.5.0 and later) | Not supported |                                                                                                                                                                                                                                                                              |
| `UserboundSASWithOAuth` (hadoop-azure 3.5.0 and later)                                                                         | Not supported |                                                                                                                                                                                                                                                                              |

Custom provider classes hand each token request to Java code the native scan cannot call, and the refresh token and user password flows have no `object_store` counterpart. When Hadoop selects one of these, the scan fails with an error naming the auth type and, where one is configured, the class. Configure a supported mechanism for that account, or keep the path on Spark's own scan. Values arrive as Hadoop resolved them, trimmed and defaulted where the ABFS driver trims and defaults them, so the store sees what the driver would have used.

Client credentials and Workload Identity token requests go over HTTPS only. The managed identity request goes to the IMDS endpoint over HTTP, as Hadoop's does. For client credentials, Hadoop posts to `fs.azure.account.oauth2.client.endpoint` as given, while `object_store` posts to `<authority host>/<tenant>/oauth2/v2.0/token`. Comet takes the tenant from the path segment before the last `oauth2` and everything before that segment as the authority host, so a v1 `/oauth2/token` endpoint and a proxy with a path prefix both resolve. The token request path is always the v2.0 one. An `http://` endpoint fails.

### Environment variables

The `AZURE_*` variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_USE_AZURE_CLI`, and the others `object_store` reads) and `IDENTITY_ENDPOINT` are read only when Hadoop configures no authentication for the account at all: none of `fs.azure.account.auth.type`, `fs.azure.account.key`, `fs.azure.account.keyprovider`, `fs.azure.shellkeyprovider.script`, `fs.azure.account.oauth.provider.type`, `fs.azure.sas.token.provider.type`, `fs.azure.sas.fixed.token` or any `fs.azure.account.oauth2.*` key resolves for it. That case is what lets AKS Workload Identity work with no Hadoop configuration for tables whose files Spark does not list through Hadoop, such as Iceberg tables. A plain Parquet path still needs Hadoop configured for Spark's own listing.

Once Hadoop names anything, only the resolved values reach the store. A resolution error (a missing mandatory key, an invalid account key, an unknown provider class) fails the scan with Hadoop's exception. Messages that could quote a credential, such as the key provider's, are kept in the Spark driver log and the scan error names the exception class. The environment is never a fallback. One read happens inside `object_store` rather than Comet: under a resolved managed identity, its IMDS provider picks up `IDENTITY_HEADER` when it fetches a token.

### Credentials in transit

The resolved values travel in the native plan from the driver to the executors, as the static `fs.s3a.access.key` and `fs.s3a.secret.key` values of an S3 configuration do. Spark's RPC channel is plaintext by default. `spark.network.crypto.enabled` or `spark.ssl.rpc.enabled` protects it. See [Wire encryption](s3-credential-providers.md#wire-encryption).

### Examples

**Example 1: Shared account key**

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.key.myaccount.dfs.core.windows.net=my-account-key
...
```

**Example 2: Workload Identity (AKS)**

In an AKS pod with Workload Identity enabled and no `fs.azure.*` authentication configured, the `AZURE_*` variables the webhook injects are picked up, so nothing Comet-specific is needed. To configure it through Hadoop instead (hadoop-azure 3.4.1 and later):

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.auth.type.myaccount.dfs.core.windows.net=OAuth \
--conf spark.hadoop.fs.azure.account.oauth.provider.type.myaccount.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider \
--conf spark.hadoop.fs.azure.account.oauth2.client.id.myaccount.dfs.core.windows.net=<client-id> \
--conf spark.hadoop.fs.azure.account.oauth2.msi.tenant.myaccount.dfs.core.windows.net=<tenant-id>
...
```

The token file defaults to `/var/run/secrets/azure/tokens/azure-identity-token`, the path AKS mounts. Set `fs.azure.account.oauth2.token.file` for another location.

**Example 3: OAuth2 client credentials**

The tenant comes from the endpoint URL, so `fs.azure.account.oauth2.msi.tenant` is not needed:

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.auth.type.myaccount.dfs.core.windows.net=OAuth \
--conf spark.hadoop.fs.azure.account.oauth.provider.type.myaccount.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider \
--conf spark.hadoop.fs.azure.account.oauth2.client.id.myaccount.dfs.core.windows.net=<client-id> \
--conf spark.hadoop.fs.azure.account.oauth2.client.secret.myaccount.dfs.core.windows.net=<client-secret> \
--conf spark.hadoop.fs.azure.account.oauth2.client.endpoint.myaccount.dfs.core.windows.net=https://login.microsoftonline.com/<tenant-id>/oauth2/token
...
```

**Example 4: SAS token**

Hadoop's ABFS driver (3.4.1 and later) reads the fixed token under `fs.azure.account.auth.type=SAS`, and the native scan receives the same value, so one configuration serves the driver and the executors:

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.auth.type.myaccount.dfs.core.windows.net=SAS \
--conf spark.hadoop.fs.azure.sas.fixed.token.myaccount.dfs.core.windows.net='sv=2020-08-04&sig=...'
...
```

To give one container its own token on hadoop-azure 3.4.2 and later, set `fs.azure.sas.fixed.token.mycontainer.myaccount.dfs.core.windows.net`. Hadoop reads it before the account-level key, and the native scan receives whichever one Hadoop picked. The OAuth keys take the same container-scoped form on those versions.

### Changes from Comet 1.1

Comet 1.1 started from the environment and overlaid the Hadoop keys it found by probing the account name under two endpoint suffixes and bare. Now Hadoop decides. What this changes:

- Container-scoped keys work on hadoop-azure 3.4.2 and later, as they do for Spark.
- Transport variables such as `AZURE_STORAGE_ENDPOINT` and `AZURE_ALLOW_HTTP` no longer apply beside a Hadoop mechanism. They are read only when Hadoop configures none.
- The WASB key `fs.azure.sas.<container>.<account>`, which the ABFS driver never read, is no longer read by the native scan either. Use `fs.azure.sas.fixed.token`, which the native scan now reads.
- A lower-case `oauth` auth type, or OAuth keys without `fs.azure.account.auth.type`, fail the way they fail for Spark. In the second case Hadoop defaults to `SharedKey` and the key provider fails. On hadoop-azure 3.4.1 and later the driver log names the missing key. 3.3.4 reports only `Failure to initialize configuration`.
- A configured mechanism the native scan cannot build fails with an error instead of silently reading the environment.

### Limitations

1. **Authentication keys only**: Only the keys in the table above reach the native store. Other `fs.azure.*` settings (read-ahead, retries, HTTP tuning) do not apply to the native scan.

2. **Supported schemes**: Only `abfs` and `abfss` are routed to the native Azure store. `wasb[s]`, `az`, `azure`, and `adl` are not supported. `wasb[s]` is not recognised by `object_store` at all; `az`, `azure`, and `adl` are recognised by `object_store` but treat the URL host as the _container_ rather than the _account_, which is incompatible with Hadoop's account-scoped configuration keys.

3. **URL shape**: URLs must include the account in the host, i.e. `abfss://<container>@<account>.dfs.core.windows.net/<path>`. Bare `abfs://<container>/<path>` (fsspec-style, no account in the URL) is not supported because Comet cannot resolve the storage account name.

4. **Endpoint hosts**: `object_store` builds an Azure store only for hosts under `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com` and `blob.fabric.microsoft.com`. Sovereign-cloud endpoints such as `dfs.core.chinacloudapi.cn` are not supported, whatever keys are configured for them.

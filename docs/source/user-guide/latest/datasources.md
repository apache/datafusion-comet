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

Comet accelerates Iceberg scans of Parquet files. See the [Iceberg Guide] for more information.

[Iceberg Guide]: iceberg.md

### CSV

Comet provides experimental Rust-based CSV scan support. When `spark.comet.scan.csv.v2.enabled` is enabled, CSV files
are read in Rust for improved performance. This feature is experimental and performance benefits are
workload-dependent.

Alternatively, when `spark.comet.convert.csv.enabled` is enabled, data from Spark's CSV reader is immediately
converted into Arrow format, allowing the Comet pipeline to take over after that.

### JSON

Comet does not provide a Rust-based JSON scan, but when `spark.comet.convert.json.enabled` is enabled, data is immediately
converted into Arrow format, allowing the Comet pipeline to take over after that.

## Data Catalogs

### Apache Iceberg

See the dedicated [Comet and Iceberg Guide](iceberg.md).

## Supported Storages

Comet supports most standard storage systems, such as local file system and object storage.

### HDFS

The Apache DataFusion Comet Rust-based reader seamlessly scans files from remote HDFS for [supported formats](#supported-spark-data-sources)

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

2. **Custom credential providers**: Custom implementations of AWS credential providers are not supported. The implementation only supports the standard credential providers listed in the table above. We are planning to add support for custom credential providers through a JNI-based adapter that will allow calling Java credential providers from Rust code. See [issue #1829](https://github.com/apache/datafusion-comet/issues/1829) for more details.

## Azure

Comet's Parquet scan reads Azure Data Lake Storage Gen2 (ADLS Gen2) through the
[`object_store` crate](https://crates.io/crates/object_store), using the `abfs` and `abfss` URL schemes.
As with S3, standard Hadoop ABFS configurations (the `fs.azure.*` keys you already set in `core-site.xml` or via `spark.hadoop.*`) are translated into the `object_store` crate's format, so existing configurations continue to work.

URLs use the same shape Spark and Hadoop emit: `abfss://<container>@<account>.dfs.core.windows.net/<path>`. The account is taken from the host and the container from the URL user-info. The `wasb`, `wasbs`, `az`, `azure`, and `adl` schemes are not supported by the native scan.

### Root CA Certificates

Azure scans discover Root CA Certificates the same way S3 scans do. See [Root CA Certificates](#root-ca-certificates) above. The Rust-based scan uses system Root CA Certificates rather than the Java Trust Store.

### Supported Authentication

Comet first calls `MicrosoftAzureBuilder::from_env()`, so any `AZURE_*` environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`, `AZURE_STORAGE_*`) are honored out of the box. This is what makes AKS Workload Identity work in a stock pod with no extra configuration. Any Hadoop `fs.azure.*` keys below are then applied on top, overriding the environment.

| Authentication method     | Hadoop keys                                                                                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shared account key        | `fs.azure.account.key`                                                                                                                                                 |
| OAuth2 client credentials | `fs.azure.account.oauth2.client.id`, `fs.azure.account.oauth2.client.secret`, `fs.azure.account.oauth2.client.endpoint` (tenant id is extracted from the endpoint URL) |
| Managed identity (MSI)    | `fs.azure.account.oauth2.msi.tenant`, `fs.azure.account.oauth2.msi.endpoint`, `fs.azure.account.oauth2.msi.authority`                                                  |
| Workload Identity         | `fs.azure.account.oauth2.client.id`, `fs.azure.account.oauth2.msi.tenant`, `fs.azure.account.oauth2.token.file`                                                        |
| SAS token                 | `fs.azure.sas.<container>.<account>`                                                                                                                                   |

### Hadoop-to-object_store key mapping

Internally, each Hadoop key is translated into a specific `AzureConfigKey` on the underlying `object_store` `MicrosoftAzureBuilder`:

| Hadoop key (account-scoped suffix omitted) | `AzureConfigKey`         |
| ------------------------------------------ | ------------------------ |
| `fs.azure.account.key`                     | `AccessKey`              |
| `fs.azure.account.oauth2.client.id`        | `ClientId`               |
| `fs.azure.account.oauth2.client.secret`    | `ClientSecret`           |
| `fs.azure.account.oauth2.client.endpoint`  | `AuthorityId` (from URL) |
| `fs.azure.account.oauth2.msi.tenant`       | `AuthorityId`            |
| `fs.azure.account.oauth2.msi.endpoint`     | `MsiEndpoint`            |
| `fs.azure.account.oauth2.msi.authority`    | `AuthorityHost`          |
| `fs.azure.account.oauth2.token.file`       | `FederatedTokenFile`     |
| `fs.azure.sas.<container>.<account>`       | `SasKey`                 |

Anything beyond these keys is not translated and falls through to whatever `from_env()` or the URL itself provided.

### Tenant id resolution

`AuthorityId` (the AAD tenant id) can be supplied in two ways:

- **Directly**, via `fs.azure.account.oauth2.msi.tenant`.
- **Indirectly**, via `fs.azure.account.oauth2.client.endpoint`, which is a full token URL such as `https://login.microsoftonline.com/<tenant>/oauth2/token`. Comet extracts the tenant id from the first path segment of that URL.

If both are set, the value from `msi.tenant` wins.

### Account-scoped key lookup order

Account-scoped keys take precedence over global ones, mirroring Hadoop ABFS's own precedence. For each Hadoop key above, Comet probes the following names in order and uses the first match:

1. `<key>.<account>.dfs.core.windows.net`
2. `<key>.<account>.blob.core.windows.net`
3. `<key>.<account>`
4. `<key>` (unscoped / global)

The SAS namespace follows the same shape with the container inlined into the key name: `fs.azure.sas.<container>.<account>.dfs.core.windows.net`, then `fs.azure.sas.<container>.<account>.blob.core.windows.net`, then `fs.azure.sas.<container>.<account>`.

The Hadoop values, when present, override anything already picked up from the `AZURE_*` environment.

### Examples

**Example 1: Shared account key**

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.key.myaccount.dfs.core.windows.net=my-account-key
...
```

**Example 2: Workload Identity (AKS)**

In an AKS pod with Workload Identity enabled, the `AZURE_*` environment variables injected by the webhook are picked up automatically, so no Comet-specific configuration is required. To set the values explicitly instead:

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.oauth2.client.id.myaccount.dfs.core.windows.net=<client-id> \
--conf spark.hadoop.fs.azure.account.oauth2.msi.tenant.myaccount.dfs.core.windows.net=<tenant-id> \
--conf spark.hadoop.fs.azure.account.oauth2.token.file.myaccount.dfs.core.windows.net=/var/run/secrets/azure/tokens/azure-identity-token
...
```

**Example 3: OAuth2 client credentials with tenant embedded in the endpoint URL**

If only `fs.azure.account.oauth2.client.endpoint` is set, the tenant id is parsed from the endpoint path automatically — there is no need to set `msi.tenant` separately.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.account.oauth2.client.id.myaccount.dfs.core.windows.net=<client-id> \
--conf spark.hadoop.fs.azure.account.oauth2.client.secret.myaccount.dfs.core.windows.net=<client-secret> \
--conf spark.hadoop.fs.azure.account.oauth2.client.endpoint.myaccount.dfs.core.windows.net=https://login.microsoftonline.com/<tenant-id>/oauth2/token
...
```

**Example 4: SAS token scoped to a container**

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.hadoop.fs.azure.sas.mycontainer.myaccount.dfs.core.windows.net='sv=2020-08-04&sig=...'
...
```

### Limitations

1. **Partial Hadoop ABFS configuration support**: Only the `fs.azure.*` keys listed above are translated and applied to the underlying `object_store` crate.

2. **Supported schemes**: Only `abfs` and `abfss` are routed to the native Azure store. `wasb[s]`, `az`, `azure`, and `adl` are not supported. `wasb[s]` is not recognised by `object_store` at all; `az`, `azure`, and `adl` are recognised by `object_store` but treat the URL host as the _container_ rather than the _account_, which is incompatible with Hadoop's account-scoped configuration keys.

3. **URL shape**: URLs must include the account in the host, i.e. `abfss://<container>@<account>.dfs.core.windows.net/<path>`. Bare `abfs://<container>/<path>` (fsspec-style, no account in the URL) is not supported because Comet cannot resolve the storage account name.

4. **Endpoint suffixes for account-scoped lookup**: Comet probes for account-scoped keys under `dfs.core.windows.net` and `blob.core.windows.net`. Custom or sovereign-cloud endpoint suffixes (e.g. `dfs.core.chinacloudapi.cn`, Fabric endpoints) are not probed; use the unscoped `<key>.<account>` form for those.

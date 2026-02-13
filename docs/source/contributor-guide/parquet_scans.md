<!--
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

# Comet Parquet Scan Implementations

Comet currently has two distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation. The default setting is `spark.comet.scan.impl=auto`, which
currently always uses the `native_iceberg_compat` implementation. Most users should not need to change this setting.
However, it is possible to force Comet to try and use a particular implementation for all scan operations by setting
this configuration property to one of the following implementations.

The two implementations are `native_datafusion` and `native_iceberg_compat`. They both delegate to DataFusion's
`DataSourceExec`. The main difference between these implementations is that `native_datafusion` runs fully natively, and
`native_iceberg_compat` is a hybrid JVM/Rust implementation that can support some Spark features that
`native_datafusion` can not, but has some performance overhead due to crossing the JVM/Rust boundary.

The `native_datafusion` and `native_iceberg_compat` scans share the following limitations. Unless otherwise noted,
unsupported features cause Comet to fall back to Spark, so queries will still return correct results.

- When reading Parquet files written by systems other than Spark that contain columns with the logical type `UINT_8`
  (unsigned 8-bit integers), Comet may produce different results than Spark. Spark maps `UINT_8` to `ShortType`, but
  Comet's Arrow-based readers respect the unsigned type and read the data as unsigned rather than signed. Since Comet
  cannot distinguish `ShortType` columns that came from `UINT_8` versus signed `INT16`, by default Comet falls back to
  Spark when scanning Parquet files containing `ShortType` columns. This behavior can be disabled by setting
  `spark.comet.scan.unsignedSmallIntSafetyCheck=false`. Note that `ByteType` columns are always safe because they can
  only come from signed `INT8`, where truncation preserves the signed value.
- No support for default values that are nested types (e.g., maps, arrays, structs). Comet falls back to Spark.
  Literal default values are supported.
- **Potential incorrect results:** No support for datetime rebasing detection or the
  `spark.comet.exceptionOnDatetimeRebase` configuration. When reading Parquet files containing dates or timestamps
  written before Spark 3.0 (which used a hybrid Julian/Gregorian calendar), dates/timestamps will be read as if they
  were written using the Proleptic Gregorian calendar. This does not fall back to Spark and may produce incorrect
  results for dates before October 15, 1582.
- No support for Spark's Datasource V2 API. When `spark.sql.sources.useV1SourceList` does not include `parquet`,
  Spark uses the V2 API for Parquet scans. The DataFusion-based implementations only support the V1 API, so Comet
  will fall back to Spark when V2 is enabled.
- No support for Parquet encryption. When Parquet encryption is enabled (i.e., `parquet.crypto.factory.class` is
  set), Comet will fall back to Spark.
- No support for Spark metadata columns (e.g., `_metadata.file_path`). When metadata columns are referenced in the
  query, Comet falls back to Spark.

The `native_datafusion` scan has some additional limitations. All of these cause Comet to fall back to Spark.

- No support for row indexes
- No support for reading Parquet field IDs
- No support for Dynamic Partition Pruning (DPP)
- No support for `input_file_name()`, `input_file_block_start()`, or `input_file_block_length()` SQL functions.
  The `native_datafusion` scan does not use Spark's `FileScanRDD`, so these functions cannot populate their values.
- No support for `ignoreMissingFiles` or `ignoreCorruptFiles` being set to `true`

The `native_iceberg_compat` scan has some additional limitations:

- **Potential incorrect results:** Some Spark configuration values are hard-coded to their defaults rather than
  respecting user-specified values. This does not fall back to Spark and may produce incorrect results when
  non-default values are set. The affected configurations are `spark.sql.parquet.binaryAsString`,
  `spark.sql.parquet.int96AsTimestamp`, `spark.sql.caseSensitive`, `spark.sql.parquet.inferTimestampNTZ.enabled`,
  and `spark.sql.legacy.parquet.nanosAsLong`. See
  [issue #1816](https://github.com/apache/datafusion-comet/issues/1816) for more details.

## S3 Support

The `native_datafusion` and `native_iceberg_compat` Parquet scan implementations completely offload data loading
to native code. They use the [`object_store` crate](https://crates.io/crates/object_store) to read data from S3 and
support configuring S3 access using standard [Hadoop S3A configurations](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration) by translating them to
the `object_store` crate's format.

This implementation maintains compatibility with existing Hadoop S3A configurations, so existing code will
continue to work as long as the configurations are supported and can be translated without loss of functionality.

#### Additional S3 Configuration Options

Beyond credential providers, the `native_datafusion` and `native_iceberg_compat` implementations support additional
S3 configuration options:

| Option                          | Description                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| `fs.s3a.endpoint`               | The endpoint of the S3 service                                                                     |
| `fs.s3a.endpoint.region`        | The AWS region for the S3 service. If not specified, the region will be auto-detected.             |
| `fs.s3a.path.style.access`      | Whether to use path style access for the S3 service (true/false, defaults to virtual hosted style) |
| `fs.s3a.requester.pays.enabled` | Whether to enable requester pays for S3 requests (true/false)                                      |

All configuration options support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

#### Examples

The following examples demonstrate how to configure S3 access with the `native_datafusion` and `native_iceberg_compat`
Parquet scan implementations using different authentication methods.

**Example 1: Simple Credentials**

This example shows how to access a private S3 bucket using an access key and secret key. The `fs.s3a.aws.credentials.provider` configuration can be omitted since `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` is included in Hadoop S3A's default credential provider chain.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.comet.scan.impl=native_datafusion \
--conf spark.hadoop.fs.s3a.access.key=my-access-key \
--conf spark.hadoop.fs.s3a.secret.key=my-secret-key
...
```

**Example 2: Assume Role with Web Identity Token**

This example demonstrates using an assumed role credential to access a private S3 bucket, where the base credential for assuming the role is provided by a web identity token credentials provider.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.comet.scan.impl=native_datafusion \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider \
--conf spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam::123456789012:role/my-role \
--conf spark.hadoop.fs.s3a.assumed.role.session.name=my-session \
--conf spark.hadoop.fs.s3a.assumed.role.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
...
```

#### Limitations

The S3 support of `native_datafusion` and `native_iceberg_compat` has the following limitations:

1. **Partial Hadoop S3A configuration support**: Not all Hadoop S3A configurations are currently supported. Only the configurations listed in the tables above are translated and applied to the underlying `object_store` crate.

2. **Custom credential providers**: Custom implementations of AWS credential providers are not supported. The implementation only supports the standard credential providers listed in the table above. We are planning to add support for custom credential providers through a JNI-based adapter that will allow calling Java credential providers from native code. See [issue #1829](https://github.com/apache/datafusion-comet/issues/1829) for more details.

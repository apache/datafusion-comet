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

# Comet Parquet Scan

Comet provides a fully native Parquet scan, which requires `spark.comet.exec.enabled=true` because the
scan node must be wrapped by `CometExecRule`.

The following features are not supported by the Comet Parquet scan, and Comet will fall back to Spark in
these scenarios:

- `ShortType` columns, by default. When reading Parquet files written by systems other than Spark that contain
  columns with the logical type `UINT_8` (unsigned 8-bit integers), Comet may produce different results than Spark.
  Spark maps `UINT_8` to `ShortType`, but Comet's Arrow-based readers respect the unsigned type and read the data as
  unsigned rather than signed. Since Comet cannot distinguish `ShortType` columns that came from `UINT_8` versus
  signed `INT16`, by default Comet falls back to Spark when scanning Parquet files containing `ShortType` columns.
  This behavior can be disabled by setting `spark.comet.scan.unsignedSmallIntSafetyCheck=false`. Note that `ByteType`
  columns are always safe because they can only come from signed `INT8`, where truncation preserves the signed value.
- Default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.
- Spark's Datasource V2 API. When `spark.sql.sources.useV1SourceList` does not include `parquet`, Spark uses the
  V2 API for Parquet scans. The Comet Parquet scan only supports the V1 API.
- Spark metadata columns (e.g., `_metadata.file_path`)
- No support for AQE Dynamic Partition Pruning (DPP). Non-AQE DPP is supported.
- No support for row indexes
- No support for reading Parquet field IDs
- No support for `input_file_name()`, `input_file_block_start()`, or `input_file_block_length()` SQL functions.
  The Comet Parquet scan does not use Spark's `FileScanRDD`, so these functions cannot populate their values.
- No support for `ignoreMissingFiles` or `ignoreCorruptFiles` being set to `true`

The following limitations may produce incorrect results without falling back to Spark:

- No support for datetime rebasing. When reading Parquet files containing dates or timestamps written before
  Spark 3.0 (which used a hybrid Julian/Gregorian calendar), dates/timestamps will be read as if they were
  written using the Proleptic Gregorian calendar. This may produce incorrect results for dates before
  October 15, 1582.

Duplicate field names in case-insensitive mode (e.g., a Parquet file with both `B` and `b` columns)
are detected at read time and raise a `SparkRuntimeException` with error class `_LEGACY_ERROR_TEMP_2093`,
matching Spark's behavior.

## S3 Support

The Comet Parquet scan completely offloads data loading to native code. It uses the
[`object_store` crate](https://crates.io/crates/object_store) to read data from S3 and supports configuring
S3 access using standard [Hadoop S3A configurations](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration) by translating them to the `object_store` crate's format.

This maintains compatibility with existing Hadoop S3A configurations, so existing code will
continue to work as long as the configurations are supported and can be translated without loss of functionality.

#### Additional S3 Configuration Options

Beyond credential providers, the Comet Parquet scan supports additional S3 configuration options:

| Option                          | Description                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| `fs.s3a.endpoint`               | The endpoint of the S3 service                                                                     |
| `fs.s3a.endpoint.region`        | The AWS region for the S3 service. If not specified, the region will be auto-detected.             |
| `fs.s3a.path.style.access`      | Whether to use path style access for the S3 service (true/false, defaults to virtual hosted style) |
| `fs.s3a.requester.pays.enabled` | Whether to enable requester pays for S3 requests (true/false)                                      |

All configuration options support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

#### Examples

The following examples demonstrate how to configure S3 access for the Comet Parquet scan using different
authentication methods.

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

#### Limitations

Comet's S3 support has the following limitations:

1. **Partial Hadoop S3A configuration support**: Not all Hadoop S3A configurations are currently supported. Only the configurations listed in the tables above are translated and applied to the underlying `object_store` crate.

2. **Custom credential providers**: Custom implementations of AWS credential providers are not supported. The implementation only supports the standard credential providers listed in the table above. We are planning to add support for custom credential providers through a JNI-based adapter that will allow calling Java credential providers from native code. See [issue #1829](https://github.com/apache/datafusion-comet/issues/1829) for more details.

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

Comet currently has three distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation. The default setting is `spark.comet.scan.impl=auto`, and
Comet will choose the most appropriate implementation based on the Parquet schema and other Comet configuration
settings. Most users should not need to change this setting. However, it is possible to force Comet to try and use
a particular implementation for all scan operations by setting this configuration property to one of the following
implementations.

| Implementation          | Description                                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `native_comet`          | This implementation provides strong compatibility with Spark but does not support complex types. This is the original scan implementation in Comet and may eventually be removed.    |
| `native_iceberg_compat` | This implementation delegates to DataFusion's `DataSourceExec` but uses a hybrid approach of JVM and native code. This scan is designed to be integrated with Iceberg in the future. |
| `native_datafusion`     | This experimental implementation delegates to DataFusion's `DataSourceExec` for full native execution. There are known compatibility issues when using this scan.                    |

The `native_datafusion` and `native_iceberg_compat` scans provide the following benefits over the `native_comet`
implementation:

- Leverages the DataFusion community's ongoing improvements to `DataSourceExec`
- Provides support for reading complex types (structs, arrays, and maps)
- Removes the use of reusable mutable-buffers in Comet, which is complex to maintain
- Improves performance

The `native_datafusion` and `native_iceberg_compat` scans share the following limitations:

- When reading Parquet files written by systems other than Spark that contain columns with the logical types `UINT_8`
  or `UINT_16`, Comet will produce different results than Spark because Spark does not preserve or understand these
  logical types. Arrow-based readers, such as DataFusion and Comet do respect these types and read the data as unsigned
  rather than signed. By default, Comet will fall back to `native_comet` when scanning Parquet files containing `byte` or `short`
  types (regardless of the logical type). This behavior can be disabled by setting
  `spark.comet.scan.allowIncompatible=true`.
- No support for default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.

The `native_datafusion` scan has some additional limitations:

- Bucketed scans are not supported
- No support for row indexes
- `PARQUET_FIELD_ID_READ_ENABLED` is not respected [#1758]
- There are failures in the Spark SQL test suite [#1545]
- Setting Spark configs `ignoreMissingFiles` or `ignoreCorruptFiles` to `true` is not compatible with Spark

## S3 Support

There are some 

### `native_comet`

The default `native_comet` Parquet scan implementation reads data from S3 using the [Hadoop-AWS module](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html), which 
is identical to the approach commonly used with vanilla Spark. AWS credential configuration and other Hadoop S3A 
configurations works the same way as in vanilla Spark.

### `native_datafusion` and `native_iceberg_compat`

The `native_datafusion` and `native_iceberg_compat` Parquet scan implementations completely offload data loading 
to native code. They use the [`object_store` crate](https://crates.io/crates/object_store) to read data from S3 and 
support configuring S3 access using standard [Hadoop S3A configurations](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration) by translating them to 
the `object_store` crate's format.

This implementation maintains compatibility with existing Hadoop S3A configurations, so existing code will 
continue to work as long as the configurations are supported and can be translated without loss of functionality.

#### Additional S3 Configuration Options

Beyond credential providers, the `native_datafusion` implementation supports additional S3 configuration options:

| Option | Description |
|--------|-------------|
| `fs.s3a.endpoint` | The endpoint of the S3 service |
| `fs.s3a.endpoint.region` | The AWS region for the S3 service. If not specified, the region will be auto-detected. |
| `fs.s3a.path.style.access` | Whether to use path style access for the S3 service (true/false, defaults to virtual hosted style) |
| `fs.s3a.requester.pays.enabled` | Whether to enable requester pays for S3 requests (true/false) |

All configuration options support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

#### Examples

The following examples demonstrate how to configure S3 access with the `native_datafusion` Parquet scan implementation using different authentication methods.

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

The S3 support of `native_datafusion` has the following limitations:

1. **Partial Hadoop S3A configuration support**: Not all Hadoop S3A configurations are currently supported. Only the configurations listed in the tables above are translated and applied to the underlying `object_store` crate.

2. **Custom credential providers**: Custom implementations of AWS credential providers are not supported. The implementation only supports the standard credential providers listed in the table above. We are planning to add support for custom credential providers through a JNI-based adapter that will allow calling Java credential providers from native code. See [issue #1829](https://github.com/apache/datafusion-comet/issues/1829) for more details.



[#1545]: https://github.com/apache/datafusion-comet/issues/1545
[#1758]: https://github.com/apache/datafusion-comet/issues/1758

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

# Parquet Compatibility

Comet currently has two distinct implementations of the Parquet scan operator.

| Scan Implementation     | Notes                  |
| ----------------------- | ---------------------- |
| `native_datafusion`     | Fully native scan      |
| `native_iceberg_compat` | Hybrid JVM/native scan |

The configuration property `spark.comet.scan.impl` is used to select an implementation. The default setting is
`spark.comet.scan.impl=auto`, which attempts to use `native_datafusion` first, and falls back to Spark if the scan
cannot be converted (e.g., due to unsupported features). Most users should not need to change this setting. However,
it is possible to force Comet to use a particular implementation for all scan operations by setting this
configuration property to one of the following implementations. For example: `--conf spark.comet.scan.impl=native_datafusion`.

## Shared Limitations

The following features are not supported by either scan implementation, and Comet will fall back to Spark in these scenarios:

- Decimals encoded in binary format.
- `ShortType` columns, by default. When reading Parquet files written by systems other than Spark that contain
  columns with the logical type `UINT_8` (unsigned 8-bit integers), Comet may produce different results than Spark.
  Spark maps `UINT_8` to `ShortType`, but Comet's Arrow-based readers respect the unsigned type and read the data as
  unsigned rather than signed. Since Comet cannot distinguish `ShortType` columns that came from `UINT_8` versus
  signed `INT16`, by default Comet falls back to Spark when scanning Parquet files containing `ShortType` columns.
  This behavior can be disabled by setting `spark.comet.scan.unsignedSmallIntSafetyCheck=false`. Note that `ByteType`
  columns are always safe because they can only come from signed `INT8`, where truncation preserves the signed value.
- Default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.
- Spark's Datasource V2 API. When `spark.sql.sources.useV1SourceList` does not include `parquet`, Spark uses the
  V2 API for Parquet scans. The DataFusion-based implementations only support the V1 API.
- Spark metadata columns (e.g., `_metadata.file_path`)
- No support for AQE Dynamic Partition Pruning (DPP). Non-AQE DPP is supported.

The following shared limitation may produce incorrect results without falling back to Spark:

- No support for datetime rebasing. When reading Parquet files containing dates or timestamps written before
  Spark 3.0 (which used a hybrid Julian/Gregorian calendar), dates/timestamps will be read as if they were
  written using the Proleptic Gregorian calendar. This may produce incorrect results for dates before
  October 15, 1582.

## `native_datafusion` Limitations

The `native_datafusion` scan has some additional limitations, mostly related to Parquet metadata. The following
cause Comet to fall back to Spark (including when using `auto` mode). Note that the `native_datafusion` scan
requires `spark.comet.exec.enabled=true` because the scan node must be wrapped by `CometExecRule`.

- No support for row indexes
- No support for reading Parquet field IDs
- No support for `input_file_name()`, `input_file_block_start()`, or `input_file_block_length()` SQL functions.
  The `native_datafusion` scan does not use Spark's `FileScanRDD`, so these functions cannot populate their values.
- No support for `ignoreMissingFiles` or `ignoreCorruptFiles` being set to `true`
- Duplicate field names in case-insensitive mode (e.g., a Parquet file with both `B` and `b` columns)
  are detected at read time and raise a `SparkRuntimeException` with error class `_LEGACY_ERROR_TEMP_2093`,
  matching Spark's behavior.

The following limitation may produce different behavior or incorrect results without falling back to Spark:

- Silent type coercion on Parquet schema mismatch. When the requested read schema does not match the
  Parquet file's physical schema, Spark's vectorized reader normally throws a `SparkException`. The
  `native_datafusion` scan inherits DataFusion's more permissive reader, which silently accepts or
  coerces many of these reads. For value-preserving widenings (such as `INT32` read as `INT64`) the data
  is read correctly but no exception is raised, which differs from Spark 3.x behavior. For incompatible
  reads (such as binary read as timestamp, `TimestampLTZ` read as `TimestampNTZ`, or decimals with
  incompatible precision/scale) the result may be incorrect. See
  [issue #3720](https://github.com/apache/datafusion-comet/issues/3720) for the affected cases. This
  applies on all supported Spark versions.

## `native_iceberg_compat` Limitations

The `native_iceberg_compat` scan has the following additional limitation that may produce incorrect results
without falling back to Spark:

- Some Spark configuration values are hard-coded to their defaults rather than respecting user-specified values.
  This may produce incorrect results when non-default values are set. The affected configurations are
  `spark.sql.parquet.binaryAsString`, `spark.sql.parquet.int96AsTimestamp`, `spark.sql.caseSensitive`,
  `spark.sql.parquet.inferTimestampNTZ.enabled`, and `spark.sql.legacy.parquet.nanosAsLong`. See
  [issue #1816](https://github.com/apache/datafusion-comet/issues/1816) for more details.

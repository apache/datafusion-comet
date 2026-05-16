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

The following shared limitation may produce incorrect results without falling back to Spark:

- No support for datetime rebasing. When reading Parquet files containing dates or timestamps written before
  Spark 3.0 (which used a hybrid Julian/Gregorian calendar), dates/timestamps will be read as if they were
  written using the Proleptic Gregorian calendar. This may produce incorrect results for dates before
  October 15, 1582.

The following shared limitation raises an error at scan time rather than falling back to Spark:

- Invalid UTF-8 bytes in `STRING` columns. Spark permits arbitrary byte sequences in a `STRING`
  column (for example from `CAST(X'C1' AS STRING)`), but Comet's native execution path is built on
  Arrow, whose string type is strictly UTF-8. Reading a Parquet file whose `STRING` column contains
  non-UTF-8 bytes fails with `Parquet error: encountered non UTF-8 data`. Disable Comet for the
  query, or cast the column to `BINARY` before persisting, if you need to preserve non-UTF-8 bytes.
  See [#4121](https://github.com/apache/datafusion-comet/issues/4121).

## `native_datafusion` Limitations

The `native_datafusion` scan has some additional limitations, mostly related to Parquet metadata. All of these
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

The following `native_datafusion` limitations may produce incorrect results on Spark versions prior to 4.0
without falling back to Spark:

- Reading `TimestampLTZ` as `TimestampNTZ`. On Spark 3.x, Spark raises an error per
  [SPARK-36182](https://issues.apache.org/jira/browse/SPARK-36182) because LTZ encodes UTC-adjusted instants
  that cannot be safely reinterpreted as timezone-free values. Comet does not raise this error and instead
  returns the raw UTC instant as a `TimestampNTZ` value. This applies to all LTZ physical encodings (INT96,
  TIMESTAMP_MICROS, TIMESTAMP_MILLIS). On Spark 4.0+, this read is permitted
  ([SPARK-47447](https://issues.apache.org/jira/browse/SPARK-47447)) and Comet matches Spark's behavior.
  See [#4219](https://github.com/apache/datafusion-comet/issues/4219).

### Schema Mismatch Handling

The issues in this subsection apply only when the requested read schema differs from the schema written
to the Parquet file. They do **not** affect a plain `spark.read.parquet(path)` that infers the schema
from file metadata, because in that case the requested schema and file schema match by construction.
Schema mismatch happens in two real-world scenarios:

1. The user provides an explicit read schema: `spark.read.schema(<schema>).parquet(path)` (or the
   equivalent DataFrame API).
2. **Schema evolution / partitioned reads** where files in a single dataset were written at different
   times with different types, or a table-format catalog (Iceberg, Delta) records a logical schema
   that has evolved past one or more underlying Parquet files. Spark coerces the file types to the
   table types at read time.

Spark's vectorized Parquet reader fully validates these conversions in `ParquetVectorUpdaterFactory.getUpdater`
and throws `SchemaColumnConvertNotSupportedException` for unsupported pairs. `native_datafusion` mirrors
that validation in its schema adapter; the entries below are the remaining gaps.

Note that the exact set of accepted conversions has changed between Spark versions
(for example, Spark 3.x's `schemaEvolution.enabled` flag gates `INT32 → INT64`, `FLOAT → DOUBLE`,
and `INT32 → DOUBLE` widening that Spark 4.0+ accepts unconditionally; `TimestampLTZ → TimestampNTZ`
is rejected by Spark 3.x but accepted by Spark 4.0+). Comet aims to follow the per-version Spark
behavior.

- **`ParquetSchemaConvert` errors do not include the file path**. The mismatch itself is detected and
  rejected correctly, but the resulting Spark error message reads
  `Encountered error while reading file . Data type mismatches…` (note the empty path). Behavior is
  consistent across Spark versions. See
  [#4316](https://github.com/apache/datafusion-comet/issues/4316).

## `native_iceberg_compat` Limitations

The `native_iceberg_compat` scan has the following additional limitation that may produce incorrect results
without falling back to Spark:

- Some Spark configuration values are hard-coded to their defaults rather than respecting user-specified values.
  This may produce incorrect results when non-default values are set. The affected configurations are
  `spark.sql.parquet.binaryAsString`, `spark.sql.parquet.int96AsTimestamp`, `spark.sql.caseSensitive`,
  `spark.sql.parquet.inferTimestampNTZ.enabled`, and `spark.sql.legacy.parquet.nanosAsLong`. See
  [issue #1816](https://github.com/apache/datafusion-comet/issues/1816) for more details.

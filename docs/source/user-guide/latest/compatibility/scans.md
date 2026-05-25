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

Comet's Parquet scan offloads decoding to native code and produces Arrow batches for the rest of
the plan. Comet falls back to Spark when the scan cannot be converted (for example, due to one of
the unsupported features listed below).

## Parquet Scan Limitations

The following features are not supported and cause Comet to fall back to Spark:

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
  V2 API for Parquet scans. Comet's Parquet scan only supports the V1 API.
- Spark metadata columns (e.g., `_metadata.file_path`)
- No support for row indexes
- No support for `input_file_name()`, `input_file_block_start()`, or `input_file_block_length()` SQL functions.
  Comet's Parquet scan does not use Spark's `FileScanRDD`, so these functions cannot populate their values.
- No support for `ignoreMissingFiles` or `ignoreCorruptFiles` being set to `true`
- Duplicate field names in case-insensitive mode (e.g., a Parquet file with both `B` and `b` columns)
  are detected at read time and raise a `SparkRuntimeException` with error class `_LEGACY_ERROR_TEMP_2093`,
  matching Spark's behavior.
- `spark.sql.parquet.enableVectorizedReader=false`. Disabling the vectorized reader opts into
  Spark's parquet-mr semantics (silent overflow, null-on-narrowing), which Comet's native reader
  does not replicate. By default Comet falls back to Spark in this case. Set
  `spark.comet.scan.allowDisabledParquetVectorizedReader=true` to opt in to running the
  Comet Parquet scan regardless. See
  [#4352](https://github.com/apache/datafusion-comet/issues/4352).

The following limitation may produce incorrect results without falling back to Spark:

- No support for datetime rebasing. When reading Parquet files containing dates or timestamps written before
  Spark 3.0 (which used a hybrid Julian/Gregorian calendar), dates/timestamps will be read as if they were
  written using the Proleptic Gregorian calendar. This may produce incorrect results for dates before
  October 15, 1582.

The following limitation raises an error at scan time rather than falling back to Spark:

- Invalid UTF-8 bytes in `STRING` columns. Spark permits arbitrary byte sequences in a `STRING`
  column (for example from `CAST(X'C1' AS STRING)`), but Comet's native execution path is built on
  Arrow, whose string type is strictly UTF-8. Reading a Parquet file whose `STRING` column contains
  non-UTF-8 bytes fails with `Parquet error: encountered non UTF-8 data`. Disable Comet for the
  query, or cast the column to `BINARY` before persisting, if you need to preserve non-UTF-8 bytes.
  See [#4121](https://github.com/apache/datafusion-comet/issues/4121).

The following limitation may produce incorrect results on Spark versions prior to 4.0
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
and throws `SchemaColumnConvertNotSupportedException` for unsupported pairs. Comet's Parquet scan
mirrors that validation in its schema adapter; the entries below are the remaining gaps.

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
- **Spark 3.x: extra `SparkException` layer in the cause chain**. The native error is translated to a
  `SparkException` whose cause is `SchemaColumnConvertNotSupportedException` (matching what Spark would
  throw); on Spark 3.x the executor / task error handling re-wraps this once more on the way back to
  the driver, producing a two-level chain (`SparkException → SparkException →
SchemaColumnConvertNotSupportedException`) instead of the one-level chain Spark's own vectorized
  reader produces. Code that catches `SparkException` and inspects only the immediate cause via
  `e.getCause.isInstanceOf[SchemaColumnConvertNotSupportedException]` will see the inner
  `SparkException` instead. Walk the cause chain to recover the
  `SchemaColumnConvertNotSupportedException`. Spark 4.0+ produces a single-level chain, matching
  vanilla Spark. See [#4354](https://github.com/apache/datafusion-comet/issues/4354).

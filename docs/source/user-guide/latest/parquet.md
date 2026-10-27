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

# Parquet

Comet's Parquet scan offloads decoding to native code (Rust, through DataFusion) and produces Arrow
batches for the rest of the plan. Comet falls back to Spark when the scan cannot be converted, for
example because the schema holds an unsupported type or the query uses one of the features listed
below. When the scan falls back to Spark, enabling `spark.comet.convert.parquet.enabled` converts
the data into Arrow format immediately, so the Comet pipeline can take over after the scan, although
the conversion may not be efficient.

This page covers what the scan supports, where its results can differ from Spark, how to tune it,
and the known issues. Parquet files managed by Apache Iceberg are read by the native Iceberg
reader, described on the [Iceberg](iceberg.md) page. Reading from object stores and HDFS is
described in [Supported Data Sources](datasources.md#supported-storages).

## Reading

### Unsupported features: fall back to Spark

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
- `_metadata.row_index`, `_metadata.file_block_start` and `_metadata.file_block_length`. When Spark splits a file,
  the native scan and Spark can read a row group in different splits, so the block values would differ. Other
  `_metadata` columns (`file_path`, `file_name`, `file_size`, `file_modification_time`) are supported.
- No support for `input_file_name()`, `input_file_block_start()`, or `input_file_block_length()` SQL functions.
  Comet's Parquet scan does not use Spark's `FileScanRDD`, so these functions cannot populate their values.
- No support for `ignoreMissingFiles` or `ignoreCorruptFiles` being set to `true`
- `spark.sql.parquet.enableVectorizedReader=false`. Disabling the vectorized reader opts into
  Spark's parquet-mr semantics (silent overflow, null-on-narrowing), which Comet's native reader
  does not replicate. By default Comet falls back to Spark in this case. Set
  `spark.comet.scan.allowDisabledParquetVectorizedReader=true` to opt in to running the
  Comet Parquet scan regardless.
- A read schema that repeats a Parquet field id, at the top level or within a struct, when
  `spark.sql.parquet.fieldId.read.enabled=true`.
- A read schema with sibling struct fields whose names collide case-insensitively, when
  `spark.sql.caseSensitive=false`. Spark's analyzer normally rejects such a schema before the scan
  is planned.

Some Parquet reads return results that differ from Spark without falling back to Spark, such as
reads of dates and timestamps written with the legacy hybrid Julian/Gregorian calendar
([#5010](https://github.com/apache/datafusion-comet/issues/5010)).
[Reads that differ from Spark](#reads-that-differ-from-spark) lists them, with the
issues that track them.

### Unsupported features: error at scan time

The following limitations raise an error at scan time rather than falling back to Spark. More
reads that fail where Spark succeeds are listed in
[Reads that fail where Spark succeeds](#reads-that-fail-where-spark-succeeds):

- Selecting a field by name when multiple physical siblings match, including inside structs,
  arrays, and maps. Comet raises a duplicate-field error instead of resolving the collision.
  Checks cover referenced columns, including predicates; unselected roots do not prevent
  reading a unique field by name or field ID. Exact-name projections of unique children in
  structs and arrays of structs remain supported. Casts that cannot use this pruning reject
  byte-identical duplicate siblings anywhere in the decoded physical subtree, including maps.
  Field-ID resolution retains precedence, but selecting a byte-identically duplicated physical
  root name still raises a duplicate-field error, even when the requested field is renamed.
  Names in separate groups do not collide. Spark may read a duplicate-bearing file with an
  explicit schema in case-sensitive mode, but its choice of sibling depends on the field shape
  and can produce unexpected values. Spark rejects schema inference from a single file with
  duplicate names; inference across files can depend on merge order.
  Resolution is tracked in [#5884](https://github.com/apache/datafusion-comet/issues/5884),
  with mixed-type behavior in [#5964](https://github.com/apache/datafusion-comet/issues/5964).
- Invalid UTF-8 bytes in `STRING` columns. Spark permits arbitrary byte sequences in a `STRING`
  column (for example from `CAST(X'C1' AS STRING)`), but Comet's native execution path is built on
  Arrow, whose string type is strictly UTF-8. Reading a Parquet file whose `STRING` column contains
  non-UTF-8 bytes fails with `Parquet error: encountered non UTF-8 data`. Disable Comet for the
  query, or cast the column to `BINARY` before persisting, if you need to preserve non-UTF-8 bytes.
  By contrast, Comet decodes non-UTF-8 bytes at the JVM-to-native Arrow FFI boundaries using
  JVM-compatible replacement semantics. This covers native query input from the JVM-exported Arrow
  stream (including JVM scans, shuffle reads, and `mapInArrow`), columnar-to-row conversion, and JVM
  UDF results. It does not change the native Parquet reader: rejecting invalid UTF-8 there remains
  Gap A of [#4764](https://github.com/apache/datafusion-comet/issues/4764). See
  [Strings with non-UTF-8 bytes](compatibility/index.md#strings-with-non-utf-8-bytes),
  [#4121](https://github.com/apache/datafusion-comet/issues/4121), and the tracking issue above.
- Reading `TimestampLTZ` as `TimestampNTZ` on Spark 3.x. Spark raises an error per
  [SPARK-36182](https://issues.apache.org/jira/browse/SPARK-36182) because LTZ encodes UTC-adjusted
  instants that cannot be safely reinterpreted as timezone-free values, and Comet matches this by
  rejecting the read. This applies to all LTZ physical encodings (INT96, TIMESTAMP_MICROS,
  TIMESTAMP_MILLIS). On Spark 4.0+, this read is permitted
  ([SPARK-47447](https://issues.apache.org/jira/browse/SPARK-47447)) and Comet matches Spark's
  behavior.

### Missing Nested Fields

When all requested leaf fields under a nested column are missing from a Parquet file, Spark 3.4
through 4.0 return NULL for the entire missing subtree, including nested arrays and maps. Comet
follows this behavior. On Spark 4.1+, the default (`false`) preserves the file's parent nullness.
Set `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing=true` for legacy behavior.
A field that exists in the file but contains only NULL values is not a missing field.

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

- **Conversion errors precede pushed row filters**. Empty files and files whose row groups or
  pages are all pruned read without decode-time conversion errors. When row-filter pushdown is
  enabled, Comet checks rejected conversions before reading surviving data pages, so a row filter
  cannot suppress the error by discarding every row. Legacy LIST shape mismatches that Spark cannot
  clip are rejected when the file is opened, including for empty and fully pruned files.
- **List conversion error paths assume Spark's standard encoding**. Comet inserts `list`
  before the element name when reporting a rejected array element conversion. Arrow's schema
  omits the repeated group name, so paths for legacy LIST encodings or custom group names may
  differ from Spark's Parquet column path.
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

### Read Error Wrapping

- **Spark 3.x: `TIMESTAMP_MILLIS` overflow past the first batch is wrapped in a file-read error**.
  When a Parquet `TIMESTAMP_MILLIS` value overflows on conversion to microseconds, Spark's reader
  throws `ArithmeticException: long overflow`. Spark 3.4 and 3.5 wrap a reader error in a
  `SparkException` that names the file only while reading the first batch of each file split. For
  the vectorized reader, that batch is the first `spark.sql.parquet.columnarReaderBatchSize` rows
  (4096 by default), or the first row group if that is smaller. An overflow in a later batch fails
  the task with the bare `ArithmeticException`. Spark 4.0+ wraps errors from every batch, and so does
  Comet on every Spark version. So on Spark 3.x, an overflow past the first batch produces a Comet
  error that names the file and has one more `SparkException` above the `ArithmeticException` in
  its cause chain than Spark's error.

## Writing

Comet can also write Parquet files natively. The native Parquet writer is for testing only and
should not be used in production. It is disabled by default and enabled with
`spark.comet.write.parquet.enabled=true`. It handles only non-partitioned, non-bucketed writes;
partitioned and bucketed writes fall back to Spark, as do writes with
`spark.sql.files.maxRecordsPerFile` set on Spark 4.0 and later. Parts of Spark's Parquet write
behavior are not reproduced, as listed under [Known Issues](#known-issues). Iceberg writes use a
different native writer, described on the [Iceberg](iceberg.md#writes) page.

## Tuning

### Filter Pushdown / Late Materialization

Setting `spark.comet.parquet.rowFilterPushdown.enabled=true` pushes filter evaluation into the Parquet
decode step and lazily materializes projected columns for surviving rows. This can significantly reduce
CPU and memory when the filter is highly selective on a small subset of columns. It is disabled by default
because it can hurt when the filter is not selective or when most columns must be read anyway. Row-group,
page-index, and bloom-filter pruning happen regardless of this flag whenever Spark's
`spark.sql.parquet.filterPushdown` is on, except for filters that compare `FLOAT` or `DOUBLE` values,
which do not prune while this flag is on (see [Floating-point Number Comparison](compatibility/floating-point.md)).

### Row Group Assignment to Splits

Spark and DataFusion's native Parquet scans use different rules to decide which row groups belong to a
given scan range (split). Spark assigns a row group to a split if the row group's start offset falls
within `[split.start, split.start + split.length)`, guaranteeing that every task Spark plans reads at
least one row group when the file layout permits. DataFusion's `prune_by_range` also checks whether a
row group's start offset falls within the split's byte range, but because row group sizes are not aligned
with Spark's split boundaries, the two systems can disagree on which split "owns" a given row group.

When a file contains row groups whose sizes are close to `spark.sql.files.maxPartitionBytes`, this
mismatch can leave some Comet scan tasks with no row groups to read. Those tasks still load Parquet
metadata but return zero rows, while neighboring tasks end up reading more row groups than Spark
intended. The overall effect is that Comet uses only a fraction of the parallelism that Spark planned
for the scan stage, and end-to-end scan latency increases even though the total amount of data read
is unchanged.

Symptoms to look for:

- A subset of scan tasks completes almost immediately and reports 0 input rows, while the remaining
  tasks read noticeably more rows than the equivalent Spark tasks would.
- The Comet scan stage has the same number of planned tasks as Spark but a much lower count of tasks
  that actually do work.

Workaround: lower `spark.sql.files.maxPartitionBytes` so that each split is smaller than a single row
group. For example, if the file's row groups are around 120 MB and `spark.sql.files.maxPartitionBytes`
is left at the 128 MB default, most splits will contain at most one row group boundary and the
mismatch is amplified; setting `spark.sql.files.maxPartitionBytes` below 120 MB (for example, 64 MB)
distributes row groups across more splits and reduces the number of idle tasks. Smaller values produce
more splits overall, so some idle tasks may remain — tune the value against your file layout.

See [#3817](https://github.com/apache/datafusion-comet/issues/3817#issuecomment-4193279630) for a
worked example and further discussion.

### Prefetching the Next File's Metadata

A native Parquet scan opens a task's files one after another, and each open waits for a round trip to read
the file's footer and page index before its data fetch can start. For tasks that read many small files from
high-latency storage such as S3, that wait is paid once per file. By default, Comet loads the next file's
footer and page index in the background while the current file is read. Set
`spark.comet.parquet.prefetchNextFileMetadata.enabled` to `false` to turn this off.

The cost is one extra footer and page index read for a file that is never opened, such as when a scan stops
early or the file is pruned before it opens. Scans of encrypted Parquet files never prefetch. The
`scan_io_metadata_prefetches` metric counts the opens that used a load which read storage ahead of them.

## Known Issues

This section lists open issues in which Comet's Parquet reads and writes give a different result
from Spark, or fail where Spark succeeds, without falling back. Each entry links to its tracking
issue. The cases where Comet falls back to Spark are listed under [Reading](#reading). Iceberg
tables have their own list under the Iceberg page's [Known Issues](iceberg.md#known-issues).
Storage-specific issues are listed under the [S3](datasources.md#s3-limitations) and
[Azure](datasources.md#azure-limitations) limitations.

Which entries apply depends on which native paths are enabled:

| Native path    | Setting                             | Default                |
| -------------- | ----------------------------------- | ---------------------- |
| Parquet scan   | `spark.comet.scan.enabled`          | `true`                 |
| Parquet writer | `spark.comet.write.parquet.enabled` | `false` (testing only) |

`spark.comet.scan.enabled` is a testing setting that Comet's own test suites use. It is not meant
for production, so do not turn it off to avoid an entry on this page.

### Reads that differ from Spark

- **Dates and timestamps written with the legacy calendar.** Spark before 3.0, and later versions
  with `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`, write these values in the hybrid
  Julian/Gregorian calendar. The native Parquet scan reads them as proleptic Gregorian, so values
  before October 15, 1582 are wrong in projections and predicates. It ignores
  `spark.sql.parquet.datetimeRebaseModeInRead` and the file's `org.apache.spark.legacyDateTime`
  metadata ([#5010](https://github.com/apache/datafusion-comet/issues/5010)). The
  [Delta scan](delta.md) rebases.
- **INT96 timestamps inside legacy-format lists.** INT96 timestamps outside the years 1677 to 2262
  read back wrong inside lists written in a legacy layout, as Hive and
  `spark.sql.parquet.writeLegacyFormat=true` write them
  ([#7029](https://github.com/apache/datafusion-comet/issues/7029)).
- **Nested BINARY values that are not valid UTF-8, read as STRING.** This applies to a BINARY
  column without the UTF8 annotation that is read as `string`, through the read schema or
  `spark.sql.parquet.binaryAsString=true`. Spark returns the bytes. Comet returns NULL for each
  such nested value. Top-level values match Spark in a release build, because the invalid bytes
  reach an Arrow string array unchecked. A debug build panics on them
  ([#7032](https://github.com/apache/datafusion-comet/issues/7032), part of
  [#4764](https://github.com/apache/datafusion-comet/issues/4764)).
- **Field-id reads where one file column matches a field by id and a sibling by name.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, Comet returns NULL for the sibling at the top
  level. Inside a struct, it returns the values of the field matched by id. Spark's own readers
  disagree here ([#6994](https://github.com/apache/datafusion-comet/issues/6994)).
- **`TIMESTAMP_MILLIS` values that overflow when converted to microseconds.** In nested fields, and
  in every column of a scan that has data filters, Comet returns NULL where Spark fails with
  `ArithmeticException: long overflow`
  ([#5553](https://github.com/apache/datafusion-comet/issues/5553),
  [#5739](https://github.com/apache/datafusion-comet/issues/5739)).
- **Reads that Spark rejects.** Comet returns rows where Spark fails the read for `INT` columns
  read as `INTERVAL DAY TO SECOND` ([#5188](https://github.com/apache/datafusion-comet/issues/5188))
  and Variant-annotated fields read as a plain struct
  ([#5741](https://github.com/apache/datafusion-comet/issues/5741)).

### Writes that differ from Spark

With `spark.comet.write.parquet.enabled=true`:

- **The datetime rebase modes are ignored.** On Spark 3.4 and 3.5, whose default rebase mode is
  `EXCEPTION`, the native writer writes dates and timestamps before October 15, 1582 that Spark
  refuses to write, and Spark then fails to read them back under its default read mode. With
  `spark.sql.parquet.datetimeRebaseModeInRead=LEGACY`, Spark reads them shifted by several days, on
  every version. The files lack the `org.apache.spark.version` footer key that tells Spark they need
  no rebasing ([#7047](https://github.com/apache/datafusion-comet/issues/7047),
  [#3427](https://github.com/apache/datafusion-comet/issues/3427)).
- **`spark.sql.parquet.outputTimestampType` and `spark.sql.parquet.writeLegacyFormat` are
  ignored.** Timestamps are always written as `INT64` microseconds, never as Spark's default
  `INT96`, so with `TIMESTAMP_MILLIS` they keep the sub-millisecond digits that Spark drops. Spark's
  schema footer key is not written either, so a path-based read loses column metadata such as
  comments, and with `spark.sql.parquet.inferTimestampNTZ.enabled=false` it reads `TIMESTAMP_NTZ`
  columns as `TIMESTAMP` ([#7048](https://github.com/apache/datafusion-comet/issues/7048),
  [#3425](https://github.com/apache/datafusion-comet/issues/3425)).
- **Spark 3.4 and 3.5: `INSERT INTO` leaves the target stale.** Comet skips the steps Spark runs
  after a write: file index refresh, cache refresh, partition refresh and statistics update. Later
  queries can return the table's previous contents until `REFRESH TABLE`
  ([#6315](https://github.com/apache/datafusion-comet/issues/6315)).
- **Spark's Parquet writer properties are ignored.** `parquet.block.size`, page size, dictionary,
  writer version and statistics settings never reach the native writer
  ([#5304](https://github.com/apache/datafusion-comet/issues/5304)).
- **An input with no partitions writes no file.** Spark writes one file that holds the schema
  ([#5303](https://github.com/apache/datafusion-comet/issues/5303)).
- **Spark SQL tests that still fail with the native writer**, not yet classified:
  - CTAS schema ([#3418](https://github.com/apache/datafusion-comet/issues/3418))
  - CTAS with `CHAR_AS_VARCHAR` ([#3419](https://github.com/apache/datafusion-comet/issues/3419))
  - CTAS with a union ([#3429](https://github.com/apache/datafusion-comet/issues/3429))
  - `INSERT INTO` with default columns ([#3421](https://github.com/apache/datafusion-comet/issues/3421))
  - `INSERT INTO` after `ALTER TABLE ADD COLUMNS` ([#3422](https://github.com/apache/datafusion-comet/issues/3422))
  - `INSERT INTO` from non-projection queries ([#3423](https://github.com/apache/datafusion-comet/issues/3423))
  - the `INSERT OVERWRITE` output schema ([#3424](https://github.com/apache/datafusion-comet/issues/3424))
  - multiple inserts ([#3430](https://github.com/apache/datafusion-comet/issues/3430))
  - `INSERT OVERWRITE LOCAL DIRECTORY` ([#3420](https://github.com/apache/datafusion-comet/issues/3420))
  - empty files ([#3417](https://github.com/apache/datafusion-comet/issues/3417))
  - `onTaskCommit` ([#3428](https://github.com/apache/datafusion-comet/issues/3428))

### Reads that fail where Spark succeeds

- **STRING columns that hold invalid UTF-8**, and **duplicate field names**. See
  [Unsupported features: error at scan time](#unsupported-features-error-at-scan-time).
- **A duplicate field id in a column the query does not read.** With a valid read schema and
  `spark.sql.parquet.fieldId.read.enabled=true`, a file in which two root columns carry the same
  field id fails the scan whenever the table schema has a field with that id, even when the query
  does not read that field ([#7006](https://github.com/apache/datafusion-comet/issues/7006)).
- **A nested encoding that arrow-rs rejects.** A file fails to read when any column in its schema
  uses such an encoding, even a column the query does not read
  ([#7035](https://github.com/apache/datafusion-comet/issues/7035)).
- **A `DATE` column read as `INTERVAL YEAR TO MONTH`.** Spark reads it, and Comet fails the read
  ([#5188](https://github.com/apache/datafusion-comet/issues/5188)).

### Writes that fail where Spark succeeds

With `spark.comet.write.parquet.enabled=true`:

- **Row groups that outgrow the memory pool.** The native writer cuts row groups by row count
  only, where Spark uses `parquet.block.size`, and it cannot spill. With wide or nested rows, the
  open row group can outgrow the off-heap pool and fail the task. The audit did not reproduce a
  failure, because a 239 MB row group fit in a 256 MB pool
  ([#5304](https://github.com/apache/datafusion-comet/issues/5304)).

### Differences that are not Comet bugs

- **Field-id reads with a read schema that swaps two nested field names.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, Spark's vectorized reader pairs the file's columns
  with the read fields by the file's names, so it fails with a type mismatch or returns no rows.
  Comet resolves the fields by id.

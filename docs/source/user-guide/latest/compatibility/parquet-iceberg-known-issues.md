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

# Known Issues in Parquet and Iceberg Reads and Writes

This page lists open issues in Comet's Parquet and Iceberg reads and writes where Comet does not
fall back to Spark. Each entry links to its tracking issue. The entries are in two sections:

- [Results that differ from Spark](#results-that-differ-from-spark): wrong values, a NULL where
  Spark returns a value or the reverse, rows returned for a read that Spark rejects, and written
  files that Spark or other engines read differently from the files Spark writes.
- [Queries that fail where Spark succeeds](#queries-that-fail-where-spark-succeeds).

The cases where Comet falls back to Spark are documented in [Parquet Compatibility](scans.md), in
the [current limitations](../iceberg.md#current-limitations) of the Iceberg guide and in
[Iceberg Writes](../iceberg-writes.md).

Which entries apply depends on which native paths are enabled:

| Native path    | Setting                                  | Default                |
| -------------- | ---------------------------------------- | ---------------------- |
| Parquet scan   | `spark.comet.scan.enabled`               | `true`                 |
| Iceberg scan   | `spark.comet.scan.icebergNative.enabled` | `true`                 |
| Iceberg writer | `spark.comet.write.iceberg.enabled`      | `false`                |
| Parquet writer | `spark.comet.write.parquet.enabled`      | `false` (testing only) |

`spark.comet.scan.enabled` is a testing setting that Comet's own test suites use. It is not meant
for production, so do not turn it off to avoid an entry on this page.

## Results that differ from Spark

### Parquet reads

- **Dates and timestamps written with the legacy calendar.** Spark before 3.0, and later versions
  with `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`, write these values in the hybrid
  Julian/Gregorian calendar. The native Parquet scan reads them as proleptic Gregorian, so values
  before October 15, 1582 are wrong in projections and predicates. It ignores
  `spark.sql.parquet.datetimeRebaseModeInRead` and the file's `org.apache.spark.legacyDateTime`
  metadata ([#5010](https://github.com/apache/datafusion-comet/issues/5010)). The
  [Delta scan](../delta.md) rebases.
- **INT96 timestamps inside legacy-format lists.** INT96 timestamps outside the years 1677 to 2262
  read back wrong inside lists written in a legacy layout, as Hive and
  `spark.sql.parquet.writeLegacyFormat=true` write them
  ([#7029](https://github.com/apache/datafusion-comet/issues/7029)).
- **A struct or array with none of its requested fields in the file.** Spark returns NULL for the
  column. Comet returns a non-null struct or array that holds NULLs. This applies to Spark 3.4, 3.5
  and 4.0, and to Spark 4.1 and later with
  `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing=true`
  ([#7030](https://github.com/apache/datafusion-comet/issues/7030)).
- **Nested BINARY values that are not valid UTF-8, read as STRING.** This applies to a BINARY
  column without the UTF8 annotation that is read as `string`, through the read schema or
  `spark.sql.parquet.binaryAsString=true`. Spark returns the bytes. Comet returns NULL for each
  such nested value. Top-level values match Spark in a release build, because the invalid bytes
  reach an Arrow string array unchecked. A debug build panics on them
  ([#7032](https://github.com/apache/datafusion-comet/issues/7032), part of
  [#4764](https://github.com/apache/datafusion-comet/issues/4764)).
- **Spark 3.4: a struct matched by field id under a different name.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, a struct column that the read schema matches to
  the file by field id but under another name reads as NULL when Spark prunes its nested fields
  ([#7031](https://github.com/apache/datafusion-comet/issues/7031)).
- **Field-id reads where one file column matches a field by id and a sibling by name.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, Comet returns NULL for the sibling at the top
  level. Inside a struct, it returns the values of the field matched by id. Spark's own readers
  disagree here ([#6994](https://github.com/apache/datafusion-comet/issues/6994)).
- **`TIMESTAMP_MILLIS` values that overflow when converted to microseconds.** In nested fields, and
  in every column of a scan that has data filters, Comet returns NULL where Spark fails with
  `ArithmeticException: long overflow`
  ([#5553](https://github.com/apache/datafusion-comet/issues/5553),
  [#5739](https://github.com/apache/datafusion-comet/issues/5739)).
- **Reads that Spark rejects.** Comet returns rows where Spark fails the read for
  `TIMESTAMP(NANOS)` columns read as `timestamp` or `timestamp_ntz`
  ([#7034](https://github.com/apache/datafusion-comet/issues/7034)), `INT` columns read as
  `INTERVAL DAY TO SECOND` ([#5188](https://github.com/apache/datafusion-comet/issues/5188)), and
  Variant-annotated fields read as a plain struct
  ([#5741](https://github.com/apache/datafusion-comet/issues/5741)).

### Iceberg reads

- **Migrated tables that use a name mapping.** The `migrate`, `snapshot` and `add_files`
  procedures set `schema.name-mapping.default` for files without Iceberg field ids. When a query
  also reads a top-level primitive column, the native scan returns NULL for nested fields (struct
  fields, list elements and map values). Filters on those fields then select the wrong rows
  ([#6790](https://github.com/apache/datafusion-comet/issues/6790)). The fix needs an iceberg-rust
  upgrade ([#6934](https://github.com/apache/datafusion-comet/issues/6934)).
- **Data files with neither field ids nor a name mapping.** Spark reads every nested field of such
  a file as NULL, and the native scan returns the values stored in the file
  ([#6794](https://github.com/apache/datafusion-comet/issues/6794)).
- **`VERSION AS OF` reads of a column renamed after the snapshot.** The renamed column reads as
  NULL, and two columns that swapped names return each other's values
  ([#6822](https://github.com/apache/datafusion-comet/issues/6822)).
- **Reflection failures while the scan is planned.** If Comet fails to read the table's schema or
  metadata through reflection, it continues with an empty or partial result instead of falling
  back, which can resolve a column to a different field id
  ([#5257](https://github.com/apache/datafusion-comet/issues/5257)).

### Parquet writes

The native Parquet writer is experimental and disabled by default. With
`spark.comet.write.parquet.enabled=true`:

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

### Iceberg writes

The native Iceberg writer is disabled by default. With `spark.comet.write.iceberg.enabled=true`:

- **Brotli tables.** The native writer commits brotli-compressed files. JVM Parquet readers can
  read them only if Hadoop's `BrotliCodec` is on the classpath. iceberg-java fails the write
  instead ([#6960](https://github.com/apache/datafusion-comet/issues/6960)).
- **`write.parquet.compression-codec=none`.** The native writer writes the table uncompressed, and
  iceberg-java rejects the value. The native writer also trims or lowercases a few other settings
  that iceberg-java reads as written
  ([#7008](https://github.com/apache/datafusion-comet/issues/7008)).

The physical differences that every native data file has, and why they are safe, are listed in
[Accepted divergences](../iceberg-writes.md#accepted-divergences).

## Queries that fail where Spark succeeds

In these cases a Comet read or write fails with an error where Spark, or iceberg-java, succeeds.
Comet does not fall back to Spark for them.

### Parquet reads that fail

- **STRING columns that hold invalid UTF-8.** The read fails with
  `Parquet error: encountered non UTF-8 data`, where Spark returns the bytes
  ([#4121](https://github.com/apache/datafusion-comet/issues/4121), part of
  [#4764](https://github.com/apache/datafusion-comet/issues/4764)).
- **Duplicate field names.** Selecting a field by name when several physical siblings match it,
  including inside structs, arrays and maps, fails with a duplicate-field error
  ([#5884](https://github.com/apache/datafusion-comet/issues/5884),
  [#5964](https://github.com/apache/datafusion-comet/issues/5964)).
  [Parquet Compatibility](scans.md) describes the reads this affects.
- **A duplicate field id in a column the query does not read.** With a valid read schema and
  `spark.sql.parquet.fieldId.read.enabled=true`, a file in which two root columns carry the same
  field id fails the scan whenever the table schema has a field with that id, even when the query
  does not read that field ([#7006](https://github.com/apache/datafusion-comet/issues/7006)).
- **A nested encoding that arrow-rs rejects.** A file fails to read when any column in its schema
  uses such an encoding, even a column the query does not read
  ([#7035](https://github.com/apache/datafusion-comet/issues/7035)).
- **A `DATE` column read as `INTERVAL YEAR TO MONTH`.** Spark reads it, and Comet fails the read
  ([#5188](https://github.com/apache/datafusion-comet/issues/5188)).

### Iceberg reads that fail

- **A data file that omits an optional nested field.** The native scan fails on a data file that
  lacks an optional struct field, which a write with `write.spark.accept-any-schema=true` can
  produce without any schema change
  ([#7033](https://github.com/apache/datafusion-comet/issues/7033)).
- **`VERSION AS OF` reads of a column whose type was promoted after the snapshot**
  ([#6993](https://github.com/apache/datafusion-comet/issues/6993)).
- **An equality delete keyed on a nested field.** Every query the native scan runs on the table
  fails with `field not found`, even one that reads the key
  ([#6782](https://github.com/apache/datafusion-comet/issues/6782)).
- **Column names that only newer Unicode case data folds together**, such as a pair of Vithkuqi
  letters, which Unicode 14 added. Queries on such a table fail with a duplicate-field error
  ([#5540](https://github.com/apache/datafusion-comet/issues/5540)).

### Object store and credential settings

The native Parquet and Iceberg scans resolve storage settings with their own clients rather than
with Hadoop's. Where the two disagree, a native read can use a different identity or route than
Spark, and fail where Spark succeeds:

- **S3 settings that Hadoop's S3A client resolves differently**, such as JCEKS-backed secrets and
  the precedence of per-bucket settings. The native Parquet scan forwards them without checking
  for a difference ([#5662](https://github.com/apache/datafusion-comet/issues/5662)).
- **AWS profile credential providers.** The native scans ignore `fs.s3a.auth.profile.name` and
  `fs.s3a.auth.profile.file` ([#4245](https://github.com/apache/datafusion-comet/issues/4245)),
  and resolve some profiles differently from Hadoop 3.4 and later, for example profiles that use
  `credential_process` or chains of assumed roles
  ([#6575](https://github.com/apache/datafusion-comet/issues/6575)).
- **Azure credentials.** Container-scoped OAuth settings, Fabric hosts and a few other
  configurations resolve differently from Hadoop's ABFS driver
  ([#6605](https://github.com/apache/datafusion-comet/issues/6605)).
- **Iceberg tables whose metadata location has no host.** The native Iceberg scan skips the
  credential provider configured with `s3.comet.credential.provider.class`
  ([#6536](https://github.com/apache/datafusion-comet/issues/6536)).

### Parquet writes that fail

With `spark.comet.write.parquet.enabled=true`:

- **Row groups that outgrow the memory pool.** The native writer cuts row groups by row count
  only, where Spark uses `parquet.block.size`, and it cannot spill. With wide or nested rows, the
  open row group can outgrow the off-heap pool and fail the task. The audit did not reproduce a
  failure, because a 239 MB row group fit in a 256 MB pool
  ([#5304](https://github.com/apache/datafusion-comet/issues/5304)).

### Iceberg writes that fail

With `spark.comet.write.iceberg.enabled=true`:

- **A write that keeps one file open and outgrows the off-heap pool.** An unpartitioned or
  clustered write fails its task when its open file outgrows the pool, where iceberg-java writes
  it on the JVM heap. [Failure handling](../iceberg-writes.md#failure-handling) describes the
  error and the settings that let such a write fit.

## Differences that are not Comet bugs

In these cases Comet's result differs from Spark's, but the bug is in Spark or iceberg-java:

- **iceberg-java 1.9 and later: `IS NULL` on a float or double field under a NULL struct.**
  iceberg-java leaves the rows where the struct is NULL out of the field's null count, so a filter
  such as `WHERE s.x IS NULL` can skip data files that hold matching rows, with or without Comet
  ([apache/iceberg#17561](https://github.com/apache/iceberg/issues/17561)). Files written by Comet's
  native Iceberg writer are not affected.
- **iceberg-java before 1.12: a NULL struct whose first field is an identity partition source.**
  iceberg-java reads the struct as a struct of NULLs, and Comet returns NULL, as iceberg-java 1.12
  does. [apache/iceberg#17320](https://github.com/apache/iceberg/pull/17320) is titled for nested
  initial defaults, but it fixes this case too: it keeps one file column in the read, so the
  struct's definition level survives when every projected field is a constant, and an identity
  partition value is such a constant.
- **Field-id reads with a read schema that swaps two nested field names.** Spark's vectorized
  reader pairs the file's columns with the read fields by the file's names, so it fails with a type
  mismatch or returns no rows. Comet resolves the fields by id.

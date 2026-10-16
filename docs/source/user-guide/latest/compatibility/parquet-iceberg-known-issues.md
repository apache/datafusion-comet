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

# Known Correctness Issues in Parquet and Iceberg Reads and Writes

This page lists the open issues in which a Comet Parquet or Iceberg read or write gives a different
result from Spark without falling back to Spark: wrong values, a NULL where Spark returns a value or
the reverse, rows returned for a read that Spark rejects, and written files that Spark or other
engines read differently from the files Spark writes. Each entry links to the issue that tracks it.

Queries that fail where Spark succeeds, and the cases where Comet falls back to Spark, are
documented in [Parquet Compatibility](scans.md), in the
[current limitations](../iceberg.md#current-limitations) of the Iceberg guide and in
[Iceberg Writes](../iceberg-writes.md).

Which entries apply depends on which native paths are enabled:

| Native path    | Setting                                  | Default                |
| -------------- | ---------------------------------------- | ---------------------- |
| Parquet scan   | `spark.comet.scan.enabled`               | `true`                 |
| Iceberg scan   | `spark.comet.scan.icebergNative.enabled` | `true`                 |
| Iceberg writer | `spark.comet.write.iceberg.enabled`      | `false`                |
| Parquet writer | `spark.comet.write.parquet.enabled`      | `false` (testing only) |

## Parquet reads

- **Dates and timestamps written with the legacy calendar.** Comet does not rebase dates and
  timestamps written with the hybrid Julian/Gregorian calendar, which Spark versions before 3.0
  wrote, and later versions write with `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`. Comet
  reads them as proleptic Gregorian values, so values before October 15, 1582 are wrong in both
  projections and predicates. Comet also ignores `spark.sql.parquet.datetimeRebaseModeInRead` and
  the file's `org.apache.spark.legacyDateTime` metadata, which would tell it to rebase
  ([#5010](https://github.com/apache/datafusion-comet/issues/5010)).
- **INT96 timestamps inside legacy-format lists.** INT96 timestamps outside the years 1677 to 2262
  read back wrong inside lists written in a legacy layout, as Hive and
  `spark.sql.parquet.writeLegacyFormat=true` write them
  ([#7029](https://github.com/apache/datafusion-comet/issues/7029)).
- **A struct or array with none of its requested fields in the file.** Spark returns NULL for the
  column, and Comet returns a non-null struct or array that holds NULLs. That is Spark's only
  behavior on Spark 3.4, 3.5 and 4.0, and its behavior on Spark 4.1 and later with
  `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing=true`
  ([#7030](https://github.com/apache/datafusion-comet/issues/7030)).
- **Nested BINARY values that are not valid UTF-8, read as STRING.** When a BINARY column without
  the UTF8 annotation is read as STRING, because the read schema says `string` or because of
  `spark.sql.parquet.binaryAsString=true`, Comet returns NULL for each nested value that is not
  valid UTF-8 where Spark returns the bytes. Top-level values read this way skip UTF-8 validation
  ([#7032](https://github.com/apache/datafusion-comet/issues/7032), part of
  [#4764](https://github.com/apache/datafusion-comet/issues/4764)).
- **Spark 3.4: a struct matched by field id under a different name.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, a struct column that the read schema matches to
  the file by field id but under another name reads as NULL when Spark prunes its nested fields
  ([#7031](https://github.com/apache/datafusion-comet/issues/7031)).
- **Field-id reads in which one file column matches a field by id and a sibling by name.** With
  `spark.sql.parquet.fieldId.read.enabled=true`, Comet returns NULL for the sibling at the top
  level, and the values of the field matched by id inside a struct. Spark's own readers disagree
  with each other here ([#6994](https://github.com/apache/datafusion-comet/issues/6994)).
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
- **Dynamic partition pruning under AQE.** Two exchanges whose scans differ only in their dynamic
  partition pruning filter can compare equal, so AQE reuses the first for the second and the rows of
  the second branch are lost. It shows when no coalesced shuffle read sits between the stages, as
  with large shuffles or with partition coalescing off, and it applies to the native Iceberg scan
  as well ([#6264](https://github.com/apache/datafusion-comet/issues/6264)).

## Iceberg reads

- **Migrated tables that use a name mapping.** For data files without Iceberg field ids that are
  read through `schema.name-mapping.default`, which the `migrate`, `snapshot` and `add_files`
  procedures set, the native scan returns NULL for nested fields (struct fields, list elements and
  map values) whenever the query also reads a top-level primitive column, so filters on them select
  the wrong rows ([#6790](https://github.com/apache/datafusion-comet/issues/6790)). The fix comes
  with an iceberg-rust upgrade ([#6934](https://github.com/apache/datafusion-comet/issues/6934)).
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

## Parquet writes

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
- **Spark 3.4 and 3.5: `INSERT INTO` leaves the target stale.** The steps Spark runs after the
  write (file index refresh, cache refresh, partition refresh and statistics update) are skipped,
  so later queries can read the table's previous contents until `REFRESH TABLE`
  ([#6315](https://github.com/apache/datafusion-comet/issues/6315)).
- **An input with no partitions writes no file.** Spark writes one file that holds the schema
  ([#5303](https://github.com/apache/datafusion-comet/issues/5303)).
- **Spark SQL tests that still fail with the native writer**, not yet classified: CTAS schema
  ([#3418](https://github.com/apache/datafusion-comet/issues/3418)), CTAS with `CHAR_AS_VARCHAR`
  ([#3419](https://github.com/apache/datafusion-comet/issues/3419)), CTAS with a union
  ([#3429](https://github.com/apache/datafusion-comet/issues/3429)), `INSERT INTO` with default
  columns ([#3421](https://github.com/apache/datafusion-comet/issues/3421)) and after
  `ALTER TABLE ADD COLUMNS` ([#3422](https://github.com/apache/datafusion-comet/issues/3422)),
  `INSERT INTO` from non-projection queries
  ([#3423](https://github.com/apache/datafusion-comet/issues/3423)), the `INSERT OVERWRITE` output
  schema ([#3424](https://github.com/apache/datafusion-comet/issues/3424)), multiple inserts
  ([#3430](https://github.com/apache/datafusion-comet/issues/3430)),
  `INSERT OVERWRITE LOCAL DIRECTORY`
  ([#3420](https://github.com/apache/datafusion-comet/issues/3420)), empty files
  ([#3417](https://github.com/apache/datafusion-comet/issues/3417)) and `onTaskCommit`
  ([#3428](https://github.com/apache/datafusion-comet/issues/3428)).

## Iceberg writes

The native Iceberg writer is disabled by default. With `spark.comet.write.iceberg.enabled=true`:

- **Brotli tables.** A native write commits brotli-compressed files that no JVM Parquet reader can
  read unless Hadoop's `BrotliCodec` is on its classpath, where iceberg-java fails the write
  ([#6960](https://github.com/apache/datafusion-comet/issues/6960)).
- **`write.parquet.compression-codec=none`.** The native writer writes the table uncompressed,
  where iceberg-java rejects the value. It also trims or lowercases a few other settings that
  iceberg-java reads as written ([#7008](https://github.com/apache/datafusion-comet/issues/7008)).

The physical differences that every native data file has, and why they are safe, are listed in
[Accepted divergences](../iceberg-writes.md#accepted-divergences).

## Differences that are not Comet bugs

In these cases Comet's result differs from Spark's, but the bug is in Spark or iceberg-java:

- **iceberg-java 1.9 and later: `IS NULL` on a float or double field under a NULL struct.**
  iceberg-java leaves the rows where the struct is NULL out of the field's null count, so a filter
  such as `WHERE s.x IS NULL` can skip data files that hold matching rows, with or without Comet
  ([apache/iceberg#17561](https://github.com/apache/iceberg/issues/17561)). Files written by Comet's
  native Iceberg writer are not affected.
- **iceberg-java before 1.12: a NULL struct whose first field is an identity partition source.**
  iceberg-java reads the struct as a struct of NULLs, and Comet returns NULL, as iceberg-java 1.12
  does ([apache/iceberg#17320](https://github.com/apache/iceberg/pull/17320)).
- **Field-id reads with a read schema that swaps two nested field names.** Spark's vectorized
  reader pairs the file's columns with the read fields by the file's names, so it fails with a type
  mismatch or returns no rows. Comet resolves the fields by id.

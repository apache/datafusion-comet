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

# Parquet Audits

> Audit notes for the native Parquet scan and the native Parquet writer. Absence of an entry means
> the area has not been audited yet. Open issues are listed for users in
> [Known Issues in Parquet and Iceberg Reads and Writes].

## Native Parquet scan

### Nested types (audited 2026-10-15)

- **Scope:** structs, arrays and maps. NULL structs, arrays and maps, NULLs inside them, Parquet
  field ids, case-insensitive names and read schemas that differ from the file's.
- **Method:** about 3,000 queries compared with Spark with Comet disabled, on Spark 3.4.3, 3.5.9
  and 4.1.3 at `main` 66c63bd307. A code review of DataFusion 55.2 and arrow-rs 59.3 ran alongside,
  and each finding was reproduced before it was filed.
- **Filed:** [#7029] (INT96 timestamps inside legacy-format lists), [#7030] (a nested column with
  none of its requested fields in the file), [#7031] (Spark 3.4: a struct matched by field id under
  a new name, under nested pruning), [#7032] (nested BINARY values that are not valid UTF-8, read as
  STRING), [#7034] (`TIMESTAMP(NANOS)` read as `timestamp`), [#7035] (a nested encoding that
  arrow-rs rejects fails the whole file), [#7036] and [#7037] (nested pruning gaps), and
  [apache/datafusion#26393] upstream for [#7029]. All are tracked in [#7040].
- **Matched Spark:** NULL structs, arrays and maps, empty arrays and maps, and NULLs inside them at
  every level, in deep (eight levels) and wide (300 fields) structs. Spark-written files in five
  layouts (default, tiny row groups and pages, the legacy format, v2 pages without dictionaries and
  v2 tiny pages), read with batch sizes of 1 to 3, with row-filter pushdown on and off and with
  filter pushdown off. Avro, Thrift, Hive and protobuf list and map encodings, maps with struct and
  array keys, nested type promotion, and nested timestamp, decimal, boolean and binary leaves
  (micros, millis, and INT96 in standard layouts). ENUM, JSON, BSON, unsigned and binary-decimal
  annotations, files written by arrow-rs, partitioned tables, nested schemas that differ across the
  files of one scan, case-insensitive nested names, nested field ids (renamed, missing and partly
  present), and both modes of SPARK-53535 on Spark 4.1.
- **Not Comet bugs:** with `spark.sql.parquet.fieldId.read.enabled=true` and a read schema that
  swaps two nested field names, Spark's vectorized reader pairs the file's columns with the read
  fields by the file's names, so it fails or returns no rows, while Comet resolves the fields by
  id. On Spark 3.5 and later, nested schema pruning drops a root field id, so Spark reads a renamed
  root as NULL, and Comet matches it there. A map with NULL keys from a non-Spark writer fails in
  both engines.
- **Not covered:** Variant ([#5438]), the V2 Parquet source, encrypted files, and the JVM-side
  conversion paths.

## Native Parquet writer

### Nested types (audited 2026-10-16)

- **Scope:** NULL structs, arrays and maps, NULLs inside them, Parquet field ids, column statistics,
  and the file's schema and footer.
- **Method:** each write made natively and by Spark with Comet disabled, on Spark 3.4.3, 3.5.9 and
  4.1.3 at `main` c230bf4e98. The comparison covered the values read back by Spark and by Comet,
  the Parquet schema with its repetition and field ids, the footer key-value metadata and the
  statistics of every leaf. A code review of the writer and arrow-rs 59.3 ran alongside, and each
  finding was reproduced before it was filed.
- **Filed:** [#7047] (the datetime rebase modes are ignored, so Spark rejects or shifts the
  writer's dates before 1582), [#7048] (`outputTimestampType`, `writeLegacyFormat` and Spark's
  schema footer key), and a measurement on [#5304]: for nested rows the row-count cap gives one row
  group per file, 239 MB for 100,000 rows of 100-element arrays, where Spark writes row groups of
  about 128 MB. All are tracked in [#7051].
- **Matched Spark:** values, repetition and field ids for NULL structs, arrays and maps, empty
  arrays and maps, and NULLs inside them at every level, in deep and wide schemas. That includes
  input sliced by `LIMIT` and `OFFSET`, filtered input, and nested values computed by expressions
  before the write. Field ids at every level, with `spark.sql.parquet.fieldId.write.enabled` on and
  off. Null counts, value counts, minimums and maximums for every leaf. Nested timestamp, decimal,
  boolean and binary leaves.
- **Did not reproduce:** a write of void (`NullType`) columns on Spark 3.4 and 3.5, which Spark
  rejects at analysis before Comet plans the write. A write of ANSI interval columns, which did not
  reach the native writer on any of the three versions. A task failure from the in-progress row
  group outgrowing a 256 MB off-heap pool: the 239 MB row group fit.
- **Not covered:** Variant ([#5433]), partitioned and bucketed writes, which the native writer does
  not support ([#3193], [#3194]), object-store and HDFS outputs, and write throughput, which needs a
  release build.

[Known Issues in Parquet and Iceberg Reads and Writes]: ../../user-guide/latest/compatibility/parquet-iceberg-known-issues.md
[apache/datafusion#26393]: https://github.com/apache/datafusion/issues/26393
[#3193]: https://github.com/apache/datafusion-comet/issues/3193
[#3194]: https://github.com/apache/datafusion-comet/issues/3194
[#5304]: https://github.com/apache/datafusion-comet/issues/5304
[#5433]: https://github.com/apache/datafusion-comet/issues/5433
[#5438]: https://github.com/apache/datafusion-comet/issues/5438
[#7029]: https://github.com/apache/datafusion-comet/issues/7029
[#7030]: https://github.com/apache/datafusion-comet/issues/7030
[#7031]: https://github.com/apache/datafusion-comet/issues/7031
[#7032]: https://github.com/apache/datafusion-comet/issues/7032
[#7034]: https://github.com/apache/datafusion-comet/issues/7034
[#7035]: https://github.com/apache/datafusion-comet/issues/7035
[#7036]: https://github.com/apache/datafusion-comet/issues/7036
[#7037]: https://github.com/apache/datafusion-comet/issues/7037
[#7040]: https://github.com/apache/datafusion-comet/issues/7040
[#7047]: https://github.com/apache/datafusion-comet/issues/7047
[#7048]: https://github.com/apache/datafusion-comet/issues/7048
[#7051]: https://github.com/apache/datafusion-comet/issues/7051

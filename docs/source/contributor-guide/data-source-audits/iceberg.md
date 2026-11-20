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

# Iceberg Audits

> Audit notes for the native Iceberg scan and the native Iceberg writer. Absence of an entry means
> the area has not been audited yet. Open issues are listed for users in
> [Known Issues in the Iceberg guide].

## Native Iceberg scan

### Nested types (audited 2026-10-15)

- **Scope:** structs, arrays and maps. NULLs at every level, field ids, name mappings, schema
  evolution, deletes and partitions.
- **Method:** queries compared with Spark with Comet disabled, on Spark 4.1.3 with Iceberg 1.11 at
  `main` 66c63bd307. A code review of iceberg-rust at the current pin ran alongside, and each
  finding was reproduced before it was filed.
- **Filed:** [#7033] (a data file that omits an optional nested field fails the scan), [#7038]
  (residual predicates on struct fields are not pushed into iceberg-rust) and [#7039] (a file is
  read in full when the query needs none of its columns). The audit also confirmed the open issues
  [#6790], [#6794], [#6822], [#6993], [#6782], [#5540] and [#5257]. All are tracked in [#7040].
- **Matched Spark:** the nested data of the Parquet scan audit written by iceberg-java,
  merge-on-read position deletes across three-row pages (format v2, and v3 deletion vectors),
  equality deletes including one with a NULL key, required nested fields under NULL parents, nested
  UUID, fixed, timestamptz and timestamp_ntz leaves, nested type promotion inside structs, lists
  and maps, the nested schema evolution fallback for lists and maps, `_partition` with NULL values
  and spec evolution, and nested partition sources.
- **Did not reproduce:** reordered struct siblings of the same type swapping values when the column
  holds a map, in more than 20 probes, CTAS included.
- **Not Comet bugs:** on a table identity-partitioned by a struct field, iceberg-java before 1.12
  reads a NULL struct as a struct of NULLs when the partition source is the struct's first field.
  [apache/iceberg#17320] fixed it in 1.12.0. Its title names nested initial defaults, but the
  change also keeps a file column in the read when every projected field of a struct is a constant,
  and an identity partition value is such a constant. The native scan returns NULL, as 1.12 does.
- **Not covered:** Variant ([#5438]), encrypted tables, and Iceberg versions other than 1.11.

## Native Iceberg writer

### Nested types (audited 2026-10-16)

- **Scope:** NULL structs, arrays and maps, NULLs inside them, field ids at every level, manifest
  metrics, nested partition sources, sort orders, copy-on-write DML, and `NOT NULL` columns.
- **Method:** each write made natively and with Comet disabled, on Iceberg 1.5.2 (Spark 3.4.3),
  1.8.1 (Spark 3.5.9) and 1.11.0 (Spark 4.1.3) at `main` c230bf4e98. The comparison covered the
  values read back, the manifest metrics of each data file (value, null and NaN counts, lower and
  upper bounds), the Parquet footer, and the files pruned for nested predicates. A code review of
  the writer, arrow-rs 59.3 and iceberg-rust ran alongside, and each finding was reproduced before
  it was filed.
- **Filed:** [#7049] (writes into `NOT NULL` columns fall back on Spark 3.5 and later when no
  exchange sits in front of the writer, because Comet has no native `AssertNotNull`) and [#7050]
  (the user guide's claim about float and double null counts). Both are tracked in [#7051].
- **Matched iceberg-java:** values, pruning, and field ids for struct fields, list elements and map
  keys and values, on all three versions. Manifest metrics under the default and `full` metrics
  modes on all three versions, and on 1.11.0 also under `counts`, `truncate(4)`, per-column
  overrides on nested fields, `max-inferred-column-defaults`, and tiny pages and row groups. Nested
  partition sources (identity on `s.a`, `bucket`, `truncate` and `days(ts.t)`), a sort order on a
  nested field, and copy-on-write `DELETE`, `UPDATE` and `MERGE`. Required nested fields under NULL
  parents written straight from a native scan, `fixed` fields under NULL structs, and copy-on-write
  `UPDATE` with `AssertNotNull`, whose check still runs in Spark in front of the columnar shuffle.
- **Not Comet bugs:** on Iceberg 1.9 and later, the value and null counts of a float or double field
  under a NULL struct differ from iceberg-java's ([#6659], closed), and iceberg-java's are the wrong
  ones ([apache/iceberg#17561], with a fix in [apache/iceberg#17560]). With Comet disabled on
  1.11.0, `WHERE s.x IS NULL` returned 0 rows instead of 5, while 1.5.2 and 1.8.1 returned 5.
- **Did not reproduce:** a task failure from validating a required struct under an optional struct,
  with a required child, when a native scan feeds the writer. A task failure from `fixed(16)` values
  of the wrong length hidden under NULL structs.
- **Not measured:** the copy of every column that the writer makes when a float or double sits
  under a list or map, to count NaNs correctly.
- **Not covered:** merge-on-read writes (`WriteDelta`), format v3 types, encryption, object stores,
  and write throughput, which needs a release build.

[Known Issues in the Iceberg guide]: ../../user-guide/latest/iceberg.md#known-issues
[apache/iceberg#17320]: https://github.com/apache/iceberg/pull/17320
[apache/iceberg#17560]: https://github.com/apache/iceberg/pull/17560
[apache/iceberg#17561]: https://github.com/apache/iceberg/issues/17561
[#5257]: https://github.com/apache/datafusion-comet/issues/5257
[#5438]: https://github.com/apache/datafusion-comet/issues/5438
[#5540]: https://github.com/apache/datafusion-comet/issues/5540
[#6659]: https://github.com/apache/datafusion-comet/issues/6659
[#6782]: https://github.com/apache/datafusion-comet/issues/6782
[#6790]: https://github.com/apache/datafusion-comet/issues/6790
[#6794]: https://github.com/apache/datafusion-comet/issues/6794
[#6822]: https://github.com/apache/datafusion-comet/issues/6822
[#6993]: https://github.com/apache/datafusion-comet/issues/6993
[#7033]: https://github.com/apache/datafusion-comet/issues/7033
[#7038]: https://github.com/apache/datafusion-comet/issues/7038
[#7039]: https://github.com/apache/datafusion-comet/issues/7039
[#7040]: https://github.com/apache/datafusion-comet/issues/7040
[#7049]: https://github.com/apache/datafusion-comet/issues/7049
[#7050]: https://github.com/apache/datafusion-comet/issues/7050
[#7051]: https://github.com/apache/datafusion-comet/issues/7051

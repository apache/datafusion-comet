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

# Accelerating Apache Iceberg Writes using Comet

Comet routes Iceberg writes through the `iceberg-rust` Parquet writer when the table and operation
are within scope, and falls back to Spark's standard Iceberg writer otherwise. The feature is
disabled by default; enable it with `spark.comet.write.icebergNative.enabled=true`.

Commit, conflict detection, isolation levels, and snapshot management remain in the JVM-side
Iceberg `Transaction`. Comet only owns the per-task Arrow→Parquet file writing.

The restrictions documented below reflect what iceberg-rust and parquet-rs implement at the time
of writing. As those libraries gain features, individual fall-back triggers may be relaxed in
future Comet releases.

## Operation-level fallback

Comet falls back to Spark for every Iceberg write outside this set:

| Operation                                                  | Status  |
| ---------------------------------------------------------- | ------- |
| `INSERT INTO` (V2)                                         | Native  |
| `INSERT OVERWRITE` (static + dynamic, V2)                  | Native  |
| `RewriteDataFiles` (binPack / sort / zOrder, V2)           | Native  |
| Copy-on-write `MERGE` / `UPDATE` / `DELETE` (V2)           | Native  |
| Merge-on-read `MERGE` / `UPDATE` / `DELETE`                | Fallback |
| `RewritePositionDeleteFiles`                               | Fallback |
| Streaming writes                                           | Fallback |
| ORC / Avro data files                                      | Fallback |
| Format-version 3 tables                                    | Fallback |

iceberg-rust does not implement a positional delete writer, so Merge-on-Read DML (which emits
positional deletes by default through Spark's Iceberg DSv2 path) and `RewritePositionDeleteFiles`
fall back. iceberg-rust does ship an `EqualityDeleteFileWriterBuilder`, but Spark's DSv2 DML path
does not produce equality deletes, so it is not exercised here. Format-version 3 requires row
lineage and introduces deletion vectors, variant types, and revised manifest rules that iceberg-rust
does not produce.

## Table-property fallback triggers

If any of the following are set on the target table, the write falls back to Spark. The conditions
are evaluated once per write; partial acceleration of a single statement is never attempted.

| Property                                                       | Triggers fallback when         | Why |
| -------------------------------------------------------------- | ------------------------------ | --- |
| `write.format.default`                                         | not `parquet`                  | only Parquet is implemented in iceberg-rust's writer stack |
| `write.object-storage.enabled`                                 | `true`                         | the hashed-prefix object-storage location generator is not implemented in iceberg-rust |
| `write.location-provider.impl`                                 | set                            | custom Java `LocationProvider` classes cannot run in Rust |
| `format-version`                                               | `>= 3`                         | format-version 3 requires row lineage, which is not supported in iceberg-rust |
| `encryption.*` (any key)                                       | set                            | iceberg-rust's `ParquetWriter` does not wire Parquet modular encryption through to parquet-rs (parquet-rs supports it, but the integration layer does not) |
| `write.metadata.metrics.default`                               | mentions `counts`              | Iceberg's `counts` mode emits null/value counts without min/max bounds; parquet-rs has no writer mode that produces counts without also producing bounds (when statistics are enabled, parquet-rs writes both) |
| `write.metadata.metrics.column.<col>`                          | `counts`                       | same as above, applied per column |
| `write.parquet.bloom-filter-max-bytes`                         | set                            | parquet-rs has no global byte cap on bloom filters; an explicit limit would diverge from iceberg-java |
| `write.parquet.bloom-filter-enabled.column.<col>`              | `true`                         | parquet-rs has no equivalent of iceberg-java's 1 MiB default cap, so non-trivial NDV columns would produce larger filters than iceberg-java writes |
| `write.parquet.row-group-check-min-record-count`               | set                            | the row-group sizing cadence differs between parquet-rs and parquet-mr (see Known divergences) |
| `write.parquet.row-group-check-max-record-count`               | set                            | same as above |
| `write.metadata.metrics.max-inferred-column-defaults`          | smaller than the schema column count | iceberg-java applies `MetricsModes.None` to columns past this index; Comet's translation does not currently emit the matching per-column overrides in iceberg-rust |

## Known divergences (no fallback, behaviour differs)

The native and JVM writers produce Parquet files that read identically through Iceberg but are not
byte-for-byte identical:

- **Row group boundaries**. parquet-rs checks row-group close after every record batch; parquet-mr
  checks every 100–10,000 rows. With `write.parquet.row-group-size-bytes` honoured by both writers,
  the target row-group size matches, but exact split row counts differ slightly and per-row-group
  statistics granularity differs.
- **`created_by` metadata**. Comet stamps `Apache Iceberg <ver> (Comet <ver>)` into the Parquet
  `created_by` field so Comet-written files are identifiable. iceberg-java writes
  `parquet-mr version X.Y.Z (...)`.
- **Aborted tasks**. iceberg-rust does not delete partial data files written by a failed task on
  abort; rely on Iceberg's `expire_orphan_files` maintenance to reclaim them.

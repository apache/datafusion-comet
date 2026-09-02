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

# Iceberg Writes: Comet's Split-Operator Plan (Experimental)

**This feature is experimental and disabled by default.** Enable it only after validating it
against your own workloads.

## Overview

Spark writes an Iceberg table through a single physical operator that combines data-file
writing with metadata writing, committing, and catalog validation. Because that operator sits
outside Spark's Adaptive Query Execution (AQE), the sub-query feeding the write — the scans,
projects, sorts, and exchanges producing the rows — cannot be re-planned at runtime.

When `spark.comet.write.iceberg.splitOperator.enabled=true`, Comet rewrites eligible Iceberg
writes into two operators:

1. **`IcebergWrite`** — writes the data files on the executors, exactly as iceberg-java does
   today, and returns each task's serialized commit message. This operator and the sub-query
   feeding it run inside AQE.
2. **`IcebergCommit`** — collects the commit messages on the driver and performs the normal
   Iceberg commit (including commit-time validation), outside AQE, exactly once.

With only the split plan enabled, data files are still written by iceberg-java; only the plan
shape changes. The split makes the write's input visible to AQE and to Comet's columnar rules,
and it is the foundation for the second toggle: when
`spark.comet.iceberg.write.enabled=true` and the write passes the eligibility check below, the
`IcebergWrite` operator's per-task Parquet write is delegated to
[iceberg-rust](https://github.com/apache/iceberg-rust) via Comet's native execution pipeline
([#5308](https://github.com/apache/datafusion-comet/issues/5308)).

## How the native write works

The JVM-side planner marshals everything iceberg-rust needs — the write schema and partition
spec as JSON, the data location, the resolved parquet writer settings, the writer mode
(unpartitioned / fanout / clustered, mirroring `SparkWrite`'s own choice), object-store
configuration (the table's `FileIO` properties, e.g. REST-vended credentials, merged over
`fs.s3a.*` settings translated from the session Hadoop configuration — the same translation
the native scan uses, since `HadoopFileIO` carries its S3 configuration in the Hadoop
Configuration rather than in `FileIO` properties), and per-task IDs — into the serialized
native plan. On each task, iceberg-rust writes the Parquet files and
returns its `DataFile` metadata packed as a single in-memory Iceberg V2 data manifest; the JVM
decodes those bytes with Iceberg's own `ManifestFiles.read`, re-derives each file's manifest
metrics from the written Parquet footer with Iceberg's `MetricsConfig` logic (so metrics modes,
truncation, and bounds decisions are iceberg-java's by construction), and wraps the result in
the same `TaskCommit` message the JVM writer would have produced. Everything iceberg-java does
post-write — snapshot assignment, manifest-list aggregation, commit validation and retries —
is untouched: `IcebergCommit` performs the normal `BatchWrite.commit`.

## Configuration

Standard Comet + Iceberg setup (see [`iceberg.md`](iceberg.md)) plus the write-side toggle:

```
# Standard Comet / Iceberg wiring
spark.plugins=org.apache.spark.CometPlugin
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<name>.type=hadoop                          # or hive / glue / rest / ...
spark.sql.catalog.<name>.warehouse=...

# Split-operator plan (experimental, off by default)
spark.comet.write.iceberg.splitOperator.enabled=true

# Native-write eligibility detection (experimental, off by default; requires the split plan)
spark.comet.iceberg.write.enabled=true
```

## Supported operations

The split-operator plan supports the following operations on every Spark version Comet
supports:

- `INSERT INTO` / DataFrame `append` (`AppendData`)
- `INSERT OVERWRITE`, static and dynamic (`OverwriteByExpression`,
  `OverwritePartitionsDynamic`)
- Copy-on-write `DELETE` / `UPDATE` / `MERGE` (`ReplaceData`)

The mechanism behind row-level DML differs by Spark version: on Spark 4.0+ the analyzer emits
operation-coded rows that Comet's writer dispatches through `ReplaceData`'s projections, while
on Spark 3.4/3.5 the rewritten rows are written as a plain row stream. The supported set of
operations is the same either way.

On Spark 4.1+ the split plan matches two further stock-Spark behaviours: MERGE metrics are
forwarded to the writer's commit (Iceberg 1.11+ records them in the snapshot summary), and
cached catalog tables are recached by name after a write so cache entries survive schema
changes.

## When Comet falls back to Spark's write operator

The rewrite is skipped — and the write runs through Spark's stock combined operator — when:

- `spark.comet.write.iceberg.splitOperator.enabled` is `false` (the default);
- the write is not an Iceberg `SparkWrite` (any other V2 data source);
- the table uses merge-on-read: delta writes (Iceberg `WriteDelta`) are not intercepted;
- the statement is CTAS / RTAS on Spark 3.4, where the staged exec writes inline; on Spark
  3.5+ those statements re-plan their inner append, which is intercepted normally;
- the write requires Spark's commit coordinator, which Comet's per-task commit protocol does
  not use;
- Comet cannot reflect the Iceberg internals needed to build the two-operator plan (for
  example an unrecognised write class or a `ReplaceData` projection it cannot map).

In every fallback case the write is planned as if Comet were absent; there is no correctness
trade-off, only no plan change.

## Native Parquet write eligibility

When `spark.comet.iceberg.write.enabled=true`
([#5308](https://github.com/apache/datafusion-comet/issues/5308)), the `IcebergWrite` operator's
per-task Parquet write is delegated to [iceberg-rust](https://github.com/apache/iceberg-rust).
The native writer must produce the same outcome as iceberg-java — the same Parquet features,
statistics, and manifest metadata — so a write is only eligible when every table property it
depends on is one the native path reproduces exactly, and additionally only when the plan
feeding the write is fully Comet-native. For a partitioned table that plan includes the hash
distribution and local sort Iceberg requests on its partition transforms; those stay native
because the transforms themselves have native implementations (see
[Iceberg system functions](iceberg.md)). Ineligible writes run through iceberg-java unchanged,
with the reason reported as a fall-back reason in Comet's extended EXPLAIN output.

**Most Iceberg write settings are not supported.** Detection is an allowlist: a write is
eligible only when its entire effective configuration matches the table below, and anything
else — any other write-affecting property, any key added by a future Iceberg version, any
value outside the supported set, any reflection failure while inspecting the write — falls
back to iceberg-java with a reason reported in extended EXPLAIN. Checks run on the effective
configuration: table properties overlaid with `SparkWrite.writeProperties`, which is where
iceberg-java resolves per-write options and `spark.sql.iceberg.*` session overrides.

A write is eligible only when ALL of the following hold:

| Setting                                                                                                                                     | Supported values                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| resolved write format (`write-format` option overlaid on `write.format.default`)                                                            | `parquet`                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `format-version`                                                                                                                            | `1` or `2`                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `write.parquet.compression-codec` / `compression-level` / `row-group-size-bytes` / `page-size-bytes` / `page-row-limit` / `dict-size-bytes` | the sizes/limit must be positive Java ints (`Integer.parseInt` semantics — no trimming, no values past `Int.MaxValue` — matching iceberg-java, whose writer fails on anything else); `compression-level` must be a Java int within the native writer's per-codec range (zstd 1–22, gzip 0–9, brotli 0–11; ignored by both writers for snappy/lz4/none) — iceberg-java never validates the level, so an out-of-range value falls back rather than becoming a native task failure |
| `write.parquet.row-group-check-min-record-count`                                                                                            | unset or `100` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `write.parquet.row-group-check-max-record-count`                                                                                            | unset or `10000` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `write.parquet.page-version`                                                                                                                | unset or `v1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `write.parquet.shred-variants`                                                                                                              | unset or `false` (Spark 4.x / Iceberg 1.11 resolve this into every parquet write)                                                                                                                                                                                                                                                                                                                                                                                               |
| `write.parquet.variant-inference-buffer-size`                                                                                               | any value (only meaningful when shredding, which is gated)                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `write.parquet.bloom-filter-enabled.column.<col>`                                                                                           | unset or `false`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `write.metadata.metrics.*`                                                                                                                  | any value (manifest metrics are re-derived on the JVM with Iceberg's own logic)                                                                                                                                                                                                                                                                                                                                                                                                 |
| `write.spark.fanout.enabled`                                                                                                                | any value (the native writer implements both clustered and fanout modes)                                                                                                                                                                                                                                                                                                                                                                                                        |
| `write.target-file-size-bytes`                                                                                                              | any value (file rolling cadence differs; see accepted divergences)                                                                                                                                                                                                                                                                                                                                                                                                              |
| data location URI scheme                                                                                                                    | `file`, `memory`, `s3`, `s3a`, `gs`                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| partition spec                                                                                                                              | any (but see partition paths under accepted divergences)                                                                                                                                                                                                                                                                                                                                                                                                                        |
| column types                                                                                                                                | any except `uuid` (Spark plans it as a string; no Arrow cast reaches `fixed(16)`)                                                                                                                                                                                                                                                                                                                                                                                               |

Within the namespaces that shape data-file bytes — `write.parquet.*` and `parquet.*` —
everything not listed above must be absent: unvetted `write.parquet.*` keys (e.g.
`bloom-filter-max-bytes`, `stats-enabled.column.*`, keys added by future Iceberg versions),
any `parquet.*` table property (including `parquet.enable.dictionary`), and any `parquet.*`
key in the session Hadoop configuration (with `HadoopFileIO`-backed output those reach
iceberg-java's writer but not the native one). Also gated explicitly: any `encryption.*` key,
`write.object-storage.enabled=true`, `write.location-provider.impl`, and `io-impl`.

Two checks look past properties at the table's instantiated state, because both can be
configured at the catalog level (or by a custom `TableOperations`) without any table or write
property changing: `table.io()` must be a recognized `FileIO` (the same allowlist the native
scan uses, minus the `EncryptingFileIO` family — the native writer produces plaintext files,
so an encrypting `FileIO` is rejected on the write side), and `table.encryption()` must be
Iceberg's `PlaintextEncryptionManager`. Anything else falls back.

Other `write.*` properties are intentionally not gated because they cannot make the native
writer produce different data files: distribution and ordering settings shape the Spark plan
identically on both paths, WAP / branch / snapshot properties act on the JVM committer,
`write.avro.*` / `write.orc.*` apply only to formats already excluded, and merge-on-read
settings route the write through `WriteDelta`, which the split plan never intercepts. Every
rule is pinned by `CometIcebergWriteDetectionSuite`.

Manifest `DataFile` metrics are assembled on the JVM before commit: each written file's
metrics are re-derived from its parquet footer through the version-matched
`ParquetUtil.footerMetrics` and `MetricsConfig.forTable`, with float/double NaN counts and
bounds carried over from the native writer's tracked state. iceberg-java's metadata decisions
— metrics modes, the inferred-column cap
(`write.metadata.metrics.max-inferred-column-defaults`), bound truncation, and list/map bounds
suppression — are therefore applied by iceberg-java's own code regardless of what the native
writer reports. This costs one footer-sized ranged read per written file at write time.

## Failure handling

Eligibility is decided entirely at plan time. That includes the reflection surface: every
iceberg-java class, method, and constructor the executor-side commit-message assembly uses is
eagerly resolved by the eligibility gate on the driver, so an Iceberg release that moves any
of them declines the native path with a fall-back reason instead of failing tasks mid-write.
Once planned, the physical plan is fixed — there is no per-task re-decision or runtime switch
back to the JVM writer.

When a native write fails partway through a task (an object-store error, a data-dependent cast
failure), the error propagates as an ordinary Spark task failure and Spark's task retry
re-executes it — through the native writer again. Retries cannot collide: each attempt's task
attempt id is embedded in its data file names.

Partial results are never committed. The commit set is exactly the commit messages returned by
successful tasks — a failed task contributes none — and if the job fails, the driver-side
commit operator aborts without committing anything. Data files already finalized by a failed
task attempt are not deleted by that task (iceberg-java's writer abort deletes them; the
native path has no abort hook yet — tracked in
[#5618](https://github.com/apache/datafusion-comet/issues/5618)): they are invisible to every
reader, since readers resolve files through committed manifests only, and are reclaimed by
Iceberg's normal `remove_orphan_files` maintenance.

A failure during the driver-side commit itself behaves exactly as on the stock path: the
commit messages carry genuine `SparkWrite$TaskCommit` objects, so Iceberg's own
`SparkWrite.abort` cleanup (which deletes the files listed in the commit messages for
cleanable failures) applies unchanged.

## Accepted divergences behind the toggle

Some differences between parquet-mr and the pinned parquet-rs / iceberg-rust are unconditional —
they apply to every native write and cannot be configured away. Enabling
`spark.comet.iceberg.write.enabled` accepts them. They fall into three classes with very
different blast radius: differences confined to the physical bytes of a data file (cosmetic —
no reader decision is based on them), differences visible in manifest metadata (these outlive
the write and feed later readers' pruning decisions, so each one is analyzed individually
below), and one operational path-layout caveat.

### Physical file layout only (cosmetic)

No Iceberg reader bases a planning or correctness decision on these; they change the bytes of
a data file but not what any reader computes from it:

- Footer key-value metadata differs: native files carry an `ARROW:schema` entry and no
  `iceberg.schema` entry; iceberg-java files are the opposite.
- The Parquet root schema element is named `arrow_schema` (iceberg-java: `table`).
- `created_by` identifies parquet-rs, not parquet-mr.
- No page CRC checksums and no page-header statistics (parquet-mr writes both by default).
  Absent page-header statistics can only make a reader scan more pages, never skip pages it
  should have read; page pruning uses the column index, which the native writer does produce.
- Dictionary-encoded pages are labeled `RLE_DICTIONARY` (parquet-mr v1 files: `PLAIN_DICTIONARY`).
- Fixed-length binary columns (`uuid`, `fixed`, decimals with precision > 18) are not
  dictionary-encoded (parquet-mr dictionary-encodes them).
- Row-group boundaries: parquet-mr flushes by byte size at a record-count check cadence,
  parquet-rs buffers by row count. File rolling and file naming follow the same cadence-style
  differences (iceberg-java checks the target file size every 1000 rows and names files
  `<partition>-<task>-<operation>-<count>`; iceberg-rust checks per batch and uses a
  process-local counter).
- Compressed page bytes are implementation-defined: the codec and any explicit level are
  translated, but parquet-rs and parquet-mr embed different encoder implementations and
  defaults (zstd default levels, LZ4 framing), so byte-identical output is not achievable even
  for a default `zstd` table. The decompressed data is identical. For the same reason,
  codec-level side channels (`zlib.compress.level`, `compression.brotli.quality`,
  `io.compression.codec.zstd.level` — the last is present in every Hadoop configuration by
  default) are not gated: they can only shift compressed bytes, which are already accepted as
  divergent.

### Manifest metadata visible to later readers

Manifest metrics drive partition- and file-level pruning for every future reader of the table,
so a divergence here would outlive the write. This class is deliberately kept almost empty:
`DataFile` metrics are not taken from the native writer's manifest but re-derived on the JVM
from each written file's parquet footer through iceberg-java's own `ParquetUtil.footerMetrics`
and `MetricsConfig.forTable` (see above). Metrics modes, lower/upper bound truncation, the
null-count conventions, and list/map bounds suppression are therefore iceberg-java's code
making iceberg-java's decisions, and the parity suite compares committed manifests
byte-for-byte against JVM-written ones. Two footer-derived values can still differ from what
iceberg-java's _writer-tracked_ state would have recorded, and both are analyzed safe:

- Float/double bounds involving zero may differ in sign: parquet-rs normalises footer
  statistics to min `-0.0` / max `+0.0` (the parquet-format recommendation), while
  iceberg-java's writer-tracked bounds preserve the exact sign it saw. The native path's
  manifest bounds inherit the normalised values — a strictly conservative widening that cannot
  change pruning decisions.
- On Iceberg 1.10+, manifest `value_counts` / `null_value_counts` for float/double columns
  nested under a nullable struct count rows whose parent struct is null (they come from the
  parquet footer), while iceberg-java's writer-tracked counts do not. Both counts inflate by
  the same amount, so the derived null ratios and `IS NULL` / `IS NOT NULL` pruning decisions
  are unaffected.

### Operational caveat

- Partition paths are not URL-escaped: iceberg-java percent-encodes partition directory names
  and values (`region=a%2Fb`), iceberg-rust writes them raw (`region=a/b`). Readers resolve
  files through manifest metadata, not paths, so query results are unaffected — but the
  directory layout differs from iceberg-java's, and partition values containing characters
  that are invalid in a URI (`:`, `#`, newline) may produce paths that `HadoopFileIO`-based
  readers cannot open.

All content not listed above — the logical data, encodings for non-FLBA columns, statistics
values, and manifest metadata — must match iceberg-java exactly, or the write falls back.

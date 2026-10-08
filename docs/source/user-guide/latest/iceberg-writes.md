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
writing with metadata writing, committing, and catalog validation. Spark's Adaptive Query
Execution (AQE) already re-plans the sub-query feeding that operator — the scans, projects,
sorts, and exchanges producing the rows — but the operator itself sits outside AQE, so the
data-file writing cannot be re-planned in response to how its input ran. And because data-file
writing is bundled with the metadata and commit steps, there is no separate step for Comet to
replace.

When `spark.comet.write.iceberg.splitOperator.enabled=true`, Comet rewrites eligible Iceberg
writes into two operators:

1. **`IcebergWrite`** — writes the data files on the executors, exactly as iceberg-java does
   today, and returns each task's serialized commit message. This operator and the sub-query
   feeding it run inside AQE.
2. **`IcebergCommit`** — collects the commit messages on the driver and performs the normal
   Iceberg commit (including commit-time validation), outside AQE, exactly once.

With only the split plan enabled, data files are still written by iceberg-java; only the plan
shape changes. The split moves data-file writing inside AQE and separates it from the commit,
and it is the foundation for the second toggle: when
`spark.comet.write.iceberg.enabled=true` and the write passes the eligibility check below, the
`IcebergWrite` operator's per-task Parquet write is delegated to
[iceberg-rust](https://github.com/apache/iceberg-rust) via Comet's native execution pipeline
([#5361](https://github.com/apache/datafusion-comet/pull/5361)).

## How the native write works

The JVM-side planner marshals everything iceberg-rust needs — the write schema and partition
spec as JSON, the data location, the resolved parquet writer settings, the writer mode
(unpartitioned / fanout / clustered, mirroring `SparkWrite`'s own choice), object-store
configuration (the table's `FileIO` properties, e.g. REST-vended credentials, merged over
`fs.s3a.*` settings translated from the effective Hadoop configuration carried by the table's
`FileIO`, including catalog-specific `hadoop.*` overrides, since `HadoopFileIO` carries its S3
configuration in the Hadoop Configuration rather than in `FileIO` properties), and per-task IDs —
into the serialized native plan. On each task, iceberg-rust writes the Parquet files and
returns its `DataFile` metadata packed as a single in-memory Iceberg V2 data manifest; the JVM
decodes those bytes with Iceberg's own `ManifestFiles.read`, re-derives each file's manifest
metrics from the written Parquet footer with Iceberg's `MetricsConfig` logic (so metrics modes,
truncation, and bounds decisions are iceberg-java's by construction), and wraps the result in
the same `TaskCommit` message the JVM writer would have produced. Everything iceberg-java does
post-write — snapshot assignment, manifest-list aggregation, commit validation and retries —
is untouched: `IcebergCommit` performs the normal `BatchWrite.commit`.

For `ResolvingFileIO`, Hadoop settings are taken from the delegate opening the data location.
An S3 location handled by `S3FileIO` uses its initialized FileIO properties, so Hadoop options
on the wrapper do not change the native endpoint or encryption settings. If that delegate cannot
be resolved, the write falls back to iceberg-java.

Before a task opens a partition's first file, it holds the partition's initial rows in memory
to choose which columns to dictionary-encode using parquet-mr's size accounting (see the
accepted divergences below). It normally waits for at least `write.parquet.page-row-limit`
rows (at least 100 when the configured limit is lower), or until the partition ends. The
buffering threshold is `write.parquet.row-group-size-bytes`, shared across all partitions in
a fanout write. Reaching it makes each partition still holding rows choose from the rows it
already has, even if they do not fill its first page.

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

# Native Parquet writer (experimental, off by default; requires the split plan)
spark.comet.write.iceberg.enabled=true

# Lets writes whose input is a local relation (INSERT ... VALUES, a local DataFrame) use the
# native writer; see "Native Parquet write eligibility" below
spark.comet.exec.localTableScan.enabled=true
```

## Supported operations

The split-operator plan supports the following operations on every Spark version Comet
supports:

- `INSERT INTO` / DataFrame `append` (`AppendData`)
- `INSERT OVERWRITE`, static and dynamic (`OverwriteByExpression`,
  `OverwritePartitionsDynamic`)
- Copy-on-write `DELETE` / `UPDATE` / `MERGE` (`ReplaceData`)

For an unpartitioned copy-on-write `MERGE`, the native Iceberg writer is reachable on Spark
3.5+ when `spark.comet.exec.mergeRows.enabled=true`. With the flag disabled, the JVM
`MergeRowsExec` breaks the fully-native child chain required by `CometIcebergWriteExec`.
On Spark 4.1+, the native MergeRows path also preserves the semantic counters required by the
summary-aware writer commit contract.

For copy-on-write row-level DML, the mechanism differs by Spark version: on Spark 4.0+ the
analyzer emits operation-coded rows that Comet's writer dispatches through `ReplaceData`'s
projections, while on Spark 3.4/3.5 the rewritten rows are written as a plain row stream. The
supported set of operations is the same either way.

On Spark 3.5+, merge-on-read uses Spark's `WriteDelta`. The split plan intercepts that command so
Comet can keep the same driver commit and reporting path, but task-side row-level writes stay on
Iceberg's JVM `DeltaWriter`; `CometIcebergWriteExec` is never used for position-delta rows.
Spark 3.4 leaves `WriteDelta` on Spark's stock write plan.

On Spark 4.1+ the split plan matches two further stock-Spark behaviours: MERGE metrics are
forwarded to the writer's commit (Iceberg 1.11+ records them in the snapshot summary), and
cached catalog tables are recached by name after a write so cache entries survive schema
changes.

## When Comet falls back to Spark's write operator

The rewrite is skipped — and the write runs through Spark's stock combined operator — when:

- `spark.comet.write.iceberg.splitOperator.enabled` is `false` (the default);
- the write is neither an Iceberg `SparkWrite` nor a supported Iceberg position-delta write;
- the table uses merge-on-read on Spark 3.4; Spark 3.5+ `WriteDelta` is intercepted but remains
  on Iceberg's JVM `DeltaWriter`;
- the statement is CTAS / RTAS on Spark 3.4, where the staged exec writes inline; on Spark
  3.5+ those statements re-plan their inner append, which is intercepted normally;
- the write requires Spark's commit coordinator, which Comet's per-task commit protocol does
  not use;
- Comet cannot reflect the Iceberg internals needed to build the two-operator plan (for
  example an unrecognised write class or a `ReplaceData` projection it cannot map).

In every fallback case the write is planned as if Comet were absent; there is no correctness
trade-off, only no plan change.

## Native Parquet write eligibility

When `spark.comet.write.iceberg.enabled=true`
([#5361](https://github.com/apache/datafusion-comet/pull/5361)), the `IcebergWrite` operator's
per-task Parquet write is delegated to [iceberg-rust](https://github.com/apache/iceberg-rust).
The native writer must produce the same outcome as iceberg-java — the same Parquet features,
statistics, and manifest metadata — so a write is only eligible when every table property it
depends on is one the native path reproduces exactly, and additionally only when the plan
feeding the write is fully Comet-native. For a partitioned table that plan includes the hash
distribution and local sort Iceberg requests on its partition transforms; those stay native
because the transforms themselves have native implementations (see
[Iceberg system functions](iceberg.md)). Ineligible writes run through iceberg-java unchanged,
with the reason reported as a fall-back reason in Comet's extended EXPLAIN output. A write that
runs natively shows `CometIcebergWrite` under `IcebergCommit` in the physical plan; an ineligible
write keeps `IcebergWrite`.

The native writer reads its input as Arrow batches from a Comet operator, so the write's input
must itself run in Comet. A write whose input is a local relation, such as `INSERT ... VALUES` or
`df.writeTo(...).append()` on a DataFrame built from local data, is fed by Spark's
`LocalTableScanExec`, which Comet only converts when `spark.comet.exec.localTableScan.enabled=true`
(off by default). Without that setting such writes run through iceberg-java even when both write
flags are on.

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
| `format-version`                                                                                                                            | `1`, `2` or `3` (on `3`, writes that carry row lineage fall back; see below)                                                                                                                                                                                                                                                                                                                                                                                                    |
| `write.parquet.compression-codec` / `compression-level` / `row-group-size-bytes` / `page-size-bytes` / `page-row-limit` / `dict-size-bytes` | the sizes/limit must be positive Java ints (`Integer.parseInt` semantics — no trimming, no values past `Int.MaxValue` — matching iceberg-java, whose writer fails on anything else); `compression-level` must be a Java int within the native writer's per-codec range (zstd 1–22, gzip 0–9, brotli 0–11; ignored by both writers for snappy/lz4/none) — iceberg-java never validates the level, so an out-of-range value falls back rather than becoming a native task failure |
| `write.parquet.row-group-check-min-record-count`                                                                                            | unset or `100` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `write.parquet.row-group-check-max-record-count`                                                                                            | unset or `10000` (the default)                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `write.parquet.page-version`                                                                                                                | unset or `v1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `write.parquet.shred-variants`                                                                                                              | unset or `false` (Spark 4.x / Iceberg 1.11 resolve this into every parquet write)                                                                                                                                                                                                                                                                                                                                                                                               |
| `write.parquet.variant-inference-buffer-size`                                                                                               | any value (only meaningful when shredding, which is gated)                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `write.parquet.bloom-filter-enabled.column.<col>`                                                                                           | unset or `false`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `write.metadata.metrics.*`                                                                                                                  | any value (manifest metrics are re-derived on the JVM with Iceberg's own logic)                                                                                                                                                                                                                                                                                                                                                                                                 |
| `write.spark.fanout.enabled`                                                                                                                | any value (the native writer implements both clustered and fanout modes)                                                                                                                                                                                                                                                                                                                                                                                                        |
| `write.target-file-size-bytes`                                                                                                              | any value (the two writers can choose different roll points; see accepted divergences)                                                                                                                                                                                                                                                                                                                                                                                          |
| data location URI scheme                                                                                                                    | `file`, `memory`, `s3`, `s3a`, `gs`, matched case-sensitively (`S3://` falls back). `s3`, `s3a` and `gs` need a bucket in the authority (`s3://bucket/...`), so a hostless form such as `s3:/bucket/key` falls back. `gs` only when the `FileIO` opening the data location is a `GCSFileIO`; see below                                                                                                                                                                          |
| resolved `table.locationProvider()`                                                                                                         | Iceberg's built-in `DefaultLocationProvider`                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Hadoop S3A settings for an `s3` / `s3a` data location                                                                                       | only `fs.s3a.access.key`, `secret.key`, `session.token`, `endpoint`, `endpoint.region`, and `path.style.access`, including their `fs.s3a.bucket.<data-bucket>.*` forms; any other effective `fs.s3a.*` setting falls back                                                                                                                                                                                                                                                       |
| Iceberg `FileIO` S3 settings for an `s3` / `s3a` data location                                                                              | the S3 endpoint, region, static/session credentials, path-style, SSE (`none`, `s3`, `kms`, or `custom`; not `dsse-kms`), assume-role, anonymous/config-chain settings parsed by the pinned iceberg-rust version, plus Comet's credential-provider class and built-in web-identity properties. When a custom provider is configured, its vendor-owned `s3.*` / `client.*` properties are also forwarded; unsupported Iceberg-defined S3 settings still fall back                 |
| partition spec                                                                                                                              | any, except an identity partition on a `float` or `double` column, or a `void` field whose source column was dropped beside a live field (see below)                                                                                                                                                                                                                                                                                                                            |
| column types                                                                                                                                | any except `uuid` (Spark plans it as a string; no Arrow cast reaches `fixed(16)`) and the v3 types `variant`, `unknown`, `timestamp_ns`, `geometry` and `geography`                                                                                                                                                                                                                                                                                                             |

Within the namespaces that shape data-file bytes — `write.parquet.*` and `parquet.*` —
everything not listed above must be absent: unvetted `write.parquet.*` keys (e.g.
`bloom-filter-max-bytes`, `stats-enabled.column.*`, keys added by future Iceberg versions),
any `parquet.*` table property (including `parquet.enable.dictionary`), and any `parquet.*`
key in the session Hadoop configuration other than the reader-only
`parquet.hadoop.vectored.io.enabled` (with `HadoopFileIO`-backed output those reach
iceberg-java's writer but not the native one). Also gated explicitly: any `encryption.*` key,
`write.object-storage.enabled=true`, `write.location-provider.impl`, and `io-impl`.

Three checks look past properties at the table's instantiated state, because they can be
configured at the catalog level (or by a custom `TableOperations`) without any table or write
property changing: `table.locationProvider()` must be Iceberg's `DefaultLocationProvider`;
`table.io()` must be a recognized `FileIO` (the same allowlist the native scan uses, minus the
`EncryptingFileIO` family — the native writer produces plaintext files, so an encrypting `FileIO`
is rejected on the write side); and `table.encryption()` must be Iceberg's
`PlaintextEncryptionManager`. Anything else falls back.

A `gs` data location additionally requires that the `FileIO` actually opening it is a
`GCSFileIO` (for a `ResolvingFileIO`, the delegate it instantiates for that location,
which is a `HadoopFileIO` when the GCS `FileIO` cannot be loaded or initialized). A
`HadoopFileIO` takes its GCS credentials, endpoint and project from `fs.gs.*` in the Hadoop
Configuration, and only `fs.s3a.*` is translated into the native `FileIO`, so the native writer
could resolve a different storage identity or endpoint than the JVM writer would. That
combination falls back; a `GCSFileIO` carries its `gcs.*` settings in `FileIO.properties()`,
which are forwarded.

For an `s3` or `s3a` data location, the gate also inspects both the table FileIO's effective Hadoop
configuration and `table.io().properties()`. These are separate allowlists because Hadoop S3A
keys are translated before they reach iceberg-rust, while Iceberg `FileIO` keys are forwarded
directly. When the FileIO exposes a Hadoop configuration, its initialized values govern both the
gate and native translation, including after session or catalog options change. FileIO
implementations without a Hadoop configuration, such as `S3FileIO`, use only their initialized
properties; session and catalog Hadoop options are neither checked nor forwarded. Hadoop's built-in
`core-default.xml` values are not treated as explicit settings, but
programmatic settings and values from site or custom `*-default.xml` resources are. Spark's
session-wide S3A vectored-read and `downgrade.syncable.exceptions` compatibility settings are also
ignored because they cannot alter an Iceberg data-file write request. Unknown explicit
`fs.s3a.*`, `s3.*`, or `client.*` settings therefore fall back at planning time instead of being
silently ignored by the native storage backend. The exception is a vendor-owned `s3.*` /
`client.*` property when
`s3.comet.credential.provider.class` is configured: the provider receives the unfiltered FileIO
bag and can consume that property. Iceberg-defined settings that the native storage path cannot
honour still fall back even with a provider. A per-bucket Hadoop setting counts only for the exact
data-bucket name, so configuration for a longer dotted bucket does not by itself disable the
native write. If Iceberg's AWS property classes cannot be loaded, vendor `s3.*` / `client.*` keys
fall back too and planning still completes. The fall-back reason reports only sorted property
names, never their values, so credentials and tokens do not enter EXPLAIN or plan logs.

An identity partition on a `float` or `double` column falls back. iceberg-rust compares float
partition values with an equality that treats `-0.0` and `0.0` as one value, so the native writer
would put rows with either value in the same partition, where iceberg-java writes two. A read
that prunes on the other value's partition would then miss rows. The fall-back stays until
iceberg-rust distinguishes the two values
([apache/iceberg-rust#3325](https://github.com/apache/iceberg-rust/issues/3325)).

On a format-version 3 table with Iceberg 1.10 or newer, a write that rewrites existing rows falls
back. Copy-on-write `DELETE`, `UPDATE` and `MERGE` and `rewrite_data_files` write the row lineage
columns `_row_id` and `_last_updated_sequence_number` into the new data files, so that rewritten
rows keep their ids, and the native writer does not write those columns. Appends, overwrites,
`CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` write the data columns only, and
Iceberg assigns the new rows their ids when it commits, so those run natively. Earlier Iceberg
versions write no lineage columns, so the rule never applies to them.

A format-version-1 table keeps a dropped partition field as a `void` field, and its source column
can be dropped afterwards. A spec that mixes such a field with a live one falls back: iceberg-java
cannot write through it either, so the write fails with iceberg-java's own error rather than in
the native writer ([#6141](https://github.com/apache/datafusion-comet/issues/6141)). A spec whose
fields are all `void` writes unpartitioned and stays eligible.

Other `write.*` properties are intentionally not gated because they cannot make the native
writer produce different data files: distribution and ordering settings shape the Spark plan
identically on both paths, WAP / branch / snapshot properties act on the JVM committer,
`write.avro.*` / `write.orc.*` apply only to formats already excluded, and merge-on-read
settings route the write through `WriteDelta`, whose task writer remains iceberg-java rather
than the native writer. Every native eligibility rule is pinned by
`CometIcebergWriteDetectionSuite`.

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

The native writer's buffers are charged to Comet's memory pool, the off-heap budget Comet's other
native operators draw on, where iceberg-java's buffers sit on the JVM heap. A fanout write keeps a
data file open for every partition a task writes to. Each open file holds the row group it is
writing in memory, up to `write.parquet.row-group-size-bytes`, and on S3 or GCS also the last row
group it flushed, which is uploaded once the next one is complete or the file closes. So a task
writing to many partitions needs memory in proportion to them. When the pool cannot grant it, the
task fails with a `CometNativeException` reading `Additional allocation failed for IcebergWriteExec`
instead of exceeding the executor's memory, and Spark retries it like any other task failure. Such a
write fits in less memory with the fanout writer disabled (`write.spark.fanout.enabled=false`):
Spark then sorts each task's rows by partition, and the task keeps one file open at a time. A
smaller row-group size also helps. Otherwise the write needs a larger `spark.memory.offHeap.size`.

Partial results are never committed. The commit set is exactly the commit messages returned by
successful tasks — a failed task contributes none — and if the job fails, the driver-side
commit operator aborts without committing anything. A failed task attempt also deletes the
data files it created, as iceberg-java's writer abort does. The native writer records every
location it hands to a file writer, and cleanup has no ownership gap. Native keeps its cleanup
guard armed after yielding the output batch. The JVM reads those locations into a task failure
listener first, then polls the native output to EOF; that EOF is the acknowledgement that lets
native disarm. A failure in this narrow handoff window can therefore trigger best-effort deletion
on both sides, which is harmless. The native guard still covers a failed write, a task torn down
before the write completed — for example because the operator feeding it threw — and a failure
encoding the manifest or building the output batch. The handoff does not depend on decoding the
manifest: the locations are read before the manifest is decoded, so a failure in that decode still
cleans up. Cleanup never masks the original failure; anything it misses is invisible to every
reader, since readers resolve files through committed manifests only, and is reclaimed by Iceberg's
normal `remove_orphan_files` maintenance.

When one task fails, the tasks that had already completed leave committed-nothing data files
too. The committer collects each task's commit message as that task finishes, so on a job
failure it aborts with the completed messages and deletes their data files through the table
`FileIO`. (Iceberg's own `SparkWrite.abort` skips cleanup unless a commit failed with a
cleanable error, so on the stock path those files are left for `remove_orphan_files`.) A
failure during the driver-side commit itself behaves exactly as on the stock path: the commit
messages carry genuine `SparkWrite$TaskCommit` objects, so Iceberg's own `SparkWrite.abort`
cleanup (which deletes the files listed in the commit messages for cleanable failures) applies
unchanged.

## Accepted divergences behind the toggle

Some differences between parquet-mr and the pinned parquet-rs / iceberg-rust are unconditional —
they apply to every native write and cannot be configured away. Enabling
`spark.comet.write.iceberg.enabled` accepts them. They fall into three classes with very
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
- The native writer uses parquet-mr's size accounting to choose dictionary encoding: a column
  whose sampled rows show the dictionary saving no space is written plain, with no dictionary page,
  instead of carrying a dictionary page that every selective read of it would have to fetch
  ([#6114](https://github.com/apache/datafusion-comet/issues/6114)). The native writer decides
  once per partition, normally from that partition's first page of rows in the task, and keeps the
  decision for every file and row group it writes for the partition; parquet-mr decides again
  for every row group. If buffered rows reach `write.parquet.row-group-size-bytes`, the choice
  uses the rows collected so far. In a fanout write, all partitions share this threshold, so
  each partition still buffering can make its choice before it has a full first page. Later
  rows could have changed parquet-mr's decision, so this difference is not limited to columns
  close to the size cut-off. Where the page size rather than `write.parquet.page-row-limit` ends a
  column's first page, the native page ends at the first row past parquet-mr's size threshold,
  while parquet-mr only ends it at its next periodic size check, so a column close to the
  cut-off can be decided the other way. Close to the cut-off both encodings take about the same
  space. A column that keeps its dictionary and later fills it falls back to plain on both
  writers, but parquet-rs's dictionary page then holds every entry, where parquet-mr's holds
  only the entries earlier pages used.
- Row-group boundaries: parquet-mr flushes by byte size at a record-count check cadence,
  parquet-rs buffers by row count. File naming follows the same cadence-style difference
  (iceberg-java names files `<partition>-<task>-<operation>-<count>`; iceberg-rust uses a
  process-local counter).
- Partition directory names match iceberg-java 1.8+'s `PartitionSpec.partitionToPath` for every
  partition type the native writer accepts. Identity partitions on `float` and `double` columns
  fall back (see [Native Parquet write eligibility](#native-parquet-write-eligibility)), so
  iceberg-java names those directories itself. On Iceberg 1.5.x,
  which the Spark 3.4 profile pins, iceberg-java itself spelled `timestamp` and `timestamptz`
  directories with `LocalDateTime.toString()` / `OffsetDateTime.toString()`
  (`ts=1969-12-31T23:59:58.500Z`) and left the partition field name unescaped; Comet uses the
  1.8+ spelling on every profile. Distinct partition values still get distinct directories in all
  cases, and no reader parses these names: files are resolved through committed manifests.
- File rolling lands on the same row grid as iceberg-java but not necessarily on the same row.
  Both writers re-check the current file's size against `write.target-file-size-bytes` once
  every 1000 rows of that file (iceberg-java's `RollingFileWriter.ROWS_DIVISOR`; Comet hands the
  iceberg-rust writer rows in 1000-row units, per partition file, to get the same grid), so each
  writer rolls only on a 1000-row boundary of its own file.
  The shared grid is all that is shared. What each writer compares against the target differs —
  flushed bytes plus parquet-rs's estimate of the open row group, versus parquet-mr's file
  position plus its buffered size — and the two use different threshold comparisons. These are
  independent size estimates, so nothing bounds how far apart the two writers' roll points are:
  they may cross the target several grid steps apart, and the resulting files can differ in row
  count by an arbitrary number of 1000-row blocks. Do not rely on file-layout parity between the
  two writers; rely only on each file rolling on its own 1000-row boundary.
- A fanout write lists a task's data files in file-path order, where iceberg-java lists them in
  its own `StructLikeMap` iteration order. Both are stable across runs, and neither is a
  documented ordering, but the manifest entry order becomes the scan-task order and so the row
  order of an unordered `SELECT *`. On a format-version 3 table it also decides which row ids the
  commit gives each file's rows, so the same rows can get different `_row_id` values from the two
  writers. The ids are unique either way, and across tasks iceberg-java's own assignment already
  depends on the order in which the tasks finish. Only the sorted order is reproducible on the
  native path: iceberg-rust's `FanoutWriter` closes its per-partition writers out of a `HashMap`,
  which under Rust's per-process `RandomState` would otherwise give a different order on every
  run. Clustered and unpartitioned writes append in creation order on both paths and are
  unaffected.
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
making iceberg-java's decisions. The parity tests write the same rows through both writers and
compare the committed `readable_metrics` (value, null and NaN counts, and lower and upper bounds)
for the column types they cover. Two footer-derived values can still differ from what
iceberg-java's _writer-tracked_ state would have recorded, and both are analyzed safe:

- Float/double bounds involving zero may differ in sign: parquet-rs normalises footer
  statistics to min `-0.0` / max `+0.0` (the parquet-format recommendation), while
  iceberg-java's writer-tracked bounds preserve the exact sign it saw. The native path's
  manifest bounds inherit the normalised values — a strictly conservative widening that cannot
  change pruning decisions.
- On Iceberg 1.9+, manifest `value_counts` / `null_value_counts` for float/double columns
  nested under a nullable struct count rows whose parent struct is null (they come from the
  parquet footer), while iceberg-java's writer-tracked counts do not. Both counts inflate by
  the same amount, so the derived null ratios and `IS NULL` / `IS NOT NULL` pruning decisions
  are unaffected.

All content not listed above — the logical data, encodings for non-FLBA columns, statistics
values, and manifest metadata — must match iceberg-java exactly, or the write falls back.

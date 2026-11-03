<!--
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

# Scan

This document describes how Comet reads data files. It covers which scan path a query gets, how
`CometScanRule` decides whether Comet may claim a scan at all, how the plan reaches the native
reader, and how the native reader reconciles the Parquet file schema with the schema Spark asked
for.

For the user-facing view of supported formats, storage systems, and credentials, see
[Supported Spark Data Sources](../user-guide/latest/datasources.md).

## Overview

Comet's Parquet scan runs entirely in Rust. File planning stays on the JVM, because Spark owns
partition pruning, bucketing, and file listing, and the resulting `FilePartition`s are serialized to
protobuf. Everything from opening a file onwards happens natively, on DataFusion's `DataSourceExec`
over a `ParquetSource`.

There is no JVM-side Comet Parquet reader. Comet used to ship one, selected by a
`native_comet` scan implementation, and it was removed in
[#3358](https://github.com/apache/datafusion-comet/pull/3358) and
[#3396](https://github.com/apache/datafusion-comet/pull/3396). A scan Comet cannot read natively
falls back to Spark's own reader, optionally with the output converted to Arrow immediately
afterwards (see [Conversion instead of native reading](#conversion-instead-of-native-reading)).

One rule runs through the whole subsystem, and most of the design below is it applied at a
different level: **Comet declines what it cannot verify.** A planning gate that cannot prove a
table is readable falls back rather than guessing. A scheme allow list names what iceberg-rust can
actually open rather than what `object_store` merely recognizes. An upstream DataFusion option is
ignored until somebody audits it for Spark semantics. A case fold that cannot reach the JVM aborts
the batch rather than substituting Rust's Unicode tables. In each case the cheap failure is losing
native execution and the expensive one is a silent wrong answer, so the code always takes the
first.

```{note}
`native/core/src/execution/operators/scan.rs` is **not** part of this subsystem. That file holds
`ScanExec`, the operator that imports `ColumnarBatch`es from a JVM iterator over the Arrow C Stream
interface. It is the input boundary for a native plan, not a file reader. See [Arrow FFI](ffi.md).
```

## Which Scan Path Runs

`CometScanRule` is a physical plan rule registered by `CometSparkSessionExtensions`. It matches on
`FileSourceScanExec` (DataSource V1) and `BatchScanExec` (DataSource V2) and routes each one.

| Source                             | Claimed by        | Planning node                                   | Execution node               |
| ---------------------------------- | ----------------- | ----------------------------------------------- | ---------------------------- |
| V1 Parquet (`ParquetFileFormat`)   | `transformV1Scan` | `CometScanExec`                                 | `CometNativeScanExec`        |
| V2 Iceberg (matched by class name) | `transformV2Scan` | `CometBatchScanExec` with Iceberg scan metadata | `CometIcebergNativeScanExec` |
| V2 CSV (`CSVScan`)                 | `transformV2Scan` | `CometBatchScanExec`                            | `CometCsvNativeScanExec`     |

Anything else is offered to the contrib SPI first and otherwise left unchanged for Spark. Two
consequences of that table are easy to miss.

**V2 Parquet is not read natively.** `transformV2Scan` has no arm for
`org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan`, so a V2 Parquet scan falls
through to the catch-all and reports `Unsupported scan`. Only the V1 path (`FileSourceScanExec`,
which is what `spark.sql.sources.useV1SourceList` gives you for Parquet by default) reaches the
native Parquet reader. This is why `ParquetReadSuite` has a `ParquetReadV1Suite` subclass that pins
`USE_V1_SOURCE_LIST`.

**Only Spark's own `ParquetFileFormat` counts.** `CometScanExec.isFileFormatSupported` compares the
format's class with `classOf[ParquetFileFormat]` exactly rather than with `isInstanceOf`, so a
subclass such as Delta's does not match. Formats like that are the reason the contrib hook exists.

### Scan contribs

`CometScanContrib` is a `ServiceLoader` SPI that lets an out-of-tree format (Delta, Lance, and so
on) claim a scan before any of Comet's built-in handling runs. Core holds no compile-time reference
to any contrib and names none of them, and a default build ships no service file, so the registry
is empty and there is no contrib surface at runtime.

The hook runs **first**, ahead of every built-in guard, on both the V1 and V2 paths. That ordering
is deliberate: a contrib may support things Comet's built-in scan does not, so applying the
built-in guards first would decline such a scan before its owner was ever offered it. The
Iceberg metadata-table guard in `transformV2Scan` sits immediately after the hook for the same
reason, because it matches on a table-name suffix that a contrib's own table could legitimately
end with.

Contribs are offered a scan one at a time and the first claim wins, so a contrib that claims a
scan it does not own hides it from the contrib that could have read it, with an outcome that
depends on unspecified `ServiceLoader` ordering. The full ownership contract is in the
`CometScanContrib` scaladoc. `spark.comet.scan.contrib.detectConflicts.enabled` is a diagnostic
that offers a scan to every contrib and warns when more than one claims it.

### Conversion instead of native reading

When a scan falls back, Comet can still convert Spark's output to Arrow right above the scan so the
rest of the pipeline stays native. This is `CometSparkToColumnarExec`, gated per source by
`spark.comet.convert.parquet.enabled`, `spark.comet.convert.csv.enabled`, and
`spark.comet.convert.json.enabled`. It is a separate mechanism from everything else in this
document: no Comet code reads the file, and the conversion itself costs something, so it is a
consolation path rather than a scan path.

## The Fallback Gate Model

`CometScanRule` is, structurally, a list of gates. Each one asks whether some property of the scan
is something the native reader handles, and a gate that says no records a reason and hands the scan
back to Spark unchanged.

**Gates fail closed**, the general rule from the Overview applied at the planning boundary. When a
gate cannot determine the answer, it declines. The Iceberg path makes this explicit: almost all of
its checks reach into Iceberg's classes reflectively, and every `catch` around that reflection adds
a fallback reason rather than assuming the scan is safe. A gate that failed open would turn an
unverifiable table into a wrong answer or a native crash.

**Declining is not silent.** A gate calls `withFallbackReason(node, reason)` or
`withFallbackReasons(node, reasons)`, which accumulate onto the node's `FALLBACK_REASONS` tag.
`ExtendedExplainInfo` surfaces them in `EXPLAIN EXTENDED`, and
`spark.comet.explain.fallback.log.enabled` logs them. A gate that returns the scan unchanged
without tagging a reason leaves a user with a slow query and nothing to read.

A gate goes in one of four places, and never before the contrib hook, which is first on both paths:

- In `transformV1Scan` / `transformV2Scan`, after the hook: checks that apply to every built-in
  format.
- In `nativeScan`: checks specific to the native Parquet reader. The object-store scheme gates live
  here, so contrib scans are unaffected by them.
- In `CometNativeScan.isSupported`: checks that belong with the serde. This runs during
  `CometExecRule`, after `CometScanExec` already exists, and short-circuits if the node is already
  tagged.
- In the Iceberg arm of `transformV2Scan`, which is structured differently from the three above.
  Those return as soon as a gate declines. The Iceberg gates accumulate into a `fallbackReasons`
  buffer and are decided once at the end, so a user sees every reason the table was rejected rather
  than only the first.

### Gates that exist today

The V1 Parquet gates, grouped by the function that owns them:

| Gate                                        | Owner             | Why                                                                                                 |
| ------------------------------------------- | ----------------- | --------------------------------------------------------------------------------------------------- |
| `SKIP_COMET_SCAN_TAG`                       | `_apply`          | Set by `CometSpark34AqeDppFallbackRule` to keep a peer scan Spark-native for SMJ self-join symmetry |
| `spark.comet.scan.enabled`                  | `_apply`          | Test-only kill switch                                                                               |
| Metadata columns                            | `transformV1Scan` | Only file-constant ones (`file_path`, `file_size`, ...) are supported, `_metadata.row_index` is not |
| AQE DPP on Spark 3.4                        | `transformV1Scan` | `injectQueryStageOptimizerRule` is unavailable there, so Comet's DPP rewrite rule cannot run        |
| File format                                 | `transformV1Scan` | Exactly `ParquetFileFormat`                                                                         |
| Nested-type default values                  | `transformV1Scan` | `getExistenceDefaultValues` yielding a map, struct, or array                                        |
| `spark.comet.exec.enabled`                  | `nativeScan`      | The native scan is a native operator                                                                |
| Filesystem scheme                           | `nativeScan`      | Asked of the native layer via `NativeBase.isObjectStoreSchemeSupported`, not a hardcoded list       |
| Multi-bucket alias paths                    | `nativeScan`      | One object store is registered per `FilePartition`, keyed on the first file                         |
| Path rejected by `object_store`             | `nativeScan`      | A recognized scheme can still carry a key `Path::from_url_path` refuses, e.g. a newline             |
| `parquet.enableVectorizedReader=false`      | `nativeScan`      | Opts into parquet-mr's permissive behavior, which Comet has no equivalent backend for               |
| Parquet encryption config                   | `nativeScan`      | Only the configurations `CometParquetUtils.isEncryptionConfigSupported` recognizes                  |
| `input_file_name` and friends               | `nativeScan`      | Read a thread-local set by `FileScanRDD`, which the native scan does not use                        |
| Row index generation                        | `nativeScan`      | `_metadata.row_index` is produced per row by the reader                                             |
| Schema and partition schema                 | `nativeScan`      | `CometScanTypeChecker`, below                                                                       |
| `ignoreCorruptFiles` / `ignoreMissingFiles` | `isSupported`     | Spark's permissive file handling has no native equivalent                                           |

The Iceberg arm adds many more, covering the table format version, `FileIO` compatibility, V3
column defaults, delete-file formats and equality-delete column types, partition transform support,
encryption key length, metadata-location scheme, and DPP subquery shape. They are documented inline
at each `fallbackReasons +=` site.

### Type support

`CometScanTypeChecker` extends the shared `DataTypeSupport` trait. Every override it adds is a case
where the native read would disagree with Spark, so a type not listed here is decided by asking that
same question:

- `ShortType` when `spark.comet.scan.unsignedSmallIntSafetyCheck` is on, because the native reader
  may mishandle an unsigned `UINT_8` column
- Collated strings, declined so the whole query falls back
- A shredded Variant struct, which Spark 4.0's `PushVariantIntoScan` rewrites into typed fields
  that the native scan does not honor
- Empty structs
- Duplicate Parquet field ids among sibling fields, when `spark.sql.parquet.fieldId.read.enabled`
  is set, because Comet reads such a struct positionally while Spark raises an ambiguity error

## Planning to Execution

The V1 Parquet path runs through two rules and three node types.

```
FileSourceScanExec
        │  CometScanRule
        ▼
CometScanExec                     ← planning intermediate, doExecute throws
        │  CometExecRule → CometNativeScan.convert
        ▼
CometNativeScanExec               ← holds the serialized NativeScanCommon
        │  JNI, per task
        ▼
DataSourceExec over ParquetSource ← planner.rs, OpStruct::NativeScan
```

`CometScanExec` is never executed. It exists so that `CometScanRule` can record "Comet claims this
scan" in the plan, and `CometExecRule` converts it unconditionally: on a conversion failure it
substitutes the wrapped `FileSourceScanExec` rather than leaving `CometScanExec` in place.

### Split serialization

A table with many partitions produces a large file list, and shipping the whole list to every task
wastes memory. `CometNativeScanExec` therefore serializes the plan in two pieces
([#3349](https://github.com/apache/datafusion-comet/pull/3349)):

- `NativeScanCommon`, built once on the driver, holding schemas, filters, the projection vector,
  object-store options, and the session-derived flags.
- `SparkFilePartition`, one per task, holding only that partition's files. It is serialized lazily
  at execution time, which is also what lets dynamic partition pruning resolve first.

The two halves are matched at execution time by a key formed from `common.source` and a
driver-computed hash, reassembled by `PlanDataInjector`. Scans that participate in this carry the
`CometScanWithPlanData` trait.

### Key classes

JVM side, under `spark/src/main/` (`CometFileKeyUnwrapper` in `java/`, the rest in `scala/`):

| Class                    | Role                                                                                         |
| ------------------------ | -------------------------------------------------------------------------------------------- |
| `CometScanRule`          | The gate list. Decides whether Comet claims a scan, for both V1 and V2.                      |
| `CometScanContrib`       | `ServiceLoader` SPI giving an out-of-tree format first claim on a scan.                      |
| `CometScanTypeChecker`   | Scan-specific `DataTypeSupport` overrides. Lives in `CometScanRule.scala`.                   |
| `CometScanExec`          | V1 planning intermediate. Owns file listing, partition pruning, and the driver-side metrics. |
| `CometNativeScan`        | Serde. Builds `NativeScanCommon` and applies the serde-stage gates.                          |
| `CometNativeScanExec`    | V1 execution node. Holds the split-serialized plan data.                                     |
| `CometBatchScanExec`     | V2 planning node, for both the Iceberg and CSV paths.                                        |
| `CometIcebergNativeScan` | Iceberg serde, including the task and delete-file translation.                               |
| `CometParquetUtils`      | Encryption config predicates and `spark.sql.parquet.fieldId.read.enabled` lookup.            |
| `CometFileKeyUnwrapper`  | Bridges Parquet key unwrapping back to the JVM KMS client during a native encrypted read.    |

Rust side, under `native/core/src/parquet/`:

| File                                 | Role                                                                                                        |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `parquet_exec.rs`                    | `init_datasource_exec`, which assembles `DataSourceExec`, `ParquetSource`, and the option bags.             |
| `schema_adapter.rs`                  | The physical expression adapter: name and field-id remapping, Spark's conversion rejection matrix.          |
| `parquet_support.rs`                 | `SparkParquetOptions`, `spark_parquet_convert`, struct and map field matching, object-store preparation.    |
| `name_fold.rs`                       | The single case-folding policy shared by the adapter, the nested convert, and the plan-time projection.     |
| `cast_column.rs`                     | `CometCastColumnExpr`, the expression the adapter swaps in to apply a Spark-compatible conversion.          |
| `eager_page_index_reader_factory.rs` | Forces the page index to load with the footer so it lands in the metadata cache, plus the scan I/O metrics. |
| `encryption_support.rs`              | `CometEncryptionFactory`, which calls back to the JVM key unwrapper over JNI.                               |
| `objectstore/`                       | S3, Azure, and S3-compliant alias support.                                                                  |

The native entry point is `OpStruct::NativeScan` in `native/core/src/execution/planner.rs`, which
unpacks the proto and calls `init_datasource_exec`.

## Schemas and Projection

Three schemas cross the boundary, and getting their relationship wrong is the most common source of
scan bugs.

| Field in `NativeScanCommon` | What it is                                                                       |
| --------------------------- | -------------------------------------------------------------------------------- |
| `data_schema`               | The relation's full data schema. The base schema `ParquetSource` is built over.  |
| `required_schema`           | The columns the query projects.                                                  |
| `partition_schema`          | Partition columns, **plus** synthetic fields for file-constant metadata columns. |
| `projection_vector`         | Indices into `data_schema ++ partition_schema`, in output order.                 |

**Constant metadata columns ride on the partition schema.** `file_path`, `file_name`, `file_size`,
`file_block_start`, `file_block_length`, and `file_modification_time` are known before the file is
opened and are constant for every row in it, exactly like partition values, so the serde appends
them to the partition schema and lets DataFusion's partition-value substitution supply them.
DataFusion substitutes those values **by name**, so the serde renames each one to
`_comet_metadata_<name>`, uniquified against the data and partition schemas, to stop a user column
of the same name from silently receiving the metadata value instead of its own.

**Variant is pruned out of the data schema.** Spark's required schema can prune a Variant column
that the relation schema still contains, including one nested under an unrequested struct. The
serde therefore drops unread Variant-bearing roots from the data schema and replaces requested ones
with their already-validated pruned form, so an unsupported type never enters the native reader
even though nothing in the query touches it.

### Default values

Columns added by `ALTER TABLE ... ADD COLUMN ... DEFAULT` are absent from older files.
`getExistenceDefaultValues` gives the serde the evaluated literals, which travel as a parallel pair
of `default_values` and `default_values_indexes` lists and are rebuilt native-side into a
`Column -> ScalarValue` map handed to the expression adapter. Defaults for nested types are
declined at the planning gate.

## The Schema Adapter

`SparkPhysicalExprAdapterFactory` is where the Parquet file's actual schema is reconciled with the
schema Spark asked for. DataFusion calls it at plan time for each file, and it rewrites the
expressions the scan will evaluate.

Its job is not "cast the column". Its job is to produce **exactly** what Spark's vectorized Parquet
reader would have produced for the same file, including the errors.

### Name and field-id matching

`remap_physical_schema` mirrors Spark's `ParquetReadSupport.clipParquetGroupFields`: a requested
field that carries a `PARQUET:field_id` is matched by id, and everything else is matched by
case-insensitive name. The remap changes only top-level field names, so that DataFusion's
exact-name lookup hits. Indices, types, nullability, and metadata stay as they are in the file, and
the original names are restored before the stream is consumed.

Case folding is centralized in `name_fold.rs` for a reason. The fold happens in three places, the
top-level adapter, the nested struct convert in `parquet_support.rs`, and the plan-time projection
in `parquet_exec.rs`, and those three copies drifting apart is what produced
[#5495](https://github.com/apache/datafusion-comet/issues/5495). The policy is Spark's:
`name.toLowerCase(Locale.ROOT)`. Pure-ASCII names are folded inline, which is provably identical to
Java for ASCII. Anything else is delegated over JNI to `CometSchemaUtils.toLowerCaseRoot` and
memoized, because Rust's Unicode tables and the JVM's are not guaranteed to agree and a native
guess would be a silent wrong answer. A JNI failure aborts the batch rather than falling back to a
Rust fold.

### The rejection matrix

`check_conversion` and `check_leaf_conversion` reimplement the accept/reject decisions of Spark's
`ParquetVectorUpdaterFactory.getUpdater`, including its error text. Spark runs `getUpdater` on every
leaf regardless of nesting, so the check walks same-shape complex pairs and applies the leaf rules
at each leaf, extending the column path the way `descriptor.getPath()` does. The first non-accepting
verdict in leaf order wins, matching Spark raising on the first offending column it initializes.

A rejection does not fail the plan. It becomes a `RejectOnNonEmpty` expression that raises only when
a non-empty batch actually arrives, mirroring Spark's per-row-group check. A file whose offending
row groups are all pruned reads successfully, as it does in Spark.

### Conversion

When a conversion is accepted and is not a no-op, the adapter swaps DataFusion's `CastExpr` for
`CometCastColumnExpr`, which runs `spark_parquet_convert`. The exception is
`is_pure_structural_narrowing`: for a cast that only drops unrequested struct or list fields,
DataFusion's own generic nested cast is byte-identical, so the adapter leaves `CastExpr` in place
and DataFusion's leaf-pruning can then see through it and read fewer Parquet leaves. That predicate
is deliberately an allow list, because a deny list would fail open the moment
`parquet_convert_array` grew a case nobody thought to exclude.

### `SparkParquetOptions`

Everything about the conversion that depends on the Spark session or the Spark version travels in
one struct. The version-dependent members are the ones to watch:

| Option                                     | Source                                                                                                                              |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `allow_type_promotion`                     | `ShimCometConf.COMET_SCHEMA_EVOLUTION_ENABLED`: false on 3.x, true on 4.x                                                           |
| `allow_timestamp_ltz_to_ntz`               | `ShimCometConf.COMET_ALLOW_TIMESTAMP_LTZ_AS_NTZ`: false on 3.x, true on 4.x (SPARK-47447)                                           |
| `return_null_struct_if_all_fields_missing` | `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing`, defaulting to true before Spark 4.1 and false from 4.1 (SPARK-53535) |
| `use_field_id` / `ignore_missing_field_id` | `spark.sql.parquet.fieldId.read.enabled` and `.ignoreMissing`                                                                       |
| `case_sensitive`                           | `spark.sql.caseSensitive`                                                                                                           |
| `checked_timestamp_overflow`               | Derived: true only for an unfiltered scan, see below                                                                                |

## Filter Pushdown

Data filters reach native as serialized expressions and are applied through
`ParquetSource::try_pushdown_filters` rather than `with_predicate`. That is the contract
DataFusion's own optimizer uses, and it correctly classifies a filter `ParquetSource` cannot
evaluate as not-pushed-down. The predicate still feeds row-group, page-index, and bloom-filter
pruning even when per-row `RowFilter` evaluation is disabled.

Comet discards the parent pushdown result, because Spark's `Filter` above the scan re-evaluates
every data filter anyway. That is what makes it safe to push a filter down without also inserting a
`FilterExec` natively.

`has_data_filters` is a separate boolean from a non-empty `data_filters` list, and the difference
matters. It is true whenever Spark supplied any Parquet data filter, including when Comet could not
serialize a single one of them. The reason is `checked_timestamp_overflow`: Spark can discard values
through pruning paths Comet cannot mirror, so a filtered scan keeps the lenient
overflow-to-NULL conversion for `TIMESTAMP_MILLIS`, and only a scan where every value is
necessarily read uses the checked conversion. Deriving that from the serialized list alone would
silently switch a filtered scan to checked conversion whenever serialization failed.

## Object Stores

Native reads go through the `object_store` crate, not the Hadoop `FileSystem` API, which is why so
many of the V1 gates are about URL schemes.

`prepare_object_store_with_configs` registers one object store per `FilePartition`, keyed on the
**first file in that partition**, and strips the authority from every file's object key. A
partition spanning two buckets would therefore read every file from the first file's bucket. The
V1 gate declines that case for opt-in alias schemes. Plain multi-bucket `s3://` has the same
limitation today and is not yet declined, because doing so would newly fall back scans that
currently work by luck. The Iceberg gate is stricter and declines multi-bucket `s3a://` too.

Which schemes are readable is answered by the native layer itself, through
`NativeBase.isObjectStoreSchemeSupported`, so the planner cannot drift from `object_store`'s actual
support. Two sets sit outside that answer:

- **libhdfs schemes**, from `fs.comet.libhdfs.schemes`, default `hdfs`. These are readable through
  the libhdfs bridge even though `object_store` does not claim them, so they must not be declined.
  The JVM default deliberately mirrors the native default in `is_hdfs_scheme`.
- **S3-compliant aliases**, from `fs.comet.s3Compliant.schemes`, default empty. Read from the Hadoop
  config rather than SQLConf so `core-site.xml` is honored.

The Iceberg path keeps its own narrower allow list, `icebergReadableSchemes`, because
`object_store` recognizes schemes that iceberg-rust's storage factory cannot build. Admitting one
of those would turn a clean JVM fallback into a native runtime error. That list must stay in
lockstep with `storage_factory_for` in `native/core/src/execution/operators/iceberg_common.rs`.

## Encryption

Comet reads encrypted Parquet by registering a DataFusion `EncryptionFactory` under
`comet.jni_kms_encryption`. Key unwrapping stays on the JVM: `CometEncryptionFactory` calls back
through JNI into `CometFileKeyUnwrapper`, which drives the user's configured KMS client. The native
side works with object-store paths while Spark's KMS interface wants full URIs, so the factory
caches a `uri_base` prefix to reattach.

Only configurations `CometParquetUtils.isEncryptionConfigSupported` recognizes are claimed. A
projected Variant column combined with encryption is rejected outright in `init_datasource_exec`.

## Page Index and Metadata Caching

DataFusion's opener defers loading the page index until row-group pruning shows it is still needed,
and that deferred load bypasses the file metadata cache. On a predicate that never fully resolves
from row-group statistics, the page index is then re-fetched, uncached, on every open
([#3978](https://github.com/apache/datafusion-comet/issues/3978)).
`EagerPageIndexReaderFactory` forces the page index to load on the first fetch so it is cached with
the footer, trading away the skip's benefit in the cases where it would have applied. This is a
workaround for [apache/datafusion#23978](https://github.com/apache/datafusion/issues/23978) and
should be reverted when that is fixed.

The same factory owns the scan I/O counters. `bytes_scanned` keeps its existing meaning, counting
requested data and bloom-filter ranges, because Spark's `inputMetrics.bytesRead` and
`scan_efficiency_ratio` both derive from it. Footer and page-index reads are reported separately
rather than folded in.

## DataFusion Reader Options

`get_options` seeds `TableParquetOptions` from the session so that
`spark.comet.datafusion.execution.parquet.*` and `spark.comet.parquet.rowFilterPushdown.enabled`
reach the reader. It is an explicit allow list, not a bulk copy of the session options, which is the
fail-closed rule applied to upstream defaults. A new DataFusion reader option is silently ignored
until someone checks whether it is safe for Spark semantics, rather than silently active. Recheck
the list whenever the DataFusion dependency version changes.

Two options are hardcoded and not session-overridable. `coerce_int96` is pinned to `us`, and
`coerce_int96_tz` to `UTC` so that the schema adapter can tell an INT96-derived TimestampLTZ from a
true TimestampNTZ source and apply the pre-Spark-4 SPARK-36182 rejection.

## Configuration

The generated [configuration reference](../user-guide/latest/configs.md) has the scan category with
current defaults. The table below is the cross-category view the generator cannot produce: the
configs that affect the scan but are filed elsewhere, or are not public.

| Config                                             | Purpose                                                    |
| -------------------------------------------------- | ---------------------------------------------------------- |
| `spark.comet.scan.enabled`                         | Test-only kill switch for native scans as a whole          |
| `spark.comet.scan.contrib.detectConflicts.enabled` | Diagnostic. Warns when more than one contrib claims a scan |
| `spark.comet.scan.csv.v2.enabled`                  | Experimental native CSV V2 scan                            |
| `spark.comet.parquet.rowFilterPushdown.enabled`    | Per-row `RowFilter` evaluation in the native reader        |
| `spark.comet.convert.parquet.enabled`              | Convert a fallen-back Parquet scan's output to Arrow       |
| `fs.comet.libhdfs.schemes`                         | Hadoop key. Schemes to route through the libhdfs bridge    |
| `fs.comet.s3Compliant.schemes`                     | Hadoop key. Schemes to treat as `s3://` aliases            |

Type promotion and TimestampLTZ-as-NTZ are **not** configs. They are per-Spark-version constants in
`ShimCometConf`. The `spark.comet.schemaEvolution.enabled` conf that used to control type promotion
was removed in [#4298](https://github.com/apache/datafusion-comet/issues/4298).

## Testing

| Suite                                     | Covers                                                                                       |
| ----------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ParquetReadSuite` (`ParquetReadV1Suite`) | End-to-end reads. The base class is abstract, and the V1 subclass pins `USE_V1_SOURCE_LIST`. |
| `CometScanRuleSuite`                      | Gate decisions, by applying `CometScanRule` to a plan and counting node types.               |
| `CometScanSchemeFallbackSuite`            | Which URL schemes the V1 gate claims and which it declines.                                  |
| `CometScanContribSuite`                   | The contrib SPI, including first-claim-wins and conflict detection.                          |
| `CometNativeScanSuite`                    | The serde.                                                                                   |
| `CometScanWithPlanDataSuite`              | Split serialization and the injector.                                                        |
| `ParquetEncryptionITCase`                 | Encrypted reads end to end.                                                                  |
| `ParquetTimestampLtzAsNtzSuite`           | The version-dependent TimestampLTZ rule.                                                     |
| `ParquetReadFromS3Suite`                  | S3 reads against a MinIO testcontainer, including the `blob` alias scheme.                   |
| `ParquetReadFromFakeHadoopFsSuite`        | A native read through libhdfs. Excluded from CI and run by hand, see its scaladoc.           |
| `CometIcebergNativeSuite`                 | The Iceberg scan path.                                                                       |

Rust unit tests live beside the code under `native/core/src/parquet/`. They need the JVM on the
library path, because `name_fold` calls into it. See
[Development Guide](development.md) for the `LD_LIBRARY_PATH` setup.

Spark's own Parquet suites run against Comet through `dev/diffs/`, and they are the strongest
available check that the reader matches Spark. They report in the merge queue rather than on the
pull request, so see [Continuous Integration](ci.md) for how to get a verdict before merging.

## Further Reading

- [Comet Plugin Overview](plugin_overview.md) for where `CometScanRule` sits among the other rules
- [Comet and Iceberg](../user-guide/latest/iceberg.md) for the Iceberg path
- [S3 Credential Provider Design](s3-credential-provider-design.md) for custom credential providers

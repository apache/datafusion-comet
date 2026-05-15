# Wire format & split-mode serialization

This document describes the protobuf shapes added to
`native/proto/src/proto/operator.proto` and the *split-mode*
serialization scheme that lets Comet send a per-scan common header
once while streaming per-partition file lists separately.

## Why split-mode

Without split-mode, every Spark task would receive the entire `tasks`
list for the whole scan, plus the entire `common` block. For a scan
over a 10k-file Delta table with 200 Spark partitions that's ~50×
duplication of the task list. Two existing Comet operators have the
same problem and solve it the same way: `NativeScan` (plain parquet)
and `IcebergNativeScan`.

Split-mode keys off two facts:
  1. The serialized native plan only needs to travel to executors
     once per stage — that's the one-time cost.
  2. The `tasks` field of the `DeltaScan` operator is the *only*
     field that varies per partition. Everything else (schemas,
     filters, projections, options, mappings) is plan-wide.

So at planning time the `DeltaScan` proto is built with
`tasks: [] / common: {…}`. The full task list is held side-channel.
At execution time, the operator emits one byte slice per partition
containing only `DeltaScan { tasks: [partition's slice] }` — no
`common`. On the executor, `DeltaPlanDataInjector` re-merges the two:
parses the cached `common`, parses this partition's `tasks`, builds
the full operator that the native planner expects.

## Operator-tree placement

The Delta scan plugs into the existing operator-tree union at field
117:

```
oneof op_struct {
    ...
    NativeScan native_scan        = 100;
    IcebergScan iceberg_scan      = 116;
    DeltaScan delta_scan          = 117;
    ...
}
```

## DeltaScanCommon — sent once per scan

```protobuf
message DeltaScanCommon {
  repeated SparkStructField required_schema    = 1;
  repeated SparkStructField data_schema        = 2;
  repeated SparkStructField partition_schema   = 3;
  repeated spark_expression.Expr data_filters  = 4;
  repeated int64 projection_vector             = 5;
  string session_timezone                      = 6;
  bool   case_sensitive                        = 7;
  map<string,string> object_store_options      = 8;
  string table_root                            = 9;
  optional uint64 snapshot_version             = 10;
  uint32 data_file_concurrency_limit           = 11;
  string source                                = 12;
  repeated DeltaColumnMapping column_mappings  = 13;
  optional string materialized_row_id_column_name             = 14;
  optional string materialized_row_commit_version_column_name = 15;
  bool   ignore_missing_files                  = 16;
}
```

Field-by-field semantics:

  - **`required_schema`**: user-visible projection in *logical* names.
    What upstream operators see as the scan's output schema.
  - **`data_schema`**: full file schema, with partition columns
    *removed*. Names are *physical* when column mapping is active
    (Delta's parquet files use physical names at every nesting level
    under column mapping).
  - **`partition_schema`**: partition columns. Always logical names.
  - **`data_filters`**: per-row predicates pushed into ParquetSource.
    Bound to `required_schema`. Filters that reference nested-access
    (`GetArrayItem` etc.) are filtered out on the Scala side.
  - **`projection_vector`**: maps each output position in
    `required_schema` to either `data_schema` index *i* (data column)
    or `data_schema.length + p` (partition column #p). Mirrors what
    Spark's `FileSourceScanExec` does internally.
  - **`session_timezone`** / **`case_sensitive`**: forwarded session
    state.
  - **`object_store_options`**: full `NativeConfig.extractObjectStoreOptions`
    output, both kernel-style and Hadoop-style keys.
  - **`table_root`**: normalized URL with trailing slash. The native
    side uses this to register the object store and as a fallback for
    any task whose `file_path` is relative.
  - **`snapshot_version`**: when set, pins the scan to a specific Delta
    version (time travel). Unset means "use latest" — but in practice
    Delta's analyzer always pins the version before Comet sees the
    plan, so this is almost always set.
  - **`data_file_concurrency_limit`**: per-task parallelism inside the
    parquet reader. Default 1 (= the existing single-file-at-a-time
    behavior).
  - **`source`**: human-readable label for `EXPLAIN` / debug.
  - **`column_mappings`**: top-level logical→physical pairs. Nested
    physical names are baked into `data_schema` directly.
  - **`materialized_row_id_column_name`** /
    **`materialized_row_commit_version_column_name`**: when set, the
    physical column the materialised row-tracking values live in.
    Currently the rewrite of these into the scan output happens on the
    Scala side via a `Project(coalesce(...))` — the proto fields exist
    for a future native-side rewrite.
  - **`ignore_missing_files`**: enables `IgnoreMissingFileSource`.

## DeltaScan — per-partition envelope

```protobuf
message DeltaScan {
  DeltaScanCommon common  = 1;
  repeated DeltaScanTask tasks = 2;
}
```

Built two ways:
  1. *Planning-time* (driver, Scala): `setCommon(common); /* no tasks */`.
  2. *Execution-time* (driver, Scala, just before sending each
     partition's data): `/* no common */ addAllTasks(slice)`.

The native planner only ever sees the merged form (post
`DeltaPlanDataInjector`), with both `common` and `tasks` populated.

## DeltaScanTask — one parquet file (or chunk)

```protobuf
message DeltaScanTask {
  string                       file_path                   = 1;
  uint64                       file_size                   = 2;
  optional uint64              record_count                = 3;
  repeated DeltaPartitionValue partition_values            = 4;
  repeated uint64              deleted_row_indexes         = 5;
  optional int64               base_row_id                 = 6;
  optional int64               default_row_commit_version  = 7;
  optional uint64              byte_range_start            = 8;
  optional uint64              byte_range_end              = 9;
  // 10, 11 reserved for future per-task residual / per-file schema
}
```

Field-by-field:

  - **`file_path`**: absolute URL the parquet reader can open
    directly. The kernel JNI path (`resolve_file_path`) joins relative
    AddFile paths against the table root; the pre-materialised path
    (`buildTaskListFromAddFiles`) does the same join in Scala. Already-
    absolute paths (URI-scheme prefixed) are passed through —
    important for SHALLOW CLONE tables that record absolute paths into
    AddFile.
  - **`file_size`** / **`record_count`**: from kernel's
    `ScanFile.size` and `ScanFile.stats.num_records`. Used for
    splitting decisions and metric population.
  - **`partition_values`**: ordered list keyed by *logical* partition
    column name. The Rust side has already translated kernel's
    physical-keyed map to logical keys for column-mapped tables.
  - **`deleted_row_indexes`**: sorted ascending, 0-based into the
    file's physical parquet row space. Empty = no DV in use. Each
    DV-bearing task gets its own `FileGroup` on the native side so
    the running-row-offset DV applicator works.
  - **`base_row_id`** / **`default_row_commit_version`**: row-tracking
    metadata. Populated by the pre-materialised AddFile path
    (`buildTaskListFromAddFiles`), unset on the kernel path (kernel
    0.19.x consumes them internally for `TransformSpec`). Currently
    unused by the native side — row-tracking synthesis is done in a
    Scala-side `Project(coalesce(...))` over partition-channel
    constants from `RowTrackingAugmentedFileIndex`.
  - **`byte_range_start`** / **`byte_range_end`**: when set, the
    parquet reader only materialises row groups whose start offsets
    fall in `[start, end)`. Both unset = whole file. Set together by
    `splitTasks`.

## DeltaPartitionValue

```protobuf
message DeltaPartitionValue {
  string name           = 1;
  optional string value = 2;
}
```

Delta represents every partition value as a string in the log. The
native side parses the string into the correct Arrow scalar driven by
`partition_schema`. `value` unset means NULL. Date / Timestamp /
Decimal parsing uses `session_timezone` from the common block.

## DeltaScanTaskList — JNI return value

```protobuf
message DeltaScanTaskList {
  uint64 snapshot_version  = 1;
  string table_root        = 2;
  repeated DeltaScanTask tasks = 3;
  repeated DeltaColumnMapping column_mappings = 5;
  repeated string unsupported_features = 4;
}
```

Returned by `Java_org_apache_comet_Native_planDeltaScan` once per
query. The Scala side pulls it apart, applies static partition pruning
and file splitting, then reassembles the per-partition `DeltaScan`
slices.

## DeltaColumnMapping

```protobuf
message DeltaColumnMapping {
  string logical_name  = 1;
  string physical_name = 2;
}
```

Top-level only. Nested physical names are folded into `data_schema`
field names directly during serde so the native parquet reader doesn't
need a separate nested-name map.

## Wire compatibility

Field numbers are intentionally left sparse and grouped by concern, not
sequential. New fields go at the *end* of each message to preserve
backward compatibility. The `DeltaScanTask` message reserves slots
10/11 explicitly for future per-task residual predicates and per-file
schema evolution.

## End-to-end byte path

```
Driver (Scala): planDeltaScan() → DeltaScanTaskList bytes
   ↓ parsed
Driver (Scala): prunePartitions + splitTasks  → filtered task list
   ↓
Driver (Scala): convert() builds DeltaScan {common, tasks=[]} + lastTaskListBytes (full)
   ↓ encoded once into the operator tree
Driver (Scala): CometDeltaNativeScanExec.buildPerPartitionBytes()
   ↓ per partition: DeltaScan {tasks=[slice]} bytes (no common)
   ↓
Spark broadcast: serialized native plan (common header) + serializable plan-data map
   ↓
Executor (Rust): DeltaPlanDataInjector.inject(op, common_bytes, partition_bytes)
   ↓ rebuilds DeltaScan {common, tasks=[slice]}
   ↓
Executor (Rust): planner.rs OpStruct::DeltaScan arm → DataFusion ExecutionPlan
   ↓
Executor (Rust): ParquetSource (+ DeltaDvFilterExec + Projection rename if column-mapped)
   ↓ ColumnarBatch
Executor (JVM): CometExecRDD pulls Arrow batches and feeds upstream operators
```

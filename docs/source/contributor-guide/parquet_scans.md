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

# Comet Parquet Scan Implementations

Comet currently has three distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation. The default setting is `spark.comet.scan.impl=auto`, and
Comet will choose the most appropriate implementation based on the Parquet schema and other Comet configuration
settings. Most users should not need to change this setting. However, it is possible to force Comet to try and use
a particular implementation for all scan operations by setting this configuration property to one of the following
implementations.

| Implementation          | Description                                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `native_comet`          | This implementation provides strong compatibility with Spark but does not support complex types. This is the original scan implementation in Comet and may eventually be removed.    |
| `native_iceberg_compat` | This implementation delegates to DataFusion's `DataSourceExec` but uses a hybrid approach of JVM and native code. This scan is designed to be integrated with Iceberg in the future. |
| `native_datafusion`     | This experimental implementation delegates to DataFusion's `DataSourceExec` for full native execution. There are known compatibility issues when using this scan.                    |

The `native_datafusion` and `native_iceberg_compat` scans provide the following benefits over the `native_comet`
implementation:

- Leverages the DataFusion community's ongoing improvements to `DataSourceExec`
- Provides support for reading complex types (structs, arrays, and maps)
- Removes the use of reusable mutable-buffers in Comet, which is complex to maintain
- Improves performance

The `native_datafusion` and `native_iceberg_compat` scans share the following limitations:

- When reading Parquet files written by systems other than Spark that contain columns with the logical types `UINT_8`
  or `UINT_16`, Comet will produce different results than Spark because Spark does not preserve or understand these
  logical types. Arrow-based readers, such as DataFusion and Comet do respect these types and read the data as unsigned
  rather than signed. By default, Comet will fall back to `native_comet` when scanning Parquet files containing `byte` or `short`
  types (regardless of the logical type). This behavior can be disabled by setting
  `spark.comet.scan.allowIncompatible=true`.
- No support for default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.

The `native_datafusion` scan has some additional limitations:

- Bucketed scans are not supported
- No support for row indexes
- `PARQUET_FIELD_ID_READ_ENABLED` is not respected [#1758]
- There are failures in the Spark SQL test suite [#1545]
- Setting Spark configs `ignoreMissingFiles` or `ignoreCorruptFiles` to `true` is not compatible with Spark

## S3 Support

There are some 

### `native_comet`

The default `native_comet` Parquet scan implementation reads data from S3 using the [Hadoop-AWS module](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html), which 
is identical to the approach commonly used with vanilla Spark. AWS credential configuration and other Hadoop S3A 
configurations works the same way as in vanilla Spark.

### `native_datafusion` and `native_iceberg_compat`

The `native_datafusion` and `native_iceberg_compat` Parquet scan implementations completely offload data loading 
to native code. They use the [`object_store` crate](https://crates.io/crates/object_store) to read data from S3 and 
support configuring S3 access using standard [Hadoop S3A configurations](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration) by translating them to 
the `object_store` crate's format.

This implementation maintains compatibility with existing Hadoop S3A configurations, so existing code will 
continue to work as long as the configurations are supported and can be translated without loss of functionality.

#### Additional S3 Configuration Options

Beyond credential providers, the `native_datafusion` implementation supports additional S3 configuration options:

| Option | Description |
|--------|-------------|
| `fs.s3a.endpoint` | The endpoint of the S3 service |
| `fs.s3a.endpoint.region` | The AWS region for the S3 service. If not specified, the region will be auto-detected. |
| `fs.s3a.path.style.access` | Whether to use path style access for the S3 service (true/false, defaults to virtual hosted style) |
| `fs.s3a.requester.pays.enabled` | Whether to enable requester pays for S3 requests (true/false) |

All configuration options support bucket-specific overrides using the pattern `fs.s3a.bucket.{bucket-name}.{option}`.

#### Examples

The following examples demonstrate how to configure S3 access with the `native_datafusion` Parquet scan implementation using different authentication methods.

**Example 1: Simple Credentials**

This example shows how to access a private S3 bucket using an access key and secret key. The `fs.s3a.aws.credentials.provider` configuration can be omitted since `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` is included in Hadoop S3A's default credential provider chain.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.comet.scan.impl=native_datafusion \
--conf spark.hadoop.fs.s3a.access.key=my-access-key \
--conf spark.hadoop.fs.s3a.secret.key=my-secret-key
...
```

**Example 2: Assume Role with Web Identity Token**

This example demonstrates using an assumed role credential to access a private S3 bucket, where the base credential for assuming the role is provided by a web identity token credentials provider.

```shell
$SPARK_HOME/bin/spark-shell \
...
--conf spark.comet.scan.impl=native_datafusion \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider \
--conf spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam::123456789012:role/my-role \
--conf spark.hadoop.fs.s3a.assumed.role.session.name=my-session \
--conf spark.hadoop.fs.s3a.assumed.role.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
...
```

#### Limitations

The S3 support of `native_datafusion` has the following limitations:

1. **Partial Hadoop S3A configuration support**: Not all Hadoop S3A configurations are currently supported. Only the configurations listed in the tables above are translated and applied to the underlying `object_store` crate.

2. **Custom credential providers**: Custom implementations of AWS credential providers are not supported. The implementation only supports the standard credential providers listed in the table above. We are planning to add support for custom credential providers through a JNI-based adapter that will allow calling Java credential providers from native code. See [issue #1829](https://github.com/apache/datafusion-comet/issues/1829) for more details.

## Architecture Diagrams

This section provides detailed architecture diagrams for each of the three Parquet scan implementations.

### `native_comet` Scan Architecture

The `native_comet` scan is a hybrid approach where the JVM reads Parquet files and passes data to native execution.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         NATIVE_COMET SCAN FLOW                              │
└─────────────────────────────────────────────────────────────────────────────┘

JVM (Scala/Java)                                    Native (Rust)
════════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────┐
│ CometScanExec                 │  Physical operator for DataSource V1
│ (CometScanExec.scala:61)      │
│                               │
│ scanImpl = "native_comet"     │
└───────────┬───────────────────┘
            │
            │ wraps
            ↓
┌───────────────────────────────┐
│ FileSourceScanExec            │  Original Spark scan (from Catalyst)
│ (from Spark)                  │
└───────────┬───────────────────┘
            │
            │ creates RDD using
            ↓
┌───────────────────────────────┐
│ CometParquetFileFormat        │  Custom Parquet file format handler
│ (CometParquetFileFormat.scala)│
│                               │
│ Key methods:                  │
│ - buildReaderWithPartition    │
│   Values()                    │
│ - supportBatch()              │
└───────────┬───────────────────┘
            │
            │ creates via factory
            ↓
┌───────────────────────────────┐
│ CometParquetPartitionReader   │  Factory for creating partition readers
│ Factory (with prefetch)       │  Supports asynchronous prefetch optimization
│ (.../CometParquetPartition    │
│  ReaderFactory.scala)         │
└───────────┬───────────────────┘
            │
            │ creates
            ↓
┌───────────────────────────────┐
│ BatchReader                   │  Main Parquet reader
│ (BatchReader.java:90)         │  Reads Parquet files via Hadoop APIs
│                               │
│ Key components:               │
│ ├─ ParquetFileReader         │  Opens Parquet file, reads metadata
│ ├─ PageReadStore             │  Reads column pages from file
│ ├─ ColumnDescriptors         │  Parquet column metadata
│ └─ Capacity (batch size)     │  Configurable batch size
│                               │
│ Process per row group:        │
│ 1. Read file footer/metadata  │
│ 2. For each column:           │
│    - Get ColumnDescriptor     │
│    - Read pages via           │
│      PageReadStore            │
│    - Create CometVector       │
│      from native data         │
│ 3. Return ColumnarBatch       │
└───────────┬───────────────────┘
            │
            │ Uses JNI to access native decoders
            │ (not for page reading, only for
            │  specialized operations if needed)
            │
            ↓
┌───────────────────────────────┐
│ ColumnarBatch                 │
│ with CometVector[]            │  Arrow-compatible columnar vectors
│                               │
│ Each CometVector wraps:       │
│ - Arrow ColumnVector          │
│ - Native memory buffers       │
└───────────┬───────────────────┘
            │
            │ Passed to native execution via JNI
            │
            ↓                            ┌───────────────────────────┐
┌───────────────────────────────┐       │ ScanExec                  │
│ CometExecIterator             │──────→│ (operators/scan.rs:54)    │
│ (JNI boundary)                │  JNI  │                           │
└───────────────────────────────┘       │ Reads batches from JVM    │
                                        │ via CometBatchIterator    │
                                        │                           │
                                        │ Key operations:           │
                                        │ ├─ next_batch()           │
                                        │ ├─ FFI Arrow conversion   │
                                        │ └─ Selection vectors      │
                                        │    (for deletes)          │
                                        └───────────┬───────────────┘
                                                    │
                                                    │ RecordBatch
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ Native Execution Pipeline │
                                        │ (DataFusion operators)    │
                                        │                           │
                                        │ - FilterExec              │
                                        │ - ProjectExec             │
                                        │ - AggregateExec           │
                                        │ - etc.                    │
                                        └───────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
STORAGE ACCESS (native_comet)
═══════════════════════════════════════════════════════════════════════════════

┌────────────────────────────────┐
│ Hadoop FileSystem API          │  Used by BatchReader
│                                │
│ - LocalFileSystem              │  For local files
│ - HDFS                         │  For HDFS
│ - S3A (Hadoop-AWS module)      │  For S3 (using AWS Java SDK)
│   └─ Uses AWS Java SDK         │
│                                │
│ Configuration:                 │
│ - fs.s3a.access.key            │
│ - fs.s3a.secret.key            │
│ - fs.s3a.endpoint              │
│ - Standard Hadoop S3A configs  │
└────────────────────────────────┘

Key Characteristics:
- ✅ Maximum Spark compatibility (uses Spark's Parquet reading)
- ✅ All Hadoop FileSystem implementations supported (HDFS, S3A, etc.)
- ✅ Standard Hadoop S3A configuration
- ✅ Can use prefetch for improved performance
- ❌ No complex type support (structs, arrays, maps)
- ❌ Cannot read UINT_8/UINT_16 types by default
- ⚠️  Data crosses JVM→Native boundary via FFI
```

### `native_datafusion` Scan Architecture

The `native_datafusion` scan is a fully native implementation using DataFusion's ParquetExec.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      NATIVE_DATAFUSION SCAN FLOW                            │
└─────────────────────────────────────────────────────────────────────────────┘

JVM (Scala/Java)                                    Native (Rust)
════════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────┐
│ CometScanExec                 │  Initial scan created by CometScanRule
│ (CometScanExec.scala:61)      │
│                               │
│ scanImpl = "native_datafusion"│
│                               │
│ Note: This is temporary!      │  Only exists between CometScanRule
│ CometExecRule converts this   │  and CometExecRule optimizer passes
│ to CometNativeScanExec        │
└───────────┬───────────────────┘
            │
            │ CometExecRule transformation
            │ (CometExecRule.scala:160-162)
            ↓
┌───────────────────────────────┐
│ CometNativeScanExec           │  Fully native scan operator
│ (CometNativeScanExec.scala:46)│
│                               │  Extends CometLeafExec
│ Fields:                       │  (native execution root)
│ ├─ nativeOp: Operator         │  Protobuf operator definition
│ ├─ relation: HadoopFsRelation │  File relation metadata
│ ├─ output: Seq[Attribute]     │  Output schema
│ ├─ requiredSchema: StructType │  Projected schema
│ ├─ dataFilters: Seq[Expr]     │  Pushed-down filters
│ └─ serializedPlanOpt          │  Serialized native plan
└───────────┬───────────────────┘
            │
            │ Serialization via QueryPlanSerde
            │ operator2Proto(scan)
            ↓
┌───────────────────────────────┐
│ Protobuf Operator             │  Serialized scan operator
│ (Operator protobuf message)   │
│                               │  Contains:
│ Contains:                     │  - File paths
│ ├─ file_paths: Vec<String>    │  - Schema
│ ├─ schema: Schema             │  - Filters (as Expr protobuf)
│ ├─ filters: Vec<Expr>         │  - Projection
│ └─ projection: Vec<usize>     │
└───────────┬───────────────────┘
            │
            │ Deserialization in native code
            │
            ↓                            ┌───────────────────────────┐
                                        │ PhysicalPlanner           │
                                        │ (planner.rs)              │
                                        │                           │
                                        │ Deserializes protobuf     │
                                        │ Creates physical plan     │
                                        └───────────┬───────────────┘
                                                    │
                                                    │ calls
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ init_datasource_exec()    │
                                        │ (parquet_exec.rs)         │
                                        │                           │
                                        │ Creates DataFusion        │
                                        │ DataSourceExec with:      │
                                        │                           │
                                        │ ├─ ParquetSource          │
                                        │ ├─ SparkSchemaAdapter     │
                                        │ │  (for Spark/Parquet     │
                                        │ │   type compatibility)   │
                                        │ ├─ Projection             │
                                        │ ├─ Filters                │
                                        │ └─ Partition fields       │
                                        └───────────┬───────────────┘
                                                    │
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ DataFusion                │
                                        │ DataSourceExec            │
                                        │                           │
                                        │ ExecutionPlan trait       │
                                        └───────────┬───────────────┘
                                                    │
                                                    │ execute()
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ ParquetSource             │
                                        │                           │
                                        │ Opens and reads Parquet   │
                                        │ files using arrow-rs      │
                                        │ ParquetRecordBatchReader  │
                                        └───────────┬───────────────┘
                                                    │
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ Arrow Parquet Reader      │
                                        │ (from arrow-rs crate)     │
                                        │                           │
                                        │ Operations:               │
                                        │ ├─ Open file              │
                                        │ ├─ Read metadata/footer   │
                                        │ ├─ Decode pages           │
                                        │ ├─ Build RecordBatches    │
                                        │ └─ Apply filters/projection│
                                        └───────────┬───────────────┘
                                                    │
                                                    │ RecordBatch stream
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ Native Execution Pipeline │
                                        │ (all DataFusion)          │
                                        │                           │
                                        │ - FilterExec              │
                                        │ - ProjectExec             │
                                        │ - AggregateExec           │
                                        │ - etc.                    │
                                        └───────────┬───────────────┘
                                                    │
                                                    │ Results via FFI
                                                    ↓
┌───────────────────────────────┐       ┌───────────────────────────┐
│ Spark execution continues     │←──────│ Arrow C Data Interface    │
│ (receives ColumnarBatch)      │  FFI  │ (Arrow FFI)               │
└───────────────────────────────┘       └───────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
STORAGE ACCESS (native_datafusion)
═══════════════════════════════════════════════════════════════════════════════

                                        ┌───────────────────────────┐
                                        │ object_store crate        │
                                        │ (objectstore/mod.rs)      │
                                        │                           │
                                        │ Implementations:          │
                                        │ ├─ LocalFileSystem        │
                                        │ ├─ S3 (via aws-sdk-rust)  │
                                        │ │  └─ AWS Rust SDK        │
                                        │ ├─ GCS                    │
                                        │ └─ Azure                  │
                                        │                           │
                                        │ Configuration translation:│
                                        │ Hadoop S3A configs →      │
                                        │ object_store configs      │
                                        │                           │
                                        │ - fs.s3a.access.key       │
                                        │ - fs.s3a.secret.key       │
                                        │ - fs.s3a.endpoint         │
                                        │ - fs.s3a.endpoint.region  │
                                        └───────────────────────────┘

Key Characteristics:
- ✅ Fully native execution (no JVM→Native data transfer for scan)
- ✅ Complex type support (structs, arrays, maps)
- ✅ Leverages DataFusion community improvements
- ✅ Better performance for some workloads
- ❌ More restrictions than native_comet (no bucketed scans)
- ❌ Cannot read UINT_8/UINT_16 types by default
- ❌ No support for ignoreMissingFiles/ignoreCorruptFiles
- ⚠️  Requires COMET_EXEC_ENABLED=true
- ⚠️  Uses object_store (different from Hadoop FileSystem)
```

### `native_iceberg_compat` Scan Architecture

The `native_iceberg_compat` scan is designed for Iceberg integration, similar to `native_datafusion` but with an Iceberg-specific API layer.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   NATIVE_ICEBERG_COMPAT SCAN FLOW                           │
└─────────────────────────────────────────────────────────────────────────────┘

JVM (Scala/Java)                                    Native (Rust)
════════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────┐
│ CometScanExec                 │  Physical operator for DataSource V1
│ (CometScanExec.scala:61)      │
│                               │  Auto-selected for Iceberg tables
│ scanImpl =                    │  by CometScanRule.selectScan()
│   "native_iceberg_compat"     │  (CometScanRule.scala:357)
│                               │
│ Key difference:               │
│ - Prefetch is DISABLED        │  Iceberg manages its own data access
│   (line 434: !usingDataFusion │  patterns
│    Reader)                    │
└───────────┬───────────────────┘
            │
            │ Uses Iceberg-specific reader
            ↓
┌───────────────────────────────┐
│ IcebergCometBatchReader       │  Public API for Iceberg integration
│ (IcebergCometBatchReader.java │
│  :29-43)                      │
│                               │
│ Extends: BatchReader          │  Inherits batch reading functionality
│                               │
│ Key method:                   │
│ init(AbstractColumnReader[])  │  Iceberg provides column readers
│                               │
│ Purpose:                      │
│ - Allows Iceberg to control   │
│   column reader initialization│
│ - Iceberg passes its own      │
│   AbstractColumnReader[]      │
│ - Comet provides batch reading│
│   interface                   │
└───────────┬───────────────────┘
            │
            │ Iceberg creates AbstractColumnReader[]
            │ for each Parquet column
            │
            ↓
┌───────────────────────────────┐
│ AbstractColumnReader[]        │  Iceberg-managed column readers
│ (from Iceberg)                │
│                               │
│ Each reader handles:          │
│ ├─ Column metadata            │
│ ├─ Page reading               │
│ ├─ Value decoding             │
│ └─ Deletion vectors           │  Iceberg-specific feature
│    (row-level deletes)        │
└───────────┬───────────────────┘
            │
            │ IcebergCometBatchReader delegates to
            │ parent BatchReader for batch creation
            ↓
┌───────────────────────────────┐
│ BatchReader                   │  Same as native_comet
│ (BatchReader.java:90)         │
│                               │
│ Creates ColumnarBatch with:   │
│ - Data from Iceberg readers   │
│ - CometVector[] wrappers      │
│ - Partition values            │
└───────────┬───────────────────┘
            │
            │ Uses same native path as
            │ native_datafusion for execution
            │
            ↓                            ┌───────────────────────────┐
┌───────────────────────────────┐       │ Same as native_datafusion:│
│ CometExecIterator             │──────→│                           │
│ (JNI boundary)                │  JNI  │ - init_datasource_exec()  │
└───────────────────────────────┘       │   (parquet_exec.rs)       │
                                        │ - DataSourceExec          │
                                        │ - ParquetSource           │
                                        │ - Arrow Parquet Reader    │
                                        └───────────┬───────────────┘
                                                    │
                                                    │ RecordBatch
                                                    ↓
                                        ┌───────────────────────────┐
                                        │ Native Execution Pipeline │
                                        │ (DataFusion operators)    │
                                        │                           │
                                        │ With Iceberg features:    │
                                        │ - Deletion vectors applied│
                                        │ - Schema evolution        │
                                        │ - Partition evolution     │
                                        └───────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
ICEBERG-SPECIFIC FEATURES
═══════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────┐
│ Iceberg Table Integration     │
│                               │
│ Features supported:           │
│ ├─ Table metadata             │  Via Iceberg catalog
│ ├─ Manifest files             │  List of data files
│ ├─ Data files (Parquet)       │  Actual table data
│ ├─ Deletion files             │  Row-level deletes
│ │  └─ Position deletes        │  Delete by row position
│ │  └─ Equality deletes        │  Delete by column values
│ ├─ Schema evolution           │  Add/remove/rename columns
│ └─ Partition evolution        │  Change partitioning scheme
│                               │
│ Iceberg reads via:            │
│ ├─ FileScanTask objects       │  Pre-planned file scans
│ ├─ PartitionSpec              │  Partitioning metadata
│ └─ DataFile objects           │  File-level metadata
└───────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
STORAGE ACCESS (native_iceberg_compat)
═══════════════════════════════════════════════════════════════════════════════

JVM Side:                                Native Side:

┌────────────────────────────────┐      ┌───────────────────────────┐
│ Hadoop FileSystem API          │      │ object_store crate        │
│ (used by IcebergCometBatch     │      │ (same as native_datafusion)│
│  Reader for metadata)          │      │                           │
│                                │      │ - LocalFileSystem         │
│ - Local files                  │      │ - S3 (aws-sdk-rust)       │
│ - S3A (for metadata)           │      │ - GCS                     │
│                                │      │ - Azure                   │
└────────────────────────────────┘      └───────────────────────────┘

Auto-Selection Criteria (CometScanRule.scala:303-365):
────────────────────────────────────────────────────────
✅ Iceberg table with SupportsComet trait
✅ Files on local filesystem OR S3
✅ Valid S3 configuration (if S3)
✅ No unsupported complex types
✅ COMET_EXEC_ENABLED=true
❌ Falls back to native_comet if ANY check fails

Key Characteristics:
- ✅ Designed for Iceberg integration
- ✅ Iceberg controls column reader initialization
- ✅ Supports Iceberg-specific features (deletes, schema evolution)
- ✅ Complex type support (via DataFusion)
- ✅ Same native execution as native_datafusion
- ❌ Prefetch disabled (Iceberg manages access patterns)
- ❌ Same restrictions as native_datafusion
- ⚠️  Only supports local filesystem and S3
- ⚠️  Requires COMET_EXEC_ENABLED=true
```

## Implementation File Locations

For reference, here are the key implementation files for each scan mode:

### JVM Components

| Component | File Path | Description |
|-----------|-----------|-------------|
| `CometScanExec` | `spark/src/main/scala/org/apache/spark/sql/comet/CometScanExec.scala` | Main scan operator for native_comet and native_iceberg_compat |
| `CometNativeScanExec` | `spark/src/main/scala/org/apache/spark/sql/comet/CometNativeScanExec.scala` | Fully native scan operator for native_datafusion |
| `CometParquetFileFormat` | `spark/src/main/scala/org/apache/comet/parquet/CometParquetFileFormat.scala` | Custom Parquet file format handler |
| `CometParquetPartitionReaderFactory` | `spark/src/main/scala/org/apache/comet/parquet/CometParquetPartitionReaderFactory.scala` | Factory for partition readers with prefetch |
| `BatchReader` | `common/src/main/java/org/apache/comet/parquet/BatchReader.java` | Main Parquet batch reader (native_comet) |
| `IcebergCometBatchReader` | `common/src/main/java/org/apache/comet/parquet/IcebergCometBatchReader.java` | Iceberg-specific batch reader API |
| `CometScanRule` | `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala` | Optimizer rule for scan selection |

### Native Components

| Component | File Path | Description |
|-----------|-----------|-------------|
| `parquet_exec.rs` | `native/core/src/parquet/parquet_exec.rs` | DataFusion ParquetExec initialization |
| `scan.rs` | `native/core/src/execution/operators/scan.rs` | Generic ScanExec for reading from JVM |
| `planner.rs` | `native/core/src/execution/planner.rs` | Physical plan creation from protobuf |
| `objectstore/mod.rs` | `native/core/src/parquet/objectstore/mod.rs` | Object store abstraction for file access |


[#1545]: https://github.com/apache/datafusion-comet/issues/1545
[#1758]: https://github.com/apache/datafusion-comet/issues/1758
[#1829]: https://github.com/apache/datafusion-comet/issues/1829

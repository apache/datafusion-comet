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

# Arrow FFI Usage in Comet

## Overview

Comet transfers Arrow data across the JVM/native boundary in two directions:

1. **JVM → Native**: Native code pulls batches from the JVM over the
   [Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html). The JVM exports each
   per-partition iterator once as an `ArrowArrayStream`, and native pulls every batch through a single C callback.
2. **Native → JVM**: JVM pulls batches from native code using `CometExecIterator`, via the
   [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) (one `ArrowArray`/`ArrowSchema`
   pair per column of each batch).

The following diagram shows an example of the end-to-end flow for a query stage.

![Diagram of Comet Data Flow](/_static/images/comet-dataflow.svg)

Both scenarios use the same FFI mechanism but have different ownership semantics and memory management implications.

## Arrow FFI Basics

The Arrow C Data Interface defines two C structures:

- `ArrowArray`: Contains pointers to data buffers and metadata
- `ArrowSchema`: Contains type information

The Arrow C Stream Interface builds on these with a third structure:

- `ArrowArrayStream`: A stream of `ArrowArray`s sharing one `ArrowSchema`, pulled one at a time through a
  `get_next` C callback. This is how Comet transfers JVM-sourced input (see below).

### Key Characteristics

- **Zero-copy**: Data buffers can be shared across language boundaries without copying
- **Ownership transfer**: Clear semantics for who owns and must free the data
- **Release callbacks**: Custom cleanup functions for proper resource management

## JVM → Native Data Flow (ScanExec)

### Architecture

When native code needs data from the JVM, it uses `ScanExec`, which is backed by an Arrow C Stream that the JVM
exports once per partition:

```
┌─────────────────┐
│  Spark/Scala    │
│ Iterator of     │
│ batches or rows │
└────────┬────────┘
         │ wrapped in an ArrowReader, exported once
         │ via Data.exportArrayStream
         ▼
┌─────────────────┐
│ ArrowArrayStream│ ── JVM side: one C stream struct per partition
│  (C struct)     │
└────────┬────────┘
         │ Arrow C Stream Interface
         │ (native pulls each batch via the get_next callback)
         ▼
┌─────────────────┐
│    ScanExec     │ ── owns an ArrowArrayStreamReader
│  (Rust/native)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DataFusion    │
│   operators     │
└─────────────────┘
```

### Stream Export and Import

On the JVM side, `CometArrowStream` (in `execution/arrow/CometNativeArrowSource.scala`) wraps each per-partition
input in an `org.apache.arrow.vector.ipc.ArrowReader` and exports it once with `Data.exportArrayStream`. The reader
implementation depends on the source of the data:

- `RowArrowReader`: a Spark `Iterator[InternalRow]` (row input)
- `SparkColumnarArrowReader`: a non-Arrow Spark `ColumnarBatch`
- `ColumnarBatchArrowReader`: an Arrow-backed `ColumnarBatch` (transfers `VectorSchemaRoot` ownership)

The exported `ArrowArrayStream`s are boxed into the `Array[Object]` that `CometExecIterator` / `CometExecRDD` pass
to native `createPlan`, one slot per scan input.

Not every slot is an Arrow stream. When a native operator consumes Comet shuffle output and
`spark.comet.shuffle.directRead.enabled` is set, that slot carries a `CometShuffleBlockIterator` instead, and the
compressed shuffle blocks are decoded inside the native plan by `ShuffleScanExec` rather than crossing this FFI
boundary at all. `CometExecRDD.resolveInputObjects` classifies the slots, driven by which scan slots the serialized
plan marked as `ShuffleScan`. See [Direct Read](native_shuffle.md#direct-read-shufflescan) for that path.

On the native side, `planner.rs` reads each stream's `memoryAddress` and takes ownership through arrow-rs's
`ArrowArrayStreamReader::from_raw`, importing the schema once. `ScanExec::get_next_batch` then pulls each batch
through the stream's `get_next` callback. There is no per-batch JNI call and no per-column FFI export.
`ScanExec::pull_next` passes every imported column through `import_column`, which decodes invalid UTF-8 to Spark's
rendering and otherwise uses the column as imported, without a copy.

Java's allocator only guarantees 8-byte alignment, while arrow-rs needs `Decimal128` buffers 16-byte aligned. arrow-rs
realigns under-aligned buffers on import ([apache/arrow-rs#10030](https://github.com/apache/arrow-rs/pull/10030)), and
the `realigns_under_aligned_decimal128` test in `scan.rs` guards against an arrow downgrade that would bring back the
panic ([apache/arrow-rs#10028](https://github.com/apache/arrow-rs/issues/10028)).

### Schema Reconciliation

`CometArrowStream.reconcileStreamSchema` advertises the stream's schema from the actual `CometVector` types in the
first batch rather than the consumer's Spark-declared types. Native `ScanExec` already casts its input to the
declared scan-input schema in `build_record_batch`, so the truthful first-batch schema lets that cast fire; if the
two differ, it logs one deduplicated warning naming the operator, column, and type drift.

No input stream carries a dictionary. `RowArrowReader` and `SparkColumnarArrowReader` write plain vectors, and
`ColumnarBatchArrowReader` decodes a dictionary-encoded column on the JVM before export, which is why
`reconcileStreamSchema` advertises the dictionary's value type for it.

### Memory Layout

When a batch is transferred from JVM to native:

```
JVM Heap:                           Native Memory:
┌──────────────────┐               ┌──────────────────┐
│ ColumnarBatch    │               │ FFI_ArrowArray   │
│ ┌──────────────┐ │               │ ┌──────────────┐ │
│ │ ArrowBuf     │─┼──────────────>│ │ buffers[0]   │ │
│ │ (handle)     │ │               │ │ (pointer)    │ │
│ └──────────────┘ │               │ └──────────────┘ │
└──────────────────┘               └──────────────────┘
        │                                   │
        │                                   │
Off-heap Memory:                            │
┌──────────────────┐ <──────────────────────┘
│ Actual Data      │
│ (e.g., int32[])  │
└──────────────────┘
```

**Key Point**: The actual data buffers are shared zero-copy; native only takes pointers to the off-heap buffers.

### Ownership and Lifecycle

The Arrow C Stream Interface transfers ownership by reference count: native takes ownership of each imported batch,
so it is safe to buffer batches in operators such as `SortExec` or `ShuffleWriterExec` without a deep copy.

The whole per-partition stream is exported once, so the JVM allocates one `ArrowArrayStream` per partition rather
than a per-batch, per-column `ArrowArray`/`ArrowSchema` wrapper object pair. Lifecycle is anchored at the stream: when
`ScanExec` drops its `ArrowArrayStreamReader`, the stream's release callback fires synchronously back into the JVM
and closes the `ArrowReader` and its `VectorSchemaRoot`, releasing the off-heap buffers. Because native holds those
buffers until the reader drops, an operator that buffers many batches keeps the corresponding JVM-side data alive
until then.

## Native → JVM Data Flow (CometExecIterator)

### Architecture

When the JVM needs results from native execution:

```
┌─────────────────┐
│ DataFusion Plan │
│   (native)      │
└────────┬────────┘
         │ produces RecordBatch
         ▼
┌─────────────────┐
│ prepare_output  │ ── fills one ArrowArray/ArrowSchema pair per column
│  (Rust/native)  │
└────────┬────────┘
         │ Arrow C Data Interface
         │ (structs allocated by the JVM, filled by native)
         ▼
┌─────────────────┐
│   NativeUtil    │ ◄─── CometExecIterator calls Native.executePlan
│  (Scala side)   │      once per batch
└────────┬────────┘
         │ ColumnarBatch of CometVectors
         ▼
┌─────────────────┐
│ Spark and Comet │
│  JVM operators  │
└─────────────────┘
```

### Transfer Process

`CometExecIterator.hasNext` fetches each batch through `NativeUtil.getNextBatch`, with one JNI call per batch:

1. `NativeUtil.getNextBatch` allocates one empty `ArrowArray`/`ArrowSchema` pair per output column from
   `CometArrowAllocator` and passes their memory addresses to
   `Native.executePlan(stage, partition, plan, arrayAddrs, schemaAddrs)`.
2. `executePlan` (in `jni_api.rs`) polls the native plan for its next `RecordBatch`, and `prepare_output` exports
   each column into its pair with `move_to_spark` (in `execution/utils.rs`). `move_to_spark` writes an
   `FFI_ArrowArray` over the column's `ArrayData`, and an `FFI_ArrowSchema` built from its data type and field
   metadata, into the JVM-allocated structs. `executePlan` returns the batch's row count, or `-1` at the end of the
   output. With `spark.comet.debug.enabled` set, `prepare_output` first runs `validate_full` on every column.
3. At the end of the output, `NativeUtil` releases the unused structs. Otherwise `NativeUtil.importVector` imports
   each column with `ArrowImporter.importVector`, wraps it with `CometVector.getVector`, and returns the vectors as
   a `ColumnarBatch`.

`ArrowImporter` (in `spark/src/main/java/org/apache/arrow/c/`) imports every column through one shared
`SchemaImporter`. Arrow Java's own `Data.importField` creates a new `SchemaImporter` for each field, and each one
numbers dictionaries from 0, so two dictionary-encoded columns would collide in the shared `CDataDictionaryProvider`.

### Offset Normalization

Arrow Java's C Data import ignores `ArrowArray.offset`
([apache/arrow-java#88](https://github.com/apache/arrow-java/issues/88)), so it reads an array exported with a
non-zero offset from the start of its buffers. arrow-rs folds a slice into the buffers for most types, so a sliced
`Int64Array`, `StringArray`, or `StructArray` exports offset 0. A sliced `BooleanArray` is the exception: it keeps
its bit offset in `ArrayData::offset`. So before exporting a column whose `offset()` is non-zero, `prepare_output`
`take`s it into a new array with offset 0 ([#2051](https://github.com/apache/datafusion-comet/issues/2051)).

The check looks at top-level columns only. A struct column has offset 0 even when its children are sliced, so a
sliced boolean nested in a struct still reaches the JVM with a non-zero offset, and the JVM misreads it. The JVM UDF
bridge (`JvmScalarUdfExpr` in `native/spark-expr/src/jvm_udf/mod.rs`) exports its argument arrays with no
normalization at all. Both gaps are tracked in [#6288](https://github.com/apache/datafusion-comet/issues/6288).

### Ownership and Lifecycle

Native allocates the data, and the JVM references it without copying:

- The `FFI_ArrowArray` that `move_to_spark` writes holds a reference to the column's native buffers, and its release
  callback drops that reference.
- On import, Arrow Java wraps each native buffer in an `ArrowBuf`. Closing the last `ArrowBuf` over an imported
  column runs its release callback, and native frees the buffers once no Rust reference to them remains.
- `CometExecIterator` closes the batch it returned when the consumer next calls `hasNext` or `next`, and on
  `close()`. A batch is therefore valid only until the consumer asks for the next one, and a consumer that keeps the
  data longer has to copy it.

By the time the JVM receives a batch, native has usually stopped reserving it in the memory pool, but it stays
resident until the JVM closes it. See [Crossing the FFI boundary](memory_management.md#crossing-the-ffi-boundary).

## Memory Ownership Rules

### JVM → Native

| Scenario  | Ownership   | Action Required                                                                                           |
| --------- | ----------- | --------------------------------------------------------------------------------------------------------- |
| All cases | Native owns | None; the C Stream transfers ownership by reference count. Dropping the reader releases the JVM-side data |

### Native → JVM

| Scenario  | Ownership                        | Action Required                                            |
| --------- | -------------------------------- | ---------------------------------------------------------- |
| All cases | Native allocates, JVM references | JVM must call `close()` to trigger native release callback |

## Further Reading

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow C Stream Interface Specification](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Arrow Java FFI Implementation](https://github.com/apache/arrow/tree/main/java/c)
- [Arrow Rust FFI Implementation](https://docs.rs/arrow/latest/arrow/ffi/)

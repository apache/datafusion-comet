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
   pair per batch).

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
│    ScanExec     │ ── owns an AlignedArrowStreamReader
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
to native `createPlan` (one slot per scan input; shuffle inputs pass a `CometShuffleBlockIterator` instead).

On the native side, `planner.rs` reads each stream's `memoryAddress` and takes ownership through
`AlignedArrowStreamReader::from_raw`, importing the schema once. `ScanExec::get_next_batch` then pulls each batch
through the stream's `get_next` callback. There is no per-batch JNI call and no per-column FFI export.

### Buffer Alignment (AlignedArrowStreamReader)

`AlignedArrowStreamReader` (in `execution/operators/aligned_stream_reader.rs`) wraps the imported stream and calls
`align_buffers` on every batch before constructing typed arrays. This works around the fact that Java's allocator
hands back `Decimal128` buffers at 8-byte (not 16-byte) alignment, which the stock `ArrowArrayStreamReader` rejects
([apache/arrow-rs#10028](https://github.com/apache/arrow-rs/issues/10028)). The fix
([apache/arrow-rs#10030](https://github.com/apache/arrow-rs/pull/10030)) makes import align internally and ships in
arrow 59.0.0; once Comet is on arrow >= 59 this reader can be dropped for the stock `ArrowArrayStreamReader`.

### Schema Reconciliation

`CometArrowStream.reconcileStreamSchema` advertises the stream's schema from the actual `CometVector` types in the
first batch rather than the consumer's Spark-declared types. Native `ScanExec` already casts its input to the
declared scan-input schema in `build_record_batch`, so the truthful first-batch schema lets that cast fire; if the
two differ, it logs one deduplicated warning naming the operator, column, and type drift.

### Memory Layout

When a batch is transferred from JVM to native:

```
JVM Heap:                           Native Memory:
┌──────────────────┐               ┌──────────────────┐
│ ColumnarBatch    │               │ FFI_ArrowArray   │
│ ┌──────────────┐ │               │ ┌──────────────┐ │
│ │ ArrowBuf     │─┼──────────────>│ │ buffers[0]   │ │
│ │ (off-heap)   │ │               │ │ (pointer)    │ │
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
`ScanExec` drops its `AlignedArrowStreamReader`, the stream's release callback fires synchronously back into the JVM
and closes the `ArrowReader` and its `VectorSchemaRoot`, releasing the off-heap buffers. Because native holds those
buffers until the reader drops, an operator that buffers many batches keeps the corresponding JVM-side data alive
until then.

## Native → JVM Data Flow (CometExecIterator)

### Architecture

When JVM needs results from native execution:

```
┌─────────────────┐
│ DataFusion Plan │
│   (native)      │
└────────┬────────┘
         │ produces RecordBatch
         ▼
┌─────────────────┐
│ CometExecIter   │
│  (Rust/native)  │
└────────┬────────┘
         │ Arrow FFI
         │ (transfers ArrowArray/ArrowSchema pointers)
         ▼
┌─────────────────┐
│ CometExecIter   │ ◄─── JNI call from Spark
│  (Scala side)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Actions  │
│  (collect, etc) │
└─────────────────┘
```

### FFI Transfer Process

The transfer happens in `CometExecIterator::getNextBatch()`:

```scala
// Scala side
def getNextBatch(): ColumnarBatch = {
  val batchHandle = Native.getNextBatch(nativeHandle)

  // Import from FFI structures
  val vectors = (0 until schema.length).map { i =>
    val array = Array.empty[Long](1)
    val schemaPtr = Array.empty[Long](1)

    // Get FFI pointers from native
    Native.exportVector(batchHandle, i, array, schemaPtr)

    // Import into Arrow Java
    Data.importVector(allocator, array(0), schemaPtr(0))
  }

  new ColumnarBatch(vectors.toArray, numRows)
}
```

```rust
// Native side (simplified)
#[no_mangle]
pub extern "system" fn Java_..._getNextBatch(
    env: JNIEnv,
    handle: jlong,
) -> jlong {
    let context = get_exec_context(handle)?;
    let batch = context.stream.next().await?;

    // Store batch and return handle
    let batch_handle = Box::into_raw(Box::new(batch)) as i64;
    batch_handle
}

#[no_mangle]
pub extern "system" fn Java_..._exportVector(
    env: JNIEnv,
    batch_handle: jlong,
    col_idx: jint,
    array_ptr: jlongArray,
    schema_ptr: jlongArray,
) {
    let batch = get_batch(batch_handle)?;
    let array = batch.column(col_idx);

    // Export to FFI structures
    let (array_ffi, schema_ffi) = to_ffi(array.to_data())?;

    // Write pointers back to JVM
    env.set_long_array_region(array_ptr, 0, &[array_ffi as i64])?;
    env.set_long_array_region(schema_ptr, 0, &[schema_ffi as i64])?;
}
```

### Wrapper Object Lifecycle (Native → JVM)

```
Time    Native Memory              JVM Heap              Off-heap/Native
────────────────────────────────────────────────────────────────────────
t0      RecordBatch produced       -                     Data in native
        in DataFusion

t1      FFI_ArrowArray created     -                     Data in native
        FFI_ArrowSchema created
        (native heap)

t2      Pointers exported to JVM   ArrowBuf created      Data in native
                                   (wraps native ptr)

t3      FFI structures kept alive  Spark processes       Data in native
        via batch handle           ColumnarBatch         ✓ Valid

t4      Batch handle released      ArrowBuf freed        Data freed
        Release callback runs      (triggers native      (via release
                                   release callback)     callback)
```

**Key Difference from JVM → Native**:

- Native code controls lifecycle through batch handle
- JVM creates `ArrowBuf` wrappers that point to native memory
- Release callback ensures proper cleanup when JVM is done
- No GC pressure issue because native allocator manages the data

### Release Callbacks

Critical for proper cleanup:

```rust
// Native release callback (simplified)
extern "C" fn release_batch(array: *mut FFI_ArrowArray) {
    if !array.is_null() {
        unsafe {
            // Free the data buffers
            for buffer in (*array).buffers {
                drop(Box::from_raw(buffer));
            }
            // Free the array structure itself
            drop(Box::from_raw(array));
        }
    }
}
```

When JVM is done with the data:

```java
// ArrowBuf.close() triggers the release callback
arrowBuf.close();  // → calls native release_batch()
```

## Memory Ownership Rules

### JVM → Native

| Scenario  | Ownership   | Action Required                                                                                                                              |
| --------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| All cases | Native owns | None; the C Stream transfers ownership by reference count (copy only to unpack dictionaries). Dropping the reader releases the JVM-side data |

### Native → JVM

| Scenario  | Ownership                        | Action Required                                            |
| --------- | -------------------------------- | ---------------------------------------------------------- |
| All cases | Native allocates, JVM references | JVM must call `close()` to trigger native release callback |

## Further Reading

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow C Stream Interface Specification](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Arrow Java FFI Implementation](https://github.com/apache/arrow/tree/main/java/c)
- [Arrow Rust FFI Implementation](https://docs.rs/arrow/latest/arrow/ffi/)

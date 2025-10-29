# Arrow FFI Usage in Comet

This document explains how Comet uses the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI) to transfer data between the 
JVM (Spark) and native code (DataFusion/Rust), and the memory management considerations involved.

The following diagram shows an example of the end-to-end flow for a query stage.

![Diagram of Comet Data Flow](/_static/images/comet-dataflow.svg)

## Overview

Comet uses Arrow FFI for zero-copy data transfer in two directions:

1. **JVM → Native**: Native code pulls batches from JVM using `CometBatchIterator`
2. **Native → JVM**: JVM pulls batches from native code using `CometExecIterator`

Both scenarios use the same FFI mechanism but have different ownership semantics and memory management implications.

## Arrow FFI Basics

The Arrow C Data Interface defines two C structures:
- `ArrowArray`: Contains pointers to data buffers and metadata
- `ArrowSchema`: Contains type information

### Key Characteristics
- **Zero-copy**: Data buffers can be shared across language boundaries without copying
- **Ownership transfer**: Clear semantics for who owns and must free the data
- **Release callbacks**: Custom cleanup functions for proper resource management

## JVM → Native Data Flow (ScanExec)

### Architecture

When native code needs data from the JVM, it uses `ScanExec` which calls into `CometBatchIterator`:

```
┌─────────────────┐
│  Spark/Scala    │
│ CometExecIter   │
└────────┬────────┘
         │ produces batches
         ▼
┌─────────────────┐
│ CometBatchIter  │ ◄─── JNI call from native
│  (JVM side)     │
└────────┬────────┘
         │ Arrow FFI
         │ (transfers ArrowArray/ArrowSchema pointers)
         ▼
┌─────────────────┐
│    ScanExec     │
│  (Rust/native)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DataFusion    │
│   operators     │
└─────────────────┘
```

### FFI Transfer Process

The data transfer happens in `ScanExec::get_next()`:

```rust
// 1. Allocate FFI structures on native side (Rust heap)
for _ in 0..num_cols {
    let arrow_array = Rc::new(FFI_ArrowArray::empty());
    let arrow_schema = Rc::new(FFI_ArrowSchema::empty());
    let array_ptr = Rc::into_raw(arrow_array) as i64;
    let schema_ptr = Rc::into_raw(arrow_schema) as i64;
    // Store pointers...
}

// 2. Call JVM to populate FFI structures
let num_rows: i32 = unsafe {
    jni_call!(env, comet_batch_iterator(iter).next(array_obj, schema_obj) -> i32)?
};

// 3. Import data from FFI structures
for i in 0..num_cols {
    let array_data = ArrayData::from_spark((array_ptr, schema_ptr))?;
    let array = make_array(array_data);
    // ... process array
}
```

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

**Key Point**: The actual data buffers can be off-heap, but the `ArrowArray` and `ArrowSchema` wrapper objects are **always allocated on the JVM heap**.

### Wrapper Object Lifecycle

#### When `arrow_ffi_safe=false`

When ownership is NOT transferred to native:

```
Time    JVM Heap                    Native               Off-heap
─────────────────────────────────────────────────────────────────
t0      ArrowArray created          -                    -
        ArrowSchema created

t1      Wrappers populated          FFI pointers         Data exists
                                    received

t2      -                           ArrayData created    Data exists
                                    (references JVM
                                    wrappers)

t3      Wrappers still alive        Native operator      Data exists
        (may be reused!)            holds ArrayRef
                                    ⚠️ DANGER ZONE

t4      Wrappers reused for         Native still has     Data
        next batch                  reference to old     OVERWRITTEN
        ⚠️ DATA CORRUPTION          wrappers             ⚠️
```

**Problem**: Native code holds references to JVM wrapper objects that may be reused or garbage collected, causing:
- Data corruption if wrappers are reused
- Use-after-free if wrappers are GC'd
- Crashes and undefined behavior

**Solution**: Deep copy all arrays immediately after import (see `copy_array()` in scan.rs).

#### When `arrow_ffi_safe=true`

When ownership IS transferred to native:

```
Time    JVM Heap                    Native               FFI Release Callback
─────────────────────────────────────────────────────────────────────────────
t0      ColumnarBatch created       -                    -
        ArrowBuf objects

t1      Batch exported via FFI      FFI pointers         Callback registered
        Ownership transferred       received             in ArrayData
        ⚠️ Still alive

t2      -                           ArrayData created    Callback attached
                                    from FFI import      to ArrayData

t3      -                           Arc::clone for       Batch still alive
                                    non-dict arrays      ⚠️ NOT GC-able yet

t4      -                           Operators buffer     Batch still alive
                                    arrays (SortExec)    ⚠️ Accumulating!

t5      -                           Last Arc dropped     Callback FIRES
                                                         ↓
        Batch NOW GC-able           -                    JVM notified
        ✓ Finally
```

**Problem**: Even with ownership transfer, JVM batches remain alive until **all native references are dropped**. This is because:
1. The FFI release callback is copied into the `ArrayData` during import
2. The callback only fires when the last `ArrayData` reference is dropped
3. Buffering operators keep references alive for extended periods

**This causes GC pressure** when operators such as sorts and joins accumulate batches.

Even with off-heap data buffers, each batch creates small wrapper objects on the JVM heap:

```
Per batch overhead on JVM heap:
- ArrowArray object: ~100 bytes
- ArrowSchema object: ~100 bytes
- Per column: ~200 bytes
- 100 columns × 1000 batches = ~20 MB of wrapper objects
```

**Solution**: Deep copy all arrays immediately after import (see `copy_array()` in scan.rs).

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

| Scenario | `arrow_ffi_safe` | Ownership | Action Required |
|----------|------------------|-----------|-----------------|
| Temporary scan | `false` | JVM keeps | **Must deep copy** to avoid corruption |
| Ownership transfer | `true` | Native owns | Copy only to unpack dictionaries |

### Native → JVM

| Scenario | Ownership | Action Required |
|----------|-----------|-----------------|
| All cases | Native allocates, JVM references | JVM must call `close()` to trigger native release callback |

## Further Reading

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow Java FFI Implementation](https://github.com/apache/arrow/tree/main/java/c)
- [Arrow Rust FFI Implementation](https://docs.rs/arrow/latest/arrow/ffi/)

# Arrow FFI Usage in Comet

This document explains how Comet uses the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI) to transfer data between the 
JVM (Spark) and native code (DataFusion/Rust), and the memory management considerations involved.

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
┌──────────────────┐ <────────────────────┘
│ Actual Data      │
│ (e.g., int32[])  │
└──────────────────┘
```

**Key Point**: The actual data buffers can be off-heap, but the `ArrowArray` and `ArrowSchema` wrapper objects are **always allocated on the JVM heap**.

### Wrapper Object Lifecycle

#### Without `arrow_ffi_safe=true`

When ownership is NOT transferred to native (`arrow_ffi_safe=false`):

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

**Solution**: Deep copy all arrays immediately after import (see `copy_array()` in scan.rs:669-698).

#### With `arrow_ffi_safe=true`

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

**Critical Point**: Even with ownership transfer, JVM batches remain alive until **all native references are dropped**. This is because:
1. The FFI release callback is copied into the `ArrayData` during import
2. The callback only fires when the last `ArrayData` reference is dropped
3. Buffering operators keep references alive for extended periods

**This still causes GC pressure** when operators accumulate batches, which is why the PR performs deep copies even with `arrow_ffi_safe=true` - it allows immediate GC of JVM batches after the copy is made.

### The GC Pressure Problem

Even with off-heap data buffers, each batch creates small wrapper objects on the JVM heap:

```
Per batch overhead on JVM heap:
- ArrowArray object: ~100 bytes
- ArrowSchema object: ~100 bytes
- Per column: ~200 bytes
- 100 columns × 1000 batches = ~20 MB of wrapper objects
```

When DataFusion operators buffer entire inputs (e.g., `SortExec`):

```rust
// This pattern accumulates ALL wrapper objects
while let Some(batch) = input.next().await {
    sorter.insert_batch(batch).await?;  // Keeps wrapper references alive
}
sorter.sort().await  // Only then are wrappers released
```

**Impact**:
- With reduced heap size (common when using off-heap memory)
- Thousands of small wrapper objects accumulate
- Triggers frequent GC cycles
- Can cause **10x performance degradation** on large datasets

**Solution**: Deep copy arrays in `ScanExec` before passing to operators:

```rust
let array = if arrow_ffi_safe {
    // Unpack dictionaries but avoid full copy if possible
    copy_or_unpack_array(&array, &CopyMode::UnpackOrClone)?
} else {
    // Full deep copy to avoid JVM wrapper reuse issues
    copy_array(&array)
};
```

This seems paradoxical (copying to improve performance), but:
- Copy happens once per batch during scan
- Wrapper objects are immediately GC-eligible
- Cleaner memory lifecycle prevents GC thrashing
- Net result: Better performance despite copy overhead

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

## Best Practices

### For Scan Operations (JVM → Native)

1. **Always deep copy arrays** to enable immediate JVM GC, even with `arrow_ffi_safe=true`
   - Why? Buffering operators keep native references alive, preventing JVM GC
   - The upfront copy cost is cheaper than GC thrashing
2. **Unpack dictionary arrays** before passing to operators
   - Some DataFusion operators don't support dictionary encoding
3. **Copy dictionaries' value arrays** to avoid shared buffer references
4. See implementation in `native/core/src/execution/operators/scan.rs:267-275`

**Important**: Even with ownership transfer (`arrow_ffi_safe=true`), JVM batches can only be GC'd after the FFI release callback fires, which happens when all native references are dropped. For buffering operators, this means batches accumulate in JVM heap until native execution completes.

### For Result Export (Native → JVM)

1. **Use batch handles** to control lifecycle
2. **Implement proper release callbacks** for all allocations
3. **Ensure JVM calls close()** on all ArrowBuf objects
4. See implementation in `native/core/src/execution/jni_api.rs`

### For Operators That Buffer Data

When implementing operators that accumulate batches (Sort, Hash Join, etc.):

1. **Wrap input in CopyExec** to ensure clean ownership
2. **Prefer streaming over accumulation** when possible
3. **Test with large datasets** to catch GC pressure issues
4. See usage in `native/core/src/execution/planner.rs:1228,1503,1570`

## Debugging FFI Issues

### Common Symptoms

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Segfault in native code | Use-after-free from JVM GC | Enable `arrow_ffi_safe` or add deep copy |
| Data corruption | JVM wrapper reuse | Add deep copy in scan |
| Slow performance with GC | Wrapper object accumulation | Add deep copy to allow immediate GC |
| Memory leak (JVM) | Missing `close()` call | Add proper resource cleanup |
| Memory leak (native) | Missing release callback | Implement proper FFI release function |

### Debug Flags

```properties
# Enable FFI ownership transfer (requires Spark support)
spark.comet.exec.arrowFfiSafe=true

# Monitor GC behavior
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps

# Native memory tracking
-XX:NativeMemoryTracking=detail
```

### Profiling

```bash
# Profile native memory usage
MALLOC_CONF="prof:true,prof_prefix:jeprof.out" ./spark-submit ...

# Profile JVM heap
jmap -dump:live,format=b,file=heap.bin <pid>
jhat heap.bin
```

## Configuration

### Comet Configuration

```scala
// Enable FFI ownership transfer (if Spark supports it)
.config("spark.comet.exec.arrowFfiSafe", "true")

// Increase off-heap memory to reduce heap pressure
.config("spark.memory.offHeap.enabled", "true")
.config("spark.memory.offHeap.size", "16g")

// Reduce heap size when using off-heap
.config("spark.executor.memory", "4g")
```

## Further Reading

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow Java FFI Implementation](https://github.com/apache/arrow/tree/main/java/c)
- [Arrow Rust FFI Implementation](https://docs.rs/arrow/latest/arrow/ffi/)
- [Comet Issue #963: Dictionary Array Unpacking](https://github.com/apache/datafusion-comet/issues/963)

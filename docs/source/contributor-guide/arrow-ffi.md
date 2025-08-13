# Arrow FFI Usage

## Overview

This document provides an overview of Comet's usage of Arrow FFI (Foreign Function Interface) to pass Arrow arrays
between the JVM (Java/Scala) and native (Rust) code.

## Architecture Overview

Arrow FFI is used extensively in Comet to transfer columnar data across language boundaries. The main components 
involved are:

1. **JVM Side**: Scala/Java code that manages Arrow arrays and vectors
2. **Native Side**: Rust code that processes data using DataFusion
3. **Arrow C Data Interface**: Standard FFI structures (`FFI_ArrowArray` and `FFI_ArrowSchema`)

## Key FFI Usage Patterns

### 1. JVM to Native (Data Import)

**Location**: `common/src/main/scala/org/apache/comet/vector/NativeUtil.scala`

The JVM exports Arrow data to native code through the `exportBatch` method. This code is called via JNI from 
native code.

```scala
def exportBatch(arrayAddrs: Array[Long], schemaAddrs: Array[Long], batch: ColumnarBatch): Int = {
  (0 until batch.numCols()).foreach { index =>
    val arrowSchema = ArrowSchema.wrap(schemaAddrs(index))
    val arrowArray = ArrowArray.wrap(arrayAddrs(index))
    Data.exportVector(allocator, getFieldVector(valueVector, "export"), provider, arrowArray, arrowSchema)
  }
}
```

**Memory Management**:

- `ArrowArray` and `ArrowSchema` structures are allocated by native side
- JVM uses `ArrowSchema.wrap()` and `ArrowArray.wrap()` to wrap existing pointers
- No deallocation needed on JVM side as structures are owned by native code

### 2. Native to JVM (Data Export)

**Location**: `native/core/src/execution/jni_api.rs`

Native code exports data back to JVM through the `prepare_output` function:

```rust
fn prepare_output(env: &mut JNIEnv, array_addrs: jlongArray, schema_addrs: jlongArray,
                  output_batch: RecordBatch, validate: bool) -> CometResult<jlong> {
    // Get memory addresses from JVM
    let array_addrs = unsafe { env.get_array_elements(&array_address_array, ReleaseMode::NoCopyBack)? };
    let schema_addrs = unsafe { env.get_array_elements(&schema_address_array, ReleaseMode::NoCopyBack)? };

    // Export each column
    array_ref.to_data().move_to_spark(array_addrs[i], schema_addrs[i])?;
}
```

### 3. FFI Conversion Implementation

**Location**: `native/core/src/execution/utils.rs`

The core FFI conversion logic implements the `SparkArrowConvert` trait:

```rust
impl SparkArrowConvert for ArrayData {
    fn from_spark(addresses: (i64, i64)) -> Result<Self, ExecutionError> {
        let (array_ptr, schema_ptr) = addresses;
        let array_ptr = array_ptr as *mut FFI_ArrowArray;
        let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;

        let mut ffi_array = unsafe {
            let array_data = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
            let schema_data = std::ptr::replace(schema_ptr, FFI_ArrowSchema::empty());
            from_ffi(array_data, &schema_data)?
        };

        ffi_array.align_buffers();
        Ok(ffi_array)
    }

    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError> {
        let array_ptr = array as *mut FFI_ArrowArray;
        let schema_ptr = schema as *mut FFI_ArrowSchema;

        unsafe {
            std::ptr::write(array_ptr, FFI_ArrowArray::new(self));
            std::ptr::write(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
        }
        Ok(())
    }
}
```

## Memory Lifecycle Management

### 1. Memory Allocation Strategy

**ArrowArray and ArrowSchema Structures**:

- **JVM Side**: Uses `ArrowArray.allocateNew(allocator)` and `ArrowSchema.allocateNew(allocator)` in `NativeUtil.allocateArrowStructs()`
- **Native Side**: Creates empty structures with `FFI_ArrowArray::empty()` and `FFI_ArrowSchema::empty()`

**Buffer Memory**:

- Arrow buffers are managed by Arrow's memory pool system
- Reference counting ensures proper cleanup
- Buffers can be shared between arrays through Arc<> wrappers

### 2. Ownership Transfer Patterns

**JVM to Native Transfer**:

1. JVM allocates ArrowArray/ArrowSchema structures
2. JVM exports data using Arrow C Data Interface
3. Native code imports using `from_ffi()` which transfers ownership
4. Native code processes data and may modify it
5. JVM structures remain allocated until explicitly freed

**Native to JVM Transfer**:

1. Native code writes data to JVM-allocated structures using `move_to_spark()`
2. JVM wraps the structures and creates CometVectors
3. JVM takes ownership of the data buffers
4. Native structures can be safely dropped

### 3. Memory Cleanup

**JVM Cleanup**:

- `NativeUtil.close()` closes dictionary provider which releases dictionary arrays
- Individual batches are closed via `ColumnarBatch.close()` in `CometExecIterator`
- Arrow allocator tracks memory usage but reports false positives due to FFI transfers

**Native Cleanup**:

- Rust's RAII automatically drops structures when they go out of scope
- `std::ptr::replace()` with empty structures ensures proper cleanup
- Explicit `Rc::from_raw()` calls in scan operations to avoid memory leaks

## Memory Safety Risks

**Location**: `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`

```scala
// Close previous batch if any.
// This is to guarantee safety at the native side before we overwrite the buffer memory
// shared across batches in the native side.
if (prevBatch != null) {
  prevBatch.close()
  prevBatch = null
}
```

**Risk**: The comment explicitly mentions "shared buffer memory across batches" but there's a window where:

1. Native code might still have references to a batch
2. JVM closes the previous batch, potentially freeing buffers
3. Native code accesses freed memory

**Mitigation**: In `planner.rs` we insert `CopyExec` operators to perform copies of arrays for operators
that may cache batches, but this is an area that we may improve in the future.

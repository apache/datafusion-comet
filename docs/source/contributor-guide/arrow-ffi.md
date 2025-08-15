# Comet's use of Arrow FFI

Due to the hybrid execution model, it is necessary to pass batches of data between the JVM and native code.

The foundation for Arrow FFI is the [Arrow C Data Interface], which provides a stable ABI-compatible interface for
accessing Arrow data structures from multiple languages.

[Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html

## Array Ownership and Lifecycle

Comet does not track ownership of Arrow arrays or buffers across language boundaries.

Arrays created on the JVM side are owned and managed in JVM code.

Arrays created on the native side are owned and managed in native code.

# JVM getting batches from native side

- `CometExecIterator` invokes native plans and uses Arrow FFI to read the output batches

- native side owns the memory
- native exports the arrays in prepare_output in `executePlan` and `decodeShuffleBatches`
- native side must not hold references to the arrays once they are exported to JVM

# Native getting batches from JVM

- Native `ScanExec` operators call `CometBatchIterator` via JNI to fetch input batches from the JVM
- ScanExec calls `CometBatchIterator.next` 
- `CometBatchIterator.next` will close previous batches on the JVM side. 
- Native side needs to do a deep copy in ScanExec if the batches will be buffered (such as with shuffle writer)

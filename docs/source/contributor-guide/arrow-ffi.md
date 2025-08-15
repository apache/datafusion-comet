# Comet's use of Arrow FFI

TODO this is just a quick brain dump for now - will write up and add diagrams

# JVM getting batches from native side

- native side owns the memory
- native exports the arrays in prepare_output in executePlan and decodeShuffleBatches
- native side must not hold references to the arrays once they are exported

# Native getting batches from JVM

- ScanExec calls CometBatchIterator.next 
- CometBatchIterator.next will close previous batches on the JVM side. 
- Native side needs to do a deep copy in ScanExec if the batches will be buffered (such as with shuffle writer)

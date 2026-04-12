# Batch Stash: Avoid FFI Import/Export Between Native Plans

**Issue**: https://github.com/apache/datafusion-comet/issues/3925
**Date**: 2026-04-12

## Problem

When Comet has a native ShuffleWriter and a native child plan, batches are created in native code,
exported to JVM via Arrow FFI, then immediately imported back to native for the shuffle writer. The
JVM never reads the data. This round-trip is unnecessary overhead.

### Current flow (per batch)

```
Native child plan produces RecordBatch
  -> prepare_output(): FFI export each column via move_to_spark()     [native -> JVM]
  -> NativeUtil.getNextBatch(): import into CometVector/ColumnarBatch [JVM]
  -> CometBatchIterator.next(): exportBatch() via Data.exportVector() [JVM -> native]
  -> ScanExec.get_next(): import via ArrayData::from_spark() + copy   [native]
Shuffle writer processes RecordBatch
```

Each batch crosses the JNI/FFI boundary 4 times and gets copied at least twice.

## Solution

Pass batches as opaque handles through the JVM instead of doing Arrow FFI export/import.

### New flow (per batch)

```
Native child plan produces RecordBatch
  -> stash RecordBatch in global registry, return handle (u64)  [native]
  -> JVM receives handle as jlong                               [JVM]
  -> JVM passes handle to shuffle writer's ScanExec             [JVM -> native]
  -> ScanExec retrieves RecordBatch from registry               [native]
Shuffle writer processes RecordBatch
```

Two lightweight JNI calls passing a single long value. No Arrow FFI, no data copying.

## Design

### Approach

Keep the current two-separate-native-plans architecture (one for the child plan, one for the
shuffle writer). Add a "batch stash" registry on the native side. When the child plan produces a
batch, stash it and return a handle. The shuffle writer's ScanExec retrieves the batch using the
handle.

Detection is automatic. No new config flags.

### Component 1: BatchStash (Rust)

New file: `native/core/src/execution/batch_stash.rs`

A global thread-safe registry mapping u64 handles to RecordBatch values.

- Uses `Mutex<HashMap<u64, RecordBatch>>` (simple and sufficient since contention is minimal:
  each task thread produces and consumes one batch at a time)
- `AtomicU64` counter for generating unique handles
- `stash(batch: RecordBatch) -> u64`: inserts and returns handle
- `take(handle: u64) -> Option<RecordBatch>`: removes and returns (consumed exactly once)
- `clear_for_context(id: i64)`: cleanup method for error/abort paths (not strictly needed since
  batches are consumed one-at-a-time, but provides safety)

Memory: at most one batch is stashed at a time per task (produce one, consume one). Memory
footprint matches the current approach.

### Component 2: executePlanBatchHandle (JNI)

Add a new JNI function to `Native.java` / `jni_api.rs`:

```java
// Native.java
native long executePlanBatchHandle(int stageId, int partition, long plan);
```

The Rust implementation is nearly identical to `Java_org_apache_comet_Native_executePlan` but
replaces the `prepare_output()` call (which does FFI export) with `batch_stash::stash(batch)`.
Returns the handle (positive u64 cast to jlong) or -1 for EOF.

Does not take `array_addrs` or `schema_addrs` parameters since no FFI export occurs.

### Component 3: CometExecIterator Stash Mode

Add a `stashMode` flag to `CometExecIterator`. When enabled:

- `getNextBatch` calls `nativeLib.executePlanBatchHandle()` instead of `executePlan()`
- Stores the handle internally instead of importing Arrow arrays into a ColumnarBatch
- Exposes `def nextHandle(): Long` that returns the handle or -1 for EOF

The existing `Iterator[ColumnarBatch]` interface stays unchanged for the non-stash path. The stash
path is used only by the shuffle writer, which calls `nextHandle()` directly.

`enableStashMode()` is called by `CometNativeShuffleWriter` after detecting that its input comes
from a CometExecIterator.

### Component 4: CometHandleBatchIterator (Java)

New file: `spark/src/main/java/org/apache/comet/CometHandleBatchIterator.java`

Replaces `CometBatchIterator` for the stash path. Called from native ScanExec via JNI:

```java
public class CometHandleBatchIterator {
    private final CometExecIterator source;

    public CometHandleBatchIterator(CometExecIterator source) {
        this.source = source;
    }

    // Called by native ScanExec via JNI
    // Returns batch handle or -1 for EOF
    public long nextHandle() {
        return source.nextHandle();
    }
}
```

The native JNI bridge gets a corresponding struct with a `method_next_handle` JMethodID.

### Component 5: ScanExec Handle Path

Modify `ScanExec.get_next()` to check whether its input source is a `CometHandleBatchIterator`.
If so:

1. Call `nextHandle()` via JNI to get a handle
2. Call `batch_stash::take(handle)` to retrieve the RecordBatch
3. Wrap it as an InputBatch directly (no FFI import, no copy, no dictionary unpacking)

Detection: during `createPlan` in `PhysicalPlanner`, when the planner encounters a Scan whose
source is `"ShuffleWriterInput"` and whose input object is a `CometHandleBatchIterator`, it sets a
flag on the ScanExec indicating handle mode.

### Component 6: Detection Mechanism

The challenge: `CometNativeShuffleWriter.write()` receives `Iterator[Product2[K, V]]` from Spark's
shuffle framework. The original `CometExecIterator` is wrapped in `rdd.map((0, _))` which creates
a MappedIterator, losing the type.

Solution: preserve the CometExecIterator reference through the RDD.

New class `CometShuffleWriterInputIterator`:

```scala
class CometShuffleWriterInputIterator(
    underlying: Iterator[ColumnarBatch],
    val nativeIterator: Option[CometExecIterator]
) extends Iterator[Product2[Int, ColumnarBatch]] {
  def hasNext: Boolean = underlying.hasNext
  def next(): Product2[Int, ColumnarBatch] = (0, underlying.next())
}
```

In `CometShuffleExchangeExec.prepareShuffleDependency`, replace `rdd.map((0, _))` with:

```scala
val wrappedRDD = rdd.mapPartitions { iter =>
  val nativeIter = iter match {
    case cei: CometExecIterator => Some(cei)
    case _ => None
  }
  new CometShuffleWriterInputIterator(iter, nativeIter)
}
```

Spark's `ShuffleWriteProcessor.write()` passes `rdd.iterator(partition, context)` directly to
`ShuffleWriter.write()`, so the type is preserved.

In `CometNativeShuffleWriter.write()`:

```scala
val nativeIter: Option[CometExecIterator] = inputs match {
  case swi: CometShuffleWriterInputIterator => swi.nativeIterator
  case _ => None
}

if (nativeIter.isDefined) {
  // Stash mode: enable stash on child iterator, use handle-based input
  nativeIter.get.enableStashMode()
  val handleIter = new CometHandleBatchIterator(nativeIter.get)
  // Create shuffle writer's CometExecIterator with handleIter as input
} else {
  // Fallback: existing FFI path (unchanged)
}
```

### Component 7: CometNativeShuffleWriter Changes

When stash mode is detected, the shuffle writer creates its CometExecIterator differently:

- Pass `CometHandleBatchIterator` as the input iterator (instead of `CometBatchIterator`)
- The shuffle writer's native plan (Scan -> ShuffleWriter) still has the same structure
- The difference is that the Scan's input source is a `CometHandleBatchIterator` instead of
  `CometBatchIterator`, so ScanExec uses the handle path

The `getCometIterator` method (or a new overload) needs to accept handle-based input iterators.
CometExecIterator already takes `Array[Object]` as `inputIterators`, so we can pass the
`CometHandleBatchIterator` directly as an Object, bypassing the normal CometBatchIterator wrapping.

## Edge Cases

1. **Non-native child plan**: `nativeIterator` is `None`. Falls back to the existing FFI path.
   No behavior change.

2. **Error during shuffle write**: If the shuffle writer aborts, any stashed batch that was never
   consumed would leak. The `releasePlan` cleanup path should call `batch_stash::take()` for any
   outstanding handle to prevent this. In practice, at most one batch is stashed at a time, and it
   is consumed before the next is produced.

3. **Empty batches / EOF**: `executePlanBatchHandle` returns -1 for EOF, same convention as
   `executePlan`. Zero-row batches get stashed normally.

4. **Multiple scans in shuffle writer plan**: The shuffle writer's native plan has exactly one
   Scan child (`ShuffleWriterInput`), so there is always exactly one input iterator.

5. **Memory pressure**: Batches are consumed one at a time (produce one, consume one), so at most
   one batch is in the stash at a time per task. Memory footprint matches the current approach.

6. **RangePartitioning sampling**: For RangePartitioning, `prepareShuffleDependency` creates a
   separate sampling RDD that calls `batch.rowIterator()`. This happens before the shuffle write,
   on a separate job. The sampling path does not go through `CometShuffleWriterInputIterator`.
   No conflict.

## Files Changed

| File | Change |
|------|--------|
| `native/core/src/execution/batch_stash.rs` | **New**: BatchStash registry |
| `native/core/src/execution/mod.rs` | Export batch_stash module |
| `native/core/src/execution/jni_api.rs` | Add `executePlanBatchHandle` JNI function |
| `native/core/src/execution/operators/scan.rs` | Handle-based input path in ScanExec |
| `native/jni-bridge/src/batch_iterator.rs` | Add CometHandleBatchIterator JNI bridge struct |
| `spark/src/main/java/org/apache/comet/Native.java` | Add `executePlanBatchHandle` native method |
| `spark/src/main/java/org/apache/comet/CometHandleBatchIterator.java` | **New**: handle passthrough iterator |
| `spark/src/main/scala/org/apache/comet/CometExecIterator.scala` | Add stash mode + `nextHandle()` |
| `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala` | Use CometShuffleWriterInputIterator in prepareShuffleDependency |
| `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala` | Detect native input, use stash mode |

## Testing

1. **Rust unit test**: Test BatchStash -- stash, take, verify identity. Verify take returns None
   for unknown handles. Verify take removes the entry.

2. **JVM integration test**: Run a query that triggers CometNativeShuffle with a native child plan
   (e.g., `SELECT * FROM t ORDER BY col`). Verify results match Spark. Verify the stash path was
   taken (via metric or log).

3. **Fallback test**: Run a query where the child is not CometNativeExec. Verify the FFI path
   still works.

4. **Regression**: Existing shuffle test suites (`CometShuffleSuite`, etc.) pass without
   modification.

# Comet Off-Heap Memory Analysis: Why Comet OOMs at TPC-DS 1TB

## Problem

Comet OOMs running TPC-DS at 1TB scale unless executor offHeap memory is >= 32GB.
Gluten (Velox backend) handles the same workload with ~2GB offHeap.

## Root Cause: Untracked Memory Bypassing the Memory Pool

With default settings, Spark uses SortMergeJoin (not HashJoin). Comet's hash join
replacement (`spark.comet.exec.replaceSortMergeJoin`) is experimental and disabled
by default. The operators in play — Sort, Aggregate, SortMergeJoin, Shuffle — all
have DataFusion-internal spill support that triggers on `try_grow()` failure.

**The real problem is not missing spill support.** It's that the majority of memory
allocations bypass the pool entirely, making the pool's backpressure mechanism useless.

### Untracked Memory Sources

#### 1. Input Batches from JVM (scan.rs:136-260) — CRITICAL

When `ScanExec` pulls Arrow batches from JVM via FFI, the resulting arrays are stored
in `self.batch: Arc<Mutex<Option<InputBatch>>>` but **never registered with a
MemoryReservation**. At 1TB scale, this is the bulk of data flowing through the system.

```
JVM → FFI_ArrowArray → ArrayData::from_spark() → make_array() → Vec<ArrayRef>
                                                                  ^^^ untracked
```

#### 2. FFI Array Deep Copies (copy.rs:34-93)

`copy_array()` and `copy_or_unpack_array()` make deep copies without tracking:

```rust
pub fn copy_array(array: &dyn Array) -> ArrayRef {
    let mut mutable = MutableArrayData::new(vec![&data], false, capacity);
    mutable.extend(0, 0, capacity);  // allocation not tracked
    make_array(mutable.freeze())
}
```

When `arrow_ffi_safe=false`, each incoming batch is duplicated with zero accounting.

#### 3. Selection Vector Processing (scan.rs:309-370)

Selection vector filtering creates additional Arrow arrays via FFI + `take()` that
are never tracked.

#### 4. Shuffle Decompression (shuffle_scan.rs:136-201)

`read_ipc_compressed()` decompresses full batches without pool tracking.

#### 5. Window Function Partitions (planner.rs — BoundedWindowAggExec)

`BoundedWindowAggExec` buffers entire partitions in memory. Not spillable.

### Why This Causes OOM

The memory pool's backpressure mechanism works correctly: when `acquire_from_spark()`
returns less than requested (`unified_pool.rs:121-151`), it returns
`ResourcesExhausted`, and Sort/Aggregate catch this and spill.

But the pool doesn't know about ~80% of actual memory usage. Untracked scan batches,
copies, and decompression buffers consume physical memory invisibly. The pool grants
allocations to Sort/Aggregate while the process is already near OOM from untracked data.

This explains the 32GB requirement: you need enough headroom for all untracked memory
on top of the tracked pool.

### Spark spill() Callback is Also a No-Op

`spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java:99-113`:

```java
private class NativeMemoryConsumer extends MemoryConsumer {
    public long spill(long size, MemoryConsumer trigger) {
        return 0; // No spilling
    }
}
```

Even if memory were tracked, Spark can't reclaim it. But fixing the spill callback
alone wouldn't help without fixing the tracking gaps first.

### Per-Operator Status (Default Config)

| Operator | Spillable? | Memory Tracked? | Notes |
|----------|-----------|----------------|-------|
| SortExec | Yes (DF internal) | Yes | Spills on try_grow() failure |
| AggregateExec | Yes (DF internal) | Yes | Spills on try_grow() failure |
| SortMergeJoinExec | Streams | Yes | Low memory — relies on sorted inputs |
| Shuffle writer | Yes | Yes | Only operator with `with_can_spill(true)` |
| ScanExec (FFI batches) | No | **No** | Primary untracked memory source |
| copy_array / unpack | No | **No** | Doubles batch memory untracked |
| ShuffleScanExec | No | **No** | Decompression buffers untracked |
| BoundedWindowAggExec | No | Partial | Entire partitions buffered |
| BroadcastHashJoin | N/A | No (JVM side) | Build side in JVM heap |

## Comparison with Gluten

| Capability | Comet | Gluten |
|-----------|-------|--------|
| Spark spill callback | Returns 0 (no-op) | Hierarchical spill cascade via TreeMemoryConsumer |
| Hash join spill | None | Velox spills hash tables to disk |
| Per-operator tracking | Single NativeMemoryConsumer | Per-operator MemoryTarget children |
| Retry on OOM | None | Multi-retry with exponential backoff + GC |
| Memory isolation | Proportional per-task limit | Hard per-task cap option |

### Gluten's TreeMemoryConsumer Architecture

```
TreeMemoryConsumer (registered as Spark MemoryConsumer)
  +-- Child: HashJoin (with Spiller)
  +-- Child: HashAggregate (with Spiller)
  +-- Child: Shuffle (with Spiller)
  +-- Child: Sort (with Spiller)
```

When Spark calls `spill()`, Gluten:
1. Walks children sorted by usage (largest first)
2. Calls SHRINK phase (reduce internal buffers)
3. Calls SPILL phase (write to disk)
4. Returns actual bytes freed

### Gluten's Retry-on-OOM

`RetryOnOomMemoryTarget` catches allocation failures, triggers a spill cascade
across all operators in the task, then retries. `ThrowOnOomMemoryTarget` wraps
everything with up to 9 retries with exponential backoff.

## Debugging Steps

### 1. Enable memory debug logging
```
spark.comet.debug.memory.enabled=true
```

### 2. Run targeted TPC-DS queries individually
- Join-heavy (likely OOM first): q5, q14a, q14b, q23a, q23b, q24a, q24b, q72, q95
- Sort/aggregate-heavy: q1, q4, q11, q74

### 3. Disable operator categories to isolate
```
spark.comet.exec.hashJoin.enabled=false
spark.comet.exec.sortMergeJoin.enabled=false
# or
spark.comet.exec.shuffle.enabled=false
```

### 4. Check TrackConsumersPool output on OOM
The error message should list top 10 memory consumers by usage.

---

## Solution Design

### The Real Fix: Track All Memory Through the Pool

DataFusion's spill model already works — Sort and Aggregate react to
`ResourcesExhausted` by spilling. The fix is to make the pool aware of all
memory so backpressure triggers at the right time.

### Priority 1: Track ScanExec Input Batches (Highest Impact)

**Location:** `native/core/src/execution/operators/scan.rs`

ScanExec should hold a `MemoryReservation` and call `try_grow()` for each batch
received from the JVM. If the pool denies the allocation, downstream operators
will already be under spill pressure.

```rust
// Proposed change in ScanExec
struct ScanExec {
    // ... existing fields ...
    reservation: MemoryReservation,  // NEW: track batch memory
}

impl ScanExec {
    fn get_next(&mut self) -> Result<InputBatch> {
        let batch = self.import_from_jvm()?;
        let batch_size = batch.get_array_memory_size();
        self.reservation.try_grow(batch_size)?;  // Account for it
        // ... store batch ...
    }
}
```

When the batch is consumed/dropped, `reservation.shrink()` releases the memory.

This single change would make the pool aware of the largest untracked memory
category and allow backpressure to trigger spill in downstream Sort/Aggregate.

### Priority 2: Track Array Copies (copy.rs)

`copy_array()` and `copy_or_unpack_array()` should accept a `&MemoryReservation`
and call `try_grow()` before allocating the copy. This requires threading the
reservation through from the calling ScanExec.

### Priority 3: Track Shuffle Decompression (shuffle_scan.rs)

`ShuffleScanExec` should hold a `MemoryReservation` and track decompressed batch
sizes, similar to the ScanExec fix.

### Priority 4: Implement spill() Callback

After memory tracking is in place, implement a real `spill()` in
`NativeMemoryConsumer` using the shrink-the-pool approach:

1. When Spark calls `spill(size)`, JNI into native to set a `spill_pressure` atomic
2. Pool's `try_grow()` checks `spill_pressure` and returns `ResourcesExhausted`
3. Sort/Aggregate operators react by spilling internally
4. As operators `shrink()` their reservations, decrement `spill_pressure`
5. Return actual bytes freed to Spark

This is secondary because fixing tracking (Priorities 1-3) means the pool will
naturally deny allocations when physical memory is scarce, triggering DataFusion's
existing spill without needing Spark coordination.

### Priority 5: Window Function Spill

`BoundedWindowAggExec` buffers entire partitions. For large partitions at 1TB,
this needs either:
- Use DataFusion's spillable window variant if available
- Fall back to Spark's window operator above a threshold

### Comparison with Gluten's Approach

Gluten solves this differently because Velox owns the entire execution pipeline
including scans. All memory flows through Velox's allocator, which calls back to
Java's `ReservationListener` on every allocation. The `TreeMemoryConsumer` hierarchy
then coordinates spill across operators.

Comet's challenge is the FFI boundary — Arrow arrays arrive from JVM as raw pointers
with no allocator integration. The arrays are allocated by the JVM's Arrow library,
and Comet's native side just maps them. Tracking must be explicitly added.

### Implementation Phases

**Phase 1: Measurement**
- Enable `spark.comet.debug.memory.enabled=true`
- Run TPC-DS 1TB, identify which queries OOM first
- Measure ratio of tracked vs untracked memory

**Phase 2: Track ScanExec batches (highest impact)**
- Add MemoryReservation to ScanExec
- Track batch sizes on import, release on consumption
- Re-test TPC-DS 1TB — this alone may dramatically reduce memory requirement

**Phase 3: Track remaining gaps**
- copy.rs array copies
- shuffle_scan.rs decompression buffers
- Selection vector processing

**Phase 4: Spill callback (if needed)**
- Implement shrink-the-pool mechanism in NativeMemoryConsumer
- Wire through JNI to native pool
- Allows Spark to proactively reclaim memory from native operators

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

# Memory Management

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

### Spark spill() Callback

`NativeMemoryConsumer.spill()` now forwards spill requests to the native memory pool
via JNI. When Spark calls `spill(size)`, it sets a `SpillState.pressure` atomic in the
native pool. The pool's `try_grow()` checks this and returns `ResourcesExhausted`,
causing DataFusion's Sort/Aggregate/Shuffle operators to spill internally. Freed bytes
are tracked via `shrink()` and returned to Spark.

See `SpillState` in `native/core/src/execution/memory_pools/spill.rs` and the
`requestSpill` JNI function in `native/core/src/execution/jni_api.rs`.

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
| Spark spill callback | Forwards to native via SpillState pressure | Hierarchical spill cascade via TreeMemoryConsumer |
| Hash join spill | None (experimental, disabled by default) | Velox spills hash tables to disk |
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

## Benchmark Results: TPC-H SF100, local[4], Peak RSS

Measured with `benchmarks/tpc/memory-profile.sh` using `/usr/bin/time -l` on macOS
(96GB RAM, 28 cores, Spark 3.5.8, no container memory limits).

| Config | Q1 (aggregation) | Q5 (5-way join) | Q9 (6-way join) |
|--------|-----------------|-----------------|-----------------|
| Spark 4g offHeap | 2700 MB | 5167 MB | 4580 MB |
| Comet 4g offHeap | 679 MB | 5534 MB | 5911 MB |
| Comet 8g offHeap | 665 MB | 5440 MB | 6359 MB |

### Analysis

**Aggregation (Q1)**: Comet uses 75% less memory than Spark. No memory concern.

**Join-heavy queries (Q5, Q9)**: Comet uses more memory than Spark.
- Q5: ~370 MB over Spark, flat across offHeap sizes (fixed overhead)
- Q9: 1331 MB over Spark at 4g, grows to 1779 MB at 8g (elastic — expands with pool)

**Q9's elastic growth** (450 MB increase from 4g→8g) points to operators that buffer
greedily up to the pool limit. The shuffle writer is designed this way — it accumulates
batches in `buffered_batches` until `try_grow()` fails, then spills
(`multi_partition.rs:395-435`). With more offHeap, it buffers more before spilling.

**Cross-task eviction**: With `spill()` returning 0, Spark cannot reclaim memory from
one Comet task to give to another. In a container with 16 tasks sharing 16GB, each task's
shuffle writer competes for the shared pool, and no task can force another to spill.

### Isolating Shuffle vs Non-Shuffle Overhead (Q9)

Running Q9 with `spark.comet.exec.shuffle.enabled=false` (Spark handles shuffles,
Comet handles sort/SMJ/aggregate):

| Config | Q9 Peak RSS |
|--------|-------------|
| Spark only | 4580 MB |
| Comet, shuffle disabled | 5035 MB |
| Comet, full (4g offHeap) | 5911 MB |
| Comet, full (8g offHeap) | 6359 MB |

- **Non-shuffle overhead** (sort + SMJ + agg + scan): +455 MB fixed
- **Shuffle overhead**: +876 MB at 4g, grows with offHeap size
- **Shuffle elastic growth** (4g→8g): +448 MB

Shuffle accounts for ~2/3 of the extra memory over Spark. This is also the component
that grows with offHeap size due to greedy buffering.

### New Shuffle Writer (PR #3845)

[PR #3845](https://github.com/apache/datafusion-comet/pull/3845) introduces an
"immediate mode" shuffle writer that buffers **compressed IPC-encoded blocks** per
partition instead of raw `RecordBatch` objects. Key differences:

- **Eager encoding**: Data is IPC-encoded and compressed as it arrives, per partition.
  Memory footprint is bounded by `num_partitions * batch_size`, independent of input size.
- **Current writer**: Buffers uncompressed `RecordBatch` objects in a `Vec`, with memory
  proportional to total input data. Delays all encoding until spill or final write.
- **Expected reduction**: 30-50% peak memory reduction in benchmarks.
- **Still greedy**: The new writer still buffers until `try_grow()` fails, but the
  compressed representation means each buffer unit is much smaller.

### Spill Callback Results

After implementing the spill callback (`SpillState` + `NativeMemoryConsumer.spill()`),
Q9 was re-benchmarked:

**local[4] (4 concurrent tasks):**

| Config | Q9 Before | Q9 After | Change |
|--------|-----------|----------|--------|
| Comet 4g | 5911 MB | 5896 MB | -15 MB |
| Comet 8g | 6359 MB | 6060 MB | -299 MB |
| 4g→8g delta | 448 MB | 164 MB | **-63% elastic growth** |

**local[8] (8 concurrent tasks):**

| Config | Q9 RSS |
|--------|--------|
| Spark 4g | 8476 MB |
| Comet 4g | 8525 MB |
| Comet 8g | 8448 MB |
| 4g→8g delta | -77 MB (within noise) |

With 8 concurrent tasks, Comet's memory usage is **on par with Spark** (8525 vs 8476 MB)
and the elastic growth is effectively eliminated. The spill callback enables Spark to
apply backpressure to Comet's native memory pool, preventing shuffle writers from
greedily expanding when more offHeap is available.

## Debugging Steps

### 1. Enable memory debug logging
```
spark.comet.debug.memory.enabled=true
```

### 2. Run memory profiling script
```bash
cd benchmarks/tpc
./memory-profile.sh --queries "1 5 9 21" --offheap-sizes "4g 8g 16g" --cores 4
```

### 3. Disable operator categories to isolate
```
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

### Priority 4: Implement spill() Callback — DONE

Implemented via `SpillState` + JNI `requestSpill`. The shrink-the-pool approach:

1. When Spark calls `spill(size)`, JNI into native to set `SpillState.pressure`
2. Pool's `try_grow()` checks pressure and returns `ResourcesExhausted`
3. Sort/Aggregate/Shuffle operators react by spilling internally
4. As operators call `shrink()`, freed bytes are tracked and reported back
5. Actual bytes freed returned to Spark

See `native/core/src/execution/memory_pools/spill.rs` and
`spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java`.

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

### Remaining Work

**Phase 1: Track ScanExec batches**
- Add MemoryReservation to ScanExec
- Track batch sizes on import, release on consumption

**Phase 2: Track remaining gaps**
- copy.rs array copies
- shuffle_scan.rs decompression buffers
- Selection vector processing

**Phase 3: Extend spill callback to FairUnified pool**
- Same pattern as GreedyUnified — add SpillState to CometFairMemoryPool

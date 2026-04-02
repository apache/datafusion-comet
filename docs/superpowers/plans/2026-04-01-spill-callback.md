# Spill Callback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Spark to reclaim memory from Comet's native operators via the `spill()` callback, so cross-task memory eviction works and Comet can run at lower off-heap memory settings.

**Architecture:** Add a shared `SpillState` (atomics for pressure/freed) that bridges `NativeMemoryConsumer.spill()` on the JVM side with `CometUnifiedMemoryPool.try_grow()` on the Rust side. When Spark calls `spill()`, it JNI-calls into native to set spill pressure. The pool's `try_grow()` checks this flag and returns `ResourcesExhausted`, causing DataFusion's Sort/Aggregate/Shuffle operators to spill internally. As operators release memory via `shrink()`, the freed bytes are tracked and returned to Spark.

**Tech Stack:** Rust (native memory pool), Java (JNI bridge, Spark MemoryConsumer), JNI

---

## File Map

| File                                                               | Action    | Responsibility                                                            |
| ------------------------------------------------------------------ | --------- | ------------------------------------------------------------------------- |
| `native/core/src/execution/memory_pools/spill.rs`                  | Create    | `SpillState` struct with atomics and wait/notify                          |
| `native/core/src/execution/memory_pools/unified_pool.rs`           | Modify    | Check `SpillState` in `try_grow`, track freed in `shrink`                 |
| `native/core/src/execution/memory_pools/mod.rs`                    | Modify    | Export `spill` module, pass `SpillState` through `create_memory_pool`     |
| `native/core/src/execution/memory_pools/config.rs`                 | No change | Pool config unchanged                                                     |
| `native/core/src/execution/jni_api.rs`                             | Modify    | Store `SpillState` in `ExecutionContext`, add JNI function `requestSpill` |
| `spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java` | Modify    | Implement `spill()`, store native handle, add JNI call                    |
| `spark/src/main/scala/org/apache/comet/Native.scala`               | Modify    | Add `requestSpill` native method declaration                              |
| `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`    | Modify    | Pass native plan handle back to `CometTaskMemoryManager`                  |

## Scope

This plan covers the `GreedyUnified` pool only (the default off-heap pool). The `FairUnified` pool (`fair_pool.rs`) uses the same Spark JNI path and can be updated in a follow-up using the same pattern.

---

### Task 1: Create SpillState

**Files:**

- Create: `native/core/src/execution/memory_pools/spill.rs`

- [ ] **Step 1: Create the SpillState struct**

```rust
// native/core/src/execution/memory_pools/spill.rs

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Shared state for coordinating spill requests between Spark's memory manager
/// (which calls `NativeMemoryConsumer.spill()` on a Spark thread) and DataFusion
/// operators (which call `try_grow()`/`shrink()` on tokio threads).
///
/// When Spark needs to reclaim memory from Comet, it sets `pressure` via
/// `request_spill()`. The memory pool's `try_grow()` checks this and returns
/// `ResourcesExhausted`, causing operators to spill. As operators call `shrink()`,
/// freed bytes are accumulated and the waiting Spark thread is notified.
#[derive(Debug)]
pub struct SpillState {
    /// Bytes requested to be freed. Set by Spark's spill() callback.
    pressure: AtomicUsize,
    /// Bytes actually freed since pressure was set.
    freed: AtomicUsize,
    /// Mutex + Condvar to allow the spill requester to wait for operators to react.
    notify: (Mutex<()>, Condvar),
}

impl SpillState {
    pub fn new() -> Self {
        Self {
            pressure: AtomicUsize::new(0),
            freed: AtomicUsize::new(0),
            notify: (Mutex::new(()), Condvar::new()),
        }
    }

    /// Returns the current spill pressure in bytes. Called by the memory pool's
    /// `try_grow()` to decide whether to deny allocations.
    pub fn pressure(&self) -> usize {
        self.pressure.load(Ordering::Acquire)
    }

    /// Record that `size` bytes were freed (called from pool's `shrink()`).
    /// Wakes the waiting spill requester.
    pub fn record_freed(&self, size: usize) {
        self.freed.fetch_add(size, Ordering::Release);
        let (_lock, cvar) = &self.notify;
        cvar.notify_all();
    }

    /// Called from JNI when Spark's `NativeMemoryConsumer.spill()` is invoked.
    /// Sets spill pressure and waits (up to `timeout`) for operators to free memory.
    /// Returns the actual number of bytes freed.
    pub fn request_spill(&self, size: usize, timeout: Duration) -> usize {
        // Reset freed counter and set pressure
        self.freed.store(0, Ordering::Release);
        self.pressure.store(size, Ordering::Release);

        // Wait for operators to react
        let (lock, cvar) = &self.notify;
        let mut guard = lock.lock().unwrap();
        let deadline = std::time::Instant::now() + timeout;
        while self.freed.load(Ordering::Acquire) < size {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (new_guard, _timeout_result) = cvar.wait_timeout(guard, remaining).unwrap();
            guard = new_guard;
        }

        // Clear pressure and return freed bytes
        self.pressure.store(0, Ordering::Release);
        self.freed.load(Ordering::Acquire)
    }
}

impl Default for SpillState {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 2: Add module to mod.rs**

Add to the top of `native/core/src/execution/memory_pools/mod.rs`, after the existing module declarations:

```rust
pub(crate) mod spill;
```

- [ ] **Step 3: Build to verify compilation**

Run: `cargo build --manifest-path native/Cargo.toml`
Expected: Build succeeds with no errors.

- [ ] **Step 4: Commit**

```bash
git add native/core/src/execution/memory_pools/spill.rs native/core/src/execution/memory_pools/mod.rs
git commit -m "feat: add SpillState for cross-thread spill coordination"
```

---

### Task 2: Wire SpillState into CometUnifiedMemoryPool

**Files:**

- Modify: `native/core/src/execution/memory_pools/unified_pool.rs`
- Modify: `native/core/src/execution/memory_pools/mod.rs`

- [ ] **Step 1: Add SpillState to CometUnifiedMemoryPool**

In `unified_pool.rs`, add the `spill_state` field and update the constructor:

```rust
use super::spill::SpillState;
use std::sync::Arc;
```

Change the struct definition:

```rust
pub struct CometUnifiedMemoryPool {
    task_memory_manager_handle: Arc<GlobalRef>,
    used: AtomicUsize,
    task_attempt_id: i64,
    spill_state: Arc<SpillState>,
}
```

Update `Debug` impl to include the new field:

```rust
impl Debug for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometUnifiedMemoryPool")
            .field("used", &self.used.load(Relaxed))
            .field("spill_pressure", &self.spill_state.pressure())
            .finish()
    }
}
```

Update the constructor:

```rust
pub fn new(
    task_memory_manager_handle: Arc<GlobalRef>,
    task_attempt_id: i64,
    spill_state: Arc<SpillState>,
) -> CometUnifiedMemoryPool {
    Self {
        task_memory_manager_handle,
        task_attempt_id,
        used: AtomicUsize::new(0),
        spill_state,
    }
}
```

- [ ] **Step 2: Check spill pressure in try_grow**

Replace the `try_grow` implementation:

```rust
fn try_grow(&self, _: &MemoryReservation, additional: usize) -> Result<(), DataFusionError> {
    if additional > 0 {
        // If there is spill pressure, deny the allocation to trigger operator spill
        if self.spill_state.pressure() > 0 {
            return Err(resources_datafusion_err!(
                "Task {} denied {} bytes due to spill pressure. Reserved: {}",
                self.task_attempt_id,
                additional,
                self.reserved()
            ));
        }

        let acquired = self.acquire_from_spark(additional)?;
        // If the number of bytes we acquired is less than the requested, return an error,
        // and hopefully will trigger spilling from the caller side.
        if acquired < additional as i64 {
            // Release the acquired bytes before throwing error
            self.release_to_spark(acquired as usize)?;

            return Err(resources_datafusion_err!(
                "Task {} failed to acquire {} bytes, only got {}. Reserved: {}",
                self.task_attempt_id,
                additional,
                acquired,
                self.reserved()
            ));
        }
        if let Err(prev) = self
            .used
            .fetch_update(Relaxed, Relaxed, |old| old.checked_add(acquired as usize))
        {
            return Err(resources_datafusion_err!(
                "Task {} failed to acquire {} bytes due to overflow. Reserved: {}",
                self.task_attempt_id,
                additional,
                prev
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Record freed bytes in shrink**

Replace the `shrink` implementation:

```rust
fn shrink(&self, _: &MemoryReservation, size: usize) {
    if let Err(e) = self.release_to_spark(size) {
        panic!(
            "Task {} failed to return {size} bytes to Spark: {e:?}",
            self.task_attempt_id
        );
    }
    if let Err(prev) = self
        .used
        .fetch_update(Relaxed, Relaxed, |old| old.checked_sub(size))
    {
        panic!(
            "Task {} overflow when releasing {size} of {prev} bytes",
            self.task_attempt_id
        );
    }
    // Notify the spill requester that memory was freed
    if self.spill_state.pressure() > 0 {
        self.spill_state.record_freed(size);
    }
}
```

- [ ] **Step 4: Update create_memory_pool to accept and pass SpillState**

In `native/core/src/execution/memory_pools/mod.rs`, update the function signature and the `GreedyUnified` arm:

```rust
use super::memory_pools::spill::SpillState;
```

Change the signature of `create_memory_pool`:

```rust
pub(crate) fn create_memory_pool(
    memory_pool_config: &MemoryPoolConfig,
    comet_task_memory_manager: Arc<GlobalRef>,
    task_attempt_id: i64,
    spill_state: Option<Arc<SpillState>>,
) -> Arc<dyn MemoryPool> {
```

Update the `GreedyUnified` match arm:

```rust
MemoryPoolType::GreedyUnified => {
    let spill_state = spill_state
        .unwrap_or_else(|| Arc::new(SpillState::new()));
    let memory_pool = CometUnifiedMemoryPool::new(
        comet_task_memory_manager,
        task_attempt_id,
        spill_state,
    );
    Arc::new(TrackConsumersPool::new(
        memory_pool,
        NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
    ))
}
```

- [ ] **Step 5: Update call site in jni_api.rs**

In `jni_api.rs`, update the call to `create_memory_pool` (around line 260):

```rust
let memory_pool =
    create_memory_pool(&memory_pool_config, task_memory_manager, task_attempt_id, None);
```

We pass `None` for now — Task 3 will wire in the real `SpillState`.

- [ ] **Step 6: Build to verify compilation**

Run: `cargo build --manifest-path native/Cargo.toml`
Expected: Build succeeds.

- [ ] **Step 7: Commit**

```bash
git add native/core/src/execution/memory_pools/unified_pool.rs \
        native/core/src/execution/memory_pools/mod.rs \
        native/core/src/execution/jni_api.rs
git commit -m "feat: wire SpillState into CometUnifiedMemoryPool"
```

---

### Task 3: Add JNI requestSpill Function and Store SpillState in ExecutionContext

**Files:**

- Modify: `native/core/src/execution/jni_api.rs`

- [ ] **Step 1: Add SpillState to ExecutionContext and pass through create_memory_pool**

Add field to `ExecutionContext` struct (around line 179):

```rust
pub spill_state: Option<Arc<SpillState>>,
```

Add the import near the top of `jni_api.rs`:

```rust
use crate::execution::memory_pools::spill::SpillState;
```

In the `createPlan` JNI function, create the `SpillState` before pool creation (around line 259):

```rust
let spill_state = if off_heap_mode != JNI_FALSE
    && matches!(memory_pool_config.pool_type, MemoryPoolType::GreedyUnified)
{
    Some(Arc::new(SpillState::new()))
} else {
    None
};

let memory_pool = create_memory_pool(
    &memory_pool_config,
    task_memory_manager,
    task_attempt_id,
    spill_state.clone(),
);
```

Add the field to the `ExecutionContext` initialization (around line 328):

```rust
spill_state,
```

- [ ] **Step 2: Add the JNI requestSpill function**

Add this function at module level in `jni_api.rs` (after the existing JNI functions):

```rust
/// Called from `CometTaskMemoryManager.spill()` via JNI to request that native
/// operators free memory. Returns the number of bytes actually freed.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_requestSpill(
    e: JNIEnv,
    _class: JClass,
    exec_context: jlong,
    size: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |_env| {
        let exec_context = get_execution_context(exec_context);
        if let Some(ref spill_state) = exec_context.spill_state {
            let timeout = std::time::Duration::from_secs(10);
            let freed = spill_state.request_spill(size as usize, timeout);
            Ok(freed as jlong)
        } else {
            // No spill state (not using unified pool) — can't spill
            Ok(0i64)
        }
    })
}
```

- [ ] **Step 3: Build to verify compilation**

Run: `cargo build --manifest-path native/Cargo.toml`
Expected: Build succeeds.

- [ ] **Step 4: Commit**

```bash
git add native/core/src/execution/jni_api.rs
git commit -m "feat: add JNI requestSpill function with SpillState in ExecutionContext"
```

---

### Task 4: Update JVM Side — CometTaskMemoryManager and Native

**Files:**

- Modify: `spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java`
- Modify: `spark/src/main/scala/org/apache/comet/Native.scala`

- [ ] **Step 1: Add requestSpill to Native.scala**

In `spark/src/main/scala/org/apache/comet/Native.scala`, add after the `releasePlan` method (around line 104):

```scala
  /**
   * Request that native operators spill memory. Called from CometTaskMemoryManager.spill().
   *
   * @param nativePlanHandle
   *   the native ExecutionContext pointer
   * @param size
   *   bytes requested to free
   * @return
   *   actual bytes freed
   */
  @native def requestSpill(nativePlanHandle: Long, size: Long): Long
```

- [ ] **Step 2: Update CometTaskMemoryManager to store native handle and implement spill**

Replace the contents of `CometTaskMemoryManager.java`:

```java
package org.apache.spark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.comet.Native;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * A adapter class that is used by Comet native to acquire & release memory through Spark's unified
 * memory manager. This assumes Spark's off-heap memory mode is enabled.
 */
public class CometTaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(CometTaskMemoryManager.class);

  /** The id uniquely identifies the native plan this memory manager is associated to */
  private final long id;

  private final long taskAttemptId;

  public final TaskMemoryManager internal;
  private final NativeMemoryConsumer nativeMemoryConsumer;
  private final AtomicLong used = new AtomicLong();

  /**
   * The native ExecutionContext handle. Set after native plan creation. Used to route spill()
   * requests to the native memory pool.
   */
  private volatile long nativePlanHandle = 0;

  public CometTaskMemoryManager(long id, long taskAttemptId) {
    this.id = id;
    this.taskAttemptId = taskAttemptId;
    this.internal = TaskContext$.MODULE$.get().taskMemoryManager();
    this.nativeMemoryConsumer = new NativeMemoryConsumer();
  }

  /**
   * Set the native plan handle after plan creation. This enables the spill() callback to route
   * requests to the native memory pool.
   */
  public void setNativePlanHandle(long handle) {
    this.nativePlanHandle = handle;
  }

  // Called by Comet native through JNI.
  // Returns the actual amount of memory (in bytes) granted.
  public long acquireMemory(long size) {
    if (logger.isTraceEnabled()) {
      logger.trace("Task {} requested {} bytes", taskAttemptId, size);
    }
    long acquired = internal.acquireExecutionMemory(size, nativeMemoryConsumer);
    long newUsed = used.addAndGet(acquired);
    if (acquired < size) {
      logger.warn(
          "Task {} requested {} bytes but only received {} bytes. Current allocation is {} and "
              + "the total memory consumption is {} bytes.",
          taskAttemptId,
          size,
          acquired,
          newUsed,
          internal.getMemoryConsumptionForThisTask());
      // If memory manager is not able to acquire the requested size, log memory usage
      internal.showMemoryUsage();
    }
    return acquired;
  }

  // Called by Comet native through JNI
  public void releaseMemory(long size) {
    if (logger.isTraceEnabled()) {
      logger.trace("Task {} released {} bytes", taskAttemptId, size);
    }
    long newUsed = used.addAndGet(-size);
    if (newUsed < 0) {
      logger.error(
          "Task {} used memory is negative ({}) after releasing {} bytes",
          taskAttemptId,
          newUsed,
          size);
    }
    internal.releaseExecutionMemory(size, nativeMemoryConsumer);
  }

  public long getUsed() {
    return used.get();
  }

  /**
   * A memory consumer that routes spill requests to the native memory pool. When Spark's memory
   * manager needs to reclaim memory (e.g., for another task), it calls spill() which signals the
   * native pool to apply backpressure. DataFusion operators (Sort, Aggregate, Shuffle) react by
   * spilling their internal state to disk.
   */
  private class NativeMemoryConsumer extends MemoryConsumer {
    protected NativeMemoryConsumer() {
      super(CometTaskMemoryManager.this.internal, 0, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      long handle = nativePlanHandle;
      if (handle == 0) {
        // Native plan not yet created or already destroyed
        return 0;
      }
      logger.info(
          "Task {} received spill request for {} bytes, forwarding to native",
          taskAttemptId,
          size);
      try {
        long freed = new Native().requestSpill(handle, size);
        logger.info("Task {} native spill freed {} bytes", taskAttemptId, freed);
        return freed;
      } catch (Exception e) {
        logger.warn("Task {} native spill failed: {}", taskAttemptId, e.getMessage());
        return 0;
      }
    }

    @Override
    public String toString() {
      return String.format("NativeMemoryConsumer(id=%d, taskAttemptId=%d)", id, taskAttemptId);
    }
  }
}
```

- [ ] **Step 3: Build JVM to verify compilation**

Run: `./mvnw compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java \
        spark/src/main/scala/org/apache/comet/Native.scala
git commit -m "feat: implement spill() callback in CometTaskMemoryManager"
```

---

### Task 5: Wire Native Plan Handle Back to CometTaskMemoryManager

**Files:**

- Modify: `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`

- [ ] **Step 1: Set the native plan handle after creation**

In `CometExecIterator.scala`, after the `nativeLib.createPlan(...)` call (around line 131), add:

```scala
  private val plan = {
    // ... existing code that calls nativeLib.createPlan ...

    val nativePlan = nativeLib.createPlan(
      id,
      inputIterators,
      protobufQueryPlan,
      protobufSparkConfigs,
      numParts,
      nativeMetrics,
      metricsUpdateInterval = COMET_METRICS_UPDATE_INTERVAL.get(),
      cometTaskMemoryManager,
      localDiskDirs,
      batchSize = COMET_BATCH_SIZE.get(),
      memoryConfig.offHeapMode,
      memoryConfig.memoryPoolType,
      memoryConfig.memoryLimit,
      memoryConfig.memoryLimitPerTask,
      taskAttemptId,
      taskCPUs,
      keyUnwrapper)

    // Enable spill callback by giving the memory manager a handle to the native plan
    cometTaskMemoryManager.setNativePlanHandle(nativePlan)

    nativePlan
  }
```

The existing code calls `nativeLib.createPlan(...)` and that's the last expression in the block (making it the return value). Restructure so we can call `setNativePlanHandle` and still return the handle:

```scala
    val handle = nativeLib.createPlan(
      id,
      inputIterators,
      // ... all existing args unchanged ...
      keyUnwrapper)

    // Enable spill callback by giving the memory manager a handle to the native plan
    cometTaskMemoryManager.setNativePlanHandle(handle)

    handle
  }
```

- [ ] **Step 2: Clear the handle on close**

In the `close()` method (line 231), add `cometTaskMemoryManager.setNativePlanHandle(0)`
before the `nativeLib.releasePlan(plan)` call to prevent spill callbacks after the
native plan is destroyed:

```scala
  def close(): Unit = synchronized {
    if (!closed) {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      nativeUtil.close()
      shuffleBlockIterators.values.foreach(_.close())
      cometTaskMemoryManager.setNativePlanHandle(0)
      nativeLib.releasePlan(plan)
```

- [ ] **Step 3: Build everything**

Run: `make`
Expected: Both native and JVM build succeed.

- [ ] **Step 4: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/CometExecIterator.scala
git commit -m "feat: wire native plan handle to CometTaskMemoryManager for spill routing"
```

---

### Task 6: Test with TPC-H Q9

- [ ] **Step 1: Run the memory profile script for Q9**

```bash
cd benchmarks/tpc
./memory-profile.sh --queries "9" --offheap-sizes "4g 8g" --cores 4
```

Compare peak RSS against the baseline results:

- Spark 4g: 4580 MB
- Comet 4g (before): 5911 MB
- Comet 8g (before): 6359 MB

**Expected:** The elastic growth (4g→8g delta) should be reduced because Spark can
now reclaim memory from Comet's shuffle writers when other tasks need it. The absolute
numbers may also decrease slightly.

- [ ] **Step 2: Verify spill logging**

Check the logs for spill activity:

```bash
grep -i "spill" benchmarks/tpc/memory-profile-results/logs/comet-offheap4g-q9.log
```

Expected: Log lines showing "received spill request" and "native spill freed N bytes"
from `CometTaskMemoryManager`.

- [ ] **Step 3: Run full test suite for regressions**

```bash
./mvnw test -DwildcardSuites="CometExec"
```

Expected: All existing tests pass.

- [ ] **Step 4: Commit results and update analysis**

Update `docs/source/contributor-guide/memory-management.md` with the new results.

```bash
git add docs/source/contributor-guide/memory-management.md
git commit -m "docs: add spill callback benchmark results"
```

---

## Notes

- **Timeout**: The 10-second timeout in `request_spill()` is conservative. If no operators
  are actively allocating, the spill won't trigger until one does. In practice, during
  TPC-H execution, operators allocate frequently so the response should be fast.

- **FairUnified pool**: Not covered by this plan. It uses a different pool struct
  (`CometFairMemoryPool`) but the same pattern applies — add `SpillState`, check in
  `try_grow`, record in `shrink`.

- **Thread safety**: `spill()` is called from Spark's memory manager thread.
  `try_grow()`/`shrink()` are called from tokio threads. The `AtomicUsize` + `Condvar`
  design handles this safely without locks on the hot path (`pressure()` is a single
  atomic load).

- **Multiple native plans per task**: A single Spark task may create multiple
  `CometExecIterator` instances (e.g., for subqueries). Each has its own
  `CometTaskMemoryManager` and `NativeMemoryConsumer`. Spark will call `spill()` on each
  independently, which is correct — each routes to its own pool.

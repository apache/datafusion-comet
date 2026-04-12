# Batch Stash Shuffle Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Avoid unnecessary Arrow FFI import/export when passing batches between a native child plan and a native ShuffleWriter by using an opaque batch handle passed through the JVM.

**Architecture:** A native-side `BatchStash` registry stores `RecordBatch` values keyed by `u64` handles. The child plan stashes its output batch and returns the handle to the JVM. The JVM passes the handle to the shuffle writer's `ScanExec`, which retrieves the batch directly from the stash. This eliminates 4 FFI boundary crossings per batch, replacing them with 2 lightweight JNI calls passing a single `long`.

**Tech Stack:** Rust (native), Scala/Java (JVM), JNI, Arrow RecordBatch

**Spec:** `docs/superpowers/specs/2026-04-12-batch-stash-shuffle-optimization-design.md`

---

### Task 1: BatchStash (Rust)

**Files:**
- Create: `native/core/src/execution/batch_stash.rs`
- Modify: `native/core/src/execution/mod.rs:19-33`

- [ ] **Step 1: Write the BatchStash test**

Create `native/core/src/execution/batch_stash.rs` with a test module:

```rust
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

//! A global registry for passing RecordBatch values between native execution
//! contexts through the JVM without Arrow FFI serialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);
static STASH: Lazy<Mutex<HashMap<u64, RecordBatch>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Stash a RecordBatch and return a unique handle for later retrieval.
pub fn stash(batch: RecordBatch) -> u64 {
    todo!()
}

/// Remove and return the RecordBatch for the given handle.
/// Returns `None` if the handle is not found (already consumed or invalid).
pub fn take(handle: u64) -> Option<RecordBatch> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_stash_and_take() {
        let batch = make_batch(&[1, 2, 3]);
        let handle = stash(batch);
        assert!(handle > 0);

        let retrieved = take(handle).expect("batch should be in stash");
        assert_eq!(retrieved.num_rows(), 3);
        assert_eq!(
            retrieved
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_take_removes_entry() {
        let batch = make_batch(&[10, 20]);
        let handle = stash(batch);

        assert!(take(handle).is_some());
        assert!(take(handle).is_none(), "second take should return None");
    }

    #[test]
    fn test_take_unknown_handle() {
        assert!(take(999_999_999).is_none());
    }

    #[test]
    fn test_handles_are_unique() {
        let h1 = stash(make_batch(&[1]));
        let h2 = stash(make_batch(&[2]));
        assert_ne!(h1, h2);

        // Clean up
        take(h1);
        take(h2);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd native && cargo test --lib execution::batch_stash`
Expected: FAIL with `not yet implemented`

- [ ] **Step 3: Implement stash and take**

Replace the `todo!()` bodies in `native/core/src/execution/batch_stash.rs`:

```rust
pub fn stash(batch: RecordBatch) -> u64 {
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    STASH.lock().unwrap().insert(handle, batch);
    handle
}

pub fn take(handle: u64) -> Option<RecordBatch> {
    STASH.lock().unwrap().remove(&handle)
}
```

- [ ] **Step 4: Add the module to mod.rs**

In `native/core/src/execution/mod.rs`, add after line 18 (before existing pub mod declarations):

```rust
pub(crate) mod batch_stash;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd native && cargo test --lib execution::batch_stash`
Expected: all 4 tests PASS

- [ ] **Step 6: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 7: Commit**

```bash
git add native/core/src/execution/batch_stash.rs native/core/src/execution/mod.rs
git commit -m "feat: add BatchStash registry for native batch handle passing"
```

---

### Task 2: CometHandleBatchIterator (Java + Rust JNI bridge)

**Files:**
- Create: `spark/src/main/java/org/apache/comet/CometHandleBatchIterator.java`
- Create: `native/jni-bridge/src/handle_batch_iterator.rs`
- Modify: `native/jni-bridge/src/lib.rs:181-289`

- [ ] **Step 1: Create CometHandleBatchIterator Java class**

Create `spark/src/main/java/org/apache/comet/CometHandleBatchIterator.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet;

/**
 * Iterator that passes opaque native batch handles between two native execution contexts through
 * the JVM. Used when a native child plan feeds directly into a native ShuffleWriter, avoiding
 * Arrow FFI export/import overhead.
 *
 * <p>Called from native ScanExec via JNI. The source CometExecIterator must be in stash mode.
 */
public class CometHandleBatchIterator {
  private final CometExecIterator source;

  public CometHandleBatchIterator(CometExecIterator source) {
    this.source = source;
  }

  /**
   * Get the next batch handle from the source iterator.
   *
   * @return a native batch handle (positive long), or -1 if no more batches.
   */
  public long nextHandle() {
    return source.nextHandle();
  }
}
```

- [ ] **Step 2: Create the Rust JNI bridge struct**

Create `native/jni-bridge/src/handle_batch_iterator.rs`:

```rust
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

use jni::signature::Primitive;
use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::ReturnType,
    strings::JNIString,
    Env,
};

/// A struct that holds JNI methods for the JVM `CometHandleBatchIterator` class.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometHandleBatchIterator<'a> {
    pub class: JClass<'a>,
    pub method_next_handle: JMethodID,
    pub method_next_handle_ret: ReturnType,
}

impl<'a> CometHandleBatchIterator<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometHandleBatchIterator";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometHandleBatchIterator<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;

        Ok(CometHandleBatchIterator {
            class,
            method_next_handle: env.get_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("nextHandle"),
                jni::jni_sig!("()J"),
            )?,
            method_next_handle_ret: ReturnType::Primitive(Primitive::Long),
        })
    }
}
```

- [ ] **Step 3: Register the new class in JVMClasses**

In `native/jni-bridge/src/lib.rs`, add the module declaration after line 184 (`mod shuffle_block_iterator;`):

```rust
mod handle_batch_iterator;
```

Add the import after line 189 (`use shuffle_block_iterator::CometShuffleBlockIterator;`):

```rust
use handle_batch_iterator::CometHandleBatchIterator;
```

Add the field to the `JVMClasses` struct after line 216 (`pub comet_shuffle_block_iterator: CometShuffleBlockIterator<'a>,`):

```rust
    /// The CometHandleBatchIterator class. Used for passing batch handles between native plans.
    pub comet_handle_batch_iterator: CometHandleBatchIterator<'a>,
```

Add initialization in the `JVMClasses::init` method after line 288 (`comet_shuffle_block_iterator: CometShuffleBlockIterator::new(env).unwrap(),`):

```rust
                comet_handle_batch_iterator: CometHandleBatchIterator::new(env).unwrap(),
```

- [ ] **Step 4: Verify it compiles**

Run: `cd native && cargo build`
Expected: compilation succeeds (the Java class must exist on the classpath; if building native-only fails, run `make` from the project root first)

- [ ] **Step 5: Commit**

```bash
git add spark/src/main/java/org/apache/comet/CometHandleBatchIterator.java \
        native/jni-bridge/src/handle_batch_iterator.rs \
        native/jni-bridge/src/lib.rs
git commit -m "feat: add CometHandleBatchIterator Java class and JNI bridge"
```

---

### Task 3: executePlanBatchHandle JNI function

**Files:**
- Modify: `spark/src/main/scala/org/apache/comet/Native.scala:91-96`
- Modify: `native/core/src/execution/jni_api.rs:550-830`

- [ ] **Step 1: Add the native method declaration in Native.scala**

In `spark/src/main/scala/org/apache/comet/Native.scala`, add after the `executePlan` method (after line 96):

```scala
  /**
   * Execute one step of the native query plan, stashing the output RecordBatch in the native
   * BatchStash instead of exporting via Arrow FFI. Returns the stash handle (positive long)
   * or -1 for EOF. Used when the output feeds directly into another native plan (e.g.,
   * native ShuffleWriter) to avoid unnecessary FFI round-trips.
   *
   * @param stage
   *   the stage ID, for informational purposes
   * @param partition
   *   the partition ID, for informational purposes
   * @param plan
   *   the address to native query plan.
   * @return
   *   a batch stash handle (positive), or -1 for EOF.
   */
  @native def executePlanBatchHandle(
      stage: Int,
      partition: Int,
      plan: Long): Long
```

- [ ] **Step 2: Add the stash_output helper function in jni_api.rs**

In `native/core/src/execution/jni_api.rs`, add after the `prepare_output` function (after line 618):

```rust
/// Stash the output RecordBatch in the BatchStash and return the handle.
/// Used when output feeds directly into another native plan.
fn stash_output(output_batch: RecordBatch) -> CometResult<jlong> {
    let handle = crate::execution::batch_stash::stash(output_batch);
    Ok(handle as jlong)
}
```

- [ ] **Step 3: Add the JNI function implementation**

In `native/core/src/execution/jni_api.rs`, add after the `Java_org_apache_comet_Native_executePlan` function (after line 830):

```rust
/// Like executePlan but stashes the output RecordBatch in the BatchStash and returns
/// the handle instead of exporting via Arrow FFI. Used for native-to-native batch passing.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_executePlanBatchHandle(
    e: EnvUnowned,
    _class: JClass,
    stage_id: jint,
    partition: jint,
    exec_context: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let exec_context = get_execution_context(exec_context);

        let tracing_enabled = exec_context.tracing_enabled;
        let owned_label;
        let tracing_label = if tracing_enabled {
            owned_label = exec_context.tracing_event_name.clone();
            owned_label.as_str()
        } else {
            ""
        };

        let result = with_trace(tracing_label, tracing_enabled, || {
            let exec_context_id = exec_context.id;

            // Initialize the execution stream on first call (same as executePlan)
            if exec_context.root_op.is_none() {
                let start = Instant::now();
                let planner =
                    PhysicalPlanner::new(Arc::clone(&exec_context.session_ctx), partition)
                        .with_exec_id(exec_context_id);
                let (scans, shuffle_scans, root_op) = planner.create_plan(
                    &exec_context.spark_plan,
                    &mut exec_context.input_sources.clone(),
                    exec_context.partition_count,
                )?;
                let physical_plan_time = start.elapsed();

                exec_context.plan_creation_time += physical_plan_time;
                exec_context.scans = scans;
                exec_context.shuffle_scans = shuffle_scans;

                if exec_context.explain_native {
                    let formatted_plan_str =
                        DisplayableExecutionPlan::new(root_op.native_plan.as_ref()).indent(true);
                    info!("Comet native query plan:\n{formatted_plan_str:}");
                }

                let task_ctx = exec_context.session_ctx.task_ctx();
                let stream = root_op.native_plan.execute(0, task_ctx)?;

                if exec_context.scans.is_empty() && exec_context.shuffle_scans.is_empty() {
                    let (tx, rx) = mpsc::channel(2);
                    let mut stream = stream;
                    get_runtime().spawn(async move {
                        let result = std::panic::AssertUnwindSafe(async {
                            while let Some(batch) = stream.next().await {
                                if tx.send(batch).await.is_err() {
                                    break;
                                }
                            }
                        })
                        .catch_unwind()
                        .await;

                        if let Err(panic) = result {
                            let msg = match panic.downcast_ref::<&str>() {
                                Some(s) => s.to_string(),
                                None => match panic.downcast_ref::<String>() {
                                    Some(s) => s.clone(),
                                    None => "unknown panic".to_string(),
                                },
                            };
                            let _ = tx
                                .send(Err(DataFusionError::Execution(format!(
                                    "native panic: {msg}"
                                ))))
                                .await;
                        }
                    });
                    exec_context.batch_receiver = Some(rx);
                } else {
                    exec_context.stream = Some(stream);
                }
                exec_context.root_op = Some(root_op);
            } else {
                pull_input_batches(exec_context)?;
            }

            if let Some(rx) = &mut exec_context.batch_receiver {
                match rx.blocking_recv() {
                    Some(Ok(batch)) => {
                        update_metrics(env, exec_context)?;
                        return stash_output(batch);
                    }
                    Some(Err(e)) => {
                        return Err(e.into());
                    }
                    None => {
                        log_plan_metrics(exec_context, stage_id, partition);
                        return Ok(-1);
                    }
                }
            }

            // ScanExec path: busy-poll
            get_runtime().block_on(async {
                loop {
                    let next_item = exec_context.stream.as_mut().unwrap().next();
                    let poll_output = poll!(next_item);

                    exec_context.poll_count_since_metrics_check += 1;
                    if exec_context.poll_count_since_metrics_check >= 100 {
                        exec_context.poll_count_since_metrics_check = 0;
                        if let Some(interval) = exec_context.metrics_update_interval {
                            let now = Instant::now();
                            if now - exec_context.metrics_last_update_time >= interval {
                                update_metrics(env, exec_context)?;
                                exec_context.metrics_last_update_time = now;
                            }
                        }
                        if exec_context.tracing_enabled {
                            log_memory_usage(
                                &exec_context.tracing_memory_metric_name,
                                total_reserved_for_thread(exec_context.rust_thread_id) as u64,
                            );
                        }
                    }

                    match poll_output {
                        Poll::Ready(Some(output)) => {
                            return stash_output(output?);
                        }
                        Poll::Ready(None) => {
                            log_plan_metrics(exec_context, stage_id, partition);
                            return Ok(-1);
                        }
                        Poll::Pending => {
                            tokio::task::block_in_place(|| pull_input_batches(exec_context))?;
                        }
                    }
                }
            })
        });

        if exec_context.tracing_enabled {
            #[cfg(feature = "jemalloc")]
            log_jemalloc_usage();
            log_memory_usage(
                &exec_context.tracing_memory_metric_name,
                total_reserved_for_thread(exec_context.rust_thread_id) as u64,
            );
        }

        result
    })
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cd native && cargo build`
Expected: compilation succeeds

- [ ] **Step 5: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 6: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/Native.scala \
        native/core/src/execution/jni_api.rs
git commit -m "feat: add executePlanBatchHandle JNI function for stash-mode output"
```

---

### Task 4: CometExecIterator stash mode

**Files:**
- Modify: `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`

- [ ] **Step 1: Add stash mode fields**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, add after line 81 (`private val cometTaskMemoryManager = new CometTaskMemoryManager(id, taskAttemptId)`):

```scala
  // When true, executePlan stashes output batches natively and returns handles
  // instead of exporting via Arrow FFI. Used when output feeds a native ShuffleWriter.
  @volatile private var stashMode: Boolean = false
  private var pendingHandle: Long = -1L
```

- [ ] **Step 2: Add enableStashMode and nextHandle methods**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, add after the `close()` method (after line 254, before the `traceMemoryUsage` method):

```scala
  /** Enable stash mode. Must be called before iteration begins. */
  def enableStashMode(): Unit = {
    stashMode = true
  }

  /**
   * In stash mode, advance the native plan and return the batch handle.
   * Returns a positive handle, or -1 for EOF.
   */
  def nextHandle(): Long = {
    if (closed) return -1L

    if (pendingHandle >= 0) {
      val h = pendingHandle
      pendingHandle = -1L
      return h
    }

    val ctx = TaskContext.get()
    try {
      val handle = nativeLib.executePlanBatchHandle(ctx.stageId(), partitionIndex, plan)
      if (handle == -1L) {
        close()
      }
      handle
    } catch {
      case e: Throwable => throw e
    }
  }
```

- [ ] **Step 3: Modify hasNext to support stash mode**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, replace the `hasNext` method (lines 189-214) with:

```scala
  override def hasNext: Boolean = {
    if (closed) return false

    if (stashMode) {
      // In stash mode, we use nextHandle() instead of getNextBatch.
      // hasNext is called by the shuffle writer's CometExecIterator indirectly.
      // We probe for the next handle and cache it.
      if (pendingHandle >= 0) return true
      val ctx = TaskContext.get()
      pendingHandle = nativeLib.executePlanBatchHandle(ctx.stageId(), partitionIndex, plan)
      if (pendingHandle == -1L) {
        close()
        false
      } else {
        true
      }
    } else {
      if (nextBatch.isDefined) {
        return true
      }

      if (prevBatch != null) {
        prevBatch.close()
        prevBatch = null
      }

      nextBatch = getNextBatch

      logTrace(s"Task $taskAttemptId memory pool usage is ${cometTaskMemoryManager.getUsed} bytes")

      if (nextBatch.isEmpty) {
        close()
        false
      } else {
        true
      }
    }
  }
```

- [ ] **Step 4: Modify next to handle stash mode**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, replace the `next()` method (lines 216-231) with:

```scala
  override def next(): ColumnarBatch = {
    if (stashMode) {
      // In stash mode, next() should not be called directly.
      // The shuffle writer uses nextHandle() instead.
      throw new UnsupportedOperationException(
        "next() should not be called in stash mode. Use nextHandle() instead.")
    }

    if (currentBatch != null) {
      currentBatch.close()
      currentBatch = null
    }

    if (nextBatch.isEmpty && !hasNext) {
      throw new NoSuchElementException("No more element")
    }

    currentBatch = nextBatch.get
    prevBatch = currentBatch
    nextBatch = None
    currentBatch
  }
```

- [ ] **Step 5: Build to verify compilation**

Run: `make`
Expected: build succeeds

- [ ] **Step 6: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/CometExecIterator.scala
git commit -m "feat: add stash mode to CometExecIterator for batch handle output"
```

---

### Task 5: ScanExec handle-based input path

**Files:**
- Modify: `native/core/src/execution/operators/scan.rs:57-258`

- [ ] **Step 1: Add handle_mode flag to ScanExec**

In `native/core/src/execution/operators/scan.rs`, add a new field to the `ScanExec` struct after line 79 (`arrow_ffi_safe: bool,`):

```rust
    /// When true, input comes from a CometHandleBatchIterator and batches are
    /// retrieved from the BatchStash instead of via Arrow FFI import.
    pub handle_mode: bool,
```

Update the `ScanExec::new` method to add `handle_mode: false` in the `Ok(Self { ... })` block (after the `arrow_ffi_safe,` line around line 115):

```rust
            handle_mode: false,
```

- [ ] **Step 2: Add get_next_handle method**

In `native/core/src/execution/operators/scan.rs`, add after the `get_next` method (after line 258):

```rust
    /// Pull next input batch from a CometHandleBatchIterator via batch stash handle.
    fn get_next_handle(
        exec_context_id: i64,
        iter: &JObject,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            return Ok(InputBatch::EOF);
        }

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null handle batch iterator object. Plan id: {exec_context_id}"
            ))));
        }

        JVMClasses::with_env(|env| {
            let handle: i64 = unsafe {
                jni_call!(env,
                    comet_handle_batch_iterator(iter).next_handle() -> i64)?
            };

            if handle == -1 {
                return Ok(InputBatch::EOF);
            }

            match crate::execution::batch_stash::take(handle as u64) {
                Some(batch) => {
                    let arrays: Vec<ArrayRef> = batch.columns().to_vec();
                    let num_rows = batch.num_rows();
                    Ok(InputBatch::new(arrays, Some(num_rows)))
                }
                None => Err(CometError::from(ExecutionError::GeneralError(format!(
                    "Batch stash handle {handle} not found"
                )))),
            }
        })
    }
```

- [ ] **Step 3: Modify get_next_batch to use handle mode**

In `native/core/src/execution/operators/scan.rs`, replace the `get_next_batch` method (lines 135-156) with:

```rust
    /// Pull next input batch from JVM.
    pub fn get_next_batch(&mut self) -> Result<(), CometError> {
        if self.input_source.is_none() {
            // This is a unit test. We don't need to call JNI.
            return Ok(());
        }
        let mut timer = self.baseline_metrics.elapsed_compute().timer();

        let mut current_batch = self.batch.try_lock().unwrap();
        if current_batch.is_none() {
            let next_batch = if self.handle_mode {
                ScanExec::get_next_handle(
                    self.exec_context_id,
                    self.input_source.as_ref().unwrap().as_obj(),
                )?
            } else {
                ScanExec::get_next(
                    self.exec_context_id,
                    self.input_source.as_ref().unwrap().as_obj(),
                    self.data_types.len(),
                    self.arrow_ffi_safe,
                )?
            };
            *current_batch = Some(next_batch);
        }

        timer.stop();

        Ok(())
    }
```

- [ ] **Step 4: Verify it compiles**

Run: `cd native && cargo build`
Expected: compilation succeeds

- [ ] **Step 5: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 6: Commit**

```bash
git add native/core/src/execution/operators/scan.rs
git commit -m "feat: add handle-mode input path to ScanExec for batch stash retrieval"
```

---

### Task 6: Planner detection of CometHandleBatchIterator

**Files:**
- Modify: `native/core/src/execution/planner.rs:1298-1311`

- [ ] **Step 1: Add runtime class check in planner**

In `native/core/src/execution/planner.rs`, replace the Scan creation block (lines 1298-1311) with:

```rust
                // The `ScanExec` operator will take actual arrays from Spark during execution
                let mut scan = ScanExec::new(
                    self.exec_context_id,
                    input_source.clone(),
                    &scan.source,
                    data_types,
                    scan.arrow_ffi_safe,
                )?;

                // Check if the input source is a CometHandleBatchIterator.
                // If so, enable handle mode on the scan to retrieve batches from
                // the BatchStash instead of via Arrow FFI.
                if let Some(ref source) = input_source {
                    let is_handle_iter = JVMClasses::with_env(|env| {
                        let handle_class =
                            &JVMClasses::get().comet_handle_batch_iterator.class;
                        let result = env.is_instance_of(source.as_obj(), handle_class)?;
                        Ok::<bool, CometError>(result)
                    })?;
                    if is_handle_iter {
                        scan.handle_mode = true;
                    }
                }

                Ok((
                    vec![scan.clone()],
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, Arc::new(scan), vec![])),
                ))
```

- [ ] **Step 2: Add the necessary import**

In `native/core/src/execution/planner.rs`, check if `JVMClasses` and `CometError` are already imported. If `JVMClasses` is not imported, add to the imports at the top of the file:

```rust
use datafusion_comet_jni_bridge::JVMClasses;
```

(`CometError` should already be imported via the existing error handling imports.)

- [ ] **Step 3: Verify it compiles**

Run: `cd native && cargo build`
Expected: compilation succeeds

- [ ] **Step 4: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 5: Commit**

```bash
git add native/core/src/execution/planner.rs
git commit -m "feat: detect CometHandleBatchIterator in planner and enable handle mode"
```

---

### Task 7: CometShuffleWriterInputIterator and detection in prepareShuffleDependency

**Files:**
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala:584-658`

- [ ] **Step 1: Add CometShuffleWriterInputIterator class**

In `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala`, add at the end of the file (before the final closing brace of the companion object, or after it if it's a top-level addition):

```scala
/**
 * An iterator wrapper that preserves access to the underlying CometExecIterator
 * when present. Used by CometNativeShuffleWriter to detect native child plans
 * and enable the batch stash optimization.
 */
private[shuffle] class CometShuffleWriterInputIterator(
    underlying: Iterator[ColumnarBatch],
    val nativeIterator: Option[CometExecIterator])
    extends Iterator[Product2[Int, ColumnarBatch]] {
  override def hasNext: Boolean = underlying.hasNext
  override def next(): Product2[Int, ColumnarBatch] = (0, underlying.next())
}
```

- [ ] **Step 2: Add the necessary import**

In `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala`, add to the imports:

```scala
import org.apache.comet.CometExecIterator
```

- [ ] **Step 3: Modify prepareShuffleDependency to use CometShuffleWriterInputIterator**

In `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala`, replace lines 643-646:

```scala
    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rdd.map(
        (0, _)
      ), // adding fake partitionId that is always 0 because ShuffleDependency requires it
```

with:

```scala
    val wrappedRDD = rdd.mapPartitions { iter =>
      val nativeIter = iter match {
        case cei: CometExecIterator => Some(cei)
        case _ => None
      }
      new CometShuffleWriterInputIterator(iter, nativeIter)
    }

    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      wrappedRDD,
```

- [ ] **Step 4: Build to verify compilation**

Run: `make`
Expected: build succeeds

- [ ] **Step 5: Commit**

```bash
git add spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala
git commit -m "feat: preserve CometExecIterator reference through shuffle dependency RDD"
```

---

### Task 8: CometNativeShuffleWriter stash mode integration

**Files:**
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala:65-112`
- Modify: `spark/src/main/scala/org/apache/comet/CometExecIterator.scala:84-89`

- [ ] **Step 1: Add a CometExecIterator constructor that accepts pre-built inputIterators**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, add a secondary constructor or a new `getCometIterator` overload. The simplest approach is to add a new parameter to accept pre-built input iterators.

Add a new companion-object-style factory method. In `spark/src/main/scala/org/apache/spark/sql/comet/operators.scala`, add a new `getCometIterator` overload after line 371 (after the existing overloads):

```scala
  /**
   * Create a CometExecIterator with pre-built input iterators (e.g., CometHandleBatchIterator).
   * Bypasses the normal CometBatchIterator wrapping.
   */
  def getCometIteratorWithHandleInputs(
      handleInputs: Array[Object],
      numOutputCols: Int,
      nativePlan: Operator,
      nativeMetrics: CometMetricNode,
      numParts: Int,
      partitionIdx: Int): CometExecIterator = {
    val bytes = serializeNativePlan(nativePlan)
    new CometExecIterator(
      newIterId,
      Seq.empty, // no ColumnarBatch inputs
      numOutputCols,
      bytes,
      nativeMetrics,
      numParts,
      partitionIdx,
      handleInputs = handleInputs)
  }
```

Then in `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, add an optional `handleInputs` parameter to the constructor. Modify the class declaration (line 61) to add the parameter:

```scala
class CometExecIterator(
    val id: Long,
    inputs: Seq[Iterator[ColumnarBatch]],
    numOutputCols: Int,
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode,
    numParts: Int,
    partitionIndex: Int,
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty,
    shuffleBlockIterators: Map[Int, CometShuffleBlockIterator] = Map.empty,
    handleInputs: Array[Object] = Array.empty)
```

Then modify the `inputIterators` initialization (lines 84-89) to prefer handleInputs when provided:

```scala
  private val inputIterators: Array[Object] = if (handleInputs.nonEmpty) {
    handleInputs
  } else {
    inputs.zipWithIndex.map {
      case (_, idx) if shuffleBlockIterators.contains(idx) =>
        shuffleBlockIterators(idx).asInstanceOf[Object]
      case (iterator, _) =>
        new CometBatchIterator(iterator, nativeUtil).asInstanceOf[Object]
    }.toArray
  }
```

- [ ] **Step 2: Modify CometNativeShuffleWriter.write() to detect and use stash mode**

In `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala`, add the import at the top:

```scala
import org.apache.comet.{CometExecIterator, CometHandleBatchIterator}
```

Replace lines 96-111 (from `val newInputs` through the `while` loop) with:

```scala
    // Detect if input comes from a native plan (CometExecIterator)
    val nativeIter: Option[CometExecIterator] = inputs match {
      case swi: CometShuffleWriterInputIterator => swi.nativeIterator
      case _ => None
    }

    val cometIter = nativeIter match {
      case Some(childIter) =>
        // Stash mode: child plan stashes batches, shuffle writer retrieves via handles
        childIter.enableStashMode()
        val handleIter = new CometHandleBatchIterator(childIter)
        CometExec.getCometIteratorWithHandleInputs(
          Array(handleIter.asInstanceOf[Object]),
          outputAttributes.length,
          nativePlan,
          nativeMetrics,
          numParts,
          context.partitionId())
      case None =>
        // Normal FFI mode: wrap input in CometBatchIterator as before
        val newInputs = inputs.asInstanceOf[Iterator[_ <: Product2[Any, Any]]].map(_._2)
        CometExec.getCometIterator(
          Seq(newInputs.asInstanceOf[Iterator[ColumnarBatch]]),
          outputAttributes.length,
          nativePlan,
          nativeMetrics,
          numParts,
          context.partitionId(),
          broadcastedHadoopConfForEncryption = None,
          encryptedFilePaths = Seq.empty)
    }

    while (cometIter.hasNext) {
      cometIter.next()
    }
    cometIter.close()
```

- [ ] **Step 3: Add the import for CometShuffleWriterInputIterator**

The `CometShuffleWriterInputIterator` is in the same package (`execution.shuffle`), so it should be accessible without an explicit import. Verify this compiles.

- [ ] **Step 4: Build to verify compilation**

Run: `make`
Expected: build succeeds

- [ ] **Step 5: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/CometExecIterator.scala \
        spark/src/main/scala/org/apache/spark/sql/comet/operators.scala \
        spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala
git commit -m "feat: integrate batch stash mode in CometNativeShuffleWriter"
```

---

### Task 9: End-to-end testing

**Files:**
- Modify: existing shuffle test suite (no new test files needed)

- [ ] **Step 1: Run existing shuffle tests to verify no regressions**

Run: `./mvnw test -DwildcardSuites="CometShuffleSuite" -Dtest=none`
Expected: all tests PASS

- [ ] **Step 2: Run the full native shuffle test suite**

Run: `./mvnw test -DwildcardSuites="CometNativeShuffleSuite" -Dtest=none`
Expected: all tests PASS (if this suite exists; otherwise skip)

- [ ] **Step 3: Run a broader test to cover shuffle exchange paths**

Run: `./mvnw test -DwildcardSuites="CometExec" -Dtest=none`
Expected: all tests PASS

- [ ] **Step 4: Run Rust tests**

Run: `cd native && cargo test --workspace`
Expected: all tests PASS (including the BatchStash tests from Task 1)

- [ ] **Step 5: Run clippy one final time**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 6: Format all code**

Run: `make format`
Expected: formatting applied cleanly

- [ ] **Step 7: Commit any formatting changes**

```bash
git add -A
git commit -m "style: format code"
```

---

### Task 10: Refactor to reduce duplication in jni_api.rs

The `executePlanBatchHandle` JNI function duplicates most of `executePlan`. After verifying correctness in Task 9, extract the shared logic.

**Files:**
- Modify: `native/core/src/execution/jni_api.rs`

- [ ] **Step 1: Extract shared execution logic into a helper**

In `native/core/src/execution/jni_api.rs`, create a helper enum and function:

```rust
/// How to handle the output batch from executePlan.
enum OutputMode<'a> {
    /// Export via Arrow FFI to the provided addresses.
    Ffi {
        env: &'a mut Env<'a>,
        array_addrs: JLongArray<'a>,
        schema_addrs: JLongArray<'a>,
        validate: bool,
    },
    /// Stash in BatchStash and return handle.
    Stash,
}
```

Then extract the common body of `executePlan` and `executePlanBatchHandle` into:

```rust
fn execute_plan_inner(
    env: &mut Env,
    exec_context: &mut ExecutionContext,
    stage_id: jint,
    partition: jint,
    output_mode: OutputMode,
) -> CometResult<jlong>
```

This function contains the shared initialization, stream polling, and metrics logic. The only difference is the final step: `OutputMode::Ffi` calls `prepare_output()`, `OutputMode::Stash` calls `stash_output()`.

Both `Java_org_apache_comet_Native_executePlan` and `Java_org_apache_comet_Native_executePlanBatchHandle` become thin wrappers that call `execute_plan_inner` with the appropriate `OutputMode`.

- [ ] **Step 2: Verify it compiles and tests pass**

Run: `cd native && cargo build && cargo test --workspace`
Expected: compilation succeeds, all tests pass

- [ ] **Step 3: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 4: Commit**

```bash
git add native/core/src/execution/jni_api.rs
git commit -m "refactor: extract shared execution logic from executePlan and executePlanBatchHandle"
```

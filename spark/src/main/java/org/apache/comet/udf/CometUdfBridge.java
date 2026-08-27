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

package org.apache.comet.udf;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.TaskContext;
import org.apache.spark.comet.CometTaskContextShim;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskCompletionListener;

import org.apache.comet.util.ClassLoaders;

/**
 * JNI entry point for native execution to invoke a {@link CometUDF}. Matches the static-method
 * pattern used by CometScalarSubquery so the native side can dispatch via
 * call_static_method_unchecked.
 *
 * <p>Cache invariants:
 *
 * <ol>
 *   <li>For each live Spark task attempt there is at most one {@link CometUDF} instance per class
 *       name.
 *   <li>A {@link CometUDF} instance is visible only within the Spark task attempt that instantiated
 *       it. Two task attempts observing the same class name receive distinct instances.
 *   <li>Calls for a task may arrive concurrently from different Tokio workers. Implementations with
 *       mutable state are responsible for synchronizing {@code evaluate()}.
 *   <li>All instances for a task are dropped by the {@link TaskCompletionListener} registered by
 *       {@link #registerTask(TaskContext)}. No UDF instance outlives its task.
 *   <li>When {@code taskContext} is {@code null} (unit tests, direct native driver), instances live
 *       in a process-lifetime fallback cache because no task-completion event will fire.
 * </ol>
 *
 * <p>Keying by the propagated {@link TaskContext} rather than by thread keeps the cache correct
 * under Tokio work-stealing: on the scan-free execution path the same Spark task can be polled by
 * different Tokio workers across batches, so a thread-local cache would lose per-task state on
 * migration. The context object is stable for the life of the task and remains unique when a new
 * SparkContext reuses a numeric task attempt ID.
 */
public class CometUdfBridge {

  private static final Logger LOG = LoggerFactory.getLogger(CometUdfBridge.class);

  private static final BufferAllocator ROOT_ALLOCATOR = CometUDF.rootAllocator();

  /**
   * Task-scoped UDF instances and output allocator. Entries are removed after task completion once
   * in-flight evaluations finish and the task allocator holds no memory. Exported output buffers do
   * not pin an entry: their accounting moves to the root allocator when the result is handed to
   * native execution.
   */
  private static final ConcurrentHashMap<TaskContext, TaskState> TASKS = new ConcurrentHashMap<>();

  /** Calls without a Spark task retain the existing process-lifetime fallback behavior. */
  private static final ConcurrentHashMap<String, CometUDF> NO_TASK_INSTANCES =
      new ConcurrentHashMap<>();

  /**
   * Registers task-scoped UDF state before native execution starts.
   *
   * <p>{@code CometExecIterator} calls this before registering its own completion listener. Spark
   * runs completion listeners in reverse registration order, so the native plan is released, and
   * has stopped calling UDFs, before this state attempts to close its child allocator.
   */
  public static void registerTask(TaskContext taskContext) {
    if (taskContext != null) {
      taskState(taskContext);
    }
  }

  /**
   * Called from native via JNI.
   *
   * @param udfClassName fully-qualified class name implementing CometUDF
   * @param inputArrayPtrs addresses of pre-allocated FFI_ArrowArray structs (one per input)
   * @param inputSchemaPtrs addresses of pre-allocated FFI_ArrowSchema structs (one per input)
   * @param outArrayPtr address of pre-allocated FFI_ArrowArray for the result
   * @param outSchemaPtr address of pre-allocated FFI_ArrowSchema for the result
   * @param numRows row count of the current batch. Mirrors DataFusion's {@code
   *     ScalarFunctionArgs.number_rows}; the only batch-size signal a zero-input UDF (e.g. a
   *     zero-arg non-deterministic ScalaUDF) ever sees.
   * @param taskContext propagated Spark {@link TaskContext} from the driving Spark task thread, or
   *     {@code null} outside a Spark task. Treated as ground truth for the call: installed as the
   *     thread-local on entry, with the prior value (if any) saved and restored in {@code finally}.
   *     Lets partition-sensitive built-ins ({@code Rand}, {@code Uuid}, {@code
   *     MonotonicallyIncreasingID}) work from Tokio workers and avoids reusing a stale TaskContext
   *     left on a worker by a previous task. Its object identity also keys the UDF-instance cache,
   *     so a UDF holding per-task state in fields sees a consistent instance for every call within
   *     the task regardless of which Tokio worker is polling.
   * @param classLoader context ClassLoader captured on the driving Spark task thread, or {@code
   *     null} outside a Spark task. Installed as this thread's context ClassLoader for the duration
   *     of the call, with the prior value restored in {@code finally}. Tokio workers attach through
   *     JNI and an attached thread has no context ClassLoader, so without this every lookup falls
   *     back to the ClassLoader that loaded Comet, which never holds user jars ({@code --jars} /
   *     {@code spark.jars}). Both the {@code CometUDF} resolution below and the closure
   *     deserialization in {@code CometScalaUDFCodegen} depend on it. Installed per call rather
   *     than once when the worker attaches, because the Tokio runtime is process-global: one worker
   *     interleaves work from task attempts of different jobs, and under Spark Connect from
   *     sessions with different artifact ClassLoaders. The loader is a property of the plan, not of
   *     the thread.
   */
  public static void evaluate(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows,
      TaskContext taskContext,
      ClassLoader classLoader) {
    assert udfClassName != null && !udfClassName.isEmpty() : "udfClassName must be non-empty";
    assert inputArrayPtrs != null && inputSchemaPtrs != null
        : "input pointer arrays must be non-null";
    assert inputArrayPtrs.length == inputSchemaPtrs.length
        : "input array pointer count must equal schema pointer count";
    assert numRows >= 0 : "numRows must be non-negative";
    assert outArrayPtr != 0L : "outArrayPtr must be a valid FFI pointer";
    assert outSchemaPtr != 0L : "outSchemaPtr must be a valid FFI pointer";

    // Save-and-restore rather than only-install-if-null: the propagated `taskContext` is the
    // ground truth for this call. Any value already on the thread is either (a) the same object
    // on a Spark task thread, or (b) stale from a prior task on a reused Tokio worker. The same
    // reasoning applies to the propagated `classLoader`.
    TaskContext prior = TaskContext.get();
    if (taskContext != null) {
      CometTaskContextShim.set(taskContext);
      assert TaskContext.get() == taskContext
          : "TaskContext install did not take effect on this thread";
    }
    Thread currentThread = Thread.currentThread();
    ClassLoader priorLoader = currentThread.getContextClassLoader();
    if (classLoader != null) {
      currentThread.setContextClassLoader(classLoader);
    }

    try {
      TaskState state = taskContext == null ? null : taskState(taskContext);
      if (state != null) {
        state.beginEvaluation();
      }
      try {
        evaluateInternal(
            udfClassName,
            inputArrayPtrs,
            inputSchemaPtrs,
            outArrayPtr,
            outSchemaPtr,
            numRows,
            state);
      } finally {
        if (state != null) {
          state.finishEvaluation();
        }
      }
    } finally {
      // Unconditional: a no-op when nothing was installed, and it also undoes any change the
      // user function made to the ClassLoader of a worker that outlives this call.
      currentThread.setContextClassLoader(priorLoader);
      if (taskContext != null) {
        if (prior != null) {
          CometTaskContextShim.set(prior);
        } else {
          CometTaskContextShim.unset();
        }
      }
    }
  }

  private static void evaluateInternal(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows,
      TaskState state) {
    ConcurrentHashMap<String, CometUDF> perTask =
        state == null ? NO_TASK_INSTANCES : state.instances;
    assert perTask != null : "per-task cache must be non-null after computeIfAbsent";

    CometUDF udf =
        perTask.computeIfAbsent(
            udfClassName,
            name -> {
              try {
                // Resolves through the context ClassLoader installed by `evaluate`, so a
                // user-supplied CometUDF shipped in a user jar is visible.
                return (CometUDF)
                    ClassLoaders.loadClass(name).getDeclaredConstructor().newInstance();
              } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate CometUDF: " + name, e);
              }
            });
    assert udf != null : "reflective instantiation returned null for " + udfClassName;

    BufferAllocator outputAllocator = state == null ? ROOT_ALLOCATOR : state.allocator();

    ValueVector[] inputs = new ValueVector[inputArrayPtrs.length];
    ValueVector result = null;
    FieldVector transferred = null;
    try {
      for (int i = 0; i < inputArrayPtrs.length; i++) {
        ArrowArray inArr = ArrowArray.wrap(inputArrayPtrs[i]);
        ArrowSchema inSch = ArrowSchema.wrap(inputSchemaPtrs[i]);
        // Imported native buffers are already owned/accounted by native execution. Keep them on
        // the root allocator so the output listener does not charge them a second time.
        inputs[i] = Data.importVector(ROOT_ALLOCATOR, inArr, inSch, null);
      }

      result = udf.evaluate(outputAllocator, inputs, numRows);
      if (!(result instanceof FieldVector)) {
        throw new RuntimeException(
            "CometUDF.evaluate() must return a FieldVector, got: " + result.getClass().getName());
      }
      if (result.getValueCount() != numRows) {
        throw new RuntimeException(
            "CometUDF.evaluate() returned "
                + result.getValueCount()
                + " rows, expected "
                + numRows);
      }
      ArrowArray outArr = ArrowArray.wrap(outArrayPtr);
      ArrowSchema outSch = ArrowSchema.wrap(outSchemaPtr);
      if (state != null) {
        // Accounting ownership follows the FFI boundary, mirroring the input import above: once
        // the result is handed to native execution, native operators that retain its buffers
        // (hash join builds, shuffle buffers) take their own Spark reservations, so keeping the
        // JVM charge until the FFI release would charge the same physical buffers twice.
        Field resultField = ((FieldVector) result).getField();
        transferred = transferForExport(state, outputAllocator, (FieldVector) result);
        // Export the schema from the result's own Field: a transferred complex vector rebuilds
        // child names from its runtime data vectors, which Arrow hardcodes to "$data$", losing
        // names like "item" that native execution expects. The C array carries no names, so it
        // can come from the transferred vector whose buffers the root allocator now owns.
        Data.exportField(ROOT_ALLOCATOR, resultField, null, outSch);
        Data.exportVector(ROOT_ALLOCATOR, transferred, null, outArr);
      } else {
        Data.exportVector(ROOT_ALLOCATOR, (FieldVector) result, null, outArr, outSch);
      }
    } finally {
      for (ValueVector v : inputs) {
        if (v != null) {
          try {
            v.close();
          } catch (RuntimeException ignored) {
            // do not mask the original throwable
          }
        }
      }
      if (result != null) {
        try {
          result.close();
        } catch (RuntimeException ignored) {
          // do not mask the original throwable
        }
      }
      if (transferred != null) {
        try {
          transferred.close();
        } catch (RuntimeException ignored) {
          // do not mask the original throwable
        }
      }
    }
  }

  /**
   * Moves the result's buffer accounting from the task allocator to the root allocator and drops
   * the corresponding Spark task charge. Neither the ownership transfer nor the eventual FFI
   * release is observed by the task allocator's {@link AllocationListener} (Arrow only notifies the
   * allocator owning a chunk when the chunk is destroyed), so the charge must be released here.
   *
   * <p>The returned vector shares the original buffers and must be closed by the caller after
   * export; the exported FFI array keeps the buffers alive until native execution releases them.
   */
  private static FieldVector transferForExport(
      TaskState state, BufferAllocator outputAllocator, FieldVector result) {
    long charged = chargedOutputSize(result, outputAllocator);
    TransferPair transferPair = result.getTransferPair(result.getField(), ROOT_ALLOCATOR);
    transferPair.transfer();
    state.releaseExportedCharge(charged);
    return (FieldVector) transferPair.getTo();
  }

  /**
   * Bytes of the result's buffers currently accounted against the task allocator, i.e. the portion
   * of the Spark task charge that moves to native ownership on export. {@code getAccountedSize()}
   * is non-zero only on the ledger that owns a chunk, so pass-through input buffers (owned by the
   * root allocator) and chunks whose ownership already moved on a previous export contribute
   * nothing.
   */
  private static long chargedOutputSize(FieldVector result, BufferAllocator outputAllocator) {
    long charged = 0L;
    Set<ReferenceManager> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (ArrowBuf buf : result.getBuffers(false)) {
      ReferenceManager referenceManager = buf.getReferenceManager();
      if (referenceManager.getAllocator() == outputAllocator && seen.add(referenceManager)) {
        charged += referenceManager.getAccountedSize();
      }
    }
    return charged;
  }

  /** Visible to the focused allocator test in this package. */
  static BufferAllocator taskAllocator(TaskContext taskContext) {
    return taskState(taskContext).allocator();
  }

  /** Visible to the focused allocator test in this package. */
  static int taskStateCount() {
    return TASKS.size();
  }

  /** Visible to the focused allocator test in this package. */
  static Runnable beginTaskEvaluation(TaskContext taskContext) {
    TaskState state = taskState(taskContext);
    state.beginEvaluation();
    return state::finishEvaluation;
  }

  /** Visible to the focused allocator test in this package. */
  static FieldVector transferOutputForExport(TaskContext taskContext, FieldVector result) {
    TaskState state = taskState(taskContext);
    return transferForExport(state, state.allocator(), result);
  }

  private static TaskState taskState(TaskContext taskContext) {
    return TASKS.computeIfAbsent(
        taskContext,
        context -> {
          TaskState state = new TaskState(context, CometTaskContextShim.taskMemoryManager(context));
          context.addTaskCompletionListener(
              (TaskCompletionListener) ignored -> state.taskCompleted());
          return state;
        });
  }

  /** Per-task Arrow listener and non-spillable Spark memory consumer. */
  private static final class TaskState implements AllocationListener {
    private final TaskContext taskContext;
    private final long taskAttemptId;
    private final TaskMemoryManager taskMemoryManager;
    private final TaskMemoryConsumer consumer;
    private final ConcurrentHashMap<String, CometUDF> instances = new ConcurrentHashMap<>();

    private BufferAllocator allocator;
    // Arrow updates allocator accounting after onPreAllocation returns.
    private int evaluationsInFlight;
    private boolean completed;
    private boolean closed;

    private TaskState(TaskContext taskContext, TaskMemoryManager taskMemoryManager) {
      this.taskContext = taskContext;
      this.taskAttemptId = taskContext.taskAttemptId();
      this.taskMemoryManager = taskMemoryManager;
      this.consumer =
          taskMemoryManager.getTungstenMemoryMode() == MemoryMode.OFF_HEAP
              ? new TaskMemoryConsumer(taskMemoryManager)
              : null;
    }

    private synchronized BufferAllocator allocator() {
      if (completed) {
        throw new IllegalStateException(
            "Cannot allocate JVM UDF memory after task " + taskAttemptId + " completed");
      }
      if (allocator == null) {
        allocator =
            ROOT_ALLOCATOR.newChildAllocator(
                "comet-udf-task-" + taskAttemptId, this, 0L, Long.MAX_VALUE);
      }
      return allocator;
    }

    @Override
    public void onPreAllocation(long size) {
      // Spark's executor cleanup also synchronizes on TaskMemoryManager. Keep that cleanup from
      // overtaking an admitted allocation, while leaving this TaskState monitor free for buffer
      // releases that can satisfy a blocking acquire.
      synchronized (taskMemoryManager) {
        synchronized (this) {
          if (completed) {
            throw new OutOfMemoryException(
                "Cannot allocate " + size + " JVM UDF bytes after task completion");
          }
        }

        long acquired = consumer == null ? size : consumer.acquireMemory(size);
        synchronized (this) {
          if (completed) {
            if (consumer != null) {
              consumer.freeAllMemory();
            }
            throw new OutOfMemoryException(
                "Cannot allocate " + size + " JVM UDF bytes after task completion");
          }
          if (acquired < size) {
            if (acquired > 0) {
              consumer.freeMemory(acquired);
            }
            throw new OutOfMemoryException(
                "Failed to acquire " + size + " JVM UDF bytes from Spark TaskMemoryManager");
          }
        }
      }
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      synchronized (this) {
        if (!completed && consumer != null) {
          consumer.freeMemory(size);
        }
      }
      return false;
    }

    @Override
    public void onRelease(long size) {
      synchronized (this) {
        if (!completed && consumer != null) {
          consumer.freeMemory(size);
        }
      }
      closeIfIdle();
    }

    /**
     * Drops the Spark charge for output buffers whose accounting ownership moved to the root
     * allocator at export; from that point native execution owns them and {@link #onRelease} will
     * never fire for their chunks. After completion the consumer was already drained wholesale.
     */
    private void releaseExportedCharge(long size) {
      if (size <= 0) {
        return;
      }
      synchronized (this) {
        if (!completed && consumer != null) {
          consumer.freeMemory(size);
        }
      }
    }

    private synchronized void beginEvaluation() {
      if (completed) {
        throw new IllegalStateException(
            "Cannot evaluate JVM UDF after task " + taskAttemptId + " completed");
      }
      evaluationsInFlight++;
    }

    private void taskCompleted() {
      synchronized (this) {
        completed = true;
        instances.clear();
        if (consumer != null) {
          // Spark discards all remaining task accounting immediately after completion listeners.
          // Release it here; later FFI callbacks only drive allocator cleanup.
          consumer.freeAllMemory();
        }
      }
      closeIfIdle();
    }

    private void finishEvaluation() {
      synchronized (this) {
        evaluationsInFlight--;
      }
      closeIfIdle();
    }

    private void closeIfIdle() {
      BufferAllocator toClose = null;
      boolean removeOnly = false;
      synchronized (this) {
        if (completed
            && evaluationsInFlight == 0
            && (allocator == null || allocator.getAllocatedMemory() == 0)
            && !closed) {
          closed = true;
          toClose = allocator;
          removeOnly = allocator == null;
        }
      }
      if (toClose != null) {
        close(toClose);
      } else if (removeOnly) {
        TASKS.remove(taskContext, this);
      }
    }

    private void close(BufferAllocator allocator) {
      try {
        allocator.close();
      } catch (RuntimeException e) {
        // AllocationListener.onRelease must not throw. Preserve the original task outcome and
        // leave an actionable leak report instead.
        LOG.warn("JVM UDF allocator for task {} failed to close cleanly", taskAttemptId, e);
      } finally {
        TASKS.remove(taskContext, this);
      }
    }
  }

  /**
   * Non-spillable because live UDF Arrow buffers can be referenced by Java or exported through FFI
   * and cannot be safely evicted and reconstructed.
   */
  private static final class TaskMemoryConsumer extends MemoryConsumer {
    private final AtomicLong accounted = new AtomicLong();

    private TaskMemoryConsumer(TaskMemoryManager taskMemoryManager) {
      super(taskMemoryManager, 0L, MemoryMode.OFF_HEAP);
    }

    @Override
    public long acquireMemory(long size) {
      long acquired = taskMemoryManager.acquireExecutionMemory(size, this);
      accounted.addAndGet(acquired);
      return acquired;
    }

    @Override
    public long getUsed() {
      return accounted.get();
    }

    @Override
    public void freeMemory(long size) {
      taskMemoryManager.releaseExecutionMemory(size, this);
      accounted.addAndGet(-size);
    }

    private void freeAllMemory() {
      long toFree = accounted.getAndSet(0);
      if (toFree > 0) {
        taskMemoryManager.releaseExecutionMemory(toFree, this);
      }
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      return 0L;
    }
  }
}

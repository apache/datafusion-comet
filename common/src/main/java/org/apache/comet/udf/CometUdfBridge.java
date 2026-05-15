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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.TaskContext;
import org.apache.spark.comet.CometTaskContextShim;
import org.apache.spark.util.TaskCompletionListener;

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
 *   <li>At any instant at most one thread is inside {@code evaluate()} for a given {@code
 *       taskAttemptId}. This follows from Spark executing one native future per partition and Tokio
 *       polling one future per worker at a time.
 *   <li>All instances for a task are dropped by the {@link TaskCompletionListener} registered on
 *       the first cache miss for that task. No cache entry outlives its task.
 *   <li>When {@code taskContext} is {@code null} (unit tests, direct native driver) the fallback
 *       key {@code -1L} is used; that bucket is never evicted because no task-completion event will
 *       fire.
 * </ol>
 *
 * <p>Keying by {@code taskAttemptId} rather than by thread keeps the cache correct under Tokio
 * work-stealing: on the scan-free execution path the same Spark task can be polled by different
 * Tokio workers across batches, so a thread-local cache would lose per-task state on migration. The
 * task attempt ID is stable for the life of the task regardless of which worker is polling.
 */
public class CometUdfBridge {

  /**
   * Task-scoped cache of {@link CometUDF} instances. Outer map keys are Spark task attempt IDs (or
   * {@code -1L} when no {@link TaskContext} is available). Inner maps hold one instance per UDF
   * class name for the task's lifetime. Entries are removed by the {@link TaskCompletionListener}
   * registered on the first cache miss per task.
   */
  private static final ConcurrentHashMap<Long, ConcurrentHashMap<String, CometUDF>> INSTANCES =
      new ConcurrentHashMap<>();

  /** Sentinel key for calls that carry no {@link TaskContext} (unit tests, direct driver). */
  private static final long NO_TASK_ID = -1L;

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
   *     left on a worker by a previous task. Its task attempt ID also keys the UDF-instance cache,
   *     so a UDF holding per-task state in fields sees a consistent instance for every call within
   *     the task regardless of which Tokio worker is polling.
   */
  public static void evaluate(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows,
      TaskContext taskContext) {
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
    // on a Spark task thread, or (b) stale from a prior task on a reused Tokio worker.
    TaskContext prior = TaskContext.get();
    if (taskContext != null) {
      CometTaskContextShim.set(taskContext);
      assert TaskContext.get() == taskContext
          : "TaskContext install did not take effect on this thread";
    }
    try {
      evaluateInternal(
          udfClassName,
          inputArrayPtrs,
          inputSchemaPtrs,
          outArrayPtr,
          outSchemaPtr,
          numRows,
          taskContext);
    } finally {
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
      TaskContext taskContext) {
    long taskAttemptId = (taskContext != null) ? taskContext.taskAttemptId() : NO_TASK_ID;

    ConcurrentHashMap<String, CometUDF> perTask =
        INSTANCES.computeIfAbsent(
            taskAttemptId,
            id -> {
              ConcurrentHashMap<String, CometUDF> fresh = new ConcurrentHashMap<>();
              if (taskContext != null) {
                // computeIfAbsent runs this lambda at most once per key, so the listener is
                // registered exactly once per task attempt.
                taskContext.addTaskCompletionListener(
                    (TaskCompletionListener)
                        ctx -> {
                          ConcurrentHashMap<String, CometUDF> removed = INSTANCES.remove(id);
                          assert removed != null
                              : "task-completion listener fired but cache already removed "
                                  + "entry for task "
                                  + id;
                        });
              }
              return fresh;
            });
    assert perTask != null : "per-task cache must be non-null after computeIfAbsent";

    CometUDF udf =
        perTask.computeIfAbsent(
            udfClassName,
            name -> {
              try {
                // Resolve via the executor's context classloader so user-supplied UDF jars
                // (added via spark.jars / --jars) are visible.
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                if (cl == null) {
                  cl = CometUdfBridge.class.getClassLoader();
                }
                return (CometUDF)
                    Class.forName(name, true, cl).getDeclaredConstructor().newInstance();
              } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate CometUDF: " + name, e);
              }
            });
    assert udf != null : "reflective instantiation returned null for " + udfClassName;

    BufferAllocator allocator = org.apache.comet.package$.MODULE$.CometArrowAllocator();

    ValueVector[] inputs = new ValueVector[inputArrayPtrs.length];
    ValueVector result = null;
    try {
      for (int i = 0; i < inputArrayPtrs.length; i++) {
        ArrowArray inArr = ArrowArray.wrap(inputArrayPtrs[i]);
        ArrowSchema inSch = ArrowSchema.wrap(inputSchemaPtrs[i]);
        inputs[i] = Data.importVector(allocator, inArr, inSch, null);
      }

      result = udf.evaluate(inputs, numRows);
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
      Data.exportVector(allocator, (FieldVector) result, null, outArr, outSch);
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
    }
  }
}

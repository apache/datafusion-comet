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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.TaskContext;
import org.apache.spark.comet.CometTaskContextShim;

/**
 * JNI entry point for native execution to invoke a {@link CometUDF}. Matches the static-method
 * pattern used by CometScalarSubquery so the native side can dispatch via
 * call_static_method_unchecked.
 */
public class CometUdfBridge {

  // Per-thread, bounded LRU of UDF instances keyed by class name. Comet
  // native execution threads (Tokio/DataFusion worker pool) are reused
  // across tasks within an executor, so the effective lifetime of cached
  // entries is the worker thread (i.e. the executor JVM). This is fine for
  // stateless UDFs like RegExpLikeUDF; future stateful UDFs would need
  // explicit per-task isolation.
  private static final int CACHE_CAPACITY = 64;

  private static final ThreadLocal<LinkedHashMap<String, CometUDF>> INSTANCES =
      ThreadLocal.withInitial(
          () ->
              new LinkedHashMap<String, CometUDF>(CACHE_CAPACITY, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CometUDF> eldest) {
                  return size() > CACHE_CAPACITY;
                }
              });

  /**
   * Called from native via JNI.
   *
   * @param udfClassName fully-qualified class name implementing CometUDF
   * @param inputArrayPtrs addresses of pre-allocated FFI_ArrowArray structs (one per input)
   * @param inputSchemaPtrs addresses of pre-allocated FFI_ArrowSchema structs (one per input)
   * @param outArrayPtr address of pre-allocated FFI_ArrowArray for the result
   * @param outSchemaPtr address of pre-allocated FFI_ArrowSchema for the result
   * @param numRows number of rows in the current batch. Mirrors DataFusion's {@code
   *     ScalarFunctionArgs.number_rows} and gives UDFs an explicit batch-size signal for cases
   *     where no input arg is a batch-length array (e.g. a zero-arg non-deterministic ScalaUDF).
   *     UDFs that already read size from their input vectors can ignore it.
   * @param taskContext Spark {@link TaskContext} captured on the driving Spark task thread and
   *     passed through from native. May be {@code null} when the bridge is invoked outside a Spark
   *     task (unit tests, direct native driver runs). When non-null and the current thread has no
   *     {@code TaskContext} of its own, the bridge installs it as the thread-local for the duration
   *     of the UDF call so the UDF body (including partition-sensitive built-ins like {@code Rand}
   *     / {@code Uuid} / {@code MonotonicallyIncreasingID} that read the partition index via {@code
   *     TaskContext.get().partitionId()}) sees the real context rather than null. The thread-local
   *     is cleared in a {@code finally} so Tokio workers don't leak a stale TaskContext across
   *     invocations.
   */
  public static void evaluate(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows,
      TaskContext taskContext) {
    boolean installedTaskContext = false;
    if (taskContext != null && TaskContext.get() == null) {
      CometTaskContextShim.set(taskContext);
      installedTaskContext = true;
    }
    try {
      evaluateInternal(
          udfClassName, inputArrayPtrs, inputSchemaPtrs, outArrayPtr, outSchemaPtr, numRows);
    } finally {
      if (installedTaskContext) {
        CometTaskContextShim.unset();
      }
    }
  }

  private static void evaluateInternal(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows) {
    LinkedHashMap<String, CometUDF> cache = INSTANCES.get();
    CometUDF udf = cache.get(udfClassName);
    if (udf == null) {
      try {
        // Resolve via the executor's context classloader so user-supplied UDF jars
        // (added via spark.jars / --jars) are visible.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
          cl = CometUdfBridge.class.getClassLoader();
        }
        udf =
            (CometUDF) Class.forName(udfClassName, true, cl).getDeclaredConstructor().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to instantiate CometUDF: " + udfClassName, e);
      }
      cache.put(udfClassName, udf);
    }

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

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

/**
 * JNI entry point for native execution to invoke a {@link CometUDF}. Matches the static-method
 * pattern used by CometScalarSubquery so the native side can dispatch via
 * call_static_method_unchecked.
 */
public class CometUdfBridge {

  // Process-wide cache of UDF instances keyed by class name. CometUDF
  // implementations are required to be stateless (see CometUDF), so a
  // single shared instance per class is safe across native worker threads.
  private static final ConcurrentHashMap<String, CometUDF> INSTANCES = new ConcurrentHashMap<>();

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
   *     left on a worker by a previous task.
   */
  public static void evaluate(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr,
      int numRows,
      TaskContext taskContext) {
    // Save-and-restore rather than only-install-if-null: the propagated context is the ground
    // truth for this call. Any value already on the thread is either (a) the same object on a
    // Spark task thread, or (b) stale from a prior task on a reused Tokio worker.
    TaskContext prior = TaskContext.get();
    if (taskContext != null) {
      CometTaskContextShim.set(taskContext);
    }
    try {
      evaluateInternal(
          udfClassName, inputArrayPtrs, inputSchemaPtrs, outArrayPtr, outSchemaPtr, numRows);
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
      int numRows) {
    CometUDF udf =
        INSTANCES.computeIfAbsent(
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

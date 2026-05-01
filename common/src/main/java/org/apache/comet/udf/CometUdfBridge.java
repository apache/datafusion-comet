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

import java.util.HashMap;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

/**
 * JNI entry point for native execution to invoke a {@link CometUDF}. Matches the static-method
 * pattern used by CometScalarSubquery so the native side can dispatch via
 * call_static_method_unchecked.
 */
public class CometUdfBridge {

  // Per-thread cache of UDF instances keyed by class name. Comet native
  // execution threads (Tokio/DataFusion worker pool) are reused across
  // tasks within an executor, so the effective lifetime of cached entries
  // is the worker thread (i.e. the executor JVM). This is fine for
  // stateless UDFs like RegExpLikeUDF; future stateful UDFs would need
  // explicit per-task isolation.
  private static final ThreadLocal<HashMap<String, CometUDF>> INSTANCES =
      ThreadLocal.withInitial(HashMap::new);

  /**
   * Called from native via JNI.
   *
   * @param udfClassName fully-qualified class name implementing CometUDF
   * @param inputArrayPtrs addresses of pre-allocated FFI_ArrowArray structs (one per input)
   * @param inputSchemaPtrs addresses of pre-allocated FFI_ArrowSchema structs (one per input)
   * @param outArrayPtr address of pre-allocated FFI_ArrowArray for the result
   * @param outSchemaPtr address of pre-allocated FFI_ArrowSchema for the result
   */
  public static void evaluate(
      String udfClassName,
      long[] inputArrayPtrs,
      long[] inputSchemaPtrs,
      long outArrayPtr,
      long outSchemaPtr) {
    HashMap<String, CometUDF> cache = INSTANCES.get();
    CometUDF udf = cache.get(udfClassName);
    if (udf == null) {
      try {
        udf = (CometUDF) Class.forName(udfClassName).getDeclaredConstructor().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to instantiate CometUDF: " + udfClassName, e);
      }
      cache.put(udfClassName, udf);
    }

    BufferAllocator allocator = org.apache.comet.package$.MODULE$.CometArrowAllocator();

    ValueVector[] inputs = new ValueVector[inputArrayPtrs.length];
    for (int i = 0; i < inputArrayPtrs.length; i++) {
      ArrowArray inArr = ArrowArray.wrap(inputArrayPtrs[i]);
      ArrowSchema inSch = ArrowSchema.wrap(inputSchemaPtrs[i]);
      inputs[i] = Data.importVector(allocator, inArr, inSch, null);
    }

    ValueVector result = udf.evaluate(inputs);
    if (!(result instanceof FieldVector)) {
      throw new RuntimeException(
          "CometUDF.evaluate() must return a FieldVector, got: " + result.getClass().getName());
    }
    ArrowArray outArr = ArrowArray.wrap(outArrayPtr);
    ArrowSchema outSch = ArrowSchema.wrap(outSchemaPtr);
    Data.exportVector(allocator, (FieldVector) result, null, outArr, outSch);

    for (ValueVector v : inputs) {
      v.close();
    }
    result.close();
  }
}

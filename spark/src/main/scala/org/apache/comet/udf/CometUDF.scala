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

package org.apache.comet.udf

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector

/**
 * Scalar UDF invoked from native execution via JNI. Receives Arrow vectors as input and returns
 * an Arrow vector.
 *
 *   - Vector arguments arrive at the row count of the current batch.
 *   - Scalar (literal-folded) arguments arrive as length-1 vectors and must be read at index 0.
 *   - The returned vector's length must match `numRows`.
 *   - Returned vectors and temporary buffers must use `allocator`. In off-heap mode, allocations
 *     are charged to the current Spark task.
 *
 * `numRows` mirrors DataFusion's `ScalarFunctionArgs.number_rows` and is the batch row count.
 * UDFs that always have at least one batch-length input can read length from it and ignore
 * `numRows`; UDFs that may be called with zero data columns (e.g. a zero-arg ScalaUDF through the
 * codegen dispatcher) need `numRows` to know how many rows to produce.
 *
 * Implementations must have a public no-arg constructor. A fresh instance is created per Spark
 * task attempt per class and reused for every call within that task. Instances may hold per-task
 * state in fields (counters, compiled patterns, scratch buffers); instances are dropped at task
 * completion. Do not hold state that must persist across tasks.
 *
 * Native execution may call `evaluate` concurrently from multiple Tokio workers within one task.
 * Implementations with mutable state must synchronize access.
 */
trait CometUDF {

  /**
   * Legacy entry point retained for implementations compiled against earlier Comet releases. New
   * implementations should override the allocator-aware overload below.
   */
  def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
    throw new UnsupportedOperationException("CometUDF.evaluate is not implemented")

  def evaluate(
      allocator: BufferAllocator,
      inputs: Array[ValueVector],
      numRows: Int): ValueVector = evaluate(inputs, numRows)
}

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
 *   - `allocator` is the per-task Arrow allocator backed by Spark's task memory accounting (see
 *     `CometUdfAllocator`). Implementations must allocate any returned vector or temporary
 *     buffers from this allocator so off-heap usage is charged to the executing Spark task.
 *
 * `numRows` mirrors DataFusion's `ScalarFunctionArgs.number_rows` and is the batch row count.
 * UDFs that always have at least one batch-length input can derive length from the inputs and
 * ignore `numRows`; UDFs that may be called with zero data columns (e.g. a zero-arg ScalaUDF)
 * need `numRows` to know how many rows to produce.
 *
 * Implementations must have a public no-arg constructor and must be stateless: a single instance
 * per class is cached and shared across native worker threads for the lifetime of the JVM.
 */
trait CometUDF {
  def evaluate(allocator: BufferAllocator, inputs: Array[ValueVector], numRows: Int): ValueVector
}

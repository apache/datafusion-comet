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

import org.apache.arrow.vector.ValueVector

/**
 * Scalar UDF invoked from native execution via JNI. Receives Arrow vectors as input and returns
 * an Arrow vector.
 *
 *   - Vector arguments arrive at the row count of the current batch.
 *   - Scalar (literal-folded) arguments arrive as length-1 vectors and must be read at index 0.
 *   - The returned vector's length must match `numRows`.
 *   - Inputs belong to native execution and must not be modified. Each call receives new vectors,
 *     but a scalar argument's buffers are the same on every call for the life of the expression,
 *     and a vector argument can share its buffers with the rest of the plan.
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
 * At most one thread calls `evaluate` on a given instance at a time: Spark runs one native future
 * per partition and Tokio polls one future per worker, so the per-task instance is never touched
 * concurrently even if the task's future migrates between Tokio workers across batches.
 */
trait CometUDF {
  def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector

  /**
   * The overload the bridge calls. `partitionIndex` is the index of the partition the calling
   * native plan computes, which differs from `TaskContext.partitionId()` under a union, a
   * coalesce or a cartesian product. `planId` tells apart the plans one task runs, such as the
   * parent partitions of a coalesce. The default ignores both.
   */
  def evaluate(
      inputs: Array[ValueVector],
      numRows: Int,
      partitionIndex: Int,
      planId: Long): ValueVector =
    evaluate(inputs, numRows)

  /**
   * Called once native plan `planId` has closed, so an implementation that keeps state per plan
   * can drop it before the task ends. The default does nothing.
   */
  def releasePlan(planId: Long): Unit = ()
}

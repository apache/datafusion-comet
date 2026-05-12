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

package org.apache.spark.sql.comet.shims

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 3.4 shim for the PyArrow UDF acceleration support.
 *
 * Spark 3.4 lacks several APIs that the optimization relies on (`isBarrier` on `MapInBatchExec`,
 * `arrowUseLargeVarTypes`, `JobArtifactSet`, the modern `ArrowPythonRunner` constructor), so the
 * matchers return `None` and the runner factory throws. The optimization is effectively a no-op
 * on Spark 3.4.
 */
trait ShimCometMapInBatch {

  protected def matchMapInArrow(plan: SparkPlan): Option[MapInBatchInfo] = None

  protected def matchMapInPandas(plan: SparkPlan): Option[MapInBatchInfo] = None

  /** Stub; never constructed on Spark 3.4 because the matchers always return `None`. */
  protected case class RunnerInputs()

  protected def runnerInputs(pythonUDF: PythonUDF, conf: SQLConf): RunnerInputs =
    throw new UnsupportedOperationException("CometMapInBatchExec is not supported on Spark 3.4")

  protected def computeArrowPython(
      runnerInputs: RunnerInputs,
      evalType: Int,
      argOffsets: Array[Array[Int]],
      schema: StructType,
      pythonMetrics: Map[String, SQLMetric],
      batchIter: Iterator[Iterator[ColumnarBatch]],
      partitionId: Int,
      context: TaskContext): Iterator[ColumnarBatch] =
    throw new UnsupportedOperationException("CometMapInBatchExec is not supported on Spark 3.4")
}

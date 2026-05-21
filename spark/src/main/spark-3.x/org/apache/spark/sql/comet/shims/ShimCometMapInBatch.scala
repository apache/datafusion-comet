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
 * Spark 3.x stub for the PyArrow UDF acceleration support.
 *
 * The columnar runner introduced in #4234 only targets Spark 4.0+. On Spark 3.4 / 3.5 the matchers
 * return `None`, the rewrite does not fire, and vanilla Spark handles `mapInArrow` /
 * `mapInPandas` unchanged. The runner factory throws; it is never called because the matchers
 * always return `None`. 3.x support can be added later if there is user demand.
 *
 * Shared across spark-3.4 and spark-3.5 because both are identical: 3.4 lacks the modern
 * `ArrowPythonRunner` constructor and `arrowUseLargeVarTypes`, and 3.5's `PythonArrowInput`
 * trait has a different contract (`writeIteratorToArrowStream` one-shot vs 4.x's
 * `writeNextBatchToArrowStream` batch-at-a-time), so neither version can host the columnar input
 * implementation without a separate rewrite.
 */
trait ShimCometMapInBatch {

  protected def matchMapInArrow(plan: SparkPlan): Option[MapInBatchInfo] = None

  protected def matchMapInPandas(plan: SparkPlan): Option[MapInBatchInfo] = None

  /** Stub; never constructed on Spark 3.x because the matchers always return `None`. */
  protected case class RunnerInputs()

  protected def runnerInputs(pythonUDF: PythonUDF, conf: SQLConf): RunnerInputs =
    throw new UnsupportedOperationException("CometMapInBatchExec is not supported on Spark 3.x")

  protected def computeArrowPython(
      runnerInputs: RunnerInputs,
      evalType: Int,
      argOffsets: Array[Array[Int]],
      schema: StructType,
      pythonMetrics: Map[String, SQLMetric],
      batchIter: Iterator[Iterator[ColumnarBatch]],
      partitionId: Int,
      context: TaskContext): Iterator[ColumnarBatch] =
    throw new UnsupportedOperationException("CometMapInBatchExec is not supported on Spark 3.x")
}

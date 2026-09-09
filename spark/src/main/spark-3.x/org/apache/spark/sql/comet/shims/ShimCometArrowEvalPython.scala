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
import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 3.x stub for scalar Python UDF acceleration.
 *
 * Like the `mapInArrow` / `mapInPandas` support it builds on, the columnar runner targets Spark
 * 4.0+ only, so the matcher returns `None` on 3.4 / 3.5 and vanilla Spark evaluates
 * `ArrowEvalPythonExec` unchanged. The runner factory throws; it is never called because the
 * matcher always returns `None`.
 */
trait ShimCometArrowEvalPython extends ShimPythonRunnerInputs {

  protected def matchArrowEvalPython(plan: SparkPlan): Option[ArrowEvalPythonInfo] = None

  protected def resolveArrowEvalPythonArgs(
      udfs: Seq[PythonUDF],
      childOutput: Seq[Attribute]): Option[ArrowEvalPythonArgs] = None

  protected def computeArrowEvalPython(
      runnerInputs: RunnerInputs,
      evalType: Int,
      argOffsets: Array[Array[Int]],
      schema: StructType,
      pythonMetrics: Map[String, SQLMetric],
      batchIter: Iterator[Iterator[ColumnarBatch]],
      partitionId: Int,
      context: TaskContext): Iterator[ColumnarBatch] =
    throw new UnsupportedOperationException(
      "CometArrowEvalPythonExec is not supported on Spark 3.x")
}

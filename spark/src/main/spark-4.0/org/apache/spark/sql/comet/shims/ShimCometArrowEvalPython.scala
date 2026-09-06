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
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.CometArrowEvalPythonRunner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ShimCometArrowEvalPython extends Spark4xArrowEvalPythonSupport {

  // `SQL_SCALAR_ARROW_UDF` (the `@arrow_udf` decorator) arrives in Spark 4.1.
  protected def supportedEvalTypes: Set[Int] =
    Set(PythonEvalType.SQL_ARROW_BATCHED_UDF, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

  protected def computeArrowEvalPython(
      runnerInputs: RunnerInputs,
      evalType: Int,
      argOffsets: Array[Array[Int]],
      schema: StructType,
      pythonMetrics: Map[String, SQLMetric],
      batchIter: Iterator[Iterator[ColumnarBatch]],
      partitionId: Int,
      context: TaskContext): Iterator[ColumnarBatch] =
    new CometArrowEvalPythonRunner(
      runnerInputs.chainedFunc,
      evalType,
      argOffsets,
      schema,
      runnerInputs.pythonRunnerConf,
      pythonMetrics,
      runnerInputs.jobArtifactUUID).compute(batchIter, partitionId, context)
}

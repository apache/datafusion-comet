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

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, MapInPandasExec, PythonMapInArrowExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ShimCometMapInBatch {

  protected def matchMapInArrow(plan: SparkPlan): Option[MapInBatchInfo] =
    plan match {
      case p: PythonMapInArrowExec =>
        Some(
          MapInBatchInfo(
            p.func,
            p.output,
            p.child,
            p.isBarrier,
            PythonEvalType.SQL_MAP_ARROW_ITER_UDF))
      case _ => None
    }

  protected def matchMapInPandas(plan: SparkPlan): Option[MapInBatchInfo] =
    plan match {
      case p: MapInPandasExec =>
        Some(
          MapInBatchInfo(
            p.func,
            p.output,
            p.child,
            p.isBarrier,
            PythonEvalType.SQL_MAP_PANDAS_ITER_UDF))
      case _ => None
    }

  protected def computeArrowPython(
      pythonUDF: PythonUDF,
      evalType: Int,
      argOffsets: Array[Array[Int]],
      schema: StructType,
      conf: SQLConf,
      pythonMetrics: Map[String, SQLMetric],
      batchIter: Iterator[Iterator[InternalRow]],
      partitionId: Int,
      context: TaskContext): Iterator[ColumnarBatch] = {
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pythonUDF.func)))
    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
    new ArrowPythonRunner(
      chainedFunc,
      evalType,
      argOffsets,
      schema,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      ArrowPythonRunner.getPythonRunnerConfMap(conf),
      pythonMetrics,
      jobArtifactUUID).compute(batchIter, partitionId, context)
  }
}

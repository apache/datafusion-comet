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
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, MapInArrowExec, MapInPandasExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Shared 4.x bits for `ShimCometMapInBatch`. The matchers and `getRunnerInputs` helper are
 * identical across 4.0/4.1/4.2; only the `ArrowPythonRunner` constructor parameter list differs
 * per minor, so each minor's `ShimCometMapInBatch` provides only `computeArrowPython`.
 */
trait Spark4xMapInBatchSupport {

  protected def matchMapInArrow(plan: SparkPlan): Option[MapInBatchInfo] =
    plan match {
      case p: MapInArrowExec =>
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

  /** Inputs every 4.x `ArrowPythonRunner` constructor needs in the same shape. */
  protected case class RunnerInputs(
      chainedFunc: Seq[(ChainedPythonFunctions, Long)],
      timeZoneId: String,
      largeVarTypes: Boolean,
      pythonRunnerConf: Map[String, String],
      jobArtifactUUID: Option[String])

  protected def runnerInputs(pythonUDF: PythonUDF, conf: SQLConf): RunnerInputs =
    RunnerInputs(
      chainedFunc = Seq((ChainedPythonFunctions(Seq(pythonUDF.func)), pythonUDF.resultId.id)),
      timeZoneId = conf.sessionLocalTimeZone,
      largeVarTypes = conf.arrowUseLargeVarTypes,
      pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf),
      jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid))
}

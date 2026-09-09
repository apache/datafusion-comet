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

package org.apache.spark.sql.execution.python

import java.io.DataOutputStream

import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Comet's Arrow Python runner for scalar Python UDFs on Spark 4.0. The Arrow IPC exchange lives
 * in [[CometArrowPythonRunnerBase]]; this subclass only supplies the Spark 4.0 constructor shape
 * and UDF command serialization.
 *
 * Unlike the `mapInArrow` / `mapInPandas` runner, the input columns travel as top-level columns
 * rather than beneath a struct wrapper, matching the flat `_0`, `_1`, ... schema the worker
 * expects for these eval types.
 */
class CometArrowEvalPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    override val schema: StructType,
    override val workerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
    extends BasePythonRunner[Iterator[ColumnarBatch], ColumnarBatch](
      funcs.map(_._1),
      evalType,
      argOffsets,
      jobArtifactUUID,
      pythonMetrics)
    with CometArrowPythonRunnerBase {

  override protected def wrapInputInStruct: Boolean = false

  /**
   * Spark's `ArrowEvalPythonExec` always serializes these UDFs through
   * `ArrowPythonWithNamedArgumentRunner`, so the worker reads a keyword-name flag after every
   * argument offset. Comet uses the same `ArgumentMetadata` form to stay on that protocol, always
   * with an absent name: an operator whose UDF takes a keyword argument never reaches the native
   * path, because a `NamedArgumentExpression` is not an `Attribute`.
   *
   * Spark 4.0's worker does not read an input schema for `SQL_ARROW_BATCHED_UDF` (it goes
   * straight to the profiler flag), so nothing is written before the UDFs. Spark 4.1 is the one
   * version that does; see its copy of this runner.
   */
  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    val argMetas = argOffsets.map(_.map(offset => ArgumentMetadata(offset, None)))
    PythonUDFRunner.writeUDFs(dataOut, funcs, argMetas, None)
  }
}

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

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Comet's Arrow Python runner for Spark 4.1. Extends `BaseArrowPythonRunner` parameterized over
 * `Iterator[ColumnarBatch]` input, and supplies the columnar input via `CometColumnarPythonInput`
 * instead of `BasicPythonArrowInput`.
 *
 * Spark 4.1's `PythonUDFRunner.writeUDFs` takes a `profiler: Option[String]` fourth argument; we
 * pass `None` since Comet does not support Python profiling.
 */
class CometArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    schema: StructType,
    timeZoneId: String,
    largeVarTypes: Boolean,
    workerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
    extends BaseArrowPythonRunner[Iterator[ColumnarBatch], ColumnarBatch](
      funcs,
      evalType,
      argOffsets,
      schema,
      timeZoneId,
      largeVarTypes,
      workerConf,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID)
    with CometColumnarPythonInput
    with BasicPythonArrowOutput {

  override protected def writeUDF(dataOut: DataOutputStream): Unit =
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, None)
}

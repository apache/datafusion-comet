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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Comet's Arrow Python runner for Spark 4.0. Extends `BasePythonRunner` directly because Spark
 * 4.0's `BaseArrowPythonRunner` is bound to `Iterator[InternalRow]` and mixes in
 * `BasicPythonArrowInput`, so we cannot inherit from it. Wires the SQLConf-driven fields that
 * `BaseArrowPythonRunner` provides.
 */
class CometArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    protected override val schema: StructType,
    protected override val timeZoneId: String,
    protected override val largeVarTypes: Boolean,
    override val workerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
    extends BasePythonRunner[Iterator[ColumnarBatch], ColumnarBatch](
      funcs.map(_._1),
      evalType,
      argOffsets,
      jobArtifactUUID,
      pythonMetrics)
    with CometColumnarPythonInput
    with BasicPythonArrowOutput {

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(funcs.head._1.funcs.head.pythonExec)

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
  override val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
  override val errorOnDuplicatedFieldNames: Boolean = true
  override val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  override protected def writeUDF(dataOut: DataOutputStream): Unit =
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, jobArtifactUUID)
}

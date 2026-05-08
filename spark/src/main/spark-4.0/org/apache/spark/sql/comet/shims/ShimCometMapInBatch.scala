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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ShimCometMapInBatch extends Spark4xMapInBatchSupport {

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
    val r = runnerInputs(pythonUDF, conf)
    new ArrowPythonRunner(
      r.chainedFunc,
      evalType,
      argOffsets,
      schema,
      r.timeZoneId,
      r.largeVarTypes,
      r.pythonRunnerConf,
      pythonMetrics,
      r.jobArtifactUUID,
      None).compute(batchIter, partitionId, context)
  }
}

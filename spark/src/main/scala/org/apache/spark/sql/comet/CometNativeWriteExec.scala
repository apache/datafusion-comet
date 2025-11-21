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

package org.apache.spark.sql.comet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet physical operator for native Parquet write operations.
 *
 * This operator writes data to Parquet files using the native Comet engine. It wraps the child
 * operator and adds a ParquetWriter operator on top.
 *
 * @param nativeOp
 *   The native operator representing the write operation
 * @param child
 *   The child operator providing the data to write
 * @param outputPath
 *   The path where the Parquet file will be written
 */
case class CometNativeWriteExec(nativeOp: Operator, child: SparkPlan, outputPath: String)
    extends CometNativeExec
    with UnaryExecNode {

  override def originalPlan: SparkPlan = child

  override def serializedPlanOpt: SerializedPlan = {
    val outputStream = new java.io.ByteArrayOutputStream()
    nativeOp.writeTo(outputStream)
    outputStream.close()
    SerializedPlan(Some(outputStream.toByteArray))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def nodeName: String = "CometNativeWrite"

  override def doExecute(): RDD[InternalRow] = {
    // Execute the native write
    val resultRDD = doExecuteColumnar()
    // Convert to empty InternalRow RDD (write operations typically return empty results)
    resultRDD.mapPartitions { iter =>
      // Consume all batches (they should be empty)
      iter.foreach(_.close())
      Iterator.empty
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Get the input data from the child operator
    val childRDD = if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      // If child doesn't support columnar, convert to columnar
      child.execute().mapPartitionsInternal { _ =>
        // TODO this could delegate to CometRowToColumnar, but maybe Comet
        // does not need to support this case?
        throw new UnsupportedOperationException(
          "Row-based child operators not yet supported for native write")
      }
    }

    // Capture metadata before the transformation
    val numPartitions = childRDD.getNumPartitions
    val numOutputCols = child.output.length

    // Execute native write operation
    childRDD.mapPartitionsInternal { iter =>
      val nativeMetrics = CometMetricNode.fromCometPlan(this)

      val outputStream = new java.io.ByteArrayOutputStream()
      nativeOp.writeTo(outputStream)
      outputStream.close()
      val planBytes = outputStream.toByteArray

      new CometExecIterator(
        CometExec.newIterId,
        Seq(iter),
        numOutputCols,
        planBytes,
        nativeMetrics,
        numPartitions,
        org.apache.spark.TaskContext.getPartitionId(),
        None,
        Seq.empty)

    }
  }
}

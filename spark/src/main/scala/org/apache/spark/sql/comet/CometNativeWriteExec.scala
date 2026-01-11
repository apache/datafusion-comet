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

import java.io.ByteArrayOutputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet physical operator for native Parquet write operations.
 *
 * This operator writes data to Parquet files using the native Comet engine. Files are written
 * directly to the output path. The commit protocol is handled by Spark's
 * InsertIntoHadoopFsRelationCommand which manages the FileCommitProtocol separately.
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
    val outputStream = new ByteArrayOutputStream()
    nativeOp.writeTo(outputStream)
    outputStream.close()
    SerializedPlan(Some(outputStream.toByteArray))
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def nodeName: String = "CometNativeWrite"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "files_written" -> SQLMetrics.createMetric(sparkContext, "number of written data files"),
    "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "written data"),
    "rows_written" -> SQLMetrics.createMetric(sparkContext, "number of written rows"))

  override def doExecute(): RDD[InternalRow] = {
    // Execute the native write
    val resultRDD = doExecuteColumnar()

    // Force execution by consuming all batches
    resultRDD
      .mapPartitions { iter =>
        iter.foreach(_.close())
        Iterator.empty
      }
      .count()

    // Return empty RDD as write operations don't return data
    sparkContext.emptyRDD[InternalRow]
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Get the input data from the child operator
    val childRDD = if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      child.execute().mapPartitionsInternal { _ =>
        throw new UnsupportedOperationException(
          "Row-based child operators not yet supported for native write")
      }
    }

    // Capture metadata before the transformation
    val numPartitions = childRDD.getNumPartitions
    val numOutputCols = child.output.length
    val capturedNativeOp = nativeOp

    // Execute native write operation
    childRDD.mapPartitionsInternal { iter =>
      val partitionId = org.apache.spark.TaskContext.getPartitionId()
      val taskAttemptId = org.apache.spark.TaskContext.get().taskAttemptId()

      val nativeMetrics = CometMetricNode.fromCometPlan(this)

      val outputStream = new ByteArrayOutputStream()
      capturedNativeOp.writeTo(outputStream)
      outputStream.close()
      val planBytes = outputStream.toByteArray

      val execIterator = new CometExecIterator(
        CometExec.newIterId,
        Seq(iter),
        numOutputCols,
        planBytes,
        nativeMetrics,
        numPartitions,
        partitionId,
        None,
        Seq.empty)

      // Return iterator that produces empty columnar batches (write side effect only)
      new Iterator[ColumnarBatch] {
        private var hasMore = true

        override def hasNext: Boolean = {
          if (hasMore) {
            hasMore = execIterator.hasNext
            if (hasMore) {
              // Consume any batches produced by the write operation
              execIterator.next()
            }
          }
          false // Write operations don't produce output batches
        }

        override def next(): ColumnarBatch = {
          throw new NoSuchElementException("Write operation produces no output")
        }
      }
    }
  }
}

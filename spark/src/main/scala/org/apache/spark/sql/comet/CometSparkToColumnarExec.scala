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

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.DataTypeSupport

case class CometSparkToColumnarExec(child: SparkPlan)
    extends RowToColumnarTransition
    with CometPlan {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.executeBroadcast()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "time converting Spark batches to Arrow batches"))

  // The conversion happens in next(), so wrap the call to measure time spent.
  private def createTimingIter(
      iter: Iterator[ColumnarBatch],
      numInputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      conversionTime: SQLMetric): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {

      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): ColumnarBatch = {
        val startNs = System.nanoTime()
        val batch = iter.next()
        conversionTime += System.nanoTime() - startNs
        numInputRows += batch.numRows()
        numOutputBatches += 1
        batch
      }
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val conversionTime = longMetric("conversionTime")
    val maxRecordsPerBatch = conf.arrowMaxRecordsPerBatch
    val timeZoneId = conf.sessionLocalTimeZone
    val schema = child.schema

    if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitionsInternal { sparkBatches =>
          val arrowBatches =
            sparkBatches.flatMap { sparkBatch =>
              val context = TaskContext.get()
              CometArrowConverters.columnarBatchToArrowBatchIter(
                sparkBatch,
                schema,
                maxRecordsPerBatch,
                timeZoneId,
                context)
            }
          createTimingIter(arrowBatches, numInputRows, numOutputBatches, conversionTime)
        }
    } else {
      child
        .execute()
        .mapPartitionsInternal { sparkBatches =>
          val context = TaskContext.get()
          val arrowBatches =
            CometArrowConverters.rowToArrowBatchIter(
              sparkBatches,
              schema,
              maxRecordsPerBatch,
              timeZoneId,
              context)
          createTimingIter(arrowBatches, numInputRows, numOutputBatches, conversionTime)
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometSparkToColumnarExec =
    copy(child = newChild)

}

object CometSparkToColumnarExec extends DataTypeSupport {
  override def isAdditionallySupported(dt: DataType): Boolean = dt match {
    case _: StructType => true
    case _ => false
  }
}

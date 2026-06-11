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

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.arrow.{CometArrowStream, CometNativeArrowSource, RowArrowReader, SparkColumnarArrowReader}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, DataTypeSupport}
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.operator.CometSink

case class CometSparkToColumnarExec(child: SparkPlan)
    extends RowToColumnarTransition
    with CometPlan
    with CometNativeArrowSource {
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

  override def nodeName: String = if (child.supportsColumnar) {
    "CometSparkColumnarToColumnar"
  } else {
    "CometSparkRowToColumnar"
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "time converting Spark batches to Arrow batches"))

  /**
   * Build the per-partition `ArrowReader` (columnar or row, depending on the child); the trait
   * routes it to the JVM or native consumer.
   *
   * `numOutputBatches` is incremented from the reader's per-produced-batch callback rather than
   * by counting input batches, so it stays accurate on the native path too (native drives
   * `loadNextBatch`) and counts produced Arrow batches, not Spark input batches.
   */
  override protected def mapToReaders[T: ClassTag](
      consume: (String, BufferAllocator => ArrowReader) => Iterator[T]): RDD[T] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val conversionTime = longMetric("conversionTime")
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    val sparkSchema = child.schema
    val onConversionNs: Long => Unit = ns => {
      conversionTime += ns
      numOutputBatches += 1
    }

    if (child.supportsColumnar) {
      val maxBatchInt = maxRecordsPerBatch.toInt
      child.executeColumnar().mapPartitionsInternal { sparkBatches =>
        val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
        consume(
          "CometSparkColumnarToColumnar",
          new SparkColumnarArrowReader(
            _,
            arrowSchema,
            CometArrowStream
              .countingIterator(
                sparkBatches,
                (b: ColumnarBatch) => numInputRows.add(b.numRows())),
            maxBatchInt,
            onConversionNs))
      }
    } else {
      child.execute().mapPartitionsInternal { rowIter =>
        val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
        consume(
          "CometSparkRowToColumnar",
          new RowArrowReader(
            _,
            arrowSchema,
            CometArrowStream.countingIterator(rowIter, (_: InternalRow) => numInputRows.add(1)),
            maxRecordsPerBatch,
            onConversionNs))
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometSparkToColumnarExec =
    copy(child = newChild)

}

object CometSparkToColumnarExec extends CometSink[SparkPlan] with DataTypeSupport {

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: SparkPlan): CometNativeExec = {
    CometScanWrapper(nativeOp, CometSparkToColumnarExec(op))
  }

  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = dt match {
    case _: ArrayType | _: MapType => false
    case _ => super.isTypeSupported(dt, name, fallbackReasons)
  }
}

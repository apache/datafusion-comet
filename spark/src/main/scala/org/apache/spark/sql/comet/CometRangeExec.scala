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

import scala.reflect.ClassTag

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.comet.execution.arrow.{CometArrowStream, CometNativeArrowSource, RangeArrowReader}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{LeafExecNode, RangeExec, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometSink

/**
 * Comet's version of Spark's `RangeExec`, which produces the rows of `spark.range` and SQL
 * `range()`. It writes each partition's values straight into Arrow batches on the JVM and hands
 * them to the native plan above it, so the operators that consume a range can run natively
 * without converting Spark rows first. Partitions, values and their order match Spark's generated
 * code for `RangeExec`; see [[RangeArrowReader]].
 */
case class CometRangeExec(originalPlan: RangeExec, override val output: Seq[Attribute])
    extends CometExec
    with LeafExecNode
    with CometNativeArrowSource {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def mapToReaders[T: ClassTag](
      consume: (String, BufferAllocator => ArrowReader) => Iterator[T]): RDD[T] = {
    if (originalPlan.isEmptyRange) {
      sparkContext.emptyRDD[T]
    } else {
      val start = originalPlan.start
      val step = originalPlan.step
      val numSlices = originalPlan.numSlices
      // Spark's generated code reads the element count as a long, so it is truncated the same way.
      val numElements = originalPlan.numElements.toLong
      val numOutputRows = longMetric("numOutputRows")
      val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
      val sparkSchema = originalPlan.schema
      // Spark assigns slice `i` to partition `i` in the same way.
      sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndexInternal { (index, _) =>
          val taskContext = TaskContext.get()
          val inputMetrics = taskContext.taskMetrics().inputMetrics
          val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
          val (partitionStart, partitionElements) =
            CometRangeExec.partitionBounds(index, start, step, numSlices, numElements)
          consume(
            "CometRange",
            new RangeArrowReader(
              _,
              arrowSchema,
              partitionStart,
              step,
              partitionElements,
              maxRecordsPerBatch,
              taskContext,
              rows => {
                numOutputRows.add(rows)
                inputMetrics.incRecordsRead(rows)
              }))
        }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"$nodeName (${originalPlan.start}, ${originalPlan.end}, step=${originalPlan.step}, " +
      s"splits=${originalPlan.numSlices})"
  }

  override def doCanonicalize(): SparkPlan = {
    val canonical = originalPlan.canonicalized.asInstanceOf[RangeExec]
    CometRangeExec(canonical, canonical.output)
  }

  // `originalPlan` carries every parameter that decides the rows: start, end, step and the
  // number of slices.
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometRangeExec =>
        this.originalPlan == other.originalPlan && this.output == other.output
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, output)
}

object CometRangeExec extends CometSink[RangeExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_RANGE_ENABLED)

  override def createExec(nativeOp: Operator, op: RangeExec): CometNativeExec =
    CometScanWrapper(nativeOp, CometRangeExec(op, op.output))

  /**
   * The first value of partition `index` and the number of values in it, computed the way Spark's
   * generated code for `RangeExec` does (`initRange`): in `BigInteger` arithmetic, with the
   * partition's start and end clamped to the `Long` range.
   */
  private[comet] def partitionBounds(
      index: Int,
      start: Long,
      step: Long,
      numSlices: Int,
      numElements: Long): (Long, Long) = {
    def clamp(value: BigInt): Long =
      if (value > Long.MaxValue) {
        Long.MaxValue
      } else if (value < Long.MinValue) {
        Long.MinValue
      } else {
        value.toLong
      }

    val partitionStart = clamp(BigInt(index) * numElements / numSlices * step + start)
    val partitionEnd = clamp(BigInt(index + 1) * numElements / numSlices * step + start)
    val startToEnd = BigInt(partitionEnd) - partitionStart
    val count = (startToEnd / step).toLong
    if (count < 0) {
      (partitionStart, 0L)
    } else if ((startToEnd % step).signum != 0) {
      (partitionStart, count + 1)
    } else {
      (partitionStart, count)
    }
  }
}

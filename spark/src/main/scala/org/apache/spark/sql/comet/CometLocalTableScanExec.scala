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

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.comet.CometLocalTableScanExec.createMetricsIterator
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

case class CometLocalTableScanExec(
    originalPlan: LocalTableScanExec,
    @transient rows: Seq[InternalRow],
    override val output: Seq[Attribute])
    extends CometExec
    with LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }

  @transient private lazy val rdd: RDD[InternalRow] = {
    if (rows.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val numSlices = math.min(unsafeRows.length, session.leafNodeDefaultParallelism)
      sparkContext.parallelize(unsafeRows, numSlices)
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numOutputRows")
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    val timeZoneId = conf.sessionLocalTimeZone
    rdd.mapPartitionsInternal { sparkBatches =>
      val context = TaskContext.get()
      val batches = CometArrowConverters.rowToArrowBatchIter(
        sparkBatches,
        originalPlan.schema,
        maxRecordsPerBatch,
        timeZoneId,
        context)
      createMetricsIterator(batches, numInputRows)
    }
  }

  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def supportsColumnar: Boolean = true

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometLocalTableScanExec =>
        this.originalPlan == other.originalPlan &&
        this.schema == other.schema &&
        this.output == other.output
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, originalPlan.schema, output)
}

object CometLocalTableScanExec extends CometOperatorSerde[LocalTableScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED)

  override def convert(
      op: LocalTableScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val scanTypes = op.output.flatten(attr => serializeDataType(attr.dataType))
    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(op.getClass.getSimpleName)
      .addAllFields(scanTypes.asJava)
      .setArrowFfiSafe(false)
    Some(builder.setScan(scanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: LocalTableScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, CometLocalTableScanExec(op, op.rows, op.output))
  }

  private def createMetricsIterator(
      it: Iterator[ColumnarBatch],
      numInputRows: SQLMetric): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = it.hasNext

      override def next(): ColumnarBatch = {
        val batch = it.next()
        numInputRows.add(batch.numRows())
        batch
      }
    }
  }
}

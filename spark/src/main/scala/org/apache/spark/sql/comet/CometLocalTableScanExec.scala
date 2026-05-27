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

import org.apache.arrow.c.ArrowArrayStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.comet.execution.arrow.{CometArrowStream, CometNativeArrowSource, RowArrowReader}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry, DataTypeSupport}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometSink

case class CometLocalTableScanExec(
    originalPlan: LocalTableScanExec,
    @transient rows: Seq[InternalRow],
    override val output: Seq[Attribute])
    extends CometExec
    with LeafExecNode
    with CometNativeArrowSource {

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

  private def countingRows(
      iter: Iterator[InternalRow],
      numOutputRows: SQLMetric): Iterator[InternalRow] = new Iterator[InternalRow] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): InternalRow = {
      val row = iter.next()
      numOutputRows.add(1)
      row
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    val sparkSchema = originalPlan.schema
    rdd.mapPartitionsInternal { rowIter =>
      val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
      CometArrowStream.readerBatchIter(
        "CometLocalTableScan",
        new RowArrowReader(
          _,
          arrowSchema,
          countingRows(rowIter, numOutputRows),
          maxRecordsPerBatch))
    }
  }

  override def doExecuteAsArrowStream(): RDD[ArrowArrayStream] = {
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    val sparkSchema = originalPlan.schema
    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsInternal { rowIter =>
      val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
      CometArrowStream.stream(
        "CometLocalTableScan",
        allocator =>
          new RowArrowReader(
            allocator,
            arrowSchema,
            countingRows(rowIter, numOutputRows),
            maxRecordsPerBatch))
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

object CometLocalTableScanExec extends CometSink[LocalTableScanExec] with DataTypeSupport {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED)

  // ArrowWriter (used by RowArrowReader) handles NullType via Utils.toArrowType + NullWriter;
  // other types off DataTypeSupport's allow list (TimeType, intervals, ...) have no ArrowWriter
  // coverage and must fall back to Spark.
  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = dt match {
    case _: NullType => true
    case _ => super.isTypeSupported(dt, name, fallbackReasons)
  }

  override def convert(
      op: LocalTableScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val fallbackReasons = new ListBuffer[String]()
    if (!isSchemaSupported(op.schema, fallbackReasons)) {
      withInfo(op, fallbackReasons.mkString("; "))
      None
    } else {
      super.convert(op, builder, childOp: _*)
    }
  }

  override def createExec(nativeOp: Operator, op: LocalTableScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, CometLocalTableScanExec(op, op.rows, op.output))
  }
}

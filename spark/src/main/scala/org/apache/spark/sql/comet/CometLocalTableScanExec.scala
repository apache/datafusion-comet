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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.comet.execution.arrow.{CometArrowStream, CometNativeArrowSource, RowArrowReader}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, StructType}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry, DataTypeSupport}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.supportedDataType
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

  /**
   * Build the per-partition `RowArrowReader`; the trait routes it to the JVM or native consumer.
   */
  override protected def mapToReaders[T: ClassTag](
      consume: (String, BufferAllocator => ArrowReader) => Iterator[T]): RDD[T] = {
    val numOutputRows = longMetric("numOutputRows")
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    // Normalize nested child-field nullability. An in-memory Seq/Map encodes array elements and
    // map values as non-null (containsNull=false / valueContainsNull=false), but Comet expression
    // serdes promise nullable children, so feeding a non-null child from the local scan into a
    // native kernel fails the planned-vs-actual type assertion (issue #4789). Widening children to
    // nullable is always safe: the row data contains no nulls, and Arrow map keys stay non-null via
    // Utils.toArrowField. This must agree with the widened scan schema declared in
    // CometLocalTableScanExec.scanFieldType so the native ScanExec does not cast the batch back.
    val sparkSchema =
      StructType(originalPlan.schema.map(f => f.copy(dataType = f.dataType.asNullable)))
    rdd.mapPartitionsInternal { rowIter =>
      val arrowSchema = Utils.toArrowSchema(sparkSchema, CometArrowStream.NATIVE_TIMEZONE)
      consume(
        "CometLocalTableScan",
        new RowArrowReader(
          _,
          arrowSchema,
          CometArrowStream.countingIterator(rowIter, (_: InternalRow) => numOutputRows.add(1)),
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

  // Widen nested child-field nullability in the declared scan schema so it matches both the widened
  // Arrow batch produced by CometLocalTableScanExec and the nullable children promised by
  // downstream expression serdes (issue #4789).
  override protected def scanFieldType(dt: DataType): DataType = dt.asNullable

  // RowArrowReader handles NullType and intervals, but not TimeType. Non-default string collations
  // remain unsupported here, matching DataTypeSupport's existing local-scan boundary.
  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {
    val supported = supportedDataType(
      dt,
      allowComplex = true,
      allowIntervals = true,
      allowTimeType = false,
      allowAnyStringType = false)
    if (!supported) super.isTypeSupported(dt, name, fallbackReasons)
    supported
  }

  override def convert(
      op: LocalTableScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val fallbackReasons = new ListBuffer[String]()
    if (!isSchemaSupported(op.schema, fallbackReasons)) {
      withFallbackReason(op, fallbackReasons.mkString("; "))
      None
    } else {
      super.convert(op, builder, childOp: _*)
    }
  }

  override def createExec(nativeOp: Operator, op: LocalTableScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, CometLocalTableScanExec(op, op.rows, op.output))
  }
}

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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, CreateNamedStruct, Expression, GetStructField, If, Literal, SortOrder}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.arrow.{CometArrowStream, CometNativeArrowSource, RowArrowReader, SparkColumnarArrowReader}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{CoalesceExec, CollectLimitExec, ColumnarToRowTransition, FilterExec, GlobalLimitExec, LocalLimitExec, ProjectExec, RowToColumnarTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometConf, DataTypeSupport}
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.operator.CometSink
import org.apache.comet.shims.CometTypeShim

case class CometSparkToColumnarExec(child: SparkPlan, batchSize: Option[Int] = None)
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
    val maxRecordsPerBatch = batchSize.getOrElse(CometConf.COMET_BATCH_SIZE.get(conf))
    val sparkSchema = child.schema
    val onConversionNs: Long => Unit = ns => {
      conversionTime += ns
      numOutputBatches += 1
    }

    if (child.supportsColumnar) {
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
                (b: ColumnarBatch) => numInputRows.add(b.numRows().toLong)),
            maxRecordsPerBatch,
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
            rowIter,
            maxRecordsPerBatch,
            onConversionNs,
            rows => numInputRows.add(rows.toLong)))
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometSparkToColumnarExec =
    copy(child = newChild)

}

object CometSparkToColumnarExec extends CometSink[SparkPlan] with DataTypeSupport {

  private object RowTypeSupport extends DataTypeSupport with CometTypeShim

  // RowArrowReader recursively writes the collection types supported by DataTypeSupport.
  def isRowSchemaSupported(schema: StructType, fallbackReasons: ListBuffer[String]): Boolean =
    RowTypeSupport.isSchemaSupported(schema, fallbackReasons)

  /**
   * Whether the Iceberg writer may convert this Spark row subtree to Arrow. Native import
   * normalizes invalid UTF-8, while Spark rows can retain those bytes. A new conversion is safe
   * only when its string outputs preserve already valid strings or contain validated literals.
   */
  def isRowConversionSupported(plan: SparkPlan, fallbackReasons: ListBuffer[String]): Boolean = {
    if (!isRowSchemaSupported(plan.schema, fallbackReasons)) {
      false
    } else if (hasValidStrings(plan)) {
      true
    } else {
      fallbackReasons += "Iceberg writer row conversion cannot prove that string outputs contain " +
        "valid UTF-8; native Arrow import could change their bytes"
      false
    }
  }

  private def containsString(dt: DataType): Boolean =
    dt.existsRecursively(_.isInstanceOf[StringType])

  private def hasValidStrings(plan: SparkPlan): Boolean = {
    if (!containsString(plan.schema)) {
      true
    } else {
      plan match {
        // These planning placeholders disappear without running a native import.
        case CometScanWrapper(_, source) => hasValidStrings(source)
        case CometSinkPlaceHolder(_, _, child) => hasValidStrings(child)
        case _: CometNativeExec => true
        case project: ProjectExec =>
          hasValidStrings(project.child) && project.projectList.forall(
            preservesValidStrings(_, project.child.outputSet))
        case _: SortExec | _: LocalLimitExec | _: GlobalLimitExec | _: CollectLimitExec |
            _: FilterExec | _: CoalesceExec | _: ColumnarToRowTransition =>
          plan.children.forall(hasValidStrings)
        case _ => false
      }
    }
  }

  private def preservesValidStrings(expr: Expression, input: AttributeSet): Boolean = {
    if (!containsString(expr.dataType)) {
      true
    } else {
      expr match {
        case attr: Attribute => input.contains(attr)
        case alias: Alias => preservesValidStrings(alias.child, input)
        case assertion: AssertNotNull => preservesValidStrings(assertion.child, input)
        case field: GetStructField => preservesValidStrings(field.child, input)
        case struct: CreateNamedStruct => struct.valExprs.forall(preservesValidStrings(_, input))
        case conditional: If =>
          preservesValidStrings(conditional.trueValue, input) &&
          preservesValidStrings(conditional.falseValue, input)
        case Literal(null, _) => true
        case Literal(value: UTF8String, _: StringType) => RowTypeSupport.isValidUtf8(value)
        // String-bearing collection transforms and other producers need their own proof.
        case _ => false
      }
    }
  }

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: SparkPlan): CometNativeExec = {
    CometScanWrapper(nativeOp, CometSparkToColumnarExec(op))
  }

  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = dt match {
    case ArrayType(StringType, _) => true
    case MapType(StringType, StringType, _) => true
    case _: ArrayType | _: MapType => false
    case _ => super.isTypeSupported(dt, name, fallbackReasons)
  }
}

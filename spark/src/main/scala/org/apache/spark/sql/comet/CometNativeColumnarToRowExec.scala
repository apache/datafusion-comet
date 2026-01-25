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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

import org.apache.comet.{CometConf, NativeColumnarToRowConverter}

/**
 * Native implementation of ColumnarToRowExec that converts Arrow columnar data to Spark UnsafeRow
 * format using Rust.
 *
 * This is an experimental feature that can be enabled by setting
 * `spark.comet.columnarToRow.native.enabled=true`.
 *
 * Benefits over the JVM implementation:
 *   - Zero-copy for variable-length types (strings, binary)
 *   - Better CPU cache utilization through vectorized processing
 *   - Reduced GC pressure
 *
 * @param child
 *   The child plan that produces columnar batches
 */
case class CometNativeColumnarToRowExec(child: SparkPlan)
    extends ColumnarToRowTransition
    with CometPlan {

  // supportsColumnar requires to be only called on driver side, see also SPARK-37779.
  assert(Utils.isInRunningSparkTask || child.supportsColumnar)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "convertTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in conversion"))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")

    // Get the schema and batch size for native conversion
    val localSchema = child.schema
    val batchSize = CometConf.COMET_BATCH_SIZE.get()

    child.executeColumnar().mapPartitionsInternal { batches =>
      // Create native converter for this partition
      val converter = new NativeColumnarToRowConverter(localSchema, batchSize)

      // Register cleanup on task completion
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        converter.close()
      }

      batches.flatMap { batch =>
        numInputBatches += 1
        val numRows = batch.numRows()
        numOutputRows += numRows

        val startTime = System.nanoTime()
        val result = converter.convert(batch)
        convertTime += System.nanoTime() - startTime

        result
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometNativeColumnarToRowExec =
    copy(child = newChild)
}

object CometNativeColumnarToRowExec {

  /**
   * Checks if native columnar to row conversion is enabled.
   */
  def isEnabled: Boolean = CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.get()

  /**
   * Checks if the given schema is supported by native columnar to row conversion.
   *
   * Currently supported types:
   *   - Primitive types: Boolean, Byte, Short, Int, Long, Float, Double
   *   - Date and Timestamp (microseconds)
   *   - Decimal (both inline and variable-length)
   *   - String and Binary
   *   - Struct, Array, Map (nested types)
   */
  def supportsSchema(schema: StructType): Boolean = {
    import org.apache.spark.sql.types._

    def isSupported(dataType: DataType): Boolean = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType => true
      case FloatType | DoubleType => true
      case DateType => true
      case TimestampType => true
      case _: DecimalType => true
      case StringType | BinaryType => true
      case StructType(fields) => fields.forall(f => isSupported(f.dataType))
      case ArrayType(elementType, _) => isSupported(elementType)
      case MapType(keyType, valueType, _) => isSupported(keyType) && isSupported(valueType)
      case _ => false
    }

    schema.fields.forall(f => isSupported(f.dataType))
  }
}

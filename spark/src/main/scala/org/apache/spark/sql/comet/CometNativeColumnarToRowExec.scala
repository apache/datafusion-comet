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

import java.util.UUID
import java.util.concurrent.{Future, TimeoutException, TimeUnit}

import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SparkFatalException, Utils}

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

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  private val timeout: Long = conf.broadcastTimeout

  private val runId: UUID = UUID.randomUUID

  private lazy val cometBroadcastExchange = findCometBroadcastExchange(child)

  @transient
  lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      CometBroadcastExchangeExec.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"CometNativeColumnarToRow broadcast exchange (runId $runId)",
          interruptOnCancel = true)

        val numOutputRows = longMetric("numOutputRows")
        val numInputBatches = longMetric("numInputBatches")
        val localSchema = this.schema
        val batchSize = CometConf.COMET_BATCH_SIZE.get()
        val broadcastColumnar = child.executeBroadcast()
        val serializedBatches =
          broadcastColumnar.value.asInstanceOf[Array[org.apache.spark.util.io.ChunkedByteBuffer]]

        // Use native converter to convert columnar data to rows
        val converter = new NativeColumnarToRowConverter(localSchema, batchSize)
        try {
          val rows = serializedBatches.iterator
            .flatMap(CometUtils.decodeBatches(_, this.getClass.getSimpleName))
            .flatMap { batch =>
              numInputBatches += 1
              numOutputRows += batch.numRows()
              val result = converter.convert(batch)
              // Wrap iterator to close batch after consumption
              new Iterator[InternalRow] {
                override def hasNext: Boolean = {
                  val hasMore = result.hasNext
                  if (!hasMore) {
                    batch.close()
                  }
                  hasMore
                }
                override def next(): InternalRow = result.next()
              }
            }

          val mode = cometBroadcastExchange.get.mode
          val relation = mode.transform(rows, Some(numOutputRows.value))
          val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
          val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
          SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
          promise.trySuccess(broadcasted)
          broadcasted
        } finally {
          converter.close()
        }
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(oe)
          promise.tryFailure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.tryFailure(ex)
          throw ex
        case e: Throwable =>
          promise.tryFailure(e)
          throw e
      }
    }
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    if (cometBroadcastExchange.isEmpty) {
      throw new SparkException(
        "CometNativeColumnarToRowExec only supports doExecuteBroadcast when child contains a " +
          "CometBroadcastExchange, but got " + child)
    }

    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw QueryExecutionErrors.executeBroadcastTimeoutError(timeout, Some(ex))
    }
  }

  private def findCometBroadcastExchange(op: SparkPlan): Option[CometBroadcastExchangeExec] = {
    op match {
      case b: CometBroadcastExchangeExec => Some(b)
      case b: BroadcastQueryStageExec => findCometBroadcastExchange(b.plan)
      case b: ReusedExchangeExec => findCometBroadcastExchange(b.child)
      case _ => op.children.collectFirst(Function.unlift(findCometBroadcastExchange))
    }
  }

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

        // Wrap iterator to close batch after consumption
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val hasMore = result.hasNext
            if (!hasMore) {
              batch.close()
            }
            hasMore
          }
          override def next(): InternalRow = result.next()
        }
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

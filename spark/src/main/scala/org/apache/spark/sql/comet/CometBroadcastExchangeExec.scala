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

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, Partition, SparkContext, TaskContext}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkFatalException, ThreadUtils}
import org.apache.spark.util.io.ChunkedByteBuffer

import com.google.common.base.Objects

import org.apache.comet.CometRuntimeException
import org.apache.comet.shims.ShimCometBroadcastExchangeExec

/**
 * A [[CometBroadcastExchangeExec]] collects, transforms and finally broadcasts the result of a
 * transformed SparkPlan. This is a copy of the [[BroadcastExchangeExec]] class with the necessary
 * changes to support the Comet operator.
 *
 * [[CometBroadcastExchangeExec]] will be used in broadcast join operator.
 *
 * Note that this class cannot extend `CometExec` as usual similar to other Comet operators. As
 * the trait `BroadcastExchangeLike` in Spark extends abstract class `Exchange`, it limits the
 * flexibility to extend `CometExec` and `Exchange` at the same time.
 *
 * Note that this only supports Spark 3.4 and later, because the serialization class
 * `ChunkedByteBuffer` is only serializable in Spark 3.4 and later.
 */
case class CometBroadcastExchangeExec(originalPlan: SparkPlan, child: SparkPlan)
    extends BroadcastExchangeLike
    with ShimCometBroadcastExchangeExec {
  import CometBroadcastExchangeExec._

  override val runId: UUID = UUID.randomUUID

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  override def doCanonicalize(): SparkPlan = {
    CometBroadcastExchangeExec(originalPlan.canonicalized, child.canonicalized)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  private val timeout: Long = conf.broadcastTimeout

  @transient
  private lazy val maxBroadcastRows = 512000000

  private var numPartitions: Option[Int] = None

  def setNumPartitions(numPartitions: Int): CometBroadcastExchangeExec = {
    this.numPartitions = Some(numPartitions)
    this
  }
  def getNumPartitions(): Int = {
    numPartitions.getOrElse(child.executeColumnar().getNumPartitions)
  }

  @transient
  override lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      CometBroadcastExchangeExec.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val beforeCollect = System.nanoTime()

        val countsAndBytes = child match {
          case c: CometPlan => CometExec.getByteArrayRdd(c).collect()
          case AQEShuffleReadExec(s: ShuffleQueryStageExec, _)
              if s.plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(s.plan.asInstanceOf[CometPlan]).collect()
          case AQEShuffleReadExec(s: ShuffleQueryStageExec, _) =>
            throw new CometRuntimeException(
              "Child of CometBroadcastExchangeExec should be CometExec, " +
                s"but got: ${s.plan.getClass}")
          case _ =>
            throw new CometRuntimeException(
              "Child of CometBroadcastExchangeExec should be CometExec, " +
                s"but got: ${child.getClass}")
        }

        val numRows = countsAndBytes.map(_._1).sum
        val input = countsAndBytes.iterator.map(countAndBytes => countAndBytes._2)

        longMetric("numOutputRows") += numRows
        if (numRows >= maxBroadcastRows) {
          throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableRowsError(
            maxBroadcastRows,
            numRows)
        }

        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)

        val batches = input.toArray

        val dataSize = batches.map(_.size).sum

        longMetric("dataSize") += dataSize
        if (dataSize >= MAX_BROADCAST_TABLE_BYTES) {
          throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(
            MAX_BROADCAST_TABLE_BYTES,
            dataSize)
        }

        val beforeBroadcast = System.nanoTime()
        longMetric("buildTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeBuild)

        // (3.4 only) SPARK-39983 - Broadcast the relation without caching the unserialized object.
        val broadcasted = doBroadcast(sparkContext, batches)
          .asInstanceOf[broadcast.Broadcast[Any]]
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.trySuccess(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          // Spark 3.4 has two parameters for `notEnoughMemoryToBuildAndBroadcastTableError`, which
          // is different to Spark 3.3. We simply create the error message here.
          val error =
            new OutOfMemoryError(
              "Not enough memory to build and broadcast the table to all " +
                "worker nodes. As a workaround, you can either disable broadcast by setting " +
                s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause)
          val ex = new SparkFatalException(error)
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

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("CometBroadcastExchangeExec")
  }

  override def supportsColumnar: Boolean = true

  // This is basically for unit test only.
  override def executeCollect(): Array[InternalRow] =
    ColumnarToRowExec(this).executeCollect()

  // This is basically for unit test only, called by `executeCollect` indirectly.
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val broadcasted = executeBroadcast[Array[ChunkedByteBuffer]]()

    new CometBatchRDD(sparkContext, getNumPartitions(), broadcasted)
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
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

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometBroadcastExchangeExec =>
        this.originalPlan == other.originalPlan &&
        this.output == other.output && this.child == other.child
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, child)

  override def stringArgs: Iterator[Any] = Iterator(output, child)

  override protected def withNewChildInternal(newChild: SparkPlan): CometBroadcastExchangeExec =
    copy(child = newChild)
}

object CometBroadcastExchangeExec {
  val MAX_BROADCAST_TABLE_BYTES: Long = 8L << 30

  private[comet] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool(
      "comet-broadcast-exchange",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}

/**
 * [[CometBatchRDD]] is a [[RDD]] of [[ColumnarBatch]]s that are broadcasted to the executors. It
 * is only used by [[CometBroadcastExchangeExec]] to broadcast the result of a Comet operator.
 *
 * @param sc
 *   SparkContext
 * @param numPartitions
 *   number of partitions
 * @param value
 *   the broadcasted batches which are serialized into an array of [[ChunkedByteBuffer]]s
 */
class CometBatchRDD(
    sc: SparkContext,
    numPartitions: Int,
    value: broadcast.Broadcast[Array[ChunkedByteBuffer]])
    extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = (0 until numPartitions).toArray.map { i =>
    new CometBatchPartition(i, value)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometBatchPartition]
    partition.value.value.toIterator.flatMap(CometExec.decodeBatches)
  }
}

class CometBatchPartition(
    override val index: Int,
    val value: broadcast.Broadcast[Array[ChunkedByteBuffer]])
    extends Partition {}

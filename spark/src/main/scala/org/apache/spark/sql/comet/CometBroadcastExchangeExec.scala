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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
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
 */
case class CometBroadcastExchangeExec(
    originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    mode: BroadcastMode,
    override val child: SparkPlan)
    extends BroadcastExchangeLike
    with ShimCometBroadcastExchangeExec
    with CometPlan {

  override val runId: UUID = UUID.randomUUID

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  override def doCanonicalize(): SparkPlan = {
    CometBroadcastExchangeExec(null, null, mode, child.canonicalized)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

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
        setJobGroupOrTag(sparkContext, this)
        val beforeCollect = System.nanoTime()

        val countsAndBytes = child match {
          case c: CometPlan => CometExec.getByteArrayRdd(c).collect()
          case AQEShuffleReadExec(s: ShuffleQueryStageExec, _)
              if s.plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(s.plan.asInstanceOf[CometPlan]).collect()
          case s: ShuffleQueryStageExec if s.plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(s.plan.asInstanceOf[CometPlan]).collect()
          case ReusedExchangeExec(_, plan) if plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(plan.asInstanceOf[CometPlan]).collect()
          case AQEShuffleReadExec(ShuffleQueryStageExec(_, ReusedExchangeExec(_, plan), _), _)
              if plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(plan.asInstanceOf[CometPlan]).collect()
          case ShuffleQueryStageExec(_, ReusedExchangeExec(_, plan), _)
              if plan.isInstanceOf[CometPlan] =>
            CometExec.getByteArrayRdd(plan.asInstanceOf[CometPlan]).collect()
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
        val maxBytes = maxBroadcastTableBytes(conf)
        if (dataSize >= maxBytes) {
          throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(
            maxBytes,
            dataSize)
        }

        val beforeBroadcast = System.nanoTime()
        longMetric("buildTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeBuild)

        // (3.4 only) SPARK-39983 - Broadcast the relation without caching the unserialized object.
        val broadcasted = sparkContext
          .broadcastInternal(batches, true)
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
          cancelJobGroup(sparkContext, this)
          relationFuture.cancel(true)
        }
        throw QueryExecutionErrors.executeBroadcastTimeoutError(timeout, Some(ex))
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometBroadcastExchangeExec =>
        this.originalPlan == other.originalPlan &&
        this.child == other.child
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(child)

  override def stringArgs: Iterator[Any] = Iterator(output, child)

  override protected def withNewChildInternal(newChild: SparkPlan): CometBroadcastExchangeExec =
    copy(child = newChild)
}

object CometBroadcastExchangeExec {
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
    @volatile var numPartitions: Int,
    value: broadcast.Broadcast[Array[ChunkedByteBuffer]])
    extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = (0 until numPartitions).toArray.map { i =>
    new CometBatchPartition(i, value)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometBatchPartition]
    partition.value.value.toIterator
      .flatMap(Utils.decodeBatches(_, this.getClass.getSimpleName))
  }

  def withNumPartitions(numPartitions: Int): CometBatchRDD = {
    this.numPartitions = numPartitions
    this
  }

}

class CometBatchPartition(
    override val index: Int,
    val value: broadcast.Broadcast[Array[ChunkedByteBuffer]])
    extends Partition {}

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

package org.apache.spark.sql.comet.execution.shuffle

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometScalarSubquery, PlanDataInjector}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.{ThreadUtils, Utils}

import org.apache.comet.{CometConf, CometExecIterator, CometShuffleSizeLimitException}
import org.apache.comet.serde.{OperatorOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.{CompressionCodec, Operator}
import org.apache.comet.serde.operator.schema2Proto
import org.apache.comet.shuffle.{CelebornShufflePartitionPusher, CelebornShufflePusherFactory, ShufflePartitionPusher}

/**
 * Drives the native shuffle write in a single [[CometExecIterator]] per partition. The plan is
 * `ShuffleWriter(child = childNativeOp)`; leaf iterators come from a
 * [[CometNativeShuffleInputIterator]]. `childNativeOp` is either a rich Comet native subtree
 * (when fed by [[CometShuffleExchangeExec]] with a [[org.apache.spark.sql.comet.CometNativeExec]]
 * child) or a synthetic `Scan("ShuffleWriterInput")` placeholder (the
 * [[CometShuffleExchangeExec.prepareShuffleDependency]] convenience overload). Same handling
 * either way.
 */
class CometNativeShuffleWriter[K, V](
    spec: NativeShuffleSpec,
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    metrics: Map[String, SQLMetric],
    numParts: Int,
    shuffleId: Int,
    mapId: Long,
    context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter,
    rangePartitionBounds: Option[Seq[InternalRow]] = None,
    remoteDestination: Option[CelebornNativeShuffleDestination] = None)
    extends ShuffleWriter[K, V]
    with Logging {

  private val OFFSET_LENGTH = 8

  var partitionLengths: Array[Long] = _
  var mapStatus: MapStatus = _
  private var stopped = false
  private lazy val effectivePartitionCount =
    remoteDestination.map(_.numPartitions).getOrElse(outputPartitioning.numPartitions)

  private val cancellationWatch: Option[ScheduledFuture[_]] = remoteDestination.flatMap {
    destination =>
      Option(context).map { taskContext =>
        val watcher = CelebornNativeShuffleDestination.watchForCancellation(
          taskContext,
          destination.pusher,
          failure =>
            logWarning("Could not abort an interrupted Celeborn shuffle map attempt", failure))
        taskContext.addTaskCompletionListener[Unit] { _ =>
          watcher.cancel(false)
          destination.pusher.abort()
        }
        watcher
      }
  }

  override def write(inputs: Iterator[Product2[K, V]]): Unit = {
    try {
      writeInternal(inputs)
    } catch {
      case failure: Throwable =>
        remoteDestination.foreach { destination =>
          try destination.pusher.abort()
          catch {
            case cleanupFailure: Throwable => failure.addSuppressed(cleanupFailure)
          }
          if (CometNativeShuffleWriter.isSizeLimitFailure(failure)) {
            destination.onSizeLimitExceeded(failure)
          }
        }
        throw failure
    }
  }

  private def writeInternal(inputs: Iterator[Product2[K, V]]): Unit = {
    val localOutput = if (remoteDestination.isEmpty) {
      val resolver =
        SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
      val dataFile = resolver.getDataFile(shuffleId, mapId)
      val indexFile = resolver.getIndexFile(shuffleId, mapId)
      Some(
        LocalShuffleOutput(
          resolver,
          dataFile.getPath.replace(".data", ".data.tmp"),
          indexFile.getPath.replace(".index", ".index.tmp")))
    } else {
      None
    }

    // The dep's _rdd is always a CometNativeShuffleInputRDD on this path. Pattern-match instead
    // of asInstanceOf so a future RDD-layering change produces a clear error here rather than a
    // bare ClassCastException deeper in the stack.
    val shuffleInputIter = inputs match {
      case it: CometNativeShuffleInputIterator => it
      case other =>
        throw new IllegalStateException(
          "CometNativeShuffleWriter expects its input iterator to be a " +
            "CometNativeShuffleInputIterator (produced by CometNativeShuffleInputRDD), got " +
            s"${other.getClass.getName}")
    }
    val partitionIdx = shuffleInputIter.partitionIndex
    val inputObjects = shuffleInputIter.inputObjects
    val shuffleBlockIters = shuffleInputIter.shuffleBlockIterators

    val unifiedPlan = localOutput match {
      case Some(output) => buildUnifiedPlan(output.dataFile, output.indexFile)
      case None => buildUnifiedPlan("", "")
    }
    val ctx = spec.execContext
    val finalNativePlan = if (ctx.commonByKey.nonEmpty) {
      // This partition's plan-data slice rides on the input iterator's Partition object (populated
      // in CometNativeShuffleInputRDD.getPartitions on the driver), not on the spec. The spec's
      // execContext.perPartitionByKey is emptied in prepareNativeShuffleDependency so the full
      // O(numPartitions) map stays out of the broadcast task binary.
      PlanDataInjector.injectPlanData(
        unifiedPlan,
        ctx.commonByKey,
        shuffleInputIter.planDataByKey)
    } else {
      unifiedPlan
    }

    val detailedMetrics = Seq(
      "elapsed_compute",
      "encode_time",
      "repart_time",
      "interleave_time",
      "input_batches",
      "spill_count",
      "spilled_bytes",
      "memory_spilled_bytes")
    val metricsOutputRows = new SQLMetric("outputRows")
    val metricsWriteTime = new SQLMetric("writeTime")
    val shuffleWriterSQLMetrics = Map(
      "output_rows" -> metricsOutputRows,
      "data_size" -> metrics("dataSize"),
      "write_time" -> metricsWriteTime) ++
      metrics.filter { case (name, _) => detailedMetrics.contains(name) }

    // ShuffleWriter metrics at the root; child's metric tree underneath so the SQL UI's per-node
    // breakdown matches what the split-driver flow showed.
    val nativeMetrics = CometMetricNode(shuffleWriterSQLMetrics, Seq(spec.childMetricNode))

    // The leaf scans execute inside this writer's single plan rather than a separate native
    // stage RDD, so the usual CometExecRDD.compute() bridge (operators.scala) never runs for
    // them. Report their bytes/rows to the task's input metrics here instead.
    if (ctx.hasScanInput) {
      Option(context).foreach(nativeMetrics.reportScanInputMetrics)
    }

    val cometIter = new CometExecIterator(
      CometExec.newIterId,
      inputObjects,
      outputAttributes.length,
      CometExec.serializeNativePlan(finalNativePlan),
      nativeMetrics,
      numParts,
      partitionIdx,
      ctx.broadcastedHadoopConfForEncryption,
      ctx.encryptedFilePaths,
      shuffleBlockIters,
      shufflePartitionPusher = remoteDestination.map(_.callback))

    // Register subqueries against the iterator id so native callbacks resolve them to values.
    ctx.subqueries.foreach { sub =>
      CometScalarSubquery.setSubquery(cometIter.id, sub)
    }
    Option(context).foreach { taskCtx =>
      taskCtx.addTaskCompletionListener[Unit] { _ =>
        ctx.subqueries.foreach { sub =>
          CometScalarSubquery.removeSubquery(cometIter.id, sub)
        }
      }
    }

    CometNativeShuffleWriter.drainAndClose(cometIter, () => cometIter.close())

    remoteDestination match {
      case Some(destination) =>
        val completionStart = System.nanoTime()
        val authorized = if (destination.commitAuthorized) {
          destination.commitValidator()
        } else {
          SparkEnv.get.outputCommitCoordinator.canCommit(
            context.stageId(),
            context.stageAttemptNumber(),
            context.partitionId(),
            context.attemptNumber())
        }
        if (!authorized) {
          throw CelebornShufflePusherFactory.commitDenied(context)
        }
        partitionLengths = destination.pusher.finish()
        if (destination.commitAuthorized && !destination.commitValidator()) {
          throw CelebornShufflePusherFactory.commitDenied(context)
        }
        metricsReporter.incBytesWritten(partitionLengths.sum)
        metricsReporter.incWriteTime(System.nanoTime() - completionStart)
        mapStatus = MapStatus.apply(
          SparkEnv.get.blockManager.shuffleServerId,
          partitionLengths,
          context.taskAttemptId())

      case None =>
        val output = localOutput.get
        val tempDataFilePath = Paths.get(output.dataFile)
        val tempIndexFilePath = Paths.get(output.indexFile)

        var offset = 0L
        partitionLengths = Files
          .readAllBytes(tempIndexFilePath)
          .grouped(OFFSET_LENGTH)
          .drop(1)
          .map(indexBytes => {
            val partitionOffset =
              ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getLong
            val partitionLength = partitionOffset - offset
            offset = partitionOffset
            partitionLength
          })
          .toArray
        Files.delete(tempIndexFilePath)

        metricsReporter.incBytesWritten(Files.size(tempDataFilePath))
        output.resolver.writeMetadataFileAndCommit(
          shuffleId,
          mapId,
          partitionLengths,
          Array.empty,
          tempDataFilePath.toFile)
        mapStatus =
          MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
    }

    metricsReporter.incRecordsWritten(metricsOutputRows.value)
    metricsReporter.incWriteTime(metricsWriteTime.value)
  }

  private def isSinglePartitioning(p: Partitioning): Boolean = p match {
    case SinglePartition => true
    case _: RangePartitioning =>
      // Spark sometimes generates RangePartitioning schemes with numPartitions == 1,
      // or the computed bounds results in a single target partition.
      // In this case Comet just serializes a SinglePartition scheme to native.
      effectivePartitionCount == 1 || rangePartitionBounds.forall(_.isEmpty)
    case _: HashPartitioning => effectivePartitionCount == 1
    case _ => false
  }

  /**
   * Build the unified `ShuffleWriter(child = childNativeOp)` plan with the partitioning serde,
   * compression settings, and output file paths.
   */
  private[shuffle] def buildUnifiedPlan(dataFile: String, indexFile: String): Operator = {
    val shuffleWriterBuilder = OperatorOuterClass.ShuffleWriter.newBuilder()
    remoteDestination match {
      case Some(_) =>
        shuffleWriterBuilder.setPartitionWriter(
          OperatorOuterClass.PartitionWriter
            .newBuilder()
            .setRss(OperatorOuterClass.RssPartitionWriter.getDefaultInstance)
            .build())
      case None =>
        // Keep legacy paths for older native libraries while newer libraries use the destination.
        shuffleWriterBuilder.setOutputDataFile(dataFile)
        shuffleWriterBuilder.setOutputIndexFile(indexFile)
        shuffleWriterBuilder.setPartitionWriter(
          OperatorOuterClass.PartitionWriter
            .newBuilder()
            .setLocal(
              OperatorOuterClass.LocalPartitionWriter
                .newBuilder()
                .setOutputDataFile(dataFile)
                .setOutputIndexFile(indexFile)
                .build())
            .build())
    }

    if (SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)) {
      val codec = CometConf.COMET_SHUFFLE_COMPRESSION_CODEC.get() match {
        case "zstd" => CompressionCodec.Zstd
        case "lz4" => CompressionCodec.Lz4
        case "snappy" => CompressionCodec.Snappy
        case other => throw new UnsupportedOperationException(s"invalid codec: $other")
      }
      shuffleWriterBuilder.setCodec(codec)
    } else {
      shuffleWriterBuilder.setCodec(CompressionCodec.None)
    }
    shuffleWriterBuilder.setCompressionLevel(CometConf.COMET_SHUFFLE_COMPRESSION_ZSTD_LEVEL.get())
    shuffleWriterBuilder.setWriteBufferSize(
      CometConf.COMET_SHUFFLE_NATIVE_WRITE_BUFFER_SIZE.get().min(Int.MaxValue).toInt)
    shuffleWriterBuilder.setMaxBufferBytes(CometConf.COMET_SHUFFLE_NATIVE_MAX_BUFFER_BYTES.get())

    outputPartitioning match {
      case p if isSinglePartitioning(p) =>
        val partitioning = PartitioningOuterClass.SinglePartition.newBuilder()
        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setSinglePartition(partitioning).build())
      case _: HashPartitioning =>
        val hashPartitioning = outputPartitioning.asInstanceOf[HashPartitioning]
        val partitioning = PartitioningOuterClass.HashPartition.newBuilder()
        partitioning.setNumPartitions(effectivePartitionCount)

        val partitionExprs = hashPartitioning.expressions
          .flatMap(e => QueryPlanSerde.exprToProto(e, outputAttributes))

        if (partitionExprs.length != hashPartitioning.expressions.length) {
          throw new UnsupportedOperationException(
            s"Partitioning $hashPartitioning is not supported.")
        }

        partitioning.addAllHashExpression(partitionExprs.asJava)

        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setHashPartition(partitioning).build())
      case _: RangePartitioning =>
        val rangePartitioning = outputPartitioning.asInstanceOf[RangePartitioning]
        val partitioning = PartitioningOuterClass.RangePartition.newBuilder()
        partitioning.setNumPartitions(effectivePartitionCount)

        // Detect duplicates by tracking expressions directly, similar to DataFusion's LexOrdering
        // DataFusion will deduplicate identical sort expressions in LexOrdering,
        // so we need to transform boundary rows to match the deduplicated structure
        val seenExprs = mutable.HashSet[Expression]()
        val deduplicationMap = mutable.ArrayBuffer[(Int, Boolean)]() // (originalIndex, isKept)

        rangePartitioning.ordering.zipWithIndex.foreach { case (sortOrder, idx) =>
          if (seenExprs.contains(sortOrder.child)) {
            deduplicationMap += (idx -> false)
          } else {
            seenExprs += sortOrder.child
            deduplicationMap += (idx -> true)
          }
        }

        {
          val orderingExprs = rangePartitioning.ordering
            .flatMap(e => QueryPlanSerde.exprToProto(e, outputAttributes))
          if (orderingExprs.length != rangePartitioning.ordering.length) {
            throw new UnsupportedOperationException(
              s"Partitioning $rangePartitioning is not supported.")
          }
          partitioning.addAllSortOrders(orderingExprs.asJava)
        }

        val boundarySchema = rangePartitioning.ordering.flatMap(e => Some(e.dataType))

        // rangePartitionBounds holds Spark InternalRows of partitioning boundaries: each row is a
        // boundary, each entry a value in that row (row-major, not column-major). Convert to
        // Literals and keep only the entries whose ordering expression survived deduplication, so
        // the boundary shape matches DataFusion's deduplicated LexOrdering.
        val transformedBoundaryExprs: Seq[Seq[Literal]] =
          rangePartitionBounds.get.map((row: InternalRow) => {
            val allLiterals =
              row.toSeq(boundarySchema).zip(boundarySchema).map { case (value, valueType) =>
                Literal(value, valueType)
              }
            allLiterals
              .zip(deduplicationMap)
              .filter(_._2._2)
              .map(_._1)
          })

        {
          val boundaryRows: Seq[PartitioningOuterClass.BoundaryRow] = transformedBoundaryExprs
            .map((rowLiterals: Seq[Literal]) => {
              val rowBuilder = PartitioningOuterClass.BoundaryRow.newBuilder();
              val serializedExprs =
                rowLiterals.map(lit_value =>
                  QueryPlanSerde.exprToProto(lit_value, outputAttributes).get)
              rowBuilder.addAllPartitionBounds(serializedExprs.asJava)
              rowBuilder.build()
            })
          partitioning.addAllBoundaryRows(boundaryRows.asJava)
        }

        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setRangePartition(partitioning).build())

      case _: RoundRobinPartitioning =>
        val partitioning = PartitioningOuterClass.RoundRobinPartition.newBuilder()
        partitioning.setNumPartitions(effectivePartitionCount)
        partitioning.setMaxHashColumns(
          CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_MAX_HASH_COLUMNS.get())

        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setRoundRobinPartition(partitioning).build())

      case _ =>
        throw new UnsupportedOperationException(
          s"Partitioning $outputPartitioning is not supported.")
    }

    shuffleWriterBuilder.setTracingEnabled(CometConf.COMET_TRACING_ENABLED.get())

    // Used by the native planner to cast the inlined child's output when DataFusion's
    // declared return type drifts from Spark catalyst (see comet#4515).
    val expectedFields = outputAttributes
      .map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
    schema2Proto(expectedFields).foreach(shuffleWriterBuilder.addExpectedOutputSchema)

    OperatorOuterClass.Operator
      .newBuilder()
      .setShuffleWriter(shuffleWriterBuilder)
      .addChildren(spec.childNativeOp)
      .build()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    remoteDestination match {
      case None => if (success) Some(mapStatus) else None
      case Some(destination) =>
        synchronized {
          if (stopped) {
            None
          } else {
            stopped = true
            try {
              if (success) {
                if (mapStatus == null) {
                  throw new IllegalStateException(
                    "Cannot complete a Celeborn shuffle map task before writing its data")
                }
                Some(mapStatus)
              } else {
                None
              }
            } finally {
              cancellationWatch.foreach(_.cancel(false))
              destination.pusher.abort()
            }
          }
        }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths

  private final case class LocalShuffleOutput(
      resolver: IndexShuffleBlockResolver,
      dataFile: String,
      indexFile: String)
}

private[shuffle] object CometNativeShuffleWriter {
  private[shuffle] def isSizeLimitFailure(failure: Throwable): Boolean = {
    var cause = failure
    val visited = new java.util.IdentityHashMap[Throwable, java.lang.Boolean]()
    while (cause != null && visited.put(cause, java.lang.Boolean.TRUE) == null) {
      if (cause.isInstanceOf[CometShuffleSizeLimitException]) return true
      cause = cause.getCause
    }
    false
  }

  def drainAndClose(iterator: Iterator[_], close: () => Unit): Unit = {
    Utils.tryWithSafeFinally {
      while (iterator.hasNext) {
        iterator.next()
      }
    } {
      // In particular, cleanup must not replace a typed FetchFailedException that Spark needs
      // to trigger stage recovery. The original failure retains cleanup errors as suppressed.
      close()
    }
  }
}

/** A task-owned remote destination whose callback never crosses map-attempt boundaries. */
private[shuffle] final case class CelebornNativeShuffleDestination(
    pusher: CelebornShufflePartitionPusher,
    maxFrameBytes: Int,
    numPartitions: Int,
    commitAuthorized: Boolean = false,
    commitValidator: () => Boolean = () => true,
    onSizeLimitExceeded: Throwable => Unit = _ => ()) {
  require(pusher != null, "The Celeborn shuffle partition pusher must not be null")
  require(maxFrameBytes >= 20, "The Celeborn shuffle frame limit must fit a complete frame")
  require(
    maxFrameBytes <= pusher.maxFrameBytes(),
    "The Celeborn shuffle destination cannot exceed its task-owned pusher's frame limit")
  require(
    numPartitions == pusher.numPartitions(),
    "The Celeborn shuffle destination must use its actual reducer partition count")

  // SQL execution configuration can impose a tighter bound than the executor SparkConf used to
  // create the pusher. Native planning reads the bound from its registered task-owned callback.
  private[shuffle] val callback: ShufflePartitionPusher = new ShufflePartitionPusher {
    override def pushPartitionData(partitionId: Int, data: Array[Byte], length: Int): Unit =
      pusher.pushPartitionData(partitionId, data, length)

    override def reservePartitionData(bytes: Int): Unit = pusher.reservePartitionData(bytes)

    override def releasePartitionDataReservation(): Unit =
      pusher.releasePartitionDataReservation()

    override def maxFrameBytes(): Int = CelebornNativeShuffleDestination.this.maxFrameBytes

    override def maxReservationBytes(): Int = pusher.maxReservationBytes()
  }
}

private[shuffle] object CelebornNativeShuffleDestination {
  // One daemon bounds cancellation-polling threads per executor; each task cancels its scheduled
  // future on completion, so the process-lifetime scheduler does not retain completed callbacks.
  private val cancellationWatcher =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("comet-celeborn-task-cancellation")

  private[shuffle] def watchForCancellation(
      taskContext: TaskContext,
      pusher: CelebornShufflePartitionPusher,
      reportFailure: Throwable => Unit): ScheduledFuture[_] = {
    val handled = new AtomicBoolean(false)
    cancellationWatcher.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          if (taskContext.isInterrupted() && handled.compareAndSet(false, true)) {
            try pusher.abort()
            catch {
              case failure: Throwable => reportFailure(failure)
            }
          }
        }
      },
      0L,
      25L,
      TimeUnit.MILLISECONDS)
  }
}

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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometScalarSubquery, NativeExecContext, PlanDataInjector}
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.comet.{CometConf, CometExecIterator}
import org.apache.comet.serde.{OperatorOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.{CompressionCodec, Operator}

/**
 * A [[ShuffleWriter]] that drives the native shuffle write in a single [[CometExecIterator]] per
 * partition. The unified plan it executes has [[OperatorOuterClass.ShuffleWriter]] at the root
 * with `childNativeOp` as its only child. Leaf input iterators come from
 * [[CometNativeShuffleInputIterator]] (constructed by [[CometNativeShuffleInputRDD.compute]]).
 *
 * Two flavors of `childNativeOp` are in use:
 *   - rich Comet native subtree (e.g. HashAgg / Filter / ShuffleScan), supplied by
 *     [[CometShuffleExchangeExec]] when its child is a
 *     [[org.apache.spark.sql.comet.CometNativeExec]].
 *   - synthetic `Scan("ShuffleWriterInput")` placeholder, supplied by the convenience overload of
 *     [[CometShuffleExchangeExec.prepareShuffleDependency]] for callers that already hold an
 *     `RDD[ColumnarBatch]` of native-driven batches (e.g.
 *     [[org.apache.spark.sql.comet.CometCollectLimitExec]]).
 *
 * The writer treats both shapes identically.
 */
class CometNativeShuffleWriter[K, V](
    childNativeOp: Operator,
    childMetricNode: CometMetricNode,
    nativeContext: NativeExecContext,
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    metrics: Map[String, SQLMetric],
    numParts: Int,
    shuffleId: Int,
    mapId: Long,
    context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter,
    rangePartitionBounds: Option[Seq[InternalRow]] = None)
    extends ShuffleWriter[K, V]
    with Logging {

  private val OFFSET_LENGTH = 8

  var partitionLengths: Array[Long] = _
  var mapStatus: MapStatus = _

  override def write(inputs: Iterator[Product2[K, V]]): Unit = {
    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val dataFile = shuffleBlockResolver.getDataFile(shuffleId, mapId)
    val indexFile = shuffleBlockResolver.getIndexFile(shuffleId, mapId)
    val tempDataFilename = dataFile.getPath.replace(".data", ".data.tmp")
    val tempIndexFilename = indexFile.getPath.replace(".index", ".index.tmp")
    val tempDataFilePath = Paths.get(tempDataFilename)
    val tempIndexFilePath = Paths.get(tempIndexFilename)

    // Pull the per-partition leaf iterators and partition index from the iterator handed to us
    // by Spark's ShuffleMapTask. CometNativeShuffleInputRDD.compute always returns this exact
    // iterator type; no other RDD layers between produce a Product2[Int, ColumnarBatch].
    val shuffleInputIter = inputs.asInstanceOf[CometNativeShuffleInputIterator]
    val partitionIdx = shuffleInputIter.partitionIndex
    val leafIterators = shuffleInputIter.leafIterators
    val shuffleBlockIters = shuffleInputIter.shuffleBlockIterators

    val unifiedPlan = buildUnifiedPlan(tempDataFilename, tempIndexFilename)
    val finalNativePlan = if (nativeContext.commonByKey.nonEmpty) {
      val partitionDataByKey =
        nativeContext.perPartitionByKey.map { case (k, arr) => k -> arr(partitionIdx) }
      PlanDataInjector.injectPlanData(unifiedPlan, nativeContext.commonByKey, partitionDataByKey)
    } else {
      unifiedPlan
    }

    val detailedMetrics = Seq(
      "elapsed_compute",
      "encode_time",
      "repart_time",
      "input_batches",
      "spill_count",
      "spilled_bytes")
    val metricsOutputRows = new SQLMetric("outputRows")
    val metricsWriteTime = new SQLMetric("writeTime")
    val shuffleWriterSQLMetrics = Map(
      "output_rows" -> metricsOutputRows,
      "data_size" -> metrics("dataSize"),
      "write_time" -> metricsWriteTime) ++
      metrics.filterKeys(detailedMetrics.contains)

    // ShuffleWriter metrics live at the root of the metric tree; the child operator's metric
    // tree (rich subtree or empty leaf for the Scan placeholder) is attached underneath so the
    // SQL UI sees the same per-node breakdown the split-driver flow produced.
    val nativeMetrics = CometMetricNode(shuffleWriterSQLMetrics, Seq(childMetricNode))

    val cometIter = new CometExecIterator(
      CometExec.newIterId,
      leafIterators,
      outputAttributes.length,
      PlanDataInjector.serializeOperator(finalNativePlan),
      nativeMetrics,
      numParts,
      partitionIdx,
      nativeContext.broadcastedHadoopConfForEncryption,
      nativeContext.encryptedFilePaths,
      shuffleBlockIters)

    // Register subqueries against the iterator id so native callbacks resolve them to values.
    nativeContext.subqueries.foreach { sub =>
      CometScalarSubquery.setSubquery(cometIter.id, sub)
    }
    Option(context).foreach { taskCtx =>
      taskCtx.addTaskCompletionListener[Unit] { _ =>
        nativeContext.subqueries.foreach { sub =>
          CometScalarSubquery.removeSubquery(cometIter.id, sub)
        }
      }
    }

    while (cometIter.hasNext) {
      cometIter.next()
    }
    cometIter.close()

    // get partition lengths from shuffle write output index file
    var offset = 0L
    partitionLengths = Files
      .readAllBytes(tempIndexFilePath)
      .grouped(OFFSET_LENGTH)
      .drop(1) // first partition offset is always 0
      .map(indexBytes => {
        val partitionOffset =
          ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getLong
        val partitionLength = partitionOffset - offset
        offset = partitionOffset
        partitionLength
      })
      .toArray
    Files.delete(tempIndexFilePath)

    // Total written bytes at native
    metricsReporter.incBytesWritten(Files.size(tempDataFilePath))
    metricsReporter.incRecordsWritten(metricsOutputRows.value)
    metricsReporter.incWriteTime(metricsWriteTime.value)

    // Report spill metrics to Spark's task metrics so they appear in
    // Spark UI task summaries (not just SQL metrics)
    val spilledBytes = shuffleWriterSQLMetrics.get("spilled_bytes").map(_.value).getOrElse(0L)
    if (spilledBytes > 0) {
      context.taskMetrics().incMemoryBytesSpilled(spilledBytes)
      context.taskMetrics().incDiskBytesSpilled(spilledBytes)
    }

    // commit
    shuffleBlockResolver.writeMetadataFileAndCommit(
      shuffleId,
      mapId,
      partitionLengths,
      Array.empty, // TODO: add checksums
      tempDataFilePath.toFile)
    mapStatus =
      MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }

  private def isSinglePartitioning(p: Partitioning): Boolean = p match {
    case SinglePartition => true
    case rp: RangePartitioning =>
      // Spark sometimes generates RangePartitioning schemes with numPartitions == 1,
      // or the computed bounds results in a single target partition.
      // In this case Comet just serializes a SinglePartition scheme to native.
      rp.numPartitions == 1 || rangePartitionBounds.forall(_.isEmpty)
    case hp: HashPartitioning => hp.numPartitions == 1
    case _ => false
  }

  /**
   * Build the unified `ShuffleWriter(child = childNativeOp)` plan with the partitioning serde,
   * compression settings, and output file paths.
   */
  private def buildUnifiedPlan(dataFile: String, indexFile: String): Operator = {
    val shuffleWriterBuilder = OperatorOuterClass.ShuffleWriter.newBuilder()
    shuffleWriterBuilder.setOutputDataFile(dataFile)
    shuffleWriterBuilder.setOutputIndexFile(indexFile)

    if (SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)) {
      val codec = CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC.get() match {
        case "zstd" => CompressionCodec.Zstd
        case "lz4" => CompressionCodec.Lz4
        case "snappy" => CompressionCodec.Snappy
        case other => throw new UnsupportedOperationException(s"invalid codec: $other")
      }
      shuffleWriterBuilder.setCodec(codec)
    } else {
      shuffleWriterBuilder.setCodec(CompressionCodec.None)
    }
    shuffleWriterBuilder.setCompressionLevel(
      CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL.get)
    shuffleWriterBuilder.setWriteBufferSize(
      CometConf.COMET_SHUFFLE_WRITE_BUFFER_SIZE.get().min(Int.MaxValue).toInt)

    outputPartitioning match {
      case p if isSinglePartitioning(p) =>
        val partitioning = PartitioningOuterClass.SinglePartition.newBuilder()
        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setSinglePartition(partitioning).build())
      case _: HashPartitioning =>
        val hashPartitioning = outputPartitioning.asInstanceOf[HashPartitioning]
        val partitioning = PartitioningOuterClass.HashPartition.newBuilder()
        partitioning.setNumPartitions(outputPartitioning.numPartitions)

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
        partitioning.setNumPartitions(outputPartitioning.numPartitions)

        // Detect duplicates by tracking expressions directly, similar to DataFusion's LexOrdering
        // DataFusion will deduplicate identical sort expressions in LexOrdering,
        // so we need to transform boundary rows to match the deduplicated structure
        val seenExprs = mutable.HashSet[Expression]()
        val deduplicationMap = mutable.ArrayBuffer[(Int, Boolean)]()

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
        partitioning.setNumPartitions(outputPartitioning.numPartitions)
        partitioning.setMaxHashColumns(
          CometConf.COMET_EXEC_SHUFFLE_WITH_ROUND_ROBIN_PARTITIONING_MAX_HASH_COLUMNS.get())

        val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
        shuffleWriterBuilder.setPartitioning(
          partitioningBuilder.setRoundRobinPartition(partitioning).build())

      case _ =>
        throw new UnsupportedOperationException(
          s"Partitioning $outputPartitioning is not supported.")
    }

    shuffleWriterBuilder.setTracingEnabled(CometConf.COMET_TRACING_ENABLED.get())

    OperatorOuterClass.Operator
      .newBuilder()
      .setShuffleWriter(shuffleWriterBuilder)
      .addChildren(childNativeOp)
      .build()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (success) {
      Some(mapStatus)
    } else {
      None
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}

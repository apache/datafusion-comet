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
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometScalarSubquery, PlanDataInjector}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructField

import org.apache.comet.{CometConf, CometExecIterator}
import org.apache.comet.serde.{OperatorOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.{CompressionCodec, Operator}
import org.apache.comet.serde.operator.schema2Proto

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
    val leafIterators = shuffleInputIter.leafIterators
    val shuffleBlockIters = shuffleInputIter.shuffleBlockIterators

    val unifiedPlan = buildUnifiedPlan(tempDataFilename, tempIndexFilename)
    val ctx = spec.execContext
    val finalNativePlan = if (ctx.commonByKey.nonEmpty) {
      val partitionDataByKey = ctx.perPartitionByKey.map { case (k, arr) =>
        k -> arr(partitionIdx)
      }
      PlanDataInjector.injectPlanData(unifiedPlan, ctx.commonByKey, partitionDataByKey)
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
      leafIterators,
      outputAttributes.length,
      CometExec.serializeNativePlan(finalNativePlan),
      nativeMetrics,
      numParts,
      partitionIdx,
      ctx.broadcastedHadoopConfForEncryption,
      ctx.encryptedFilePaths,
      shuffleBlockIters)

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

    // Used by the native planner to cast the inlined child's output when DataFusion's
    // declared return type drifts from Spark catalyst (see comet#4515).
    val expectedFields = outputAttributes
      .map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
      .toArray
    schema2Proto(expectedFields).foreach(shuffleWriterBuilder.addExpectedOutputSchema)

    OperatorOuterClass.Operator
      .newBuilder()
      .setShuffleWriter(shuffleWriterBuilder)
      .addChildren(spec.childNativeOp)
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

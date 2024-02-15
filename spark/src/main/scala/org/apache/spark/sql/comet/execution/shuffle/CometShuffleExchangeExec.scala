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

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.{OperatorOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType
import org.apache.comet.shims.ShimCometShuffleExchangeExec

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class CometShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
    shuffleType: ShuffleType = CometNativeShuffle,
    advisoryPartitionSize: Option[Long] = None)
    extends ShuffleExchangeLike
    with CometPlan {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "shuffleReadElapsedCompute" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle read elapsed compute at native"),
    "numPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "number of partitions")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = if (shuffleType == CometNativeShuffle) {
    "CometExchange"
  } else {
    "CometColumnarExchange"
  }

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[_] = if (shuffleType == CometNativeShuffle) {
    // CometNativeShuffle assumes that the input plan is Comet plan.
    child.executeColumnar()
  } else if (shuffleType == CometColumnarShuffle) {
    // CometColumnarShuffle assumes that the input plan is row-based plan from Spark.
    // One exception is that the input plan is CometScanExec which manually converts
    // ColumnarBatch to InternalRow in its doExecute().
    child.execute()
  } else {
    throw new UnsupportedOperationException(
      s"Unsupported shuffle type: ${shuffleType.getClass.getName}")
  }

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    new CometShuffledBatchRDD(shuffleDependency, readMetrics, partitionSpecs)

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on the partitioning
   * scheme defined in `newPartitioning`. Those partitions of the returned ShuffleDependency will
   * be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, _, _] =
    if (shuffleType == CometNativeShuffle) {
      val dep = CometShuffleExchangeExec.prepareShuffleDependency(
        inputRDD.asInstanceOf[RDD[ColumnarBatch]],
        child.output,
        outputPartitioning,
        serializer,
        metrics)
      metrics("numPartitions").set(dep.partitioner.numPartitions)
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkContext,
        executionId,
        metrics("numPartitions") :: Nil)
      dep
    } else if (shuffleType == CometColumnarShuffle) {
      val dep = CometShuffleExchangeExec.prepareJVMShuffleDependency(
        inputRDD.asInstanceOf[RDD[InternalRow]],
        child.output,
        outputPartitioning,
        serializer,
        metrics)
      metrics("numPartitions").set(dep.partitioner.numPartitions)
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkContext,
        executionId,
        metrics("numPartitions") :: Nil)
      dep
    } else {
      throw new UnsupportedOperationException(
        s"Unsupported shuffle type: ${shuffleType.getClass.getName}")
    }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "CometShuffleExchangeExec.doExecute should not be executed.")
  }

  /**
   * Comet supports columnar execution.
   */
  override val supportsColumnar: Boolean = true

  /**
   * Caches the created CometShuffledBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: CometShuffledBatchRDD = null

  /**
   * Comet returns RDD[ColumnarBatch] for columnar execution.
   */
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Returns the same CometShuffledBatchRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new CometShuffledBatchRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometShuffleExchangeExec =
    copy(child = newChild)
}

object CometShuffleExchangeExec extends ShimCometShuffleExchangeExec {
  def prepareShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rdd.map(
        (0, _)
      ), // adding fake partitionId that is always 0 because ShuffleDependency requires it
      serializer = serializer,
      shuffleWriterProcessor =
        new CometShuffleWriteProcessor(outputPartitioning, outputAttributes, metrics),
      shuffleType = CometNativeShuffle,
      partitioner = new Partitioner {
        override def numPartitions: Int = outputPartitioning.numPartitions
        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      })
    dependency
  }

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on the
   * partitioning scheme defined in `newPartitioning`. Those partitions of the returned
   * ShuffleDependency will be the input of shuffle.
   */
  def prepareJVMShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val sparkShuffleDep = ShuffleExchangeExec.prepareShuffleDependency(
      rdd,
      outputAttributes,
      newPartitioning,
      serializer,
      writeMetrics)

    val dependency =
      new CometShuffleDependency[Int, InternalRow, InternalRow](
        sparkShuffleDep.rdd,
        sparkShuffleDep.partitioner,
        sparkShuffleDep.serializer,
        shuffleWriterProcessor = sparkShuffleDep.shuffleWriterProcessor,
        shuffleType = CometColumnarShuffle,
        schema = Some(StructType.fromAttributes(outputAttributes)))
    dependency
  }
}

/**
 * A [[ShuffleWriteProcessor]] that will delegate shuffle write to native shuffle.
 * @param metrics
 *   metrics to report
 */
class CometShuffleWriteProcessor(
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    metrics: Map[String, SQLMetric])
    extends ShuffleWriteProcessor {

  private val OFFSET_LENGTH = 8

  override protected def createMetricsReporter(
      context: TaskContext): ShuffleWriteMetricsReporter = {
    new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
  }

  override def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {
    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val dataFile = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val indexFile = shuffleBlockResolver.getIndexFile(dep.shuffleId, mapId)
    val tempDataFilename = dataFile.getPath.replace(".data", ".data.tmp")
    val tempIndexFilename = indexFile.getPath.replace(".index", ".index.tmp")
    val tempDataFilePath = Paths.get(tempDataFilename)
    val tempIndexFilePath = Paths.get(tempIndexFilename)

    // Getting rid of the fake partitionId
    val cometRDD =
      rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[RDD[ColumnarBatch]]

    // Call native shuffle write
    val nativePlan = getNativePlan(tempDataFilename, tempIndexFilename)

    // Maps native metrics to SQL metrics
    val nativeSQLMetrics = Map(
      "output_rows" -> metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN),
      "elapsed_compute" -> metrics("shuffleReadElapsedCompute"))
    val nativeMetrics = CometMetricNode(nativeSQLMetrics)

    val rawIter = cometRDD.iterator(partition, context)
    val cometIter = CometExec.getCometIterator(Seq(rawIter), nativePlan, nativeMetrics)

    while (cometIter.hasNext) {
      cometIter.next()
    }

    // get partition lengths from shuffle write output index file
    var offset = 0L
    val partitionLengths = Files
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

    // Update Spark metrics from native metrics
    metrics("dataSize") += Files.size(tempDataFilePath)

    // commit
    shuffleBlockResolver.writeMetadataFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      Array.empty, // TODO: add checksums
      tempDataFilePath.toFile)
    MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }

  def getNativePlan(dataFile: String, indexFile: String): Operator = {
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val opBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == outputAttributes.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      val shuffleWriterBuilder = OperatorOuterClass.ShuffleWriter.newBuilder()
      shuffleWriterBuilder.setOutputDataFile(dataFile)
      shuffleWriterBuilder.setOutputIndexFile(indexFile)

      outputPartitioning match {
        case _: HashPartitioning =>
          val hashPartitioning = outputPartitioning.asInstanceOf[HashPartitioning]

          val partitioning = PartitioningOuterClass.HashRepartition.newBuilder()
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

        case SinglePartition =>
          val partitioning = PartitioningOuterClass.SinglePartition.newBuilder()

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setSinglePartition(partitioning).build())

        case _ =>
          throw new UnsupportedOperationException(
            s"Partitioning $outputPartitioning is not supported.")
      }

      val shuffleWriterOpBuilder = OperatorOuterClass.Operator.newBuilder()
      shuffleWriterOpBuilder
        .setShuffleWriter(shuffleWriterBuilder)
        .addChildren(opBuilder.setScan(scanBuilder).build())
        .build()
    } else {
      // There are unsupported scan type
      throw new UnsupportedOperationException(
        s"$outputAttributes contains unsupported data types for CometShuffleExchangeExec.")
    }
  }
}

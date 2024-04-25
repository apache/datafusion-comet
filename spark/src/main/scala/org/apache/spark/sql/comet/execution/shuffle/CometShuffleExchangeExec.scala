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
import java.util.function.Supplier

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

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
    originalPlan: ShuffleExchangeLike,
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
   * This is copied from Spark `ShuffleExchangeExec.needToCopyObjectsBeforeShuffle`. The only
   * difference is that we use `BosonShuffleManager` instead of `SortShuffleManager`.
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[CometShuffleManager]
    val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // This different to Spark `SortShuffleManager`.
        // Comet doesn't use Spark `ExternalSorter` to buffer records in memory, so we don't need to
        // copy.
        false
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
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
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
        // `HashPartitioning.partitionIdExpression` to produce partitioning key.
        new PartitionIdPassthrough(n)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition => new ConstantPartitioner
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        // nextInt(numPartitions) implementation has a special case when bound is a power of 2,
        // which is basically taking several highest bits from the initial seed, with only a
        // minimal scrambling. Due to deterministic seed, using the generator only once,
        // and lack of scrambling, the position values for power-of-two numPartitions always
        // end up being almost the same regardless of the index. substantially scrambling the
        // seed by hashing will help. Refer to SPARK-21782 for more details.
        val partitionId = TaskContext.get().partitionId()
        var position = new XORShiftRandom(partitionId).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection =
          UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // [SPARK-23207] Have to make sure the generated RoundRobinPartitioning is deterministic,
      // otherwise a retry task may output different rows and thus lead to data loss.
      //
      // Currently we following the most straight-forward way that perform a local sort before
      // partitioning.
      //
      // Note that we don't perform local sort if the new partitioning has only 1 partition, under
      // that case all output rows go to the same partition.
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // The comparator for comparing row hashcode, which should always be Integer.
          val prefixComparator = PrefixComparators.LONG

          // The prefix computer generates row hashcode as the prefix, so we may decrease the
          // probability that the prefixes are equal when input rows choose column values from a
          // limited range.
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(
                row: InternalRow): UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            // We are comparing binary here, which does not support radix sort.
            // See more details in SPARK-28699.
            false)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // round-robin function is order sensitive if we don't sort the input.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (CometShuffleExchangeExec.needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
          },
          isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            val mutablePair = new MutablePair[Int, InternalRow]()
            iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
          },
          isOrderSensitive = isOrderSensitive)
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new CometShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
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

/**
 * Copied from Spark `PartitionIdPassthrough` as it is private in Spark 3.2.
 */
private[spark] class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * Copied from Spark `ConstantPartitioner` as it doesn't exist in Spark 3.2.
 */
private[spark] class ConstantPartitioner extends Partitioner {
  override def numPartitions: Int = 1
  override def getPartition(key: Any): Int = 0
}

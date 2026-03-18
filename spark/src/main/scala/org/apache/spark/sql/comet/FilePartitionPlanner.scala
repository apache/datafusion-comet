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

import scala.collection.mutable.HashMap
import scala.concurrent.duration.NANOSECONDS

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.shims.ShimFilePartitionPlanner
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection._

/**
 * Utility class that encapsulates partition listing, dynamic pruning, bucketed/non-bucketed
 * splitting, and driver metric accumulation for file-based scans.
 *
 * This is used by both CometScanExec (hybrid scans) and CometNativeScanExec (native scans).
 */
class FilePartitionPlanner(
    @transient relation: HadoopFsRelation,
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    bucketedScan: Boolean)
    extends ShimFilePartitionPlanner
    with Logging {

  private val accumulatedMetrics: HashMap[String, Long] = HashMap.empty

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter),
        dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs =
      NANOSECONDS.toMillis((System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    accumulatedMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      accumulatedMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  /**
   * Compute the file partitions for this scan.
   */
  def getFilePartitions(): Seq[FilePartition] = {
    if (bucketedScan) {
      createFilePartitionsForBucketedScan(
        relation.bucketSpec.get,
        dynamicallySelectedPartitions,
        relation)
    } else {
      createFilePartitionsForNonBucketedScan(dynamicallySelectedPartitions, relation)
    }
  }

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has been
   * initialized. See SPARK-26327 for more details.
   */
  def sendDriverMetrics(
      metricsMap: Map[String, SQLMetric],
      sparkContext: SparkContext): Unit = {
    accumulatedMetrics.foreach(e => metricsMap(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(
      org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY)
    org.apache.spark.sql.execution.metric.SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
      metricsMap.filter(e => accumulatedMetrics.contains(e._1)).values.toSeq)
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      accumulatedMetrics("numFiles") = filesNum
      accumulatedMetrics("filesSize") = filesSize
    } else {
      accumulatedMetrics("staticFilesNum") = filesNum
      accumulatedMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      accumulatedMetrics("numPartitions") = partitions.length
    }
  }

  /**
   * Create file partitions for bucketed scans.
   *
   * Each partition being returned should include all the files with the same bucket id from all
   * the given Hive partitions.
   */
  private def createFilePartitionsForBucketedScan(
      bucketSpec: BucketSpec,
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): Seq[FilePartition] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions
        .flatMap { p =>
          p.files.map { f =>
            getPartitionedFile(f, p)
          }
        }
        .groupBy { f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath.toString()).getName)
            .getOrElse(
              throw QueryExecutionErrors.invalidBucketFile(f.filePath.toString()))
        }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter { f =>
        bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
      .map { numCoalescedBuckets =>
        logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
        val coalescedBuckets =
          prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
        Seq.tabulate(numCoalescedBuckets) { bucketId =>
          val partitionedFiles = coalescedBuckets
            .get(bucketId)
            .map {
              _.values.flatten.toArray
            }
            .getOrElse(Array.empty)
          FilePartition(bucketId, partitionedFiles)
        }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
          FilePartition(
            bucketId,
            prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }
  }

  /**
   * Create file partitions for non-bucketed scans.
   */
  private def createFilePartitionsForNonBucketedScan(
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): Seq[FilePartition] = {
    val openCostInBytes =
      fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled =
      fsRelation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val splitFiles = selectedPartitions
      .flatMap { partition =>
        partition.files.flatMap { file =>
          // getPath() is very expensive so we only want to call it once in this block:
          val filePath = file.getPath

          if (shouldProcess(filePath)) {
            val isSplitable = relation.fileFormat.isSplitable(
              relation.sparkSession,
              relation.options,
              filePath) &&
              // SPARK-39634: Allow file splitting in combination with row index generation
              // once the fix for PARQUET-2161 is available.
              !isNeededForSchema(requiredSchema)
            this.splitFiles(
              sparkSession = relation.sparkSession,
              file = file,
              filePath = filePath,
              isSplitable = isSplitable,
              maxSplitBytes = maxSplitBytes,
              partitionValues = partition.values)
          } else {
            Seq.empty
          }
        }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(
      relation.sparkSession,
      splitFiles,
      maxSplitBytes)
  }
}

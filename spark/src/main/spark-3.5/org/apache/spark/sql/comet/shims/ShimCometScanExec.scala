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

package org.apache.spark.sql.comet.shims

import scala.math.Ordering.Implicits._

import org.apache.hadoop.fs.Path
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.VersionUtils
import org.apache.spark.util.collection.BitSet

trait ShimCometScanExec {
  def wrapped: FileSourceScanExec

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

  /**
   * Returns file partitions after applying DPP filtering. In Spark 3.x, filters
   * Array[PartitionDirectory] manually.
   *
   * Based on FileSourceScanExec.dynamicallySelectedPartitions and
   * FileSourceScanExec.createNonBucketedReadRDD.
   */
  protected def getDppFilteredFilePartitions(
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      selectedPartitions: Array[PartitionDirectory]): Seq[FilePartition] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
    val filteredPartitions = if (dynamicPartitionFilters.nonEmpty) {
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      selectedPartitions.filter(p => boundPredicate.eval(p.values))
    } else {
      selectedPartitions
    }

    val maxSplitBytes = FilePartition.maxSplitBytes(relation.sparkSession, filteredPartitions)
    val splitFilesList = filteredPartitions
      .flatMap { partition =>
        partition.files.flatMap { file =>
          val filePath = file.getPath
          val isSplitable = relation.fileFormat.isSplitable(
            relation.sparkSession,
            relation.options,
            filePath) && file.getLen > maxSplitBytes
          splitFiles(
            relation.sparkSession,
            file,
            filePath,
            isSplitable,
            maxSplitBytes,
            partition.values)
        }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(relation.sparkSession, splitFilesList, maxSplitBytes)
  }

  /**
   * Returns file partitions for bucketed tables after applying DPP filtering. Groups files by
   * bucket ID to preserve bucket boundaries.
   *
   * Based on FileSourceScanExec.createBucketedReadRDD.
   */
  protected def getDppFilteredBucketedFilePartitions(
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      selectedPartitions: Array[PartitionDirectory],
      bucketSpec: BucketSpec,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int]): Seq[FilePartition] = {
    // First apply DPP filtering
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
    val filteredPartitions = if (dynamicPartitionFilters.nonEmpty) {
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      selectedPartitions.filter(p => boundPredicate.eval(p.values))
    } else {
      selectedPartitions
    }

    // Group files by bucket ID
    val filesGroupedToBuckets = filteredPartitions
      .flatMap { p =>
        p.files.map(f => getPartitionedFile(f, p))
      }
      .groupBy { f =>
        BucketingUtils
          .getBucketId(f.toPath.getName)
          .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.toPath.toString))
      }

    // Apply bucket pruning
    val prunedFilesGroupedToBuckets = optionalBucketSet match {
      case Some(bucketSet) =>
        filesGroupedToBuckets.filter { case (bucketId, _) => bucketSet.get(bucketId) }
      case None =>
        filesGroupedToBuckets
    }

    // Create file partitions - either coalesced or one per bucket
    optionalNumCoalescedBuckets match {
      case Some(numCoalescedBuckets) =>
        val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
        Seq.tabulate(numCoalescedBuckets) { bucketId =>
          val files =
            coalescedBuckets
              .get(bucketId)
              .map(_.values.flatten.toArray)
              .getOrElse(Array.empty[PartitionedFile])
          FilePartition(bucketId, files)
        }
      case None =>
        Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
          FilePartition(
            bucketId,
            prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty[PartitionedFile]))
        }
    }
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  def isSparkVersionAtLeast355: Boolean = {
    VersionUtils.majorMinorPatchVersion(SPARK_VERSION_SHORT) match {
      case Some((major, minor, patch)) => (major, minor, patch) >= (3, 5, 5)
      case None =>
        throw new IllegalArgumentException(s"Malformed Spark version: $SPARK_VERSION_SHORT")
    }
  }

  protected def newFileScanRDD(
      fsRelation: HadoopFsRelation,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readSchema: StructType,
      options: ParquetOptions): FileScanRDD = new FileScanRDD(
    fsRelation.sparkSession,
    readFunction,
    filePartitions,
    readSchema,
    fileConstantMetadataColumns,
    fsRelation.fileFormat.fileConstantMetadataExtractors,
    options)

  // see SPARK-39634
  protected def isNeededForSchema(sparkSchema: StructType): Boolean = false

  protected def getPartitionedFile(
      f: FileStatusWithMetadata,
      p: PartitionDirectory): PartitionedFile =
    // Use reflection to invoke the relevant method according to the spark version
    // See https://github.com/apache/datafusion-comet/issues/1572
    if (isSparkVersionAtLeast355) {
      PartitionedFileUtil.getClass
        .getMethod(
          "getPartitionedFile",
          classOf[FileStatusWithMetadata],
          classOf[Path],
          classOf[InternalRow])
        .invoke(PartitionedFileUtil, f, f.getPath, p.values)
        .asInstanceOf[PartitionedFile]
    } else {
      PartitionedFileUtil.getClass
        .getMethod("getPartitionedFile", classOf[FileStatusWithMetadata], classOf[InternalRow])
        .invoke(PartitionedFileUtil, f, p.values)
        .asInstanceOf[PartitionedFile]
    }

  protected def splitFiles(
      sparkSession: SparkSession,
      file: FileStatusWithMetadata,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    // Use reflection to invoke the relevant method according to the spark version
    // See https://github.com/apache/datafusion-comet/issues/1572
    if (isSparkVersionAtLeast355) {
      PartitionedFileUtil.getClass
        .getMethod(
          "splitFiles",
          classOf[SparkSession],
          classOf[FileStatusWithMetadata],
          classOf[Path],
          java.lang.Boolean.TYPE,
          java.lang.Long.TYPE,
          classOf[InternalRow])
        .invoke(
          PartitionedFileUtil,
          sparkSession,
          file,
          filePath,
          java.lang.Boolean.valueOf(isSplitable),
          java.lang.Long.valueOf(maxSplitBytes),
          partitionValues)
        .asInstanceOf[Seq[PartitionedFile]]
    } else {
      PartitionedFileUtil.getClass
        .getMethod(
          "splitFiles",
          classOf[SparkSession],
          classOf[FileStatusWithMetadata],
          java.lang.Boolean.TYPE,
          java.lang.Long.TYPE,
          classOf[InternalRow])
        .invoke(
          PartitionedFileUtil,
          sparkSession,
          file,
          java.lang.Boolean.valueOf(isSplitable),
          java.lang.Long.valueOf(maxSplitBytes),
          partitionValues)
        .asInstanceOf[Seq[PartitionedFile]]
    }
  }

  protected def getPushedDownFilters(
      relation: HadoopFsRelation,
      dataFilters: Seq[Expression]): Seq[Filter] = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }
}

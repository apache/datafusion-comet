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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BasePredicate, BoundReference, Expression, FileSourceConstantMetadataAttribute, Literal, Predicate}
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, ScalarSubquery, ScanFileListing}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

trait ShimCometScanExec extends ShimStreamSourceAwareSparkPlan {
  def wrapped: FileSourceScanExec

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

  /**
   * Returns file partitions after applying DPP filtering. In Spark 4.0, uses
   * ScanFileListing.filterAndPruneFiles.
   *
   * Based on FileSourceScanExec.dynamicallySelectedPartitions and
   * FileSourceScanLike.createScanFileSplitsForNonBucketedScan.
   */
  protected def getDppFilteredFilePartitions(
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      selectedPartitions: ScanFileListing): Seq[FilePartition] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
    val filteredListing = if (dynamicPartitionFilters.nonEmpty) {
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      selectedPartitions.filterAndPruneFiles(boundPredicate, Seq.empty)
    } else {
      selectedPartitions
    }

    val maxSplitBytes = FilePartition.maxSplitBytes(relation.sparkSession, filteredListing)
    val splitFilesList = filteredListing.filePartitionIterator
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
      .toSeq
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(relation.sparkSession, splitFilesList, maxSplitBytes)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]])

  protected def newFileScanRDD(
      fsRelation: HadoopFsRelation,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readSchema: StructType,
      options: ParquetOptions): FileScanRDD = {
    new FileScanRDD(
      fsRelation.sparkSession,
      readFunction,
      filePartitions,
      readSchema,
      fileConstantMetadataColumns,
      fsRelation.fileFormat.fileConstantMetadataExtractors,
      options)
  }

  // see SPARK-39634
  protected def isNeededForSchema(sparkSchema: StructType): Boolean = false

  protected def getPartitionedFile(
      f: FileStatusWithMetadata,
      p: PartitionDirectory): PartitionedFile =
    PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values, 0, f.getLen)

  protected def splitFiles(
      sparkSession: SparkSession,
      file: FileStatusWithMetadata,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] =
    PartitionedFileUtil.splitFiles(file, filePath, isSplitable, maxSplitBytes, partitionValues)

  protected def getPushedDownFilters(
      relation: HadoopFsRelation,
      dataFilters: Seq[Expression]): Seq[Filter] = {
    translateToV1Filters(relation, dataFilters, _.toLiteral)
  }

  // From Spark FileSourceScanLike
  private def translateToV1Filters(
      relation: HadoopFsRelation,
      dataFilters: Seq[Expression],
      scalarSubqueryToLiteral: ScalarSubquery => Literal): Seq[Filter] = {
    val scalarSubqueryReplaced = dataFilters.map(_.transform {
      // Replace scalar subquery to literal so that `DataSourceStrategy.translateFilter` can
      // support translating it.
      case scalarSubquery: ScalarSubquery => scalarSubqueryToLiteral(scalarSubquery)
    })

    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    // `dataFilters` should not include any constant metadata col filters
    // because the metadata struct has been flatted in FileSourceStrategy
    // and thus metadata col filters are invalid to be pushed down. Metadata that is generated
    // during the scan can be used for filters.
    scalarSubqueryReplaced
      .filterNot(_.references.exists {
        case FileSourceConstantMetadataAttribute(_) => true
        case _ => false
      })
      .flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

}

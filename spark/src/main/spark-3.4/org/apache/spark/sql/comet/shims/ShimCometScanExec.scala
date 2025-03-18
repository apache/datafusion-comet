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

import org.apache.comet.shims.ShimFileFormat

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

trait ShimCometScanExec {
  def wrapped: FileSourceScanExec

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

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
    options)

  protected def isNeededForSchema(sparkSchema: StructType): Boolean = {
    // TODO: remove after PARQUET-2161 becomes available in Parquet (tracked in SPARK-39634)
    ShimFileFormat.findRowIndexColumnIndexInSchema(sparkSchema) >= 0
  }

  protected def getPartitionedFile(f: FileStatus, p: PartitionDirectory): PartitionedFile =
    PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)

  protected def splitFiles(sparkSession: SparkSession,
                           file: FileStatus,
                           filePath: Path,
                           isSplitable: Boolean,
                           maxSplitBytes: Long,
                           partitionValues: InternalRow): Seq[PartitionedFile] =
    PartitionedFileUtil.splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)

  protected def getPushedDownFilters(relation: HadoopFsRelation , dataFilters: Seq[Expression]):  Seq[Filter] = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

}

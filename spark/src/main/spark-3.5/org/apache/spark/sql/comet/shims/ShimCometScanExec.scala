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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
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
    fsRelation.fileFormat.fileConstantMetadataExtractors,
    options)

  // see SPARK-39634
  protected def isNeededForSchema(sparkSchema: StructType): Boolean = false

  protected def getPartitionedFile(f: FileStatusWithMetadata, p: PartitionDirectory): PartitionedFile =
    try {
      PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
    } catch {
      case _: NoSuchMethodError =>
        // Fallback using reflection without the Path parameter for backward-compatability
        // See https://github.com/apache/datafusion-comet/issues/1572
        PartitionedFileUtil.getClass.getMethod("getPartitionedFile",
          classOf[FileStatusWithMetadata],
          classOf[InternalRow]
        ).invoke(PartitionedFileUtil,
          f,
          p.values
        ).asInstanceOf[PartitionedFile]
    }

  protected def splitFiles(sparkSession: SparkSession,
                           file: FileStatusWithMetadata,
                           filePath: Path,
                           isSplitable: Boolean,
                           maxSplitBytes: Long,
                           partitionValues: InternalRow): Seq[PartitionedFile] = {
    try {
      PartitionedFileUtil.splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)
    } catch {
      case _: NoSuchMethodError =>
        // Fallback using reflection without the Path parameter for backward-compatability
        // See https://github.com/apache/datafusion-comet/issues/1572
        PartitionedFileUtil.getClass.getMethod("splitFiles",
          classOf[SparkSession],
          classOf[FileStatusWithMetadata],
          java.lang.Boolean.TYPE,
          java.lang.Long.TYPE,
          classOf[InternalRow]
        ).invoke(PartitionedFileUtil,
          sparkSession,
          file,
          java.lang.Boolean.valueOf(isSplitable),
          java.lang.Long.valueOf(maxSplitBytes),
          partitionValues
        ).asInstanceOf[Seq[PartitionedFile]]
    }
  }

  protected def getPushedDownFilters(relation: HadoopFsRelation , dataFilters: Seq[Expression]):  Seq[Filter] = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }
}

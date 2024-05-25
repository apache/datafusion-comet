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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanLike, PartitionedFileUtil, ScanFileListing}
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext

trait ShimCometScanExec extends FileSourceScanLike {
  def wrapped: FileSourceScanExec

  override lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

  protected def newDataSourceRDD(
      sc: SparkContext,
      inputPartitions: Seq[Seq[InputPartition]],
      partitionReaderFactory: PartitionReaderFactory,
      columnarReads: Boolean,
      customMetrics: Map[String, SQLMetric]): DataSourceRDD =
    new DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads, customMetrics)

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

  protected def invalidBucketFile(path: String, sparkVersion: String): Throwable =
    QueryExecutionErrors.invalidBucketFile(path)

  // see SPARK-39634
  protected def isNeededForSchema(sparkSchema: StructType): Boolean = false

  protected def getPartitionedFile(f: FileStatusWithMetadata, p: PartitionDirectory): PartitionedFile =
    PartitionedFileUtil.getPartitionedFile(f, p.values, 0, f.getLen)

  protected def splitFiles(sparkSession: SparkSession,
                           file: FileStatusWithMetadata,
                           filePath: Path,
                           isSplitable: Boolean,
                           maxSplitBytes: Long,
                           partitionValues: InternalRow): Seq[PartitionedFile] =
    PartitionedFileUtil.splitFiles(file, isSplitable, maxSplitBytes, partitionValues)

  @transient override lazy val selectedPartitions: ScanFileListing = wrapped.selectedPartitions

  // exposed for testing
  override lazy val bucketedScan: Boolean = wrapped.bucketedScan

  lazy val inputRDD = wrapped.inputRDD
}

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
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext

trait ShimCometScanExec {
  def wrapped: FileSourceScanExec

  // TODO: remove after dropping Spark 3.2 support and directly call wrapped.metadataColumns
  lazy val metadataColumns: Seq[AttributeReference] = wrapped.getClass.getDeclaredMethods
    .filter(_.getName == "metadataColumns")
    .map { a => a.setAccessible(true); a }
    .flatMap(_.invoke(wrapped).asInstanceOf[Seq[AttributeReference]])

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

  protected def newDataSourceRDD(
      sc: SparkContext,
      inputPartitions: Seq[Seq[InputPartition]],
      partitionReaderFactory: PartitionReaderFactory,
      columnarReads: Boolean,
      customMetrics: Map[String, SQLMetric]): DataSourceRDD =
    new DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads, customMetrics)

  // TODO: remove after dropping Spark 3.2 support and directly call new FileScanRDD
  protected def newFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readSchema: StructType,
      options: ParquetOptions): FileScanRDD =
    classOf[FileScanRDD].getDeclaredConstructors
      .map { c =>
        c.getParameterCount match {
          case 3 => c.newInstance(sparkSession, readFunction, filePartitions)
          case 5 =>
            c.newInstance(sparkSession, readFunction, filePartitions, readSchema, metadataColumns)
          case 6 =>
            c.newInstance(
              sparkSession,
              readFunction,
              filePartitions,
              readSchema,
              fileConstantMetadataColumns,
              options)
        }
      }
      .last
      .asInstanceOf[FileScanRDD]

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
}

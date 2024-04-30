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

package org.apache.comet.shims

import scala.language.implicitConversions

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}

trait ShimCometScanExec {
  def wrapped: FileSourceScanExec

  // TODO: remove after dropping Spark 3.2 support and directly call wrapped.metadataColumns
  lazy val metadataColumns: Seq[AttributeReference] = wrapped.getClass.getDeclaredMethods
    .filter(_.getName == "metadataColumns")
    .map { a => a.setAccessible(true); a }
    .flatMap(_.invoke(wrapped).asInstanceOf[Seq[AttributeReference]])

  // TODO: remove after dropping Spark 3.2 and 3.3 support and directly call
  //       wrapped.fileConstantMetadataColumns
  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.getClass.getDeclaredMethods
      .filter(_.getName == "fileConstantMetadataColumns")
      .map { a => a.setAccessible(true); a }
      .flatMap(_.invoke(wrapped).asInstanceOf[Seq[AttributeReference]])

  // TODO: remove after dropping Spark 3.2 support and directly call new DataSourceRDD
  protected def newDataSourceRDD(
      sc: SparkContext,
      inputPartitions: Seq[Seq[InputPartition]],
      partitionReaderFactory: PartitionReaderFactory,
      columnarReads: Boolean,
      customMetrics: Map[String, SQLMetric]): DataSourceRDD = {
    implicit def flattenSeq(p: Seq[Seq[InputPartition]]): Seq[InputPartition] = p.flatten
    new DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads, customMetrics)
  }

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

  // TODO: remove after dropping Spark 3.2 and 3.3 support and directly call
  //       QueryExecutionErrors.SparkException
  protected def invalidBucketFile(path: String, sparkVersion: String): Throwable = {
    if (sparkVersion >= "3.3") {
      val messageParameters = if (sparkVersion >= "3.4") Map("path" -> path) else Array(path)
      classOf[SparkException].getDeclaredConstructors
        .filter(_.getParameterCount == 3)
        .map(_.newInstance("INVALID_BUCKET_FILE", messageParameters, null))
        .last
        .asInstanceOf[SparkException]
    } else { // Spark 3.2
      new IllegalStateException(s"Invalid bucket file ${path}")
    }
  }

  // Copied from Spark 3.4 RowIndexUtil due to PARQUET-2161 (tracked in SPARK-39634)
  // TODO: remove after PARQUET-2161 becomes available in Parquet
  private def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = {
    sparkSchema.fields.zipWithIndex.find { case (field: StructField, _: Int) =>
      field.name == ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    } match {
      case Some((field: StructField, idx: Int)) =>
        if (field.dataType != LongType) {
          throw new RuntimeException(
            s"${ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} must be of LongType")
        }
        idx
      case _ => -1
    }
  }

  protected def isNeededForSchema(sparkSchema: StructType): Boolean = {
    findRowIndexColumnIndexInSchema(sparkSchema) >= 0
  }
}

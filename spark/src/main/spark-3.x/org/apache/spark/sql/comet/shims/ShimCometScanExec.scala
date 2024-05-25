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

import scala.language.implicitConversions

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}

trait ShimCometScanExec extends DataSourceScanExec {
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
      fsRelation: HadoopFsRelation,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readSchema: StructType,
      options: ParquetOptions): FileScanRDD =
    classOf[FileScanRDD].getDeclaredConstructors
      // Prevent to pick up incorrect constructors from any custom Spark forks.
      .filter(c => List(3, 5, 6).contains(c.getParameterCount()) )
      .map { c =>
        c.getParameterCount match {
          case 3 => c.newInstance(fsRelation.sparkSession, readFunction, filePartitions)
          case 5 =>
            c.newInstance(fsRelation.sparkSession, readFunction, filePartitions, readSchema, metadataColumns)
          case 6 =>
            c.newInstance(
              fsRelation.sparkSession,
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

  protected def getPartitionedFile(f: FileStatus, p: PartitionDirectory): PartitionedFile =
    PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)

  protected def splitFiles(sparkSession: SparkSession,
                           file: FileStatus,
                           filePath: Path,
                           isSplitable: Boolean,
                           maxSplitBytes: Long,
                           partitionValues: InternalRow): Seq[PartitionedFile] =
    PartitionedFileUtil.splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)

  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs =
      NANOSECONDS.toMillis((System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
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
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  // exposed for testing
  lazy val bucketedScan: Boolean = wrapped.bucketedScan

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (ShimFileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf =
          relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(
        relation.bucketSpec.get,
        readFile,
        dynamicallySelectedPartitions,
        relation)
    } else {
      createReadRDD(readFile, dynamicallySelectedPartitions, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
                                        partitions: Seq[PartitionDirectory],
                                        static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions") = partitions.length
    }
  }
}

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
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.util.VersionUtils
import scala.math.Ordering.Implicits._

trait ShimCometScanExec {
  def wrapped: FileSourceScanExec

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] =
    wrapped.fileConstantMetadataColumns

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

  protected def getPartitionedFile(f: FileStatusWithMetadata, p: PartitionDirectory): PartitionedFile =
    // Use reflection to invoke the relevant method according to the spark version
    // See https://github.com/apache/datafusion-comet/issues/1572
    if (isSparkVersionAtLeast355) {
      PartitionedFileUtil.getClass.getMethod("getPartitionedFile",
        classOf[FileStatusWithMetadata],
        classOf[Path],
        classOf[InternalRow]
      ).invoke(PartitionedFileUtil,
        f,
        f.getPath,
        p.values
      ).asInstanceOf[PartitionedFile]
    } else {
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
    // Use reflection to invoke the relevant method according to the spark version
    // See https://github.com/apache/datafusion-comet/issues/1572
    if (isSparkVersionAtLeast355) {
      PartitionedFileUtil.getClass.getMethod("splitFiles",
        classOf[SparkSession],
        classOf[FileStatusWithMetadata],
        classOf[Path],
        java.lang.Boolean.TYPE,
        java.lang.Long.TYPE,
        classOf[InternalRow]
      ).invoke(PartitionedFileUtil,
        sparkSession,
        file,
        filePath,
        java.lang.Boolean.valueOf(isSplitable),
        java.lang.Long.valueOf(maxSplitBytes),
        partitionValues
      ).asInstanceOf[Seq[PartitionedFile]]
    } else {
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

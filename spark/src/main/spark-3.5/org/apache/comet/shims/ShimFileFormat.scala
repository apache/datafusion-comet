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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetRowIndexUtil
import org.apache.spark.sql.types.{DataType, StructType}

object ShimFileFormat {
  // A name for a temporary column that holds row indexes computed by the file format reader
  // until they can be placed in the _metadata struct.
  val ROW_INDEX_TEMPORARY_COLUMN_NAME = ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME

  // Whether Spark's text/line readers would decompress this file. Before Spark 4.1 the codec is
  // resolved purely through CompressionCodecFactory (the non-standard `.gzip`/`.zstd` extensions
  // are read raw on these versions, so they are intentionally not treated as compressed here).
  def isCompressedFile(hadoopConf: Configuration, path: Path): Boolean =
    new CompressionCodecFactory(hadoopConf).getCodec(path) != null

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int =
    ParquetRowIndexUtil.findRowIndexColumnIndexInSchema(sparkSchema)

  def fileConstantMetadataExtractors(
      fileFormat: FileFormat): Map[String, PartitionedFile => Any] =
    fileFormat.fileConstantMetadataExtractors

  // dataType is unused on this Spark version; getFileConstantMetadataColumnValue only gained
  // a dataType parameter in Spark 4.2 (SPARK-56931). Accepted here so callers can pass it
  // uniformly across Spark versions.
  def getFileConstantMetadataColumnValue(
      name: String,
      file: PartitionedFile,
      extractors: Map[String, PartitionedFile => Any],
      dataType: DataType): Literal =
    FileFormat.getFileConstantMetadataColumnValue(name, file, extractors)
}

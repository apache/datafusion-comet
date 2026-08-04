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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile, RowIndexUtil}
import org.apache.spark.sql.types.{DataType, StructType}

object ShimFileFormat {

  // A name for a temporary column that holds row indexes computed by the file format reader
  // until they can be placed in the _metadata struct.
  val ROW_INDEX_TEMPORARY_COLUMN_NAME: String = FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int =
    RowIndexUtil.findRowIndexColumnIndexInSchema(sparkSchema)

  // Spark 3.4 has no per-format metadata extractor concept (added in Spark 3.5, SPARK-43868);
  // it derives these values inline in FileFormat.updateMetadataInternalRow. Replicate that
  // logic here so callers can use the same extractor-map shape across all Spark versions.
  private val baseMetadataExtractors: Map[String, PartitionedFile => Any] = Map(
    FileFormat.FILE_PATH -> { pf: PartitionedFile =>
      // Use `new Path(Path.toString)` as a form of canonicalization
      new Path(pf.filePath.toPath.toString).toUri.toString
    },
    FileFormat.FILE_NAME -> { pf: PartitionedFile =>
      pf.filePath.toUri.getRawPath.split("/").lastOption.getOrElse("")
    },
    FileFormat.FILE_SIZE -> { pf: PartitionedFile => pf.fileSize },
    FileFormat.FILE_BLOCK_START -> { pf: PartitionedFile => pf.start },
    FileFormat.FILE_BLOCK_LENGTH -> { pf: PartitionedFile => pf.length },
    // The modificationTime from the file has millisecond granularity, but the TimestampType for
    // `file_modification_time` has microsecond granularity.
    FileFormat.FILE_MODIFICATION_TIME -> { pf: PartitionedFile => pf.modificationTime * 1000 })

  def fileConstantMetadataExtractors(
      fileFormat: FileFormat): Map[String, PartitionedFile => Any] =
    baseMetadataExtractors

  // dataType is unused on this Spark version; getFileConstantMetadataColumnValue only gained
  // a dataType parameter in Spark 4.2 (SPARK-56931). Accepted here so callers can pass it
  // uniformly across Spark versions.
  def getFileConstantMetadataColumnValue(
      name: String,
      file: PartitionedFile,
      extractors: Map[String, PartitionedFile => Any],
      dataType: DataType): Literal =
    Literal(extractors(name)(file))
}

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

package org.apache.comet.serde

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.shims.ShimFileFormat

package object operator {

  def schema2Proto(fields: Seq[StructField]): Seq[OperatorOuterClass.SparkStructField] = {
    val fieldBuilder = OperatorOuterClass.SparkStructField.newBuilder()
    fields.map { field =>
      fieldBuilder.setName(field.name)
      fieldBuilder.setDataType(serializeDataType(field.dataType).get)
      fieldBuilder.setNullable(field.nullable)
      fieldBuilder.clearMetadata()
      if (ParquetUtils.hasFieldId(field)) {
        fieldBuilder.putMetadata(
          CometParquetUtils.PARQUET_FIELD_ID_META_KEY,
          ParquetUtils.getFieldId(field).toString)
      }
      fieldBuilder.build()
    }
  }

  def partition2Proto(
      partition: FilePartition,
      partitionSchema: StructType,
      constantMetadataColumns: Seq[AttributeReference] = Seq.empty,
      fileConstantMetadataExtractors: Map[String, PartitionedFile => Any] = Map.empty)
      : OperatorOuterClass.SparkFilePartition = {
    val partitionBuilder = OperatorOuterClass.SparkFilePartition.newBuilder()
    partition.files.foreach(file => {
      // Process the partition values
      val partitionValues = file.partitionValues
      assert(partitionValues.numFields == partitionSchema.length)
      val partitionVals =
        partitionValues.toSeq(partitionSchema).zipWithIndex.map { case (value, i) =>
          val attr = partitionSchema(i)
          literalToProto(
            Literal(value, attr.dataType),
            s"partition value: $value, type: ${attr.dataType}")
        }
      // Constant metadata columns (file_path, file_name, file_size, file_block_start,
      // file_block_length, file_modification_time) are, like partition columns, known before
      // opening the file and constant for every row read from it. Reuse the same
      // partition-value wire format and projection mechanism, and Spark's own extractor
      // dispatch (which also covers custom per-format overrides), rather than a bespoke one.
      // getFileConstantMetadataColumnValue's Literal has an inferred, not declared, dataType
      // (e.g. file_modification_time's raw micros value infers as LongType, not
      // TimestampType) -- take only its value and retype against the attribute's actual
      // dataType, exactly as Spark's own FileFormat.updateMetadataInternalRow does
      // (`row.update(i, literal.value)`).
      val metadataVals = constantMetadataColumns.map { attr =>
        val value = ShimFileFormat
          .getFileConstantMetadataColumnValue(attr.name, file, fileConstantMetadataExtractors)
          .value
        literalToProto(Literal(value, attr.dataType), s"metadata column value for ${attr.name}")
      }
      val fileBuilder = OperatorOuterClass.SparkPartitionedFile.newBuilder()
      (partitionVals ++ metadataVals).foreach(fileBuilder.addPartitionValues)
      fileBuilder
        .setFilePath(file.filePath.toString)
        .setStart(file.start)
        .setLength(file.length)
        .setFileSize(file.fileSize)
      partitionBuilder.addPartitionedFile(fileBuilder.build())
    })
    partitionBuilder.build()
  }

  // In `CometScanRule`, we have already checked that all partition and metadata column values
  // are supported. So, we can safely use `get` here.
  private def literalToProto(literal: Literal, description: String): ExprOuterClass.Expr = {
    val valueProto = exprToProto(literal, Seq.empty)
    assert(valueProto.isDefined, s"Unsupported $description")
    valueProto.get
  }
}

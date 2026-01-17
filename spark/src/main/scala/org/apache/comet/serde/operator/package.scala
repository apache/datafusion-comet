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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

package object operator {

  def schema2Proto(fields: Array[StructField]): Array[OperatorOuterClass.SparkStructField] = {
    val fieldBuilder = OperatorOuterClass.SparkStructField.newBuilder()
    fields.map { field =>
      fieldBuilder.setName(field.name)
      fieldBuilder.setDataType(serializeDataType(field.dataType).get)
      fieldBuilder.setNullable(field.nullable)
      fieldBuilder.build()
    }
  }

  def partition2Proto(
      partition: FilePartition,
      partitionSchema: StructType): OperatorOuterClass.SparkFilePartition = {
    val partitionBuilder = OperatorOuterClass.SparkFilePartition.newBuilder()
    partition.files.foreach(file => {
      // Process the partition values
      val partitionValues = file.partitionValues
      assert(partitionValues.numFields == partitionSchema.length)
      val partitionVals =
        partitionValues.toSeq(partitionSchema).zipWithIndex.map { case (value, i) =>
          val attr = partitionSchema(i)
          val valueProto = exprToProto(Literal(value, attr.dataType), Seq.empty)
          // In `CometScanRule`, we have already checked that all partition values are
          // supported. So, we can safely use `get` here.
          assert(
            valueProto.isDefined,
            s"Unsupported partition value: $value, type: ${attr.dataType}")
          valueProto.get
        }
      val fileBuilder = OperatorOuterClass.SparkPartitionedFile.newBuilder()
      partitionVals.foreach(fileBuilder.addPartitionValues)
      fileBuilder
        .setFilePath(file.filePath.toString)
        .setStart(file.start)
        .setLength(file.length)
        .setFileSize(file.fileSize)
      partitionBuilder.addPartitionedFile(fileBuilder.build())
    })
    partitionBuilder.build()
  }
}

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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

case class CometCsvNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec {
  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometCsvNativeScan"

  override def outputPartitioning: Partitioning = UnknownPartitioning(
    originalPlan.inputPartitions.length)

  override def outputOrdering: Seq[SortOrder] = Nil

  override protected def doCanonicalize(): SparkPlan = {
    CometCsvNativeScanExec(nativeOp, output, originalPlan, serializedPlanOpt)
  }
}

object CometCsvNativeScanExec extends CometOperatorSerde[CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_CSV_V2_NATIVE_ENABLED)

  override def convert(
      op: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val csvScanBuilder = OperatorOuterClass.CsvScan.newBuilder()
    val schemaProto = schema2Proto(op.schema)
    val partitionsProto =
      op.inputPartitions.map(partition => partition2Proto(partition.asInstanceOf[FilePartition]))
    csvScanBuilder.addAllFilePartitions(partitionsProto.asJava)
    val hadoopConf = op.session.sessionState
      .newHadoopConfWithOptions(op.session.sparkContext.conf.getAll.toMap)
    op.inputPartitions.headOption.foreach { partitionFile =>
      val objectStoreOptions =
        NativeConfig.extractObjectStoreOptions(
          hadoopConf,
          partitionFile.asInstanceOf[FilePartition].files.head.pathUri)
      objectStoreOptions.foreach { case (key, value) =>
        csvScanBuilder.putObjectStoreOptions(key, value)
      }
    }
    csvScanBuilder.addAllSchema(schemaProto.asJava)
    Some(builder.setCsvScan(csvScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    CometCsvNativeScanExec(nativeOp, op.output, op.wrapped, SerializedPlan(None))
  }

  private def schema2Proto(schema: StructType): Seq[OperatorOuterClass.SparkStructField] = {
    val fieldBuilder = OperatorOuterClass.SparkStructField.newBuilder()
    schema.fields.map { sf =>
      fieldBuilder.setDataType(serializeDataType(sf.dataType).get)
      fieldBuilder.setName(sf.name)
      fieldBuilder.setNullable(sf.nullable)
      fieldBuilder.build()
    }.toSeq
  }

  private def partition2Proto(partition: FilePartition): OperatorOuterClass.SparkFilePartition = {
    val partitionBuilder = OperatorOuterClass.SparkFilePartition.newBuilder()
    partition.files.foreach { file =>
      val filePartitionBuilder = OperatorOuterClass.SparkPartitionedFile.newBuilder()
      filePartitionBuilder
        .setLength(file.length)
        .setFilePath(file.filePath.toString)
        .setStart(file.start)
        .setFileSize(file.fileSize)
      partitionBuilder.addPartitionedFile(filePartitionBuilder.build())
    }
    partitionBuilder.build()
  }
}

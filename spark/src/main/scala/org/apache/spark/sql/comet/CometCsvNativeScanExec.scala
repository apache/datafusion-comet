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
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.google.protobuf.ByteString

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.schema2Proto

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

  private val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_CSV_V2_NATIVE_ENABLED)

  override def convert(
      op: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val csvScanBuilder = OperatorOuterClass.CsvScan.newBuilder()
    val csvScan = op.wrapped.scan.asInstanceOf[CSVScan]
    val columnPruning = op.session.sessionState.conf.csvColumnPruning
    val timeZone = op.session.sessionState.conf.sessionLocalTimeZone

    val csvOptionsProto = csvOptions2Proto(csvScan.options, columnPruning, timeZone)
    csvScanBuilder.setCsvOptions(csvOptionsProto)

    val schemaProto = schema2Proto(op.schema.fields)
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
    csvScanBuilder.addAllRequiredSchema(schemaProto.toIterable.asJava)
    Some(builder.setCsvScan(csvScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    CometCsvNativeScanExec(nativeOp, op.output, op.wrapped, SerializedPlan(None))
  }

  private def csvOptions2Proto(
      parameters: CaseInsensitiveStringMap,
      columnPruning: Boolean,
      timeZone: String): OperatorOuterClass.CsvOptions = {
    val csvOptionsBuilder = OperatorOuterClass.CsvOptions.newBuilder()
    val options = new CSVOptions(parameters.asScala.toMap, columnPruning, timeZone)
    csvOptionsBuilder.setDelimiter(options.delimiter)
    csvOptionsBuilder.setHasHeader(options.headerFlag)
    csvOptionsBuilder.setQuote(options.quote.toString)
    csvOptionsBuilder.setEscape(options.escape.toString)
    csvOptionsBuilder.setNullValue(options.nullValue)
    if (options.isCommentSet) {
      csvOptionsBuilder.setComment(options.comment.toString)
    }
    csvOptionsBuilder.setDateFormat(options.dateFormatInRead.getOrElse(DEFAULT_DATE_FORMAT))
    csvOptionsBuilder.build()
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

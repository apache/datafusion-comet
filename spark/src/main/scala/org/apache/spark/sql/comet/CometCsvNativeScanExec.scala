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

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.{partition2Proto, schema2Proto}

/*
 * Native CSV scan operator that delegates file reading to datafusion.
 */
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

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometCsvNativeScanExec =>
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.originalPlan == other.originalPlan
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(output, serializedPlanOpt, originalPlan)
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
    val csvScan = op.wrapped.scan.asInstanceOf[CSVScan]
    val sessionState = op.session.sessionState
    val options = {
      val columnPruning = sessionState.conf.csvColumnPruning
      val timeZone = sessionState.conf.sessionLocalTimeZone
      new CSVOptions(csvScan.options.asScala.toMap, columnPruning, timeZone)
    }
    val filePartitions = op.inputPartitions.map(_.asInstanceOf[FilePartition])
    val csvOptionsProto = csvOptions2Proto(options)
    val dataSchemaProto = schema2Proto(csvScan.dataSchema.fields)
    val readSchemaFieldNames = csvScan.readDataSchema.fieldNames
    val projectionVector = csvScan.dataSchema.fields.zipWithIndex
      .filter { case (field, _) =>
        readSchemaFieldNames.contains(field.name)
      }
      .map(_._2.asInstanceOf[Integer])
    val partitionSchemaProto = schema2Proto(csvScan.readPartitionSchema.fields)
    val partitionsProto = filePartitions.map(partition2Proto(_, csvScan.readPartitionSchema))

    val objectStoreOptions = filePartitions.headOption
      .flatMap { partitionFile =>
        val hadoopConf = sessionState
          .newHadoopConfWithOptions(op.session.sparkContext.conf.getAll.toMap)
        partitionFile.files.headOption
          .map(file => NativeConfig.extractObjectStoreOptions(hadoopConf, file.pathUri))
      }
      .getOrElse(Map.empty)

    csvScanBuilder.putAllObjectStoreOptions(objectStoreOptions.asJava)
    csvScanBuilder.setCsvOptions(csvOptionsProto)
    csvScanBuilder.addAllFilePartitions(partitionsProto.asJava)
    csvScanBuilder.addAllDataSchema(dataSchemaProto.toIterable.asJava)
    csvScanBuilder.addAllProjectionVector(projectionVector.toIterable.asJava)
    csvScanBuilder.addAllPartitionSchema(partitionSchemaProto.toIterable.asJava)
    Some(builder.setCsvScan(csvScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    CometCsvNativeScanExec(nativeOp, op.output, op.wrapped, SerializedPlan(None))
  }

  private def csvOptions2Proto(options: CSVOptions): OperatorOuterClass.CsvOptions = {
    val csvOptionsBuilder = OperatorOuterClass.CsvOptions.newBuilder()
    csvOptionsBuilder.setDelimiter(options.delimiter)
    csvOptionsBuilder.setHasHeader(options.headerFlag)
    csvOptionsBuilder.setQuote(options.quote.toString)
    csvOptionsBuilder.setEscape(options.escape.toString)
    csvOptionsBuilder.setTerminator(options.lineSeparator.getOrElse("\n"))
    csvOptionsBuilder.setTruncatedRows(options.multiLine)
    if (options.isCommentSet) {
      csvOptionsBuilder.setComment(options.comment.toString)
    }
    csvOptionsBuilder.build()
  }
}

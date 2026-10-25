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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.text.TextOptions
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.{partition2Proto, schema2Proto}

/*
 * Native Text scan operator that delegates file reading to datafusion. Mirrors the native CSV
 * scan path; produces a single `value: string` column.
 */
case class CometTextNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec {
  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometTextNativeScan"

  // Show only the semantic output in EXPLAIN / the SQL tab / the event log. The default would print
  // the whole `nativeOp` protobuf, which carries every input file path (large for a lookup
  // directory of many files). Mirrors CometNativeScanExec.
  override def stringArgs: Iterator[Any] = Iterator(output)

  // The serialized partitions, which split any Spark partition that mixes object stores.
  override def outputPartitioning: Partitioning = UnknownPartitioning(
    nativeOp.getTextScan.getFilePartitionsCount)

  override def outputOrdering: Seq[SortOrder] = Nil

  // The scan reports the bytes it fetched as `bytes_scanned`, which CometMetricNode turns into the
  // task's input bytes and records, so they show on the stage UI like any other scan.
  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext) +
      ("bytes_scanned" -> SQLMetrics.createSizeMetric(sparkContext, "number of bytes scanned"))

  // Two scans of the same files must compare equal so Spark reuses their exchanges and stages.
  // That needs normalized output attribute ids and the wrapped scan's canonical form, and no
  // serialization state, which differs between otherwise identical copies.
  override protected def doCanonicalize(): SparkPlan = {
    CometTextNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      if (originalPlan == null) null else originalPlan.doCanonicalize(),
      SerializedPlan(None))
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometTextNativeScanExec =>
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

object CometTextNativeScanExec extends CometOperatorSerde[CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_TEXT_V2_NATIVE_ENABLED)

  override def convert(
      op: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val textScanBuilder = OperatorOuterClass.TextScan.newBuilder()
    val textScan = op.wrapped.scan.asInstanceOf[TextScan]
    val sessionState = op.session.sessionState
    val options = new TextOptions(textScan.options.asScala.toMap)
    // Build the Hadoop conf from the scan's read options (case preserved), matching Spark's
    // TextScan.createReaderFactory, so per-read settings like fs.s3a.endpoint reach native.
    val hadoopConf =
      sessionState.newHadoopConfWithOptions(textScan.options.asCaseSensitiveMap.asScala.toMap)
    val s3CompliantSchemes = NativeConfig.resolveS3CompliantSchemes(hadoopConf)
    // The scan gate already declined schemes that only the read options route through libhdfs.
    val libhdfsSchemes = NativeConfig
      .resolveScanLibhdfsSchemes(
        NativeConfig.resolveLibhdfsSchemes(sessionState.newHadoopConf()),
        hadoopConf)
      .libhdfs
    val inputPartitions = op.inputPartitions.map(_.asInstanceOf[FilePartition])
    val multiStoreReason = CometScanUtils.multiStoreFallbackReason(
      "Native Text scan",
      inputPartitions.view.flatMap(_.files.view.map(_.pathUri)),
      s3CompliantSchemes,
      libhdfsSchemes,
      isBucketedScan = false)
    if (multiStoreReason.nonEmpty) {
      // CometExecRule falls back to the wrapped scan, so tag it too for the explain output.
      withFallbackReason(op, multiStoreReason.get)
      withFallbackReason(op.wrapped, multiStoreReason.get)
      return None
    }
    // The options of every scheme the scan reads, or a decline for the ABFS layouts one
    // resolution cannot serve. Checked before the split below so that its reason is reported.
    val fileUris = inputPartitions.view.flatMap(_.files).map(_.pathUri)
    val objectStoreOptions =
      NativeConfig.extractScanObjectStoreOptions(hadoopConf, fileUris) match {
        case Right(options) => options
        case Left(reason) =>
          withFallbackReason(op, reason)
          withFallbackReason(op.wrapped, reason)
          return None
      }
    // Native planning reads each partition through one object store.
    val filePartitions = CometScanUtils.splitPartitionsByStore(
      inputPartitions,
      file => NativeConfig.objectStoreKey(file.pathUri, s3CompliantSchemes, libhdfsSchemes))
    // Spark 3.4 reports a one-partition scan as SinglePartition and plans no exchange above it,
    // so the operators above rely on the scan's partition count.
    if (filePartitions.length != inputPartitions.length &&
      !op.wrapped.outputPartitioning.isInstanceOf[UnknownPartitioning]) {
      val reason = "Native Text scan would split a partition that mixes object stores, but " +
        "Spark planned the operators above it for the scan's own partitioning"
      withFallbackReason(op, reason)
      withFallbackReason(op.wrapped, reason)
      return None
    }
    val textOptionsProto = textOptions2Proto(options)
    val dataSchemaProto = schema2Proto(textScan.dataSchema)
    val readSchemaFieldNames = textScan.readDataSchema.fieldNames
    val projectionVector = textScan.dataSchema.zipWithIndex
      .filter { case (field, _) =>
        readSchemaFieldNames.contains(field.name)
      }
      .map(_._2.asInstanceOf[Integer])
    val partitionSchemaProto = schema2Proto(textScan.readPartitionSchema)
    val partitionsProto = filePartitions.map(partition2Proto(_, textScan.readPartitionSchema))

    textScanBuilder.putAllObjectStoreOptions(objectStoreOptions.asJava)
    textScanBuilder.setTextOptions(textOptionsProto)
    textScanBuilder.addAllFilePartitions(partitionsProto.asJava)
    textScanBuilder.addAllDataSchema(dataSchemaProto.asJava)
    textScanBuilder.addAllProjectionVector(projectionVector.asJava)
    textScanBuilder.addAllPartitionSchema(partitionSchemaProto.asJava)
    Some(builder.setTextScan(textScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    CometTextNativeScanExec(nativeOp, op.output, op.wrapped, SerializedPlan(None))
  }

  private def textOptions2Proto(options: TextOptions): OperatorOuterClass.TextOptions = {
    val textOptionsBuilder = OperatorOuterClass.TextOptions.newBuilder()
    textOptionsBuilder.setWholeText(options.wholeText)
    // Send the exact separator bytes Spark computed (already encoded with the file's charset via
    // the `encoding` option); the native reader splits on these bytes directly.
    options.lineSeparatorInRead.foreach(bytes =>
      textOptionsBuilder.setLineSep(com.google.protobuf.ByteString.copyFrom(bytes)))
    textOptionsBuilder.build()
  }
}

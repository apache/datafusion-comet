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

package org.apache.comet.serde.operator

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.comet.{CometEmptyRelationExec, CometNativeExec, CometNativeWriteExec, CometScanWrapper}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * CometOperatorSerde implementation for DataWritingCommandExec that converts Parquet write
 * operations to use Comet's native Parquet writer.
 */
object CometDataWritingCommand extends CometOperatorSerde[DataWritingCommandExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED)

  // Native writes require Arrow-formatted input data. If the scan falls back to Spark
  // (e.g., due to unsupported complex types), the write must also fall back.
  override def requiresNativeChildren: Boolean = true

  override def getSupportLevel(op: DataWritingCommandExec): SupportLevel = {
    op.cmd match {
      case cmd: InsertIntoHadoopFsRelationCommand =>
        cmd.fileFormat match {
          case _: ParquetFileFormat =>
            // AQE can replace the write input with a zero-partition empty relation. Keep
            // Spark's writer, which creates an empty task to preserve the output file schema.
            // The native writer only maps existing partitions; see #5303. This guard is
            // conservative: an empty relation below an exchange can have nonzero partitions
            // at the write input. Revisit the guard when native empty-file handling is fixed.
            if (hasEmptyRelationInput(op.child)) {
              return Unsupported(Some(
                "Parquet writes with empty-relation inputs require Spark's empty-file handling"))
            }

            if (!cmd.outputPath.toString.startsWith("file:") && !cmd.outputPath.toString
                .startsWith("hdfs:")) {
              return Unsupported(Some("Supported output filesystems: local, HDFS"))
            }

            val hadoopConf = op.session.sessionState.newHadoopConfWithOptions(cmd.options)
            NativeWriteUtils
              .escapedHdfsDestination(
                cmd.outputPath.toString,
                hadoopConf.get(NativeWriteUtils.BASE_OUTPUT_NAME, "part"))
              .foreach(reason => return Unsupported(Some(reason)))

            if (cmd.bucketSpec.isDefined) {
              return Unsupported(Some("Bucketed writes are not supported"))
            }

            if (cmd.partitionColumns.nonEmpty || cmd.staticPartitions.nonEmpty) {
              return Unsupported(Some("Partitioned writes are not supported"))
            }

            val codec = NativeWriteUtils.parseCompressionCodec(cmd.options)
            if (!NativeWriteUtils.supportedCompressionCodecs.contains(codec)) {
              return Unsupported(Some(s"Unsupported compression codec: $codec"))
            }

            Incompatible(Some("Parquet write support is highly experimental"))
          case _ =>
            Unsupported(Some("Only Parquet writes are supported"))
        }
      case other =>
        Unsupported(Some(s"Unsupported write command: ${other.getClass}"))
    }
  }

  private def hasEmptyRelationInput(plan: SparkPlan): Boolean = plan match {
    case _: CometEmptyRelationExec => true
    case wrapper: CometScanWrapper => hasEmptyRelationInput(wrapper.originalPlan)
    case stage: QueryStageExec => hasEmptyRelationInput(stage.plan)
    case reused: ReusedExchangeExec => hasEmptyRelationInput(reused.child)
    case _ => plan.children.exists(hasEmptyRelationInput)
  }

  override def convert(
      op: DataWritingCommandExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {

    try {
      val cmd = op.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

      val scanOperator = NativeWriteUtils.buildFfiScan(cmd.query, op.id) match {
        case Some(scan) => scan
        case None =>
          withFallbackReason(op, "Cannot serialize data types for native write")
          return None
      }

      val outputPath = cmd.outputPath.toString

      val plannedCodec = NativeWriteUtils.parseCompressionCodec(cmd.options)
      val codec = NativeWriteUtils.protoCompressionCodec(plannedCodec) match {
        case Some(codec) => codec
        case None =>
          withFallbackReason(op, s"Unsupported compression codec: $plannedCodec")
          return None
      }

      val writerOpBuilder = OperatorOuterClass.ParquetWriter
        .newBuilder()
        .setOutputPath(outputPath)
        .setCompression(codec)
        .addAllColumnNames(cmd.query.output.map(_.name).asJava)
        .addAllOutputSchema(schema2Proto(
          cmd.query.schema.fields.toIndexedSeq,
          Some(
            op.session.sessionState.conf.getConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED))).asJava)
      // CometNativeWriteExec replaces output_path with the committer's exact task filename
      // at execution time, leaving work_dir unset.

      // Collect S3/cloud storage configurations
      val session = op.session
      val hadoopConf = session.sessionState.newHadoopConfWithOptions(cmd.options)
      // `outputPath` is `Path.toString`, which is not a valid URI string: it leaves spaces and
      // literal `%` unescaped, so `URI.create` would throw (and the catch below would silently
      // give the write back to Spark). Going through `Path` escapes them again.
      val objectStoreOptions =
        NativeConfig.extractObjectStoreOptions(hadoopConf, cmd.outputPath.toUri)
      objectStoreOptions.foreach { case (key, value) =>
        writerOpBuilder.putObjectStoreOptions(key, value)
      }

      val writerOp = writerOpBuilder.build()

      val writerOperator = Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(scanOperator)
        .setParquetWriter(writerOp)
        .build()

      Some(writerOperator)
    } catch {
      case e: Exception =>
        withFallbackReason(
          op,
          "Failed to convert DataWritingCommandExec to native execution: " +
            s"${e.getMessage}")
        None
    }
  }

  override def createExec(nativeOp: Operator, op: DataWritingCommandExec): CometNativeExec = {
    val cmd = op.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]
    val outputPath = cmd.outputPath.toString

    // Get the child plan from the WriteFilesExec or use the child directly
    val childPlan = op.child match {
      case writeFiles: WriteFilesExec =>
        // The WriteFilesExec child should already be a Comet operator
        writeFiles.child
      case other =>
        // Fallback: use the child directly
        other
    }

    val session = op.session
    val job = Job.getInstance(session.sessionState.newHadoopConfWithOptions(cmd.options))
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, cmd.outputPath)
    val outputWriterFactory =
      cmd.fileFormat.prepareWrite(session, job, CaseInsensitiveMap(cmd.options), cmd.query.schema)

    val committer = FileCommitProtocol.instantiate(
      session.sessionState.conf.fileCommitProtocolClass,
      UUID.randomUUID().toString,
      outputPath,
      dynamicPartitionOverwrite = false)
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", UUID.randomUUID().toString)

    CometNativeWriteExec(
      nativeOp,
      childPlan,
      outputPath,
      cmd.mode,
      committer,
      new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory)
  }

}

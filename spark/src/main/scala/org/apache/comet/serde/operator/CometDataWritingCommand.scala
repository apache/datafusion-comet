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

import java.net.URI
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.comet.{CometNativeExec, CometNativeWriteExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * CometOperatorSerde implementation for DataWritingCommandExec that converts Parquet write
 * operations to use Comet's native Parquet writer.
 */
object CometDataWritingCommand extends CometOperatorSerde[DataWritingCommandExec] {

  private val supportedCompressionCodes = Set("none", "snappy", "lz4", "zstd")

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED)

  override def getSupportLevel(op: DataWritingCommandExec): SupportLevel = {
    op.cmd match {
      case cmd: InsertIntoHadoopFsRelationCommand =>
        cmd.fileFormat match {
          case _: ParquetFileFormat =>
            if (!cmd.outputPath.toString.startsWith("file:") && !cmd.outputPath.toString
                .startsWith("hdfs:")) {
              return Unsupported(Some("Only HDFS/local filesystems output paths are supported"))
            }

            if (cmd.bucketSpec.isDefined) {
              return Unsupported(Some("Bucketed writes are not supported"))
            }

            if (cmd.partitionColumns.nonEmpty || cmd.staticPartitions.nonEmpty) {
              return Unsupported(Some("Partitioned writes are not supported"))
            }

            val codec = parseCompressionCodec(cmd)
            if (!supportedCompressionCodes.contains(codec)) {
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

  override def convert(
      op: DataWritingCommandExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {

    try {
      val cmd = op.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

      val scanOp = OperatorOuterClass.Scan
        .newBuilder()
        .setSource(cmd.query.nodeName)
        .setArrowFfiSafe(false)

      // Add fields from the query output schema
      val scanTypes = cmd.query.output.flatMap { attr =>
        serializeDataType(attr.dataType)
      }

      if (scanTypes.length != cmd.query.output.length) {
        withInfo(op, "Cannot serialize data types for native write")
        return None
      }

      scanTypes.foreach(scanOp.addFields)

      val scanOperator = Operator
        .newBuilder()
        .setPlanId(op.id)
        .setScan(scanOp.build())
        .build()

      val outputPath = cmd.outputPath.toString

      val codec = parseCompressionCodec(cmd) match {
        case "snappy" => OperatorOuterClass.CompressionCodec.Snappy
        case "lz4" => OperatorOuterClass.CompressionCodec.Lz4
        case "zstd" => OperatorOuterClass.CompressionCodec.Zstd
        case "none" => OperatorOuterClass.CompressionCodec.None
        case other =>
          withInfo(op, s"Unsupported compression codec: $other")
          return None
      }

      val writerOpBuilder = OperatorOuterClass.ParquetWriter
        .newBuilder()
        .setOutputPath(outputPath)
        .setCompression(codec)
        .addAllColumnNames(cmd.query.output.map(_.name).asJava)
      // Note: work_dir, job_id, and task_attempt_id will be set at execution time
      // in CometNativeWriteExec, as they depend on the Spark task context

      // Collect S3/cloud storage configurations
      val session = op.session
      val hadoopConf = session.sessionState.newHadoopConfWithOptions(cmd.options)
      val objectStoreOptions =
        NativeConfig.extractObjectStoreOptions(hadoopConf, URI.create(outputPath))
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
        withInfo(
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

    // Create FileCommitProtocol for atomic writes
    val jobId = java.util.UUID.randomUUID().toString
    val committer =
      try {
        // Use Spark's SQLHadoopMapReduceCommitProtocol
        val committerClass =
          classOf[org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol]
        val constructor =
          committerClass.getConstructor(classOf[String], classOf[String], classOf[Boolean])
        Some(
          constructor
            .newInstance(
              jobId,
              outputPath,
              java.lang.Boolean.FALSE // dynamicPartitionOverwrite = false for now
            )
            .asInstanceOf[org.apache.spark.internal.io.FileCommitProtocol])
      } catch {
        case e: Exception =>
          throw new SparkException(s"Could not instantiate FileCommitProtocol: ${e.getMessage}")
      }

    CometNativeWriteExec(nativeOp, childPlan, outputPath, committer, jobId)
  }

  private def parseCompressionCodec(cmd: InsertIntoHadoopFsRelationCommand) = {
    cmd.options
      .getOrElse(
        "compression",
        SQLConf.get.getConfString(
          SQLConf.PARQUET_COMPRESSION.key,
          SQLConf.PARQUET_COMPRESSION.defaultValueString))
      .toLowerCase(Locale.ROOT)
  }

}

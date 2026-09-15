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

import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.{SPARK_VERSION_SHORT, SparkException}
import org.apache.spark.sql.comet.{CometNativeExec, CometNativeWriteExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, TimestampType}

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

  private val supportedCompressionCodes =
    Set("none", "uncompressed", "snappy", "lz4", "zstd", "gzip")

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
            if (!cmd.outputPath.toString.startsWith("file:") && !cmd.outputPath.toString
                .startsWith("hdfs:")) {
              return Unsupported(Some("Supported output filesystems: local, HDFS"))
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

            // The native writer always writes proleptic Gregorian (corrected) datetime values
            // and stamps `org.apache.spark.version` with no legacy markers. Honoring a LEGACY
            // write rebase mode would require rebasing the values and stamping
            // `org.apache.spark.legacyDateTime` / `org.apache.spark.legacyINT96`, so fall back
            // to Spark rather than silently ignoring the requested mode and letting readers
            // trust a "corrected" marker over legacy-intent data. TIMESTAMP_NTZ is exempt
            // because Spark never rebases NTZ values on write.
            val hasDate = cmd.query.output.exists(a =>
              SupportLevel.containsType(a.dataType, classOf[DateType]))
            val hasTimestamp = cmd.query.output.exists(a =>
              SupportLevel.containsType(a.dataType, classOf[TimestampType]))
            // Both write rebase mode configs default to EXCEPTION in all supported Spark
            // versions.
            def isLegacyWriteMode(key: String): Boolean =
              SQLConf.get.getConfString(key, "EXCEPTION").toUpperCase(Locale.ROOT) == "LEGACY"
            val legacyModeKeys =
              ((if (hasDate || hasTimestamp) Seq(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key)
                else Seq.empty) ++
                (if (hasTimestamp) Seq(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key)
                 else Seq.empty)).filter(isLegacyWriteMode)
            if (legacyModeKeys.nonEmpty) {
              return Unsupported(
                Some(
                  "Native Parquet write always writes corrected (proleptic Gregorian) " +
                    "datetime values and does not support LEGACY rebase mode " +
                    s"(${legacyModeKeys.mkString(", ")})"))
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

      val scanOperator = NativeWriteUtils.buildFfiScan(cmd.query, op.id) match {
        case Some(scan) => scan
        case None =>
          withFallbackReason(op, "Cannot serialize data types for native write")
          return None
      }

      val outputPath = cmd.outputPath.toString

      val codec = parseCompressionCodec(cmd) match {
        case "snappy" => OperatorOuterClass.CompressionCodec.Snappy
        case "lz4" => OperatorOuterClass.CompressionCodec.Lz4
        case "zstd" => OperatorOuterClass.CompressionCodec.Zstd
        case "gzip" => OperatorOuterClass.CompressionCodec.Gzip
        case "none" | "uncompressed" => OperatorOuterClass.CompressionCodec.None
        case other =>
          withFallbackReason(op, s"Unsupported compression codec: $other")
          return None
      }

      val writerOpBuilder = OperatorOuterClass.ParquetWriter
        .newBuilder()
        .setOutputPath(outputPath)
        .setCompression(codec)
        .setSparkVersion(SPARK_VERSION_SHORT)
        .addAllColumnNames(cmd.query.output.map(_.name).asJava)
        .addAllOutputSchema(schema2Proto(
          cmd.query.schema.fields.toIndexedSeq,
          Some(
            op.session.sessionState.conf.getConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED))).asJava)
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

    CometNativeWriteExec(nativeOp, childPlan, outputPath, cmd.mode, committer, jobId)
  }

  private def parseCompressionCodec(cmd: InsertIntoHadoopFsRelationCommand) = {
    // `compression`, `parquet.compression` (i.e., ParquetOutputFormat.COMPRESSION), and
    // `spark.sql.parquet.compression.codec` are in order of precedence from highest to
    // lowest, matching Spark's own ParquetOptions.compressionCodecClassName.
    cmd.options
      .get("compression")
      .orElse(cmd.options.get(ParquetOutputFormat.COMPRESSION))
      .getOrElse(
        SQLConf.get.getConfString(
          SQLConf.PARQUET_COMPRESSION.key,
          SQLConf.PARQUET_COMPRESSION.defaultValueString))
      .toLowerCase(Locale.ROOT)
  }

}

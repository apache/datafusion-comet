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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.comet.{CometNativeExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, withFallbackReason}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.rules.CometExecRule
import org.apache.comet.serde.{CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Serde for Spark's `WriteFilesExec`, replacing the per-task Parquet write with Comet's native
 * writer while leaving the surrounding write framework (commit protocol, stats trackers, SaveMode
 * handling, `_SUCCESS`) to Spark. See [[CometWriteFilesExec]] for how the two fit together.
 */
object CometWriteFiles extends CometOperatorSerde[WriteFilesExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED)

  // Native writes require Arrow-formatted input data. If the query falls back to Spark
  // (e.g., due to unsupported complex types), the write must also fall back.
  override def requiresNativeChildren: Boolean = true

  override def getSupportLevel(op: WriteFilesExec): SupportLevel = {
    // `V1WritesUtils.getWriteFilesOpt` matches the `WriteFilesExecBase` trait on Spark 4.0+, which
    // is what makes Spark route the write through CometWriteFilesExec. Spark 3.x matches the
    // concrete `WriteFilesExec` case class instead, so a Comet node would be silently ignored and
    // the write would fall into FileFormatWriter's non-planned, row-based branch. Native writes
    // there go through CometDataWritingCommand instead; `CometExecRule` never offers a
    // WriteFilesExec to this serde on 3.x, so this guard is only a safety net.
    if (!isSpark40Plus) {
      return Unsupported(Some("Native Parquet writes require Spark 4.0 or later"))
    }

    if (!op.fileFormat.isInstanceOf[ParquetFileFormat]) {
      return Unsupported(Some("Only Parquet writes are supported"))
    }

    // The write node does not carry the output path, so CometExecRule tags it from the enclosing
    // InsertIntoHadoopFsRelationCommand. An absent tag means this write belongs to some other V1
    // write command (a Hive insert, for example) whose semantics Comet has not been verified
    // against, so decline it.
    val outputPath = outputPathOf(op) match {
      case Some(path) => path
      case None =>
        return Unsupported(Some("Only InsertIntoHadoopFsRelationCommand writes are supported"))
    }

    if (!outputPath.startsWith("file:") && !outputPath.startsWith("hdfs:")) {
      return Unsupported(Some("Supported output filesystems: local, HDFS"))
    }

    NativeWriteUtils
      .escapedHdfsDestination(outputPath, fileNamePrefix(hadoopConf(op)))
      .foreach(reason => return Unsupported(Some(reason)))

    if (op.bucketSpec.isDefined) {
      return Unsupported(Some("Bucketed writes are not supported"))
    }

    if (op.partitionColumns.nonEmpty || op.staticPartitions.nonEmpty) {
      // This also declines dynamic partition overwrite. `InsertIntoHadoopFsRelationCommand` only
      // sets `dynamicPartitionOverwrite` when `staticPartitions.size < partitionColumns.length`,
      // which implies partition columns, so a dynamic overwrite always lands here.
      return Unsupported(Some("Partitioned writes are not supported"))
    }

    if (rollsFilesByRecordCount(op)) {
      return Unsupported(
        Some("Writes with spark.sql.files.maxRecordsPerFile set are not supported"))
    }

    val codec = NativeWriteUtils.parseCompressionCodec(op.options)
    if (!NativeWriteUtils.supportedCompressionCodecs.contains(codec)) {
      return Unsupported(Some(s"Unsupported compression codec: $codec"))
    }

    NativeWriteUtils
      .legacyDatetimeRebaseWriteReason(op.child.output)
      .foreach(reason => return Unsupported(Some(reason)))

    Incompatible(Some("Parquet write support is highly experimental"))
  }

  override def convert(
      op: WriteFilesExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {

    // The native write plan reads from an Arrow stream fed by the already-native child plan, so
    // its input is a Scan carrying the child's output schema rather than `childOp`.
    val scanOperator = NativeWriteUtils.buildFfiScan(op.child, op.id) match {
      case Some(scan) => scan
      case None =>
        withFallbackReason(op, "Cannot serialize data types for native write")
        return None
    }

    // Planning-time value only, so that a plan can be inspected without a task context.
    // CometWriteFilesExec replaces it per task with the codec Parquet names the file after.
    val plannedCodec = NativeWriteUtils.parseCompressionCodec(op.options)
    val codec = NativeWriteUtils.protoCompressionCodec(plannedCodec) match {
      case Some(codec) => codec
      case None =>
        withFallbackReason(op, s"Unsupported compression codec: $plannedCodec")
        return None
    }

    // `output_path`, `column_names` and `output_schema` are filled in per task by
    // CometWriteFilesExec: the path comes from the commit protocol and the columns from
    // WriteJobDescription.dataColumns, neither of which is known at planning time.
    val writerOpBuilder = OperatorOuterClass.ParquetWriter
      .newBuilder()
      .setCompression(codec)
      .setSparkVersion(SPARK_VERSION_SHORT)

    // getSupportLevel already declined the write if the tag is absent, so this cannot be empty.
    outputPathOf(op).foreach { outputPath =>
      // The tag holds `Path.toString`, which is not a valid URI string: it leaves spaces and
      // literal `%` unescaped, so `URI.create` would throw. Round-tripping through `Path` escapes
      // them again. Only the scheme and authority matter to `extractObjectStoreOptions`, but
      // parsing has to succeed to get at them.
      NativeConfig
        .extractObjectStoreOptions(hadoopConf(op), new Path(outputPath).toUri)
        .foreach { case (key, value) => writerOpBuilder.putObjectStoreOptions(key, value) }
    }

    Some(
      Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(scanOperator)
        .setParquetWriter(writerOpBuilder.build())
        .build())
  }

  override def createExec(nativeOp: Operator, op: WriteFilesExec): CometNativeExec =
    CometWriteFilesExec(nativeOp, originalPlan = op, child = op.child)

  /** The write's output path, recorded on the node by `CometExecRule`. */
  private def outputPathOf(op: WriteFilesExec): Option[String] =
    op.getTagValue(CometExecRule.WRITE_OUTPUT_PATH)

  private def hadoopConf(op: WriteFilesExec): Configuration =
    op.session.sessionState.newHadoopConfWithOptions(op.options)

  /**
   * The leading component of every file name this write will produce. Spark's
   * `HadoopMapReduceCommitProtocol.getFilename` reads it from the task configuration, so a write
   * option or a session-level Hadoop setting can replace the usual `part`.
   */
  private def fileNamePrefix(hadoopConf: Configuration): String =
    hadoopConf.get(NativeWriteUtils.BASE_OUTPUT_NAME, NativeWriteUtils.DEFAULT_BASE_OUTPUT_NAME)

  /**
   * Whether Spark would roll to a new file every N rows within a task.
   *
   * `SingleDirectoryDataWriter.write` rolls a new file once `maxRecordsPerFile` rows have been
   * written, incrementing the `-c$fileCounter%03d` suffix. Comet's writer asks the commit
   * protocol for one file per task and always writes `-c000`, so a task that should have produced
   * several files would produce one oversized file instead - a different layout than Spark's own
   * writer with no error. Decline the write rather than silently ignore the setting.
   */
  private def rollsFilesByRecordCount(op: WriteFilesExec): Boolean = {
    // Same precedence as FileFormatWriter.write: the `maxRecordsPerFile` write option wins over
    // spark.sql.files.maxRecordsPerFile. Options are matched case-insensitively there.
    val maxRecordsPerFile = CaseInsensitiveMap(op.options)
      .get("maxRecordsPerFile")
      .map(_.toLong)
      .getOrElse(SQLConf.get.maxRecordsPerFile)
    maxRecordsPerFile > 0
  }
}

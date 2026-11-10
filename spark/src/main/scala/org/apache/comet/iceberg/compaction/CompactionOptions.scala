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

package org.apache.comet.iceberg.compaction

/** The order Iceberg starts file groups in (`rewrite-job-order`). */
sealed abstract class RewriteJobOrder(val orderName: String)

object RewriteJobOrder {
  case object BytesAsc extends RewriteJobOrder("bytes-asc")
  case object BytesDesc extends RewriteJobOrder("bytes-desc")
  case object FilesAsc extends RewriteJobOrder("files-asc")
  case object FilesDesc extends RewriteJobOrder("files-desc")
  case object NoOrder extends RewriteJobOrder("none")

  private val all: Seq[RewriteJobOrder] = Seq(BytesAsc, BytesDesc, FilesAsc, FilesDesc, NoOrder)

  def fromName(name: String): Option[RewriteJobOrder] =
    all.find(_.orderName.equalsIgnoreCase(name))
}

/**
 * The options of a bin-pack `rewrite_data_files` call, with Iceberg's defaults applied.
 *
 * Only options Comet reproduces are accepted. [[CompactionOptions.parse]] reports everything
 * else, including a value Iceberg would reject, so that the call falls back to Iceberg's
 * procedure and the user sees Iceberg's own error.
 */
final case class CompactionOptions(
    targetFileSize: Long,
    minFileSize: Long,
    maxFileSize: Long,
    minInputFiles: Int,
    rewriteAll: Boolean,
    maxGroupSize: Long,
    outputSpecId: Int,
    deleteFileThreshold: Int,
    deleteRatioThreshold: Double,
    jobOrder: RewriteJobOrder,
    maxFilesToRewrite: Option[Int],
    maxConcurrentGroupRewrites: Int,
    partialProgress: Boolean,
    maxCommits: Int,
    maxFailedCommits: Int,
    useStartingSequenceNumber: Boolean) {

  /**
   * The size the writer rolls files at. It sits halfway between the target and the maximum size,
   * so that data compressing a little worse than the estimate does not leave a tiny extra file.
   */
  def writeMaxFileSize: Long = (targetFileSize + ((maxFileSize - targetFileSize) * 0.5)).toLong
}

object CompactionOptions {
  val TargetFileSizeBytes = "target-file-size-bytes"
  val MinFileSizeBytes = "min-file-size-bytes"
  val MaxFileSizeBytes = "max-file-size-bytes"
  val MinInputFiles = "min-input-files"
  val RewriteAll = "rewrite-all"
  val MaxFileGroupSizeBytes = "max-file-group-size-bytes"
  val DeleteFileThreshold = "delete-file-threshold"
  val DeleteRatioThreshold = "delete-ratio-threshold"
  val JobOrder = "rewrite-job-order"
  val MaxFilesToRewrite = "max-files-to-rewrite"
  val MaxConcurrentFileGroupRewrites = "max-concurrent-file-group-rewrites"
  val PartialProgressEnabled = "partial-progress.enabled"
  val PartialProgressMaxCommits = "partial-progress.max-commits"
  val PartialProgressMaxFailedCommits = "partial-progress.max-failed-commits"
  val UseStartingSequenceNumber = "use-starting-sequence-number"
  val RemoveDanglingDeletes = "remove-dangling-deletes"
  val OutputSpecId = "output-spec-id"

  /** The table property that sets the default target file size. */
  val WriteTargetFileSizeBytes = "write.target-file-size-bytes"

  /** Iceberg's `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT`, 512 MiB. */
  val WriteTargetFileSizeBytesDefault: Long = 512L * 1024 * 1024

  private val MaxFileGroupSizeBytesDefault = 100L * 1024 * 1024 * 1024
  private val MinInputFilesDefault = 5
  private val MaxConcurrentFileGroupRewritesDefault = 5
  private val PartialProgressMaxCommitsDefault = 10

  private val supportedKeys: Set[String] = Set(
    TargetFileSizeBytes,
    MinFileSizeBytes,
    MaxFileSizeBytes,
    MinInputFiles,
    RewriteAll,
    MaxFileGroupSizeBytes,
    DeleteFileThreshold,
    DeleteRatioThreshold,
    JobOrder,
    MaxFilesToRewrite,
    MaxConcurrentFileGroupRewrites,
    PartialProgressEnabled,
    PartialProgressMaxCommits,
    PartialProgressMaxFailedCommits,
    UseStartingSequenceNumber,
    RemoveDanglingDeletes,
    OutputSpecId)

  /**
   * Parses the `options` map of a `CALL`. Returns the reason when Comet should leave the call to
   * Iceberg. That covers an option Comet does not reproduce, a value Iceberg would reject, and a
   * combination Iceberg refuses.
   *
   * @param options
   *   the `options` argument
   * @param tableProperties
   *   the table's properties, for the default target file size
   * @param currentSpecId
   *   the table's current partition spec id, the default and only supported `output-spec-id`
   */
  def parse(
      options: Map[String, String],
      tableProperties: Map[String, String],
      currentSpecId: Int): Either[String, CompactionOptions] = {
    val unsupported = options.keySet.diff(supportedKeys)
    if (unsupported.nonEmpty) {
      return Left(s"unsupported option(s): ${unsupported.toSeq.sorted.mkString(", ")}")
    }
    try {
      // Iceberg parses booleans with Boolean.parseBoolean, so anything but "true" is false.
      def bool(key: String, default: Boolean): Boolean =
        options.get(key).map(java.lang.Boolean.parseBoolean).getOrElse(default)

      val defaultTarget = tableProperties
        .get(WriteTargetFileSizeBytes)
        .map(_.toLong)
        .getOrElse(WriteTargetFileSizeBytesDefault)
      val target = options.get(TargetFileSizeBytes).map(_.toLong).getOrElse(defaultTarget)
      val min = options.get(MinFileSizeBytes).map(_.toLong).getOrElse((target * 0.75).toLong)
      val max = options.get(MaxFileSizeBytes).map(_.toLong).getOrElse((target * 1.80).toLong)
      require(target > 0, s"'$TargetFileSizeBytes' must be > 0")
      require(min >= 0, s"'$MinFileSizeBytes' must be >= 0")
      require(target > min, s"'$TargetFileSizeBytes' must be > '$MinFileSizeBytes'")
      require(target < max, s"'$TargetFileSizeBytes' must be < '$MaxFileSizeBytes'")

      val minInputFiles = options.get(MinInputFiles).map(_.toInt).getOrElse(MinInputFilesDefault)
      require(minInputFiles > 0, s"'$MinInputFiles' must be > 0")

      val maxGroupSize =
        options.get(MaxFileGroupSizeBytes).map(_.toLong).getOrElse(MaxFileGroupSizeBytesDefault)
      require(maxGroupSize > 0, s"'$MaxFileGroupSizeBytes' must be > 0")

      val deleteFileThreshold =
        options.get(DeleteFileThreshold).map(_.toInt).getOrElse(Integer.MAX_VALUE)
      require(deleteFileThreshold >= 0, s"'$DeleteFileThreshold' must be >= 0")

      val deleteRatioThreshold = options.get(DeleteRatioThreshold).map(_.toDouble).getOrElse(0.3)
      require(
        deleteRatioThreshold > 0 && deleteRatioThreshold <= 1,
        s"'$DeleteRatioThreshold' must be in (0, 1]")

      val jobOrder = options
        .get(JobOrder)
        .map(name =>
          RewriteJobOrder
            .fromName(name)
            .getOrElse(throw new IllegalArgumentException(s"invalid $JobOrder: $name")))
        .getOrElse(RewriteJobOrder.NoOrder)

      val maxFilesToRewrite = options.get(MaxFilesToRewrite).map(_.toInt)
      require(maxFilesToRewrite.forall(_ > 0), s"'$MaxFilesToRewrite' must be positive")

      val maxConcurrent = options
        .get(MaxConcurrentFileGroupRewrites)
        .map(_.toInt)
        .getOrElse(MaxConcurrentFileGroupRewritesDefault)
      require(maxConcurrent >= 1, s"'$MaxConcurrentFileGroupRewrites' must be positive")

      val partialProgress = bool(PartialProgressEnabled, default = false)
      val maxCommits = options
        .get(PartialProgressMaxCommits)
        .map(_.toInt)
        .getOrElse(PartialProgressMaxCommitsDefault)
      val maxFailedCommits =
        options.get(PartialProgressMaxFailedCommits).map(_.toInt).getOrElse(maxCommits)
      require(
        !partialProgress || maxCommits > 0,
        s"'$PartialProgressMaxCommits' must be positive when '$PartialProgressEnabled' is true")

      require(
        !bool(RemoveDanglingDeletes, default = false),
        s"'$RemoveDanglingDeletes' is not supported")
      val outputSpecId = options.get(OutputSpecId).map(_.toInt).getOrElse(currentSpecId)
      require(outputSpecId == currentSpecId, s"'$OutputSpecId' other than the current spec")

      Right(
        CompactionOptions(
          targetFileSize = target,
          minFileSize = min,
          maxFileSize = max,
          minInputFiles = minInputFiles,
          rewriteAll = bool(RewriteAll, default = false),
          maxGroupSize = maxGroupSize,
          outputSpecId = outputSpecId,
          deleteFileThreshold = deleteFileThreshold,
          deleteRatioThreshold = deleteRatioThreshold,
          jobOrder = jobOrder,
          maxFilesToRewrite = maxFilesToRewrite,
          maxConcurrentGroupRewrites = maxConcurrent,
          partialProgress = partialProgress,
          maxCommits = maxCommits,
          maxFailedCommits = maxFailedCommits,
          useStartingSequenceNumber = bool(UseStartingSequenceNumber, default = true)))
    } catch {
      // NumberFormatException is an IllegalArgumentException, as is a failed require.
      case e: IllegalArgumentException => Left(e.getMessage)
    }
  }
}

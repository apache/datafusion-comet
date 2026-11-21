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

/**
 * The options of a bin-pack `rewrite_data_files` call that Comet acts on: how the rewrite runs
 * and commits, with the defaults of iceberg-java's `RewriteDataFilesSparkAction`. The options
 * that decide the file groups are read and checked by Iceberg's own planner.
 *
 * [[CompactionOptions.parse]] reports an option Comet does not reproduce and a value Iceberg's
 * action would reject, so that the call runs Iceberg's procedure and the user sees Iceberg's own
 * error.
 *
 * @param removeDanglingDeletes
 *   whether the rewrite ends by running Iceberg's action that removes the delete files no data
 *   file needs any more
 */
final case class CompactionOptions(
    maxConcurrentGroupRewrites: Int,
    partialProgress: Boolean,
    maxCommits: Int,
    maxFailedCommits: Int,
    useStartingSequenceNumber: Boolean,
    removeDanglingDeletes: Boolean)

object CompactionOptions {
  val MaxConcurrentFileGroupRewrites = "max-concurrent-file-group-rewrites"
  val PartialProgressEnabled = "partial-progress.enabled"
  val PartialProgressMaxCommits = "partial-progress.max-commits"
  val PartialProgressMaxFailedCommits = "partial-progress.max-failed-commits"
  val UseStartingSequenceNumber = "use-starting-sequence-number"
  val RemoveDanglingDeletes = "remove-dangling-deletes"

  /** Read by Iceberg's planner, but listed among the options of the action. */
  val OutputSpecId = "output-spec-id"

  private val MaxConcurrentFileGroupRewritesDefault = 5
  private val PartialProgressMaxCommitsDefault = 10

  /**
   * The options of the action that Comet reproduces. They are those of iceberg-java's action,
   * less the ones that only make sense for Iceberg's own jobs, such as `cache-delete-files`.
   */
  private val actionOptions: Set[String] = Set(
    MaxConcurrentFileGroupRewrites,
    PartialProgressEnabled,
    PartialProgressMaxCommits,
    PartialProgressMaxFailedCommits,
    UseStartingSequenceNumber,
    RemoveDanglingDeletes,
    OutputSpecId)

  /**
   * Parses the `options` map of a `CALL`. Returns the reason when Comet should leave the call to
   * Iceberg: an option that neither the action nor the planner reads, or a value of the action's
   * options that Iceberg rejects.
   *
   * @param plannerOptions
   *   the options Iceberg's planner reads, `BinPackRewriteFilePlanner.validOptions()`
   */
  def parse(
      options: Map[String, String],
      plannerOptions: Set[String]): Either[String, CompactionOptions] = {
    val unsupported = options.keySet.diff(plannerOptions ++ actionOptions)
    if (unsupported.nonEmpty) {
      return Left(s"unsupported option(s): ${unsupported.toSeq.sorted.mkString(", ")}")
    }
    try {
      // Iceberg parses booleans with Boolean.parseBoolean, so anything but "true" is false, and
      // integers with Integer.parseInt.
      def bool(key: String, default: Boolean): Boolean =
        options.get(key).map(java.lang.Boolean.parseBoolean).getOrElse(default)
      def int(key: String, default: Int): Int = options.get(key).map(_.toInt).getOrElse(default)

      val maxConcurrent =
        int(MaxConcurrentFileGroupRewrites, MaxConcurrentFileGroupRewritesDefault)
      require(maxConcurrent >= 1, s"'$MaxConcurrentFileGroupRewrites' must be positive")

      val partialProgress = bool(PartialProgressEnabled, default = false)
      val maxCommits = int(PartialProgressMaxCommits, PartialProgressMaxCommitsDefault)
      require(
        !partialProgress || maxCommits > 0,
        s"'$PartialProgressMaxCommits' must be positive when '$PartialProgressEnabled' is true")

      Right(
        CompactionOptions(
          maxConcurrentGroupRewrites = maxConcurrent,
          partialProgress = partialProgress,
          maxCommits = maxCommits,
          maxFailedCommits = int(PartialProgressMaxFailedCommits, maxCommits),
          useStartingSequenceNumber = bool(UseStartingSequenceNumber, default = true),
          removeDanglingDeletes = bool(RemoveDanglingDeletes, default = false)))
    } catch {
      // NumberFormatException is an IllegalArgumentException, as is a failed require.
      case e: IllegalArgumentException => Left(e.getMessage)
    }
  }
}

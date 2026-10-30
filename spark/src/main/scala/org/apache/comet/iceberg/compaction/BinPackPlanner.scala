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

import scala.collection.mutable

/**
 * One data file of a compaction scan, reduced to the facts the bin-pack planner decides on.
 *
 * `task` is an opaque handle to the Iceberg `FileScanTask` the facts were read from. The planner
 * never looks inside it, which keeps the planning logic free of Iceberg classes.
 *
 * @param task
 *   handle to the `FileScanTask`, handed back unchanged in the planned groups
 * @param length
 *   `FileScanTask.length()`, the number of bytes the task reads. This is the weight every packing
 *   and threshold decision uses.
 * @param recordCount
 *   rows in the data file
 * @param specId
 *   the id of the partition spec the file was written with
 * @param partition
 *   the grouping key. Files with equal keys are packed together. Files written under an older
 *   partition spec than the table's current one carry an all-NULL key, the way Iceberg groups
 *   them.
 * @param deleteFileCount
 *   `FileScanTask.deletes().size()`
 * @param fileScopedDeletedRows
 *   records in the delete files of this task that apply to this data file alone (deletion vectors
 *   and position deletes scoped to one file)
 */
final case class CompactionFile(
    task: AnyRef,
    length: Long,
    recordCount: Long,
    specId: Int,
    partition: Seq[Any],
    deleteFileCount: Int,
    fileScopedDeletedRows: Long)

/**
 * Files that are rewritten together, within one partition. This is Iceberg's `RewriteFileGroup`
 * and is the unit that fails, retries and commits as a whole in partial-progress mode.
 *
 * @param globalIndex
 *   1-based position among the selected groups
 * @param partitionIndex
 *   1-based position among the selected groups of the same partition
 * @param partition
 *   the grouping key shared by every file
 * @param files
 *   the input files, in scan order
 * @param outputSpecId
 *   the partition spec the output files are written with
 * @param maxOutputFileSize
 *   the file size the writer rolls at, which is a little above the target size so that a group
 *   that compresses slightly worse than estimated does not leave a tiny remainder file
 * @param inputSplitSize
 *   how many bytes one read task takes from the group
 * @param expectedOutputFiles
 *   how many files the group should produce
 */
final case class FileGroup(
    globalIndex: Int,
    partitionIndex: Int,
    partition: Seq[Any],
    files: Seq[CompactionFile],
    outputSpecId: Int,
    maxOutputFileSize: Long,
    inputSplitSize: Long,
    expectedOutputFiles: Int) {

  def numFiles: Int = files.size

  def inputBytes: Long = files.foldLeft(0L)(_ + _.length)
}

/**
 * The result of planning.
 *
 * @param groups
 *   the groups to rewrite, in job order
 * @param totalGroupCount
 *   the number of groups before `max-files-to-rewrite` trimmed the selection. Partial-progress
 *   commits are sized from it.
 */
final case class CompactionPlan(groups: Seq[FileGroup], totalGroupCount: Int)

/**
 * Plans which data files are rewritten together by a bin-pack compaction.
 *
 * It follows iceberg-java's `BinPackRewriteFilePlanner` so that a `CALL rewrite_data_files`
 * produces the same groups either way. The steps match iceberg-go's `PlanCompaction`: bucket the
 * files by partition, keep the ones worth rewriting, pack each bucket into bins, and drop the
 * bins that would not pay for a rewrite. The two differ in bin size. Iceberg-java packs up to
 * `max-file-group-size-bytes` (100 GB by default) and splits a group into read tasks later, while
 * iceberg-go packs bins of the target file size directly. This planner keeps the iceberg-java
 * groups, and [[BinPackPlanner.planReadTasks]] makes the second step available.
 */
object BinPackPlanner {

  /** Bytes added to the split size so that a split holding a full file's worth never splits. */
  private val SplitOverhead = 5L * 1024

  /** The planning lookback for read tasks, `read.split.planning-lookback` in Iceberg. */
  val DefaultSplitLookback = 10

  def plan(files: Seq[CompactionFile], options: CompactionOptions): CompactionPlan = {
    val byPartition = mutable.LinkedHashMap.empty[Seq[Any], mutable.ArrayBuffer[CompactionFile]]
    files.foreach { f =>
      byPartition.getOrElseUpdate(f.partition, mutable.ArrayBuffer.empty[CompactionFile]) += f
    }

    val groupsByPartition: Seq[(Seq[Any], Seq[Seq[CompactionFile]])] =
      byPartition.toSeq.map { case (partition, partitionFiles) =>
        partition -> planFileGroups(partitionFiles.toSeq, options)
      }

    val selected = mutable.ArrayBuffer.empty[FileGroup]
    val partitionCounts = mutable.HashMap.empty[Seq[Any], Int]
    var filesSelected = 0

    def newGroup(partition: Seq[Any], groupFiles: Seq[CompactionFile], inputSize: Long) = {
      val partitionIndex = partitionCounts.getOrElse(partition, 0) + 1
      partitionCounts(partition) = partitionIndex
      FileGroup(
        globalIndex = selected.size + 1,
        partitionIndex = partitionIndex,
        partition = partition,
        files = groupFiles,
        outputSpecId = options.outputSpecId,
        maxOutputFileSize = options.writeMaxFileSize,
        inputSplitSize = inputSplitSize(inputSize, options),
        expectedOutputFiles = expectedOutputFiles(inputSize, options))
    }

    for {
      (partition, groups) <- groupsByPartition
      groupFiles <- groups
    } {
      // The split size and the output file count come from the whole group even when
      // `max-files-to-rewrite` trims it, which is what iceberg-java does.
      val inputSize = groupFiles.foldLeft(0L)(_ + _.length)
      options.maxFilesToRewrite match {
        case None =>
          selected += newGroup(partition, groupFiles, inputSize)
        case Some(maxFiles) if filesSelected < maxFiles =>
          val take = math.min(groupFiles.size, maxFiles - filesSelected)
          selected += newGroup(partition, groupFiles.take(take), inputSize)
          filesSelected += take
        case _ =>
      }
    }

    val totalGroupCount = groupsByPartition.foldLeft(0)(_ + _._2.size)
    CompactionPlan(orderGroups(selected.toSeq, options.jobOrder), totalGroupCount)
  }

  /**
   * Packs the read tasks of one group into bins, each of which becomes one Spark partition and,
   * with the rolling writer, about one output file. Iceberg does this when it plans the read of a
   * staged file group, with the group's [[FileGroup.inputSplitSize]] as the bin size and no cost
   * for opening a file. This is `TableScanUtil.planTaskGroups` minus the splitting of large
   * files, which the caller does first because it needs Iceberg's own tasks.
   *
   * @param tasks
   *   the read tasks, after large files have been split
   * @param sizeBytes
   *   the bytes a task reads, including the delete files it applies
   * @param filesCount
   *   the files a task opens, the data file and its delete files
   */
  def planReadTasks[T](
      tasks: Seq[T],
      splitSize: Long,
      lookback: Int = DefaultSplitLookback,
      openFileCost: Long = 0L)(sizeBytes: T => Long, filesCount: T => Int): Seq[Seq[T]] = {
    pack(tasks, splitSize, lookback, largestBinFirst = true) { task =>
      math.max(sizeBytes(task), filesCount(task).toLong * openFileCost)
    }
  }

  /**
   * Splits the groups of a rewrite into the jobs that run them, in order. A job takes groups
   * until it has `maxGroups` of them, or until the next one would take it past `maxWeight`, which
   * is counted in bins. A group heavier than `maxWeight` still gets a job of its own.
   */
  private[compaction] def chunk[T](entries: Seq[T], maxGroups: Int, maxWeight: Int)(
      weightOf: T => Int): Seq[Seq[T]] = {
    val chunks = mutable.ArrayBuffer.empty[Seq[T]]
    var current = mutable.ArrayBuffer.empty[T]
    var weight = 0
    entries.foreach { entry =>
      val entryWeight = weightOf(entry)
      val full = current.size >= maxGroups || weight.toLong + entryWeight > maxWeight
      if (current.nonEmpty && full) {
        chunks.append(current.toVector)
        current = mutable.ArrayBuffer.empty[T]
        weight = 0
      }
      current.append(entry)
      weight += entryWeight
    }
    if (current.nonEmpty) {
      chunks.append(current.toVector)
    }
    chunks.toSeq
  }

  private def planFileGroups(
      files: Seq[CompactionFile],
      options: CompactionOptions): Seq[Seq[CompactionFile]] = {
    val candidates = if (options.rewriteAll) files else files.filter(isCandidate(_, options))
    val groups =
      pack(candidates, options.maxGroupSize, lookback = 1, largestBinFirst = false)(_.length)
    if (options.rewriteAll) groups else groups.filter(isWorthRewriting(_, options))
  }

  private def isCandidate(file: CompactionFile, options: CompactionOptions): Boolean =
    outsideDesiredFileSizeRange(file, options) ||
      tooManyDeletes(file, options) ||
      tooHighDeleteRatio(file, options)

  private def isWorthRewriting(
      group: Seq[CompactionFile],
      options: CompactionOptions): Boolean = {
    val size = group.foldLeft(0L)(_ + _.length)
    val enoughInputFiles = group.size > 1 && group.size >= options.minInputFiles
    val enoughContent = group.size > 1 && size > options.targetFileSize
    val tooMuchContent = size > options.maxFileSize
    enoughInputFiles || enoughContent || tooMuchContent ||
    group.exists(tooManyDeletes(_, options)) ||
    group.exists(tooHighDeleteRatio(_, options))
  }

  private def outsideDesiredFileSizeRange(file: CompactionFile, options: CompactionOptions) =
    file.length < options.minFileSize || file.length > options.maxFileSize

  private def tooManyDeletes(file: CompactionFile, options: CompactionOptions): Boolean =
    file.deleteFileCount >= options.deleteFileThreshold

  private def tooHighDeleteRatio(file: CompactionFile, options: CompactionOptions): Boolean = {
    if (file.deleteFileCount == 0) {
      false
    } else {
      val deletedRecords = math.min(file.fileScopedDeletedRows, file.recordCount).toDouble
      // A data file with no records divides to NaN, which compares false.
      deletedRecords / file.recordCount >= options.deleteRatioThreshold
    }
  }

  /**
   * The number of files a group should be rewritten into. Rounds the last, partial file into the
   * others when that keeps the average within 10 percent of the target, so a 10.1 GB group with a
   * 1 GB target gives ten files and not eleven.
   */
  private[compaction] def expectedOutputFiles(
      inputSize: Long,
      options: CompactionOptions): Int = {
    val target = options.targetFileSize
    if (inputSize < target) {
      1
    } else {
      val withoutRemainder = inputSize / target
      val withRemainder = if (inputSize % target == 0) withoutRemainder else withoutRemainder + 1
      val averageWithoutRemainder = inputSize / withoutRemainder
      if (inputSize % target > options.minFileSize) {
        withRemainder.toInt
      } else if (averageWithoutRemainder <
          math.min(1.1 * target, options.writeMaxFileSize.toDouble)) {
        withoutRemainder.toInt
      } else {
        withRemainder.toInt
      }
    }
  }

  /**
   * How many bytes one read task takes: the input divided by the expected file count, but at
   * least the target size and at most the size the writer rolls at.
   */
  private[compaction] def inputSplitSize(inputSize: Long, options: CompactionOptions): Long = {
    val estimated = inputSize / expectedOutputFiles(inputSize, options) + SplitOverhead
    if (estimated < options.targetFileSize) {
      options.targetFileSize
    } else {
      math.min(estimated, options.writeMaxFileSize)
    }
  }

  private def orderGroups(groups: Seq[FileGroup], order: RewriteJobOrder): Seq[FileGroup] =
    order match {
      case RewriteJobOrder.BytesAsc => groups.sortBy(_.inputBytes)
      case RewriteJobOrder.BytesDesc => groups.sortBy(_.inputBytes)(Ordering[Long].reverse)
      case RewriteJobOrder.FilesAsc => groups.sortBy(_.numFiles)
      case RewriteJobOrder.FilesDesc => groups.sortBy(_.numFiles)(Ordering[Int].reverse)
      case RewriteJobOrder.NoOrder => groups
    }

  /**
   * Iceberg's `BinPacking.PackingIterator`. Items stay in input order. Each goes into the first
   * open bin it fits in, or opens a new one. When more than `lookback` bins are open, the oldest
   * (or the heaviest, with `largestBinFirst`) is closed and emitted. An item heavier than the
   * target gets a bin of its own.
   */
  private[compaction] def pack[T](
      items: Seq[T],
      targetWeight: Long,
      lookback: Int,
      largestBinFirst: Boolean)(weightOf: T => Long): Seq[Seq[T]] = {
    require(lookback > 0, s"Bin look-back size must be greater than 0: $lookback")

    final class Bin {
      val members = mutable.ArrayBuffer.empty[T]
      var weight = 0L
      def canAdd(w: Long): Boolean = weight + w <= targetWeight
      def add(item: T, w: Long): Unit = {
        weight += w
        members.append(item)
      }
    }

    val out = mutable.ArrayBuffer.empty[Seq[T]]
    val bins = new java.util.LinkedList[Bin]()

    def takeBin(): Bin = {
      if (largestBinFirst) {
        // The first bin with the largest weight, as Collections.max picks it.
        var largest = bins.getFirst
        val iter = bins.iterator()
        while (iter.hasNext) {
          val bin = iter.next()
          if (bin.weight > largest.weight) largest = bin
        }
        bins.remove(largest)
        largest
      } else {
        bins.removeFirst()
      }
    }

    items.foreach { item =>
      val weight = weightOf(item)
      var target: Bin = null
      val iter = bins.iterator()
      while (target == null && iter.hasNext) {
        val bin = iter.next()
        if (bin.canAdd(weight)) target = bin
      }
      if (target != null) {
        target.add(item, weight)
      } else {
        val bin = new Bin
        bin.add(item, weight)
        bins.addLast(bin)
        if (bins.size() > lookback) {
          out.append(takeBin().members.toVector)
        }
      }
    }
    while (!bins.isEmpty) {
      out.append(bins.removeFirst().members.toVector)
    }
    out.toSeq
  }
}

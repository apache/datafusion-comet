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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the bin-pack planner. The planner has no Iceberg dependency, so these run on
 * every Spark profile. The expected groups follow iceberg-java's `BinPackRewriteFilePlanner`.
 */
class BinPackPlannerSuite extends AnyFunSuite {

  private val MB = 1024L * 1024

  // A 100 MB target gives a 75 MB minimum, a 180 MB maximum and a 140 MB rolling size.
  private val DefaultOptions = Seq(CompactionOptions.TargetFileSizeBytes -> (100 * MB).toString)

  private def options(extra: (String, String)*): CompactionOptions =
    CompactionOptions.parse((DefaultOptions ++ extra).toMap, Map.empty, currentSpecId = 0) match {
      case Right(parsed) => parsed
      case Left(reason) => fail(s"Cannot parse the options: $reason")
    }

  private def file(
      id: String,
      megabytes: Long,
      partition: Seq[Any] = Nil,
      deleteFiles: Int = 0,
      deletedRows: Long = 0L,
      records: Long = 100L): CompactionFile =
    CompactionFile(
      task = id,
      length = megabytes * MB,
      recordCount = records,
      specId = 0,
      partition = partition,
      deleteFileCount = deleteFiles,
      fileScopedDeletedRows = deletedRows)

  private def small(
      count: Int,
      megabytes: Long,
      partition: Seq[Any] = Nil,
      prefix: String = "f") =
    (0 until count).map(i => file(s"$prefix$i", megabytes, partition))

  private def ids(group: FileGroup): Seq[String] = group.files.map(_.task.toString)

  // -- Which files are rewritten ----------------------------------------------------------

  test("enough small files in a partition form one group") {
    val plan = BinPackPlanner.plan(small(5, 10), options())
    assert(plan.groups.size == 1)
    assert(plan.totalGroupCount == 1)
    val group = plan.groups.head
    assert(ids(group) == Seq("f0", "f1", "f2", "f3", "f4"))
    assert(group.globalIndex == 1 && group.partitionIndex == 1)
    assert(group.expectedOutputFiles == 1)
    assert(group.inputBytes == 50 * MB)
  }

  test("a partition with too few small files is left alone") {
    val plan = BinPackPlanner.plan(small(4, 10), options())
    assert(plan.groups.isEmpty)
    assert(plan.totalGroupCount == 0)
  }

  test("min-input-files sets how many small files are enough") {
    assert(BinPackPlanner.plan(small(2, 10), options("min-input-files" -> "2")).groups.size == 1)
    assert(BinPackPlanner.plan(small(1, 10), options("min-input-files" -> "2")).groups.isEmpty)
  }

  test("files that add up past the target size are rewritten even when there are few") {
    val plan = BinPackPlanner.plan(Seq(file("a", 60), file("b", 60)), options())
    assert(plan.groups.size == 1)
    assert(ids(plan.groups.head) == Seq("a", "b"))
  }

  test("a single small file is never rewritten") {
    assert(BinPackPlanner.plan(Seq(file("a", 10)), options()).groups.isEmpty)
  }

  test("files within the desired size range are not rewritten") {
    val plan = BinPackPlanner.plan(small(6, 100), options())
    assert(plan.groups.isEmpty)
  }

  test("the size range is closed at both ends") {
    // 75 MB is the minimum and 180 MB the maximum, and neither is outside the range.
    val plan = BinPackPlanner.plan(small(3, 75) ++ small(3, 180, prefix = "g"), options())
    assert(plan.groups.isEmpty)
  }

  test("an oversized file is rewritten to split it") {
    val plan = BinPackPlanner.plan(Seq(file("big", 200)), options())
    assert(plan.groups.size == 1)
    assert(plan.groups.head.expectedOutputFiles == 2)
  }

  test("partitions are never mixed") {
    val files = small(6, 10, partition = Seq("a")) ++ small(3, 10, partition = Seq("b"), "g")
    val plan = BinPackPlanner.plan(files, options())
    assert(plan.groups.size == 1)
    assert(plan.groups.head.partition == Seq("a"))
  }

  test("each partition numbers its own groups") {
    val files = small(5, 10, Seq("a"), "a") ++ small(5, 10, Seq("b"), "b")
    val plan = BinPackPlanner.plan(files, options())
    assert(plan.groups.map(g => (g.globalIndex, g.partitionIndex)) == Seq((1, 1), (2, 1)))
  }

  test("max-file-group-size-bytes splits a partition into several groups") {
    val plan = BinPackPlanner.plan(
      small(10, 30),
      options("max-file-group-size-bytes" -> (100 * MB).toString, "min-input-files" -> "2"))
    // 100 MB holds three 30 MB files. The last file is alone and too small to be worth it.
    assert(plan.groups.map(_.numFiles) == Seq(3, 3, 3))
    assert(plan.totalGroupCount == 3)
  }

  test("an item heavier than the group size gets a group of its own") {
    val plan = BinPackPlanner.plan(
      Seq(file("a", 10), file("huge", 500), file("b", 10)),
      options("max-file-group-size-bytes" -> (100 * MB).toString, "min-input-files" -> "1"))
    assert(plan.groups.map(ids) == Seq(Seq("huge")))
  }

  // -- Deletes -------------------------------------------------------------------------------

  test("delete-file-threshold selects a right-sized file") {
    val files = Seq(file("a", 100, deleteFiles = 2))
    assert(BinPackPlanner.plan(files, options()).groups.isEmpty)
    val plan = BinPackPlanner.plan(files, options("delete-file-threshold" -> "2"))
    assert(plan.groups.map(ids) == Seq(Seq("a")))
  }

  test("delete-ratio-threshold selects a file whose rows are mostly deleted") {
    def plan(deleted: Long) =
      BinPackPlanner.plan(Seq(file("a", 100, deleteFiles = 1, deletedRows = deleted)), options())
    assert(plan(29).groups.isEmpty)
    assert(plan(30).groups.size == 1)
    // Deleted rows are capped at the file's own record count.
    assert(plan(1000).groups.size == 1)
  }

  test("a file with no rows is not selected by the delete ratio") {
    val files = Seq(file("a", 100, deleteFiles = 1, deletedRows = 5, records = 0))
    assert(BinPackPlanner.plan(files, options()).groups.isEmpty)
  }

  test("a file in a group with a heavily deleted file is rewritten with it") {
    val files =
      Seq(file("a", 100, deleteFiles = 1, deletedRows = 50), file("b", 100), file("c", 20))
    val plan = BinPackPlanner.plan(files, options())
    // Only the files outside the size range or with too many deletes are candidates.
    assert(plan.groups.map(ids) == Seq(Seq("a", "c")))
  }

  // -- Options -------------------------------------------------------------------------------

  test("rewrite-all rewrites files of any size") {
    val plan = BinPackPlanner.plan(small(2, 100), options("rewrite-all" -> "true"))
    assert(plan.groups.map(ids) == Seq(Seq("f0", "f1")))
  }

  test("max-files-to-rewrite trims the selection but not the group count") {
    val files = (0 until 3).flatMap(p => small(5, 10, Seq(s"p$p"), s"p$p-"))
    val plan = BinPackPlanner.plan(files, options("max-files-to-rewrite" -> "7"))
    assert(plan.totalGroupCount == 3)
    assert(plan.groups.map(_.numFiles) == Seq(5, 2))
    assert(plan.groups.map(_.partition) == Seq(Seq("p0"), Seq("p1")))
  }

  test("rewrite-job-order sorts the groups") {
    val files =
      small(5, 10, Seq("small"), "s") ++ small(7, 10, Seq("many"), "m") ++
        small(5, 30, Seq("big"), "b")
    def order(jobOrder: String) =
      BinPackPlanner
        .plan(files, options("rewrite-job-order" -> jobOrder))
        .groups
        .map(_.partition.head)
    assert(order("none") == Seq("small", "many", "big"))
    assert(order("bytes-asc") == Seq("small", "many", "big"))
    assert(order("bytes-desc") == Seq("big", "many", "small"))
    assert(order("files-asc") == Seq("small", "big", "many"))
    assert(order("files-desc") == Seq("many", "small", "big"))
  }

  test("files from an older spec group under one key") {
    val old = (0 until 3).map(i => file(s"old$i", 10, partition = Seq(null)).copy(specId = 1))
    val files = old ++ small(2, 10, partition = Seq("x"), prefix = "x")
    val plan = BinPackPlanner.plan(files, options("min-input-files" -> "2"))
    assert(plan.groups.map(ids).toSet == Set(Seq("old0", "old1", "old2"), Seq("x0", "x1")))
  }

  // -- Sizes --------------------------------------------------------------------------------

  test("the output file count rounds a small remainder into the other files") {
    val gb = 1024 * MB
    val opts = CompactionOptions
      .parse(Map(CompactionOptions.TargetFileSizeBytes -> gb.toString), Map.empty, 0)
      .toOption
      .get
    def files(inputGb: Double) = BinPackPlanner.expectedOutputFiles((inputGb * gb).toLong, opts)
    assert(files(0.5) == 1)
    assert(files(1.0) == 1)
    // A remainder under the minimum size is spread over the others while that keeps the
    // average file within 10% of the target.
    assert(files(10.1) == 10)
    assert(files(10.5) == 10)
    // A remainder over the minimum size is a file of its own.
    assert(files(10.9) == 11)
    // Spreading 0.6 GB over one file would make it 1.6 GB, which is too large.
    assert(files(1.6) == 2)
  }

  test("the split size is the input per output file, between the target and the rolling size") {
    val opts = options()
    // 200 MB into two files is 100 MB each plus the 5 KB overhead.
    assert(BinPackPlanner.inputSplitSize(200 * MB, opts) == 100 * MB + 5 * 1024)
    // Small input never splits finer than the target.
    assert(BinPackPlanner.inputSplitSize(10 * MB, opts) == 100 * MB)
    // And it never exceeds the size the writer rolls at.
    assert(BinPackPlanner.inputSplitSize(1000 * MB, opts) <= opts.writeMaxFileSize)
  }

  // -- Packing -------------------------------------------------------------------------------

  test("pack with a lookback of one fills a bin and starts the next") {
    val bins = BinPackPlanner.pack(Seq(10, 90, 20), 100L, 1, largestBinFirst = false)(_.toLong)
    assert(bins == Seq(Seq(10, 90), Seq(20)))
  }

  test("pack keeps an oversized item alone") {
    val bins = BinPackPlanner.pack(Seq(10, 500, 10), 100L, 1, largestBinFirst = false)(_.toLong)
    assert(bins == Seq(Seq(10), Seq(500), Seq(10)))
  }

  test("pack of nothing is nothing") {
    assert(
      BinPackPlanner.pack(Seq.empty[Int], 100L, 1, largestBinFirst = false)(_.toLong).isEmpty)
  }

  test("pack with a larger lookback puts an item in the first bin it fits") {
    val bins =
      BinPackPlanner.pack(Seq(60, 60, 30, 30), 100L, 10, largestBinFirst = false)(_.toLong)
    assert(bins == Seq(Seq(60, 30), Seq(60, 30)))
  }

  test("pack closes the oldest or the heaviest bin when the lookback is exceeded") {
    val items = Seq(50, 70, 60, 20)
    val oldest = BinPackPlanner.pack(items, 100L, 2, largestBinFirst = false)(_.toLong)
    val heaviest = BinPackPlanner.pack(items, 100L, 2, largestBinFirst = true)(_.toLong)
    assert(oldest == Seq(Seq(50), Seq(70, 20), Seq(60)))
    assert(heaviest == Seq(Seq(70), Seq(50, 20), Seq(60)))
  }

  test("read tasks split a group into bins no larger than the split size") {
    val sizes = Seq.fill(6)(30L)
    val bins = BinPackPlanner.planReadTasks(sizes, splitSize = 100L)(identity, _ => 1)
    assert(bins == Seq(Seq(30L, 30L, 30L), Seq(30L, 30L, 30L)))
  }

  test("read tasks pack the heaviest open bin first when there are too many") {
    // A lookback of two closes the heaviest bin when a third opens, which is how Iceberg plans
    // the read of a staged group.
    val bins =
      BinPackPlanner.planReadTasks(Seq(50L, 70L, 60L, 20L), splitSize = 100L, lookback = 2)(
        identity,
        _ => 1)
    assert(bins == Seq(Seq(70L), Seq(50L, 20L), Seq(60L)))
  }

  test("read tasks count an empty file as its open cost") {
    val bins = BinPackPlanner.planReadTasks(Seq(0L, 0L, 0L), splitSize = 10L, openFileCost = 5L)(
      identity,
      _ => 1)
    assert(bins.map(_.size) == Seq(2, 1))
  }

  test("read tasks weigh the delete files a task opens") {
    // One byte each, but every task opens three files at a cost of 10: three tasks fit a bin of
    // 100 and a fourth does not.
    val bins = BinPackPlanner.planReadTasks(
      Seq.fill(4)(1L),
      splitSize = 100L,
      openFileCost = 10L)(identity, _ => 3)
    assert(bins.map(_.size) == Seq(3, 1))
  }

  // -- Splitting groups into jobs ------------------------------------------------------------

  test("a job takes at most the number of groups asked for") {
    val jobs = BinPackPlanner.chunk(Seq("a", "b", "c", "d", "e"), 2, Int.MaxValue)(_ => 1)
    assert(jobs == Seq(Seq("a", "b"), Seq("c", "d"), Seq("e")))
  }

  test("a job stops before it passes the bin limit") {
    val jobs = BinPackPlanner.chunk(Seq("a" -> 6, "b" -> 4, "c" -> 1, "d" -> 9), 10, 10)(_._2)
    assert(jobs == Seq(Seq("a" -> 6, "b" -> 4), Seq("c" -> 1, "d" -> 9)))
  }

  test("a group over the bin limit gets a job of its own") {
    val jobs = BinPackPlanner.chunk(Seq("a" -> 1, "huge" -> 50, "b" -> 1), 10, 10)(_._2)
    assert(jobs == Seq(Seq("a" -> 1), Seq("huge" -> 50), Seq("b" -> 1)))
  }

  test("without limits all the groups are one job, and none is no job") {
    assert(
      BinPackPlanner.chunk(Seq(1, 2, 3), Int.MaxValue, Int.MaxValue)(_ => 1) == Seq(Seq(1, 2, 3)))
    assert(BinPackPlanner.chunk(Seq.empty[Int], 2, 10)(_ => 1).isEmpty)
  }
}

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

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.serde.OperatorOuterClass.{DeleteFileList, IcebergDeleteFile, IcebergFilePools, IcebergFileScanTask, IcebergPredicate, IcebergScan, IcebergScanCommon, PartitionData}
import org.apache.comet.serde.operator.IcebergScanPoolsTestUtils._

class IcebergScanPoolsSuite extends AnyFunSuite {

  // A task with every pool index replaced by the entry it points at, so tasks compare equal no
  // matter how their pools are numbered.
  private case class ResolvedTask(
      task: IcebergFileScanTask,
      deletes: Option[Seq[(IcebergDeleteFile, String)]],
      partitionData: Option[PartitionData],
      residual: Option[IcebergPredicate])

  private def resolveDelete(pools: IcebergScanCommon, idx: Int): (IcebergDeleteFile, String) = {
    val deleteFile = pools.getDeleteFilePool(idx)
    (
      deleteFile.toBuilder.clearFilePathIdx().build(),
      pools.getDeleteFilePathPool(deleteFile.getFilePathIdx))
  }

  private def resolveSet(pools: IcebergScanCommon, idx: Int): Seq[(IcebergDeleteFile, String)] =
    pools
      .getDeleteFilesPool(idx)
      .getDeleteFileIndicesList
      .asScala
      .toSeq
      .map(d => resolveDelete(pools, d))

  private def resolve(pools: IcebergScanCommon, task: IcebergFileScanTask): ResolvedTask =
    ResolvedTask(
      task.toBuilder.clearDeleteFilesIdx().clearPartitionDataIdx().clearResidualIdx().build(),
      if (task.hasDeleteFilesIdx) Some(resolveSet(pools, task.getDeleteFilesIdx)) else None,
      if (task.hasPartitionDataIdx) Some(pools.getPartitionDataPool(task.getPartitionDataIdx))
      else None,
      if (task.hasResidualIdx) Some(pools.getResidualPool(task.getResidualIdx)) else None)

  // Each partition's message as the driver ships it, and the scan the executor builds from it.
  private def shipAndInject(common: IcebergScanCommon, partitions: IndexedSeq[IcebergScan])
      : (IcebergScanCommon, IndexedSeq[(IcebergScan, IcebergScan)]) = {
    val block = IcebergScanPools.commonBlock(common)
    (
      block,
      partitions.map { p =>
        val payload = IcebergScanPools.partitionPayload(common, p)
        payload -> IcebergScanPools.forPartition(block, payload)
      })
  }

  /** Fails unless every task the executor runs resolves to the same entries as the original. */
  private def assertSameTasks(
      common: IcebergScanCommon,
      partitions: IndexedSeq[IcebergScan]): Unit = {
    val (_, shipped) = shipAndInject(common, partitions)
    partitions.indices.foreach { p =>
      val injected = shipped(p)._2
      val expected = partitions(p).getFileScanTasksList.asScala.map(resolve(common, _))
      val actual = injected.getFileScanTasksList.asScala.map(resolve(injected.getCommon, _))
      assert(actual == expected, s"partition $p resolves differently once shipped")
    }
  }

  // A pool entry, named by its pool and its index in the original pools.
  private type Entry = (String, Int)

  // Which partitions use each entry: directly from a task, or through a set or delete file that
  // holds it.
  private def users(
      common: IcebergScanCommon,
      partitions: IndexedSeq[IcebergScan]): Map[Entry, Set[Int]] = {
    val uses = for {
      p <- partitions.indices
      task <- partitions(p).getFileScanTasksList.asScala.toSeq
      entry <- {
        val set = if (task.hasDeleteFilesIdx) {
          val deletes = common
            .getDeleteFilesPool(task.getDeleteFilesIdx)
            .getDeleteFileIndicesList
            .asScala
            .toSeq
            .map(_.intValue)
          Seq("set" -> task.getDeleteFilesIdx) ++ deletes.map("delete" -> _) ++
            deletes.map(d => "path" -> common.getDeleteFilePool(d).getFilePathIdx)
        } else {
          Seq.empty
        }
        val partitionData =
          if (task.hasPartitionDataIdx) Seq("partition" -> task.getPartitionDataIdx)
          else Seq.empty
        val residual =
          if (task.hasResidualIdx) Seq("residual" -> task.getResidualIdx) else Seq.empty
        set ++ partitionData ++ residual
      }
    } yield entry -> p
    uses.groupBy(_._1).map { case (entry, ps) => entry -> ps.map(_._2).toSet }
  }

  // An entry's content, so entries can be matched across differently numbered pools.
  private def content(pools: IcebergScanCommon, entry: Entry): Any = entry match {
    case ("path", i) => pools.getDeleteFilePathPool(i)
    case ("delete", i) => resolveDelete(pools, i)
    case ("set", i) => resolveSet(pools, i)
    case ("partition", i) => pools.getPartitionDataPool(i)
    case ("residual", i) => pools.getResidualPool(i)
    case other => throw new IllegalArgumentException(s"unknown pool entry $other")
  }

  // Every entry of the five split pools, from position `from` on.
  private def entries(pools: IcebergScanCommon, from: IcebergScanCommon): Seq[Entry] = {
    def range(name: String, count: IcebergScanCommon => Int): Seq[Entry] =
      (count(from) until count(pools)).map(name -> _)
    range("path", _.getDeleteFilePathPoolCount) ++
      range("delete", _.getDeleteFilePoolCount) ++
      range("set", _.getDeleteFilesPoolCount) ++
      range("partition", _.getPartitionDataPoolCount) ++
      range("residual", _.getResidualPoolCount)
  }

  /**
   * A random scan. Entries are distinct by content, as the driver's interning makes them. Some
   * delete files share a path, like deletion vectors in one Puffin file, and some entries are
   * used by no task.
   */
  private def randomScan(rnd: Random): (IcebergScanCommon, IndexedSeq[IcebergScan]) = {
    val paths = 1 + rnd.nextInt(30)
    val deletes = 1 + rnd.nextInt(40)
    val partitionValues = 1 + rnd.nextInt(10)
    val residuals = 1 + rnd.nextInt(5)
    val common = withOtherFields(IcebergScanCommon.newBuilder())
    (0 until paths).foreach(i => common.addDeleteFilePathPool(deleteFilePath(i)))
    (0 until deletes).foreach { i =>
      common.addDeleteFilePool(positionDelete(rnd.nextInt(paths), recordCount = i + 1L))
    }
    val sets = Seq
      .fill(1 + rnd.nextInt(20))(Seq.fill(1 + rnd.nextInt(4))(rnd.nextInt(deletes)).distinct)
      .distinct
    sets.foreach { set =>
      val list = DeleteFileList.newBuilder()
      set.foreach(d => list.addDeleteFileIndices(d))
      common.addDeleteFilesPool(list)
    }
    (0 until partitionValues).foreach(i => common.addPartitionDataPool(partitionValue(i)))
    (0 until residuals).foreach(i => common.addResidualPool(notNull(s"c$i")))

    def sometimes(percent: Int, bound: Int): Option[Int] =
      if (rnd.nextInt(100) < percent) Some(rnd.nextInt(bound)) else None
    var files = 0
    val partitions = (0 until 1 + rnd.nextInt(8)).map { _ =>
      val scan = IcebergScan.newBuilder()
      (0 until rnd.nextInt(6)).foreach { _ =>
        files += 1
        scan.addFileScanTasks(
          fileTask(
            s"data-$files.parquet",
            sometimes(70, sets.size),
            sometimes(80, partitionValues),
            sometimes(50, residuals)))
      }
      scan.build()
    }
    (common.build(), partitions)
  }

  test("every task resolves to the same entries once shipped and injected") {
    (0 until 200).foreach { seed =>
      val (common, partitions) = randomScan(new Random(seed))
      withClue(s"seed $seed: ") {
        assertSameTasks(common, partitions)
      }
    }
  }

  test("a partition ships exactly the entries its tasks use, each once") {
    (0 until 200).foreach { seed =>
      val (common, partitions) = randomScan(new Random(seed))
      val (block, shipped) = shipAndInject(common, partitions)
      val usedBy = users(common, partitions)
      val empty = IcebergScanCommon.getDefaultInstance
      withClue(s"seed $seed: ") {
        assert(entries(block, empty).isEmpty, "the common block holds per-file entries")
        partitions.indices.foreach { p =>
          val expected = usedBy.collect { case (e, ps) if ps.contains(p) => content(common, e) }
          val own = asCommon(shipped(p)._1.getOwnPools)
          val sent = entries(own, empty).map(content(own, _))
          assert(sent.size == expected.size, s"partition $p ships an entry twice or extra")
          assert(sent.toSet == expected.toSet, s"partition $p ships the wrong entries")
          val injected = shipped(p)._2.getCommon
          val kept = entries(injected, empty).map(content(injected, _))
          assert(kept.toSet == expected.toSet && kept.size == expected.size)
        }
      }
    }
  }

  test("the common block and each partition's plan do not grow with the scan") {
    // Partition 0's data is the same at every scan size, so its message and plan must be too.
    // Shipping the whole scan's pools in the common block made both grow with the scan.
    val sizes = Seq(2, 100, 10000).map { buckets =>
      val (common, partitions) = bucketedScan(buckets)
      val (block, shipped) = shipAndInject(common, partitions)
      (
        common.getSerializedSize,
        block.getSerializedSize,
        shipped.head._1.getSerializedSize,
        shipped.head._2.getSerializedSize)
    }
    val unsplit = sizes.map(_._1)
    assert(unsplit.last > 100 * unsplit.head, s"the test scan does not grow: $unsplit")
    assert(sizes.map(_._2).distinct.size == 1, s"common block grows with the scan: $sizes")
    assert(sizes.map(_._3).distinct.size == 1, s"partition message grows with the scan: $sizes")
    assert(sizes.map(_._4).distinct.size == 1, s"injected plan grows with the scan: $sizes")
  }

  test("a partition's message does not grow when partitions share more delete files") {
    // Each delete file applies to two neighbouring partitions. Partition 0 must still get only
    // the two it uses, however many other partitions there are.
    def shippedSize(partitionCount: Int): (Int, Int) = {
      val common = IcebergScanCommon.newBuilder().setMetadataLocation(metadataLocation)
      (0 until partitionCount).foreach { d =>
        common.addDeleteFilePathPool(deleteFilePath(d))
        common.addDeleteFilePool(positionDelete(d, recordCount = 1))
      }
      val partitions = (0 until partitionCount).map { p =>
        val set = common.getDeleteFilesPoolCount
        common.addDeleteFilesPool(
          DeleteFileList
            .newBuilder()
            .addDeleteFileIndices(p)
            .addDeleteFileIndices((p + 1) % partitionCount))
        IcebergScan
          .newBuilder()
          .addFileScanTasks(fileTask(f"data-$p%08d.parquet", Some(set), None, None))
          .build()
      }
      val (block, shipped) = shipAndInject(common.build(), partitions)
      (block.getSerializedSize, shipped.head._1.getSerializedSize)
    }
    val sizes = Seq(3, 100, 10000).map(shippedSize)
    assert(sizes.distinct.size == 1, s"shipped sizes grow with shared delete files: $sizes")
  }

  test("a delete file every partition uses ships with each of them") {
    // An unpartitioned equality delete applies to every data file.
    val global = IcebergDeleteFile
      .newBuilder()
      .setFilePathIdx(0)
      .setContentType("EQUALITY_DELETES")
      .setFileFormat("PARQUET")
      .addEqualityIds(1)
      .setRecordCount(5)
      .build()
    val common = IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .addDeleteFilePathPool("s3://bucket/table/data/global-eq-deletes.parquet")
      .addDeleteFilePool(global)
      .addDeleteFilesPool(DeleteFileList.newBuilder().addDeleteFileIndices(0))
      .build()
    val partitions = (0 until 6).map { b =>
      IcebergScan
        .newBuilder()
        .addFileScanTasks(fileTask(s"a-$b.parquet", Some(0), None, None))
        .build()
    }
    val (_, shipped) = shipAndInject(common, partitions)
    shipped.foreach { case (payload, _) =>
      assert(payload.getOwnPools.getDeleteFilePoolList.asScala.toSeq == Seq(global))
      assert(payload.getOwnPools.getDeleteFilePathPoolCount == 1)
    }
    assertSameTasks(common, partitions)
  }

  test("a partition with no tasks ships an empty message") {
    val (common, partitions) = bucketedScan(3)
    val withEmpty = partitions :+ IcebergScan.getDefaultInstance
    val payload = IcebergScanPools.partitionPayload(common, IcebergScan.getDefaultInstance)
    assert(!payload.hasOwnPools)
    assert(payload.getSerializedSize == 0)
    assertSameTasks(common, withEmpty)
  }

  test("an index past the end of its pool fails with the pool's name") {
    val (common, partitions) = bucketedScan(2)
    val bad = partitions.head.toBuilder
      .setFileScanTasks(0, partitions.head.getFileScanTasks(0).toBuilder.setDeleteFilesIdx(99))
      .build()
    val error = intercept[IllegalStateException] {
      IcebergScanPools.partitionPayload(common, bad)
    }
    assert(error.getMessage.contains("delete_files_set index 99 is out of range"))
  }

  // The common block with the five per-file pools emptied: everything shipping must leave alone.
  private def otherFields(common: IcebergScanCommon): IcebergScanCommon =
    common.toBuilder
      .clearDeleteFilePathPool()
      .clearDeleteFilePool()
      .clearDeleteFilesPool()
      .clearPartitionDataPool()
      .clearResidualPool()
      .build()

  test("shipping keeps every field outside the five pools") {
    (0 until 50).foreach { seed =>
      val (common, partitions) = randomScan(new Random(seed))
      val (block, shipped) = shipAndInject(common, partitions)
      withClue(s"seed $seed: ") {
        assert(block == otherFields(common))
        shipped.foreach { case (_, injected) =>
          assert(otherFields(injected.getCommon) == otherFields(common))
        }
      }
    }
  }

  test("every pool index field is one the pruning knows about") {
    // A new index into a pool must be handled by IcebergScanPools, or tasks keep indices into
    // pools that no longer match. This fails when one is added without updating it.
    def indexFields(descriptor: com.google.protobuf.Descriptors.Descriptor): Set[String] =
      descriptor.getFields.asScala
        .map(_.getName)
        .filter(n => n.endsWith("_idx") || n.endsWith("_indices"))
        .toSet
    assert(
      indexFields(IcebergFileScanTask.getDescriptor) == Set(
        "schema_idx",
        "partition_spec_idx",
        "name_mapping_idx",
        "project_field_ids_idx",
        "partition_data_idx",
        "delete_files_idx",
        "residual_idx"))
    assert(indexFields(IcebergDeleteFile.getDescriptor) == Set("file_path_idx"))
    assert(indexFields(DeleteFileList.getDescriptor) == Set("delete_file_indices"))
    // A per-partition field added to IcebergScan must be carried by partitionPayload.
    assert(
      IcebergScan.getDescriptor.getFields.asScala.map(_.getName).toSet ==
        Set("common", "file_scan_tasks", "own_pools"))
    // A partition's own pools must be exactly the common block's per-file pools.
    assert(
      IcebergFilePools.getDescriptor.getFields.asScala.map(_.getName).toSet == Set(
        "partition_data_pool",
        "delete_files_pool",
        "residual_pool",
        "delete_file_pool",
        "delete_file_path_pool"))
    assert(
      IcebergScanCommon.getDescriptor.getFields.asScala
        .map(_.getName)
        .filter(_.endsWith("_pool"))
        .toSet ==
        Set(
          "schema_pool",
          "partition_type_pool",
          "partition_spec_pool",
          "name_mapping_pool",
          "project_field_ids_pool",
          "partition_data_pool",
          "delete_files_pool",
          "residual_pool",
          "delete_file_pool",
          "delete_file_path_pool"))
  }

  test("lazily built scan data compares by content without building partitions") {
    val (common, partitions) = bucketedScan(3)
    def data(c: IcebergScanCommon) =
      new IcebergPartitionPlanData(c, partitions.map(_.toByteArray).toArray)
    assert(data(common).sameAs(data(common)))
    assert(!data(common).sameAs(data(common.toBuilder.setScanHashCode(7).build())))
  }
}

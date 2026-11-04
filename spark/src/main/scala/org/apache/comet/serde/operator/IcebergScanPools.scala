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

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.protobuf.CodedInputStream

import org.apache.comet.serde.OperatorOuterClass.{DeleteFileList, IcebergDeleteFile, IcebergFilePools, IcebergPredicate, IcebergScan, IcebergScanCommon, PartitionData}

/**
 * Decides where an Iceberg scan's pool entries ship.
 *
 * Five pools grow with the number of files in the scan: delete file paths, delete files, delete
 * file sets, partition values and residuals. An entry that at least half of the partitions use,
 * such as an unpartitioned equality delete, ships once in the common block. Every other entry
 * ships with each partition whose tasks use it, as Iceberg's own Spark reader ships each task's
 * delete files with the task. Since each entry in the common block is needed by at least half of
 * the tasks, the common block is at most twice what an average partition's tasks need.
 *
 * A partition's tasks index the common entries first, then the partition's own. On the driver,
 * [[partitionPayload]] builds one partition's message. On the executor, [[forPartition]] appends
 * its own entries to the common block.
 *
 * Known limit: a coalesce without a shuffle puts several partitions' messages in one task, and
 * each carries its own copy of the entries they share. Entries that fewer than half of the
 * partitions use, such as the partition-scoped deletes of a large partition, are then repeated
 * once per coalesced partition.
 */
private[apache] object IcebergScanPools {

  /** Read access to a scan's five per-file pools. */
  trait FilePools {
    def pathCount: Int
    def path(i: Int): String
    def deleteCount: Int
    def delete(i: Int): IcebergDeleteFile
    def setCount: Int
    def set(i: Int): DeleteFileList
    def partitionValueCount: Int
    def partitionValue(i: Int): PartitionData
    def residualCount: Int
    def residual(i: Int): IcebergPredicate

    def isEmpty: Boolean =
      pathCount == 0 && deleteCount == 0 && setCount == 0 && partitionValueCount == 0 &&
        residualCount == 0
  }

  /** The pools of a built message. */
  final class MessagePools(common: IcebergScanCommon) extends FilePools {
    override def pathCount: Int = common.getDeleteFilePathPoolCount
    override def path(i: Int): String = common.getDeleteFilePathPool(i)
    override def deleteCount: Int = common.getDeleteFilePoolCount
    override def delete(i: Int): IcebergDeleteFile = common.getDeleteFilePool(i)
    override def setCount: Int = common.getDeleteFilesPoolCount
    override def set(i: Int): DeleteFileList = common.getDeleteFilesPool(i)
    override def partitionValueCount: Int = common.getPartitionDataPoolCount
    override def partitionValue(i: Int): PartitionData = common.getPartitionDataPool(i)
    override def residualCount: Int = common.getResidualPoolCount
    override def residual(i: Int): IcebergPredicate = common.getResidualPool(i)
  }

  /**
   * The pools of a serialized `IcebergScanCommon`, parsing only the entries asked for. The bytes
   * plus one offset per entry take a fraction of the heap of the parsed message, which is several
   * times its serialized size.
   */
  final class SerializedPools(val bytes: Array[Byte]) extends FilePools {
    // Each entry's offset (high 32 bits) and length (low 32 bits), per pool, in wire order.
    private val located: Array[Array[Long]] = {
      val entries = Array.fill(5)(mutable.ArrayBuilder.make[Long])
      val in = CodedInputStream.newInstance(bytes)
      var tag = in.readTag()
      while (tag != 0) {
        val pool = SerializedPools.poolOf(tag >>> 3)
        if (pool >= 0) {
          val length = in.readRawVarint32()
          entries(pool) += (in.getTotalBytesRead.toLong << 32) | length.toLong
          in.skipRawBytes(length)
        } else {
          val _ = in.skipField(tag)
        }
        tag = in.readTag()
      }
      entries.map(_.result())
    }

    private def offset(pool: Int, i: Int): Int = (located(pool)(i) >>> 32).toInt
    private def length(pool: Int, i: Int): Int = (located(pool)(i) & 0xffffffffL).toInt

    override def pathCount: Int = located(SerializedPools.Paths).length
    override def path(i: Int): String =
      new String(bytes, offset(SerializedPools.Paths, i), length(SerializedPools.Paths, i), UTF_8)
    override def deleteCount: Int = located(SerializedPools.Deletes).length
    override def delete(i: Int): IcebergDeleteFile =
      IcebergDeleteFile
        .parser()
        .parseFrom(bytes, offset(SerializedPools.Deletes, i), length(SerializedPools.Deletes, i))
    override def setCount: Int = located(SerializedPools.Sets).length
    override def set(i: Int): DeleteFileList =
      DeleteFileList
        .parser()
        .parseFrom(bytes, offset(SerializedPools.Sets, i), length(SerializedPools.Sets, i))
    override def partitionValueCount: Int = located(SerializedPools.PartitionValues).length
    override def partitionValue(i: Int): PartitionData =
      PartitionData
        .parser()
        .parseFrom(
          bytes,
          offset(SerializedPools.PartitionValues, i),
          length(SerializedPools.PartitionValues, i))
    override def residualCount: Int = located(SerializedPools.Residuals).length
    override def residual(i: Int): IcebergPredicate =
      IcebergPredicate
        .parser()
        .parseFrom(
          bytes,
          offset(SerializedPools.Residuals, i),
          length(SerializedPools.Residuals, i))
  }

  private object SerializedPools {
    val Paths = 0
    val Deletes = 1
    val Sets = 2
    val PartitionValues = 3
    val Residuals = 4

    def poolOf(field: Int): Int =
      if (field == IcebergScanCommon.DELETE_FILE_PATH_POOL_FIELD_NUMBER) Paths
      else if (field == IcebergScanCommon.DELETE_FILE_POOL_FIELD_NUMBER) Deletes
      else if (field == IcebergScanCommon.DELETE_FILES_POOL_FIELD_NUMBER) Sets
      else if (field == IcebergScanCommon.PARTITION_DATA_POOL_FIELD_NUMBER) PartitionValues
      else if (field == IcebergScanCommon.RESIDUAL_POOL_FIELD_NUMBER) Residuals
      else -1
  }

  private val Unused = -1

  private def checkIndex(name: String, i: Int, size: Int): Unit =
    if (i < 0 || i >= size) {
      throw new IllegalStateException(
        s"Iceberg $name index $i is out of range: the pool has $size entries")
    }

  /**
   * Which entries at least half of the non-empty partitions use, and each one's index in the
   * common block. With fewer than two non-empty partitions there are none.
   */
  final class Layout private[IcebergScanPools] (
      val path: Array[Int],
      val delete: Array[Int],
      val set: Array[Int],
      val partitionValue: Array[Int],
      val residual: Array[Int]) {

    val pathCount: Int = path.count(_ != Unused)
    val deleteCount: Int = delete.count(_ != Unused)
    val setCount: Int = set.count(_ != Unused)
    val partitionValueCount: Int = partitionValue.count(_ != Unused)
    val residualCount: Int = residual.count(_ != Unused)
  }

  object Layout {

    /** Finds the entries of `pools` that at least half of the non-empty `partitions` use. */
    def apply(pools: FilePools, partitions: collection.IndexedSeq[IcebergScan]): Layout = {
      val paths = new Users("delete_file_path", pools.pathCount)
      val deletes = new Users("delete_file", pools.deleteCount)
      val sets = new Users("delete_files_set", pools.setCount)
      val partitionValues = new Users("partition_data", pools.partitionValueCount)
      val residuals = new Users("residual", pools.residualCount)
      var nonEmpty = 0
      partitions.indices.foreach { p =>
        val tasks = partitions(p).getFileScanTasksList
        if (!tasks.isEmpty) nonEmpty += 1
        tasks.asScala.foreach { task =>
          if (task.hasResidualIdx) residuals.use(task.getResidualIdx, p)
          if (task.hasPartitionDataIdx) partitionValues.use(task.getPartitionDataIdx, p)
          if (task.hasDeleteFilesIdx && sets.use(task.getDeleteFilesIdx, p)) {
            val set = pools.set(task.getDeleteFilesIdx)
            (0 until set.getDeleteFileIndicesCount).foreach { j =>
              val d = set.getDeleteFileIndices(j)
              if (deletes.use(d, p)) paths.use(pools.delete(d).getFilePathIdx, p)
            }
          }
        }
      }
      // A set's delete files and a delete file's path have at least the set's users, so an entry
      // in the common block only points at entries that are there too.
      val half = if (nonEmpty >= 2) (nonEmpty + 1) / 2 else Int.MaxValue
      new Layout(
        paths.usedBy(half),
        deletes.usedBy(half),
        sets.usedBy(half),
        partitionValues.usedBy(half),
        residuals.usedBy(half))
    }

    /** How many partitions use each entry of one pool. */
    private final class Users(name: String, size: Int) {
      private val lastUser = Array.fill(size)(Unused)
      private val users = new Array[Int](size)

      // Records that partition p uses entry i. Returns true the first time p does, so the caller
      // records the entries it points to as well.
      def use(i: Int, p: Int): Boolean = {
        checkIndex(name, i, size)
        if (lastUser(i) == p) {
          false
        } else {
          lastUser(i) = p
          users(i) += 1
          true
        }
      }

      // Each entry's index among the entries at least `partitions` partitions use, or Unused.
      def usedBy(partitions: Int): Array[Int] = {
        var next = 0
        users.map { n =>
          if (n >= partitions) {
            next += 1
            next - 1
          } else {
            Unused
          }
        }
      }
    }
  }

  /**
   * The common block every task gets: the scan without its per-file pools, plus the entries at
   * least half of the partitions use.
   */
  def commonBlock(original: IcebergScanCommon, layout: Layout): IcebergScanCommon = {
    val builder = original.toBuilder
      .clearDeleteFilePathPool()
      .clearDeleteFilePool()
      .clearDeleteFilesPool()
      .clearPartitionDataPool()
      .clearResidualPool()
    def shared(positions: Array[Int]): Iterator[Int] =
      positions.indices.iterator.filter(positions(_) != Unused)
    shared(layout.path).foreach(i =>
      builder.addDeleteFilePathPool(original.getDeleteFilePathPool(i)))
    // Every partition that uses a delete file uses its path, and every one that uses a set uses
    // its delete files, so those are in the common block too.
    shared(layout.delete).foreach { i =>
      val deleteFile = original.getDeleteFilePool(i)
      builder.addDeleteFilePool(
        deleteFile.toBuilder.setFilePathIdx(layout.path(deleteFile.getFilePathIdx)))
    }
    shared(layout.set).foreach { i =>
      val set = original.getDeleteFilesPool(i)
      val remapped = DeleteFileList.newBuilder()
      (0 until set.getDeleteFileIndicesCount).foreach { j =>
        remapped.addDeleteFileIndices(layout.delete(set.getDeleteFileIndices(j)))
      }
      builder.addDeleteFilesPool(remapped)
    }
    shared(layout.partitionValue).foreach(i =>
      builder.addPartitionDataPool(original.getPartitionDataPool(i)))
    shared(layout.residual).foreach(i => builder.addResidualPool(original.getResidualPool(i)))
    builder.build()
  }

  /**
   * One partition's message, built on the driver. `tasks` indexes into `pools`. The result
   * carries the tasks, renumbered to index the common block's entries first, and in `own_pools`
   * the other entries they use.
   */
  def partitionPayload(pools: FilePools, layout: Layout, tasks: IcebergScan): IcebergScan = {
    val own = IcebergFilePools.newBuilder()
    val ownPath = mutable.HashMap.empty[Int, Int]
    val ownDelete = mutable.HashMap.empty[Int, Int]
    val ownSet = mutable.HashMap.empty[Int, Int]
    val ownPartitionValue = mutable.HashMap.empty[Int, Int]
    val ownResidual = mutable.HashMap.empty[Int, Int]

    def path(i: Int): Int = {
      checkIndex("delete_file_path", i, pools.pathCount)
      if (layout.path(i) != Unused) {
        layout.path(i)
      } else {
        ownPath.getOrElseUpdate(
          i, {
            own.addDeleteFilePathPool(pools.path(i))
            layout.pathCount + own.getDeleteFilePathPoolCount - 1
          })
      }
    }

    def delete(i: Int): Int = {
      checkIndex("delete_file", i, pools.deleteCount)
      if (layout.delete(i) != Unused) {
        layout.delete(i)
      } else {
        ownDelete.getOrElseUpdate(
          i, {
            val deleteFile = pools.delete(i)
            val pathIdx = path(deleteFile.getFilePathIdx)
            own.addDeleteFilePool(deleteFile.toBuilder.setFilePathIdx(pathIdx))
            layout.deleteCount + own.getDeleteFilePoolCount - 1
          })
      }
    }

    def set(i: Int): Int = {
      checkIndex("delete_files_set", i, pools.setCount)
      if (layout.set(i) != Unused) {
        layout.set(i)
      } else {
        ownSet.getOrElseUpdate(
          i, {
            val set = pools.set(i)
            val remapped = DeleteFileList.newBuilder()
            (0 until set.getDeleteFileIndicesCount).foreach { j =>
              remapped.addDeleteFileIndices(delete(set.getDeleteFileIndices(j)))
            }
            own.addDeleteFilesPool(remapped)
            layout.setCount + own.getDeleteFilesPoolCount - 1
          })
      }
    }

    def partitionValue(i: Int): Int = {
      checkIndex("partition_data", i, pools.partitionValueCount)
      if (layout.partitionValue(i) != Unused) {
        layout.partitionValue(i)
      } else {
        ownPartitionValue.getOrElseUpdate(
          i, {
            own.addPartitionDataPool(pools.partitionValue(i))
            layout.partitionValueCount + own.getPartitionDataPoolCount - 1
          })
      }
    }

    def residual(i: Int): Int = {
      checkIndex("residual", i, pools.residualCount)
      if (layout.residual(i) != Unused) {
        layout.residual(i)
      } else {
        ownResidual.getOrElseUpdate(
          i, {
            own.addResidualPool(pools.residual(i))
            layout.residualCount + own.getResidualPoolCount - 1
          })
      }
    }

    val renumbered = tasks.getFileScanTasksList.asScala.map { task =>
      val builder = task.toBuilder
      if (task.hasResidualIdx) builder.setResidualIdx(residual(task.getResidualIdx))
      if (task.hasPartitionDataIdx) {
        builder.setPartitionDataIdx(partitionValue(task.getPartitionDataIdx))
      }
      if (task.hasDeleteFilesIdx) builder.setDeleteFilesIdx(set(task.getDeleteFilesIdx))
      builder.build()
    }
    val result = tasks.toBuilder.clearFileScanTasks().addAllFileScanTasks(renumbered.asJava)
    if (own.getDeleteFilePathPoolCount > 0 || own.getDeleteFilePoolCount > 0 ||
      own.getDeleteFilesPoolCount > 0 || own.getPartitionDataPoolCount > 0 ||
      own.getResidualPoolCount > 0) {
      result.setOwnPools(own)
    }
    result.build()
  }

  /**
   * The scan one partition's native plan runs, built on the executor from the common block and
   * the partition's message. Its tasks already index the common entries first, then its own, so
   * this only appends its own entries to the common block.
   */
  def forPartition(common: IcebergScanCommon, partition: IcebergScan): IcebergScan = {
    val own = partition.getOwnPools
    val merged = common.toBuilder
      .addAllDeleteFilePathPool(own.getDeleteFilePathPoolList)
      .addAllDeleteFilePool(own.getDeleteFilePoolList)
      .addAllDeleteFilesPool(own.getDeleteFilesPoolList)
      .addAllPartitionDataPool(own.getPartitionDataPoolList)
      .addAllResidualPool(own.getResidualPoolList)
    partition.toBuilder.clearOwnPools().setCommon(merged).build()
  }
}

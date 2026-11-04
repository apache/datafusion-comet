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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.comet.serde.OperatorOuterClass.{DeleteFileList, IcebergFilePools, IcebergFileScanTask, IcebergScan, IcebergScanCommon}

/**
 * Decides where an Iceberg scan's pool entries ship.
 *
 * Five pools grow with the number of files in the scan: delete file paths, delete files, delete
 * file sets, partition values and residuals. Their entries ship with each Spark partition whose
 * tasks use them, as Iceberg's own Spark reader ships each task's delete files with the task. So
 * a task never holds entries that only other partitions use, and the common block every task
 * deserializes holds only the small pools (schemas, specs, name mappings, field ids) and the
 * scan's settings.
 *
 * On the driver, [[partitionPayload]] builds one partition's message from the full pools. On the
 * executor, [[forPartition]] merges it with the common block into the scan the native plan runs.
 *
 * Known limit: a coalesce without a shuffle puts several partitions' messages in one task, and
 * each carries its own copy of the entries they share. Delete files that many partitions use,
 * such as unpartitioned equality deletes, are then repeated once per coalesced partition.
 */
private[apache] object IcebergScanPools {

  /** The common block every task gets: the scan with the five per-file pools emptied. */
  def commonBlock(original: IcebergScanCommon): IcebergScanCommon =
    original.toBuilder
      .clearDeleteFilePathPool()
      .clearDeleteFilePool()
      .clearDeleteFilesPool()
      .clearPartitionDataPool()
      .clearResidualPool()
      .build()

  /**
   * One partition's message, built on the driver. `tasks` indexes into `original`. The result
   * carries the tasks, renumbered, and in `own_pools` only the per-file entries they use.
   */
  def partitionPayload(original: IcebergScanCommon, tasks: IcebergScan): IcebergScan = {
    val (pools, renumbered) = prune(original, tasks)
    val result = tasks.toBuilder.clearFileScanTasks().addAllFileScanTasks(renumbered.asJava)
    if (pools.getDeleteFilePathPoolCount > 0 || pools.getDeleteFilePoolCount > 0 ||
      pools.getDeleteFilesPoolCount > 0 || pools.getPartitionDataPoolCount > 0 ||
      pools.getResidualPoolCount > 0) {
      result.setOwnPools(pools)
    }
    result.build()
  }

  /** Whether `common` still holds per-file pool entries, which must ship with partitions. */
  def hasFileEntries(common: IcebergScanCommon): Boolean =
    common.getDeleteFilePathPoolCount > 0 || common.getDeleteFilePoolCount > 0 ||
      common.getDeleteFilesPoolCount > 0 || common.getPartitionDataPoolCount > 0 ||
      common.getResidualPoolCount > 0

  /**
   * The scan one partition's native plan runs, built on the executor from the common block and
   * the partition's message. The message already holds just the per-file entries its tasks use,
   * numbered from 0, so this only adds them to the common block.
   */
  def forPartition(common: IcebergScanCommon, partition: IcebergScan): IcebergScan = {
    if (hasFileEntries(common)) {
      throw new IllegalStateException(
        "Iceberg common block holds per-file pool entries; they must ship with each partition")
    }
    val own = partition.getOwnPools
    val merged = common.toBuilder
      .addAllDeleteFilePathPool(own.getDeleteFilePathPoolList)
      .addAllDeleteFilePool(own.getDeleteFilePoolList)
      .addAllDeleteFilesPool(own.getDeleteFilesPoolList)
      .addAllPartitionDataPool(own.getPartitionDataPoolList)
      .addAllResidualPool(own.getResidualPoolList)
    partition.toBuilder.clearOwnPools().setCommon(merged).build()
  }

  /** Entry i of one of the scan's pools, failing with the pool's name when i is out of range. */
  private def entry[T](name: String, i: Int, size: Int)(get: Int => T): T = {
    if (i < 0 || i >= size) {
      throw new IllegalStateException(
        s"Iceberg $name index $i is out of range: the pool has $size entries")
    }
    get(i)
  }

  /**
   * The per-file pool entries of `common` that `tasks` reach, and the tasks renumbered to index
   * them. The work follows the tasks, not the size of the pools.
   */
  private def prune(
      common: IcebergScanCommon,
      tasks: IcebergScan): (IcebergFilePools.Builder, Seq[IcebergFileScanTask]) = {
    val pools = IcebergFilePools.newBuilder()
    val pathIdx = mutable.HashMap.empty[Int, Int]
    val deleteIdx = mutable.HashMap.empty[Int, Int]
    val setIdx = mutable.HashMap.empty[Int, Int]
    val partitionDataIdx = mutable.HashMap.empty[Int, Int]
    val residualIdx = mutable.HashMap.empty[Int, Int]

    def path(i: Int): Int =
      pathIdx.getOrElseUpdate(
        i, {
          pools.addDeleteFilePathPool(
            entry("delete_file_path", i, common.getDeleteFilePathPoolCount)(
              common.getDeleteFilePathPool))
          pools.getDeleteFilePathPoolCount - 1
        })

    def delete(i: Int): Int =
      deleteIdx.getOrElseUpdate(
        i, {
          val deleteFile =
            entry("delete_file", i, common.getDeleteFilePoolCount)(common.getDeleteFilePool)
          pools.addDeleteFilePool(
            deleteFile.toBuilder.setFilePathIdx(path(deleteFile.getFilePathIdx)))
          pools.getDeleteFilePoolCount - 1
        })

    def set(i: Int): Int =
      setIdx.getOrElseUpdate(
        i, {
          val set =
            entry("delete_files_set", i, common.getDeleteFilesPoolCount)(
              common.getDeleteFilesPool)
          val remapped = DeleteFileList.newBuilder()
          (0 until set.getDeleteFileIndicesCount).foreach { j =>
            remapped.addDeleteFileIndices(delete(set.getDeleteFileIndices(j)))
          }
          pools.addDeleteFilesPool(remapped)
          pools.getDeleteFilesPoolCount - 1
        })

    def partitionValue(i: Int): Int =
      partitionDataIdx.getOrElseUpdate(
        i, {
          pools.addPartitionDataPool(
            entry("partition_data", i, common.getPartitionDataPoolCount)(
              common.getPartitionDataPool))
          pools.getPartitionDataPoolCount - 1
        })

    def residual(i: Int): Int =
      residualIdx.getOrElseUpdate(
        i, {
          pools.addResidualPool(
            entry("residual", i, common.getResidualPoolCount)(common.getResidualPool))
          pools.getResidualPoolCount - 1
        })

    val renumbered = tasks.getFileScanTasksList.asScala.toSeq.map { task =>
      val builder = task.toBuilder
      if (task.hasResidualIdx) builder.setResidualIdx(residual(task.getResidualIdx))
      if (task.hasPartitionDataIdx) {
        builder.setPartitionDataIdx(partitionValue(task.getPartitionDataIdx))
      }
      if (task.hasDeleteFilesIdx) builder.setDeleteFilesIdx(set(task.getDeleteFilesIdx))
      builder.build()
    }
    (pools, renumbered)
  }
}

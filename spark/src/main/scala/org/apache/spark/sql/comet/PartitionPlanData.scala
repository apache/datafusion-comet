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

package org.apache.spark.sql.comet

import scala.collection.mutable

import org.apache.comet.serde.OperatorOuterClass

/**
 * One partition's planning data for one scan. It rides in the task with its partition object.
 *
 * A coalesce packs several partitions into one task, and Java serialization writes an object they
 * share once per task stream. So a scan can hold an entry several partitions use as one shared
 * object, and a coalesced task carries it once, as Spark's Iceberg reader does with its shared
 * DeleteFile objects.
 */
trait PartitionPlanData extends Serializable {

  /** The serialized scan message this scan's [[PlanDataInjector]] reads. */
  def bytes: Array[Byte]
}

object PartitionPlanData {

  /** Plan data that is already one serialized message, with nothing shared. */
  def apply(bytes: Array[Byte]): PartitionPlanData = new Bytes(bytes)

  def wrap(perPartition: Array[Array[Byte]]): Array[PartitionPlanData] =
    perPartition.map(apply)

  /** Whether two scans' plan data match, for [[PlanDataInjector.findAllPlanData]]. */
  def same(a: Array[PartitionPlanData], b: Array[PartitionPlanData]): Boolean =
    a.length == b.length && a.indices.forall { i =>
      (a(i) eq b(i)) || java.util.Arrays.equals(a(i).bytes, b(i).bytes)
    }

  private final class Bytes(override val bytes: Array[Byte]) extends PartitionPlanData
}

/**
 * One native Iceberg scan partition's plan data.
 *
 * `scan` is the partition's IcebergScan: its file tasks, plus its partition values, residuals and
 * delete-file sets in `common`. Its delete files are kept apart, because a delete file is often
 * used by many partitions: the driver serializes each delete file once, and every partition that
 * uses it holds the same array and the same path string. A coalesced task then carries each
 * delete file once, and the driver keeps one copy of it.
 *
 * `deleteFiles(i)` is the partition's delete file `i`, an IcebergDeleteFile without
 * `file_path_idx`, and `deleteFilePaths(i)` is its path. The delete-file sets in `scan` index
 * into `deleteFiles`. Each partition orders its delete files differently, so the index into the
 * path pool is set here, when the partition's message is built.
 */
final class IcebergPartitionPlanData(
    val scan: Array[Byte],
    val deleteFiles: Array[Array[Byte]],
    val deleteFilePaths: Array[String])
    extends PartitionPlanData {

  require(
    deleteFiles.length == deleteFilePaths.length,
    s"${deleteFiles.length} delete files but ${deleteFilePaths.length} paths")

  override def bytes: Array[Byte] = {
    if (deleteFiles.isEmpty) {
      scan
    } else {
      val builder = OperatorOuterClass.IcebergScan.newBuilder().mergeFrom(scan)
      val common = builder.getCommonBuilder
      val pathIndex = mutable.HashMap[String, Int]()
      deleteFiles.indices.foreach { i =>
        val path = deleteFilePaths(i)
        val pathIdx = pathIndex.getOrElseUpdate(
          path, {
            common.addDeleteFilePathPool(path)
            pathIndex.size
          })
        common.addDeleteFilePool(
          OperatorOuterClass.IcebergDeleteFile
            .newBuilder()
            .mergeFrom(deleteFiles(i))
            .setFilePathIdx(pathIdx))
      }
      builder.build().toByteArray
    }
  }
}

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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Partition that carries per-partition Iceberg FileScanTask bytes. This ensures only relevant
 * bytes are sent to each executor (not all partition bytes).
 */
private[spark] class CometIcebergPartition(override val index: Int, val taskBytes: Array[Byte])
    extends Partition {
  override def hashCode(): Int = index
  override def equals(obj: Any): Boolean = obj match {
    case other: CometIcebergPartition => other.index == index
    case _ => false
  }
}

/**
 * A RDD that executes Spark SQL query in Comet native execution to generate ColumnarBatch.
 */
private[spark] class CometExecRDD(
    sc: SparkContext,
    customPartitions: Array[Partition],
    var f: (
        Seq[Iterator[ColumnarBatch]],
        Int,
        Int,
        Option[Array[Byte]]) => Iterator[ColumnarBatch])
    extends RDD[ColumnarBatch](sc, Nil) {

  override def compute(s: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val taskBytes = s match {
      case p: CometIcebergPartition => Some(p.taskBytes)
      case _ => None
    }
    f(Seq.empty, customPartitions.length, s.index, taskBytes)
  }

  override protected def getPartitions: Array[Partition] = customPartitions
}

/**
 * Partition class for ZippedPartitionsWithIcebergRDD that combines multiple input RDD partitions
 * with per-partition Iceberg task data.
 */
private class ZippedPartitionsWithIcebergPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]],
    @transient private val preferredLocationsFunc: Seq[RDD[_]] => Seq[Partition])
    extends Partition {

  override val index: Int = idx
  var partitions: Seq[Partition] = preferredLocationsFunc(rdds)
}

/**
 * RDD that zips multiple input RDDs while also passing per-partition IcebergScan bytes. Used for
 * joins where one side is an IcebergScan and the other is broadcast/shuffle.
 *
 * This RDD combines the functionality of:
 *   - ZippedPartitionsRDD (zipping multiple input RDDs)
 *   - CometIcebergPartition (passing per-partition taskBytes)
 *
 * Each partition receives:
 *   - Iterators from all input RDDs (for broadcast/shuffle sides)
 *   - Per-partition taskBytes for IcebergScan(s)
 */
private[spark] class ZippedPartitionsWithIcebergRDD(
    sc: SparkContext,
    var rdds: Seq[RDD[ColumnarBatch]],
    perPartitionBytes: Array[Array[Byte]])(
    var f: (Seq[Iterator[ColumnarBatch]], Int, Int, Option[Array[Byte]]) => Iterator[
      ColumnarBatch])
    extends RDD[ColumnarBatch](sc, rdds.flatMap(_.dependencies)) {

  require(
    rdds.forall(_.getNumPartitions == perPartitionBytes.length),
    "All inputs and perPartitionBytes must have same partition count. " +
      s"Input partitions: ${rdds.map(_.getNumPartitions).mkString(", ")}, " +
      s"perPartitionBytes length: ${perPartitionBytes.length}")

  override def getPartitions: Array[Partition] = {
    val numParts = perPartitionBytes.length
    Array.tabulate(numParts) { idx =>
      new ZippedPartitionsWithIcebergPartition(idx, rdds, rdds => rdds.map(_.partitions(idx)))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[ZippedPartitionsWithIcebergPartition]
    val numPartitions = getNumPartitions

    // Get iterators from all input RDDs (like ZippedPartitionsRDD)
    val inputIterators = rdds.zipWithIndex.map { case (rdd, rddsIndex) =>
      rdd.iterator(partition.partitions(rddsIndex), context)
    }

    // Get per-partition taskBytes for this partition
    val taskBytes = perPartitionBytes(partition.index)

    // Create iterator with both inputs AND taskBytes
    f(inputIterators, numPartitions, partition.index, Some(taskBytes))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
    f = null
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[ZippedPartitionsWithIcebergPartition]
    // Prefer locations from first input (usually the larger side)
    rdds.head.preferredLocations(partition.partitions.head)
  }
}

object CometExecRDD {
  // For regular execution without per-partition bytes
  def apply(sc: SparkContext, partitionNum: Int)(
      f: (Seq[Iterator[ColumnarBatch]], Int, Int) => Iterator[ColumnarBatch])
      : RDD[ColumnarBatch] =
    withScope(sc) {
      val partitions = Array.tabulate(partitionNum)(i =>
        new Partition {
          override def index: Int = i
        })
      new CometExecRDD(sc, partitions, (inputs, numParts, idx, _) => f(inputs, numParts, idx))
    }

  // For execution with per-partition bytes (Iceberg scans)
  def apply(sc: SparkContext, partitionNum: Int, perPartitionBytes: Array[Array[Byte]])(
      f: (Seq[Iterator[ColumnarBatch]], Int, Int, Option[Array[Byte]]) => Iterator[ColumnarBatch])
      : RDD[ColumnarBatch] =
    withScope(sc) {
      val partitions: Array[Partition] = perPartitionBytes.zipWithIndex.map { case (bytes, idx) =>
        new CometIcebergPartition(idx, bytes): Partition
      }
      new CometExecRDD(sc, partitions, f)
    }

  private[spark] def withScope[U](sc: SparkContext)(body: => U): U =
    RDDOperationScope.withScope[U](sc)(body)
}

object ZippedPartitionsWithIcebergRDD {
  def apply(
      sc: SparkContext,
      rdds: Seq[RDD[ColumnarBatch]],
      perPartitionBytes: Array[Array[Byte]])(
      f: (Seq[Iterator[ColumnarBatch]], Int, Int, Option[Array[Byte]]) => Iterator[ColumnarBatch])
      : RDD[ColumnarBatch] =
    CometExecRDD.withScope(sc) {
      new ZippedPartitionsWithIcebergRDD(sc, rdds, perPartitionBytes)(f)
    }
}

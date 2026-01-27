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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometExecIterator

/**
 * Partition for IcebergScanRDD that carries partition-specific task bytes.
 *
 * Each partition knows its own Iceberg FileScanTasks (serialized as protobuf bytes), avoiding the
 * need to broadcast all tasks to all executors.
 *
 * @param index
 *   Partition index
 * @param taskBytes
 *   Serialized IcebergFileScanTask protobuf bytes for this partition only
 */
private[comet] class IcebergScanPartition(override val index: Int, val taskBytes: Array[Byte])
    extends Partition

/**
 * RDD for native Iceberg scans that avoids broadcasting all partition tasks to all executors.
 *
 * Traditional approach: Driver serializes ALL partition tasks into protobuf -> broadcast to ALL
 * executors -> each executor extracts its own partition tasks. For N partitions, each executor
 * receives N*task_size bytes but only uses 1*task_size bytes (99% waste for large N).
 *
 * Optimized approach: Each IcebergScanPartition carries only its own tasks. Spark's RDD
 * serialization ensures each executor only receives the partitions it needs. For N partitions,
 * each executor receives O(1) task data instead of O(N).
 *
 * @param sc
 *   SparkContext
 * @param numPartitions
 *   Number of partitions
 * @param partitionTasks
 *   Map from partition index to serialized task bytes for that partition
 * @param createIterator
 *   Function to create CometExecIterator for each partition
 */
private[comet] class IcebergScanRDD(
    @transient private val sc: SparkContext,
    numPartitions: Int,
    partitionTasks: Map[Int, Array[Byte]],
    createIterator: (Int, Array[Byte]) => CometExecIterator)
    extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { i =>
      val taskBytes = partitionTasks.getOrElse(
        i,
        throw new IllegalStateException(
          s"No tasks found for partition $i. " +
            s"Available partitions: ${partitionTasks.keys.mkString(", ")}"))
      new IcebergScanPartition(i, taskBytes)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[IcebergScanPartition]
    // Create iterator with partition-specific task bytes
    createIterator(partition.index, partition.taskBytes)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    // Iceberg handles data locality through its own planning
    Nil
  }
}

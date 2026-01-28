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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.IcebergFilePartition

/**
 * Custom partition for split Iceberg serialization. Holds only bytes for this partition's file
 * scan tasks.
 */
private[spark] class CometIcebergSplitPartition(
    override val index: Int,
    val partitionBytes: Array[Byte])
    extends Partition

/**
 * RDD for split Iceberg scan serialization that avoids sending all partition data to every task.
 *
 * With split serialization:
 *   - commonData: serialized IcebergScanCommon (pools, metadata) - captured in closure
 *   - perPartitionData: Array of serialized IcebergFilePartition - populates Partition objects
 *
 * Each task receives commonData (via closure) + partitionBytes (via Partition), combines them
 * into an IcebergScan with split_mode=true, and passes to native execution.
 */
private[spark] class CometIcebergSplitRDD(
    sc: SparkContext,
    commonData: Array[Byte],
    @transient perPartitionData: Array[Array[Byte]],
    numParts: Int,
    var computeFunc: (Array[Byte], CometMetricNode, Int, Int) => Iterator[ColumnarBatch])
    extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    perPartitionData.zipWithIndex.map { case (bytes, idx) =>
      new CometIcebergSplitPartition(idx, bytes)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometIcebergSplitPartition]

    val combinedPlan =
      CometIcebergSplitRDD.buildCombinedPlan(commonData, partition.partitionBytes)

    // Use cached numParts to avoid triggering getPartitions() on executor
    val it = computeFunc(combinedPlan, null, numParts, partition.index)

    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        it.asInstanceOf[CometExecIterator].close()
      }
    }

    it
  }
}

object CometIcebergSplitRDD {

  def apply(
      sc: SparkContext,
      commonData: Array[Byte],
      perPartitionData: Array[Array[Byte]],
      numOutputCols: Int,
      nativeMetrics: CometMetricNode): CometIcebergSplitRDD = {

    // Create compute function that captures nativeMetrics in its closure
    val computeFunc =
      (combinedPlan: Array[Byte], _: CometMetricNode, numParts: Int, partIndex: Int) => {
        new CometExecIterator(
          CometExec.newIterId,
          Seq.empty,
          numOutputCols,
          combinedPlan,
          nativeMetrics,
          numParts,
          partIndex,
          None,
          Seq.empty)
      }

    val numParts = perPartitionData.length
    new CometIcebergSplitRDD(sc, commonData, perPartitionData, numParts, computeFunc)
  }

  private[comet] def buildCombinedPlan(
      commonBytes: Array[Byte],
      partitionBytes: Array[Byte]): Array[Byte] = {
    val common = OperatorOuterClass.IcebergScanCommon.parseFrom(commonBytes)
    val partition = IcebergFilePartition.parseFrom(partitionBytes)

    val scanBuilder = OperatorOuterClass.IcebergScan.newBuilder()
    scanBuilder.setCommon(common)
    scanBuilder.setPartition(partition)

    val opBuilder = OperatorOuterClass.Operator.newBuilder()
    opBuilder.setIcebergScan(scanBuilder)

    opBuilder.build().toByteArray
  }
}

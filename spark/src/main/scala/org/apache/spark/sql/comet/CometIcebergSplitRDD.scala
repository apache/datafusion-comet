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

import org.apache.spark.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass

/**
 * Partition that carries only its own Iceberg data, avoiding closure capture of all partitions.
 */
private[spark] class CometIcebergSplitPartition(
    override val index: Int,
    val inputPartitions: Array[Partition],
    val icebergDataByLocation: Map[String, Array[Byte]])
    extends Partition

/**
 * RDD for Iceberg scan execution that efficiently distributes per-partition data.
 *
 * Solves the closure capture problem: instead of capturing all partitions' data in the closure
 * (which gets serialized to every task), each Partition object carries only its own data.
 *
 * Supports two execution modes:
 *   - Standalone: Iceberg scan is the root operator (no input RDDs)
 *   - Nested: Iceberg scan is under other native operators (has input RDDs)
 *
 * NOTE: This RDD does not handle ScalarSubquery expressions. DPP uses InSubqueryExec which is
 * resolved in CometIcebergNativeScanExec.splitData before this RDD is created.
 */
private[spark] class CometIcebergSplitRDD(
    sc: SparkContext,
    inputRDDs: Seq[RDD[ColumnarBatch]],
    commonByLocation: Map[String, Array[Byte]],
    @transient perPartitionByLocation: Map[String, Array[Array[Byte]]],
    serializedPlan: Array[Byte],
    numOutputCols: Int,
    nativeMetrics: CometMetricNode)
    extends RDD[ColumnarBatch](sc, Nil) {

  // Cache partition count to avoid accessing @transient field on executor
  private val numParts: Int =
    perPartitionByLocation.values.headOption.map(_.length).getOrElse(0)

  override protected def getPartitions: Array[Partition] = {
    (0 until numParts).map { idx =>
      val inputParts = inputRDDs.map(_.partitions(idx)).toArray
      val icebergData = perPartitionByLocation.map { case (loc, arr) => loc -> arr(idx) }
      new CometIcebergSplitPartition(idx, inputParts, icebergData)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometIcebergSplitPartition]

    val inputs = inputRDDs.zip(partition.inputPartitions).map { case (rdd, part) =>
      rdd.iterator(part, context)
    }

    val basePlan = OperatorOuterClass.Operator.parseFrom(serializedPlan)
    val injected = IcebergPartitionInjector.injectPartitionData(
      basePlan,
      commonByLocation,
      partition.icebergDataByLocation)
    val actualPlan = IcebergPartitionInjector.serializeOperator(injected)

    val it = new CometExecIterator(
      CometExec.newIterId,
      inputs,
      numOutputCols,
      actualPlan,
      nativeMetrics,
      numParts,
      partition.index,
      None,
      Seq.empty)

    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        it.close()
      }
    }

    it
  }

  override def getDependencies: Seq[Dependency[_]] =
    inputRDDs.map(rdd => new OneToOneDependency(rdd))
}

object CometIcebergSplitRDD {

  /**
   * Creates an RDD for standalone Iceberg scan (no parent native operators).
   */
  def apply(
      sc: SparkContext,
      commonData: Array[Byte],
      perPartitionData: Array[Array[Byte]],
      numOutputCols: Int,
      nativeMetrics: CometMetricNode): CometIcebergSplitRDD = {

    // Standalone mode needs a placeholder plan for IcebergPartitionInjector to fill in.
    // metadata_location must match what's in commonData for correct matching.
    val common = OperatorOuterClass.IcebergScanCommon.parseFrom(commonData)
    val metadataLocation = common.getMetadataLocation

    val placeholderCommon = OperatorOuterClass.IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .build()
    val placeholderScan = OperatorOuterClass.IcebergScan
      .newBuilder()
      .setCommon(placeholderCommon)
      .build()
    val placeholderPlan = OperatorOuterClass.Operator
      .newBuilder()
      .setIcebergScan(placeholderScan)
      .build()
      .toByteArray

    new CometIcebergSplitRDD(
      sc,
      inputRDDs = Seq.empty,
      commonByLocation = Map(metadataLocation -> commonData),
      perPartitionByLocation = Map(metadataLocation -> perPartitionData),
      serializedPlan = placeholderPlan,
      numOutputCols = numOutputCols,
      nativeMetrics = nativeMetrics)
  }

  /**
   * Creates an RDD for nested Iceberg scan (under other native operators).
   */
  def apply(
      sc: SparkContext,
      inputRDDs: Seq[RDD[ColumnarBatch]],
      commonByLocation: Map[String, Array[Byte]],
      perPartitionByLocation: Map[String, Array[Array[Byte]]],
      serializedPlan: Array[Byte],
      numOutputCols: Int,
      nativeMetrics: CometMetricNode): CometIcebergSplitRDD = {

    new CometIcebergSplitRDD(
      sc,
      inputRDDs,
      commonByLocation,
      perPartitionByLocation,
      serializedPlan,
      numOutputCols,
      nativeMetrics)
  }
}

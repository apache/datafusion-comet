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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass

/**
 * Partition that carries per-partition planning data, avoiding closure capture of all partitions.
 */
private[spark] class CometExecPartition(
    override val index: Int,
    val inputPartitions: Array[Partition],
    val planDataByKey: Map[String, Array[Byte]])
    extends Partition

/**
 * Unified RDD for Comet native execution.
 *
 * Solves the closure capture problem: instead of capturing all partitions' data in the closure
 * (which gets serialized to every task), each Partition object carries only its own data.
 *
 * Handles three cases:
 *   - With inputs + per-partition data: injects planning data into operator tree
 *   - With inputs + no per-partition data: just zips inputs (no injection overhead)
 *   - No inputs: uses numPartitions to create partitions
 *
 * NOTE: This RDD does not handle DPP (InSubqueryExec), which is resolved in
 * CometIcebergNativeScanExec.splitData before this RDD is created. However, it DOES handle
 * ScalarSubquery expressions by registering them with CometScalarSubquery before execution.
 */
private[spark] class CometExecRDD(
    sc: SparkContext,
    inputRDDs: Seq[RDD[ColumnarBatch]],
    commonByKey: Map[String, Array[Byte]],
    @transient perPartitionByKey: Map[String, Array[Array[Byte]]],
    serializedPlan: Array[Byte],
    numPartitions: Int,
    numOutputCols: Int,
    nativeMetrics: CometMetricNode,
    subqueries: Seq[ScalarSubquery],
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty)
    extends RDD[ColumnarBatch](sc, Nil) {

  // Determine partition count: from inputs if available, otherwise from parameter
  private val numParts: Int = if (inputRDDs.nonEmpty) {
    inputRDDs.head.partitions.length
  } else if (perPartitionByKey.nonEmpty) {
    perPartitionByKey.values.head.length
  } else {
    numPartitions
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until numParts).map { idx =>
      val inputParts = inputRDDs.map(_.partitions(idx)).toArray
      val planData = perPartitionByKey.map { case (key, arr) => key -> arr(idx) }
      new CometExecPartition(idx, inputParts, planData)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometExecPartition]

    val inputs = inputRDDs.zip(partition.inputPartitions).map { case (rdd, part) =>
      rdd.iterator(part, context)
    }

    // Only inject if we have per-partition planning data
    val actualPlan = if (commonByKey.nonEmpty) {
      val basePlan = OperatorOuterClass.Operator.parseFrom(serializedPlan)
      val injected =
        PlanDataInjector.injectPlanData(basePlan, commonByKey, partition.planDataByKey)
      PlanDataInjector.serializeOperator(injected)
    } else {
      serializedPlan
    }

    val it = new CometExecIterator(
      CometExec.newIterId,
      inputs,
      numOutputCols,
      actualPlan,
      nativeMetrics,
      numParts,
      partition.index,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths)

    // Register ScalarSubqueries so native code can look them up
    subqueries.foreach(sub => CometScalarSubquery.setSubquery(it.id, sub))

    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        it.close()
        subqueries.foreach(sub => CometScalarSubquery.removeSubquery(it.id, sub))
      }
    }

    it
  }

  override def getDependencies: Seq[Dependency[_]] =
    inputRDDs.map(rdd => new OneToOneDependency(rdd))

  // Duplicates logic from Spark's ZippedPartitionsBaseRDD.getPreferredLocations
  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (inputRDDs.isEmpty) return Nil

    val idx = split.index
    val prefs = inputRDDs.map(rdd => rdd.preferredLocations(rdd.partitions(idx)))
    // Prefer nodes where all inputs are local; fall back to any input's preferred location
    val intersection = prefs.reduce((a, b) => a.intersect(b))
    if (intersection.nonEmpty) intersection else prefs.flatten.distinct
  }
}

object CometExecRDD {

  /**
   * Creates an RDD for standalone Iceberg scan (no parent native operators).
   */
  def apply(
      sc: SparkContext,
      commonData: Array[Byte],
      perPartitionData: Array[Array[Byte]],
      numOutputCols: Int,
      nativeMetrics: CometMetricNode): CometExecRDD = {

    // Standalone mode needs a placeholder plan for PlanDataInjector to fill in.
    // PlanDataInjector correlates common/partition data by key (metadata_location for Iceberg).
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

    new CometExecRDD(
      sc,
      inputRDDs = Seq.empty,
      commonByKey = Map(metadataLocation -> commonData),
      perPartitionByKey = Map(metadataLocation -> perPartitionData),
      serializedPlan = placeholderPlan,
      numPartitions = perPartitionData.length,
      numOutputCols = numOutputCols,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty)
  }

  /**
   * Creates an RDD for native execution with optional per-partition planning data.
   */
  // scalastyle:off
  def apply(
      sc: SparkContext,
      inputRDDs: Seq[RDD[ColumnarBatch]],
      commonByKey: Map[String, Array[Byte]],
      perPartitionByKey: Map[String, Array[Array[Byte]]],
      serializedPlan: Array[Byte],
      numPartitions: Int,
      numOutputCols: Int,
      nativeMetrics: CometMetricNode,
      subqueries: Seq[ScalarSubquery],
      broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
      encryptedFilePaths: Seq[String] = Seq.empty): CometExecRDD = {
    // scalastyle:on

    new CometExecRDD(
      sc,
      inputRDDs,
      commonByKey,
      perPartitionByKey,
      serializedPlan,
      numPartitions,
      numOutputCols,
      nativeMetrics,
      subqueries,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths)
  }
}

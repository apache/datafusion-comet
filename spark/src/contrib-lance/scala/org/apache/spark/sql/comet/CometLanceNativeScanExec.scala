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

import scala.jdk.CollectionConverters._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometLanceNativeScan
import org.apache.comet.serde.operator.CometLanceNativeScan.LanceNativeScanDescriptor

case class CometLanceNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    runtimeFilters: Seq[Expression],
    @transient originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    override val sourceKey: String,
    lanceDescriptor: LanceNativeScanDescriptor)
    extends CometLeafExec
    with CometLanceNativeScanLike {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometLanceNativeScan"

  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) =
    CometLanceNativeScan.serializePartitions(lanceDescriptor)

  override def commonData: Array[Byte] = serializedPartitionData._1

  override def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(perPartitionData.length)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    new CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> perPartitionData),
      serializedPlan = serializedPlan,
      defaultNumPartitions = perPartitionData.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty) {
      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
        val res = super.compute(split, context)
        Option(context).foreach(nativeMetrics.reportScanInputMetrics)
        res
      }
    }
  }

  override def convertBlock(): CometLanceNativeScanExec = {
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      SerializedPlan(Some(CometExec.serializeNativePlan(nativeOp)))
    } else {
      serializedPlanOpt
    }

    CometLanceNativeScanExec(
      nativeOp,
      output,
      runtimeFilters,
      originalPlan,
      newSerializedPlan,
      sourceKey,
      lanceDescriptor)
  }

  override protected def doCanonicalize(): CometLanceNativeScanExec = {
    CometLanceNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(runtimeFilters),
        output),
      null,
      SerializedPlan(None),
      sourceKey,
      lanceDescriptor)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, s"$sourceKey, nativeScanPlan=${lanceDescriptor.nativeScanPlanClass}")

  override def equals(obj: Any): Boolean = obj match {
    case other: CometLanceNativeScanExec =>
      this.sourceKey == other.sourceKey &&
      this.output == other.output &&
      this.runtimeFilters == other.runtimeFilters &&
      this.serializedPlanOpt == other.serializedPlanOpt
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(sourceKey, output.asJava, runtimeFilters, serializedPlanOpt)
}

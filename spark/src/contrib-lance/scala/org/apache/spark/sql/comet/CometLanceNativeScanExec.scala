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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometLanceNativeScan

case class CometLanceNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    runtimeFilters: Seq[Expression],
    requiredSchema: StructType,
    @transient originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    override val sourceKey: String,
    nativeScanPlanClassName: String)
    extends CometLeafExec
    with CometLanceNativeScanLike {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometLanceNativeScan"

  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) =
    CometLanceNativeScan.serializePartitions(sourceKey, requiredSchema, nativeScanPlanClassName)

  override def commonData: Array[Byte] = serializedPartitionData._1

  override def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(perPartitionData.length)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(
      "Native Lance scan execution is not implemented yet")
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
      requiredSchema,
      originalPlan,
      newSerializedPlan,
      sourceKey,
      nativeScanPlanClassName)
  }

  override protected def doCanonicalize(): CometLanceNativeScanExec = {
    CometLanceNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(runtimeFilters),
        output),
      requiredSchema,
      null,
      SerializedPlan(None),
      sourceKey,
      nativeScanPlanClassName)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, s"$sourceKey, nativeScanPlan=$nativeScanPlanClassName")

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

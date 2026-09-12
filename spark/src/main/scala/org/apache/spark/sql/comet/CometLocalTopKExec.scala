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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

import com.google.common.base.Objects

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.{Operator, Sort}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometLocalTopKExec {

  /** Insert the local selection before native blocks are serialized, so it owns the scan. */
  def create(op: TakeOrderedAndProjectExec): Option[CometLocalTopKExec] = {
    if (!CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.get(op.conf) || op.limit <= 0 ||
      SortOrder.orderingSatisfies(op.child.outputOrdering, op.sortOrder)) {
      return None
    }

    // Start with the reader-filter path: one direct signed integer column in a native scan.
    // Other plans retain the existing TopKInput execution path.
    (op.child, op.sortOrder) match {
      case (scan: CometNativeScanExec, Seq(order))
          if order.child.isInstanceOf[Attribute] && (order.dataType match {
            case ByteType | ShortType | IntegerType | LongType => true
            case _ => false
          }) =>
        exprToProto(order, scan.output).map { sortOrder =>
          // Spark's physical limit already includes the offset. Each partition retains that
          // many candidates; only the final TopK applies the offset after the shuffle.
          val dynamicFilterEnabled = CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.get(op.conf)
          val sort = Sort
            .newBuilder()
            .addSortOrders(sortOrder)
            .setFetch(op.limit)
            .setSkip(0)
            .setDynamicFilterEnabled(dynamicFilterEnabled)
          val nativeOp = Operator
            .newBuilder()
            .setPlanId(op.id)
            .setSort(sort)
            .addChildren(scan.nativeOp)
            .build()
          CometLocalTopKExec(
            nativeOp,
            op,
            scan.output,
            op.limit,
            op.sortOrder,
            dynamicFilterEnabled,
            scan,
            SerializedPlan(None))
        }
      case _ => None
    }
  }
}

/** Selects candidates within one Spark partition in the same native execution as its scan. */
case class CometLocalTopKExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    limit: Int,
    sortOrder: Seq[SortOrder],
    dynamicFilterEnabled: Boolean,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(limit, sortOrder, dynamicFilterEnabled, output, child)

  override def equals(obj: Any): Boolean = obj match {
    case other: CometLocalTopKExec =>
      output == other.output && limit == other.limit && sortOrder == other.sortOrder &&
      dynamicFilterEnabled == other.dynamicFilterEnabled && child == other.child &&
      serializedPlanOpt == other.serializedPlanOpt
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(output, Int.box(limit), sortOrder, Boolean.box(dynamicFilterEnabled), child)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext) ++ Map(
      "dynamic_filter_reader_filters_attached" ->
        SQLMetrics.createMetric(sparkContext, "TopK reader filters attached"),
      "dynamic_filter_reader_filters_skipped" ->
        SQLMetrics.createMetric(sparkContext, "TopK reader filters skipped"),
      "spill_count" -> SQLMetrics.createMetric(sparkContext, "number of spills"),
      "spilled_bytes" -> SQLMetrics.createSizeMetric(sparkContext, "total spilled bytes"))
}

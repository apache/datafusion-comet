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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.{Operator, RankLikeFunction}
import org.apache.comet.serde.QueryPlanSerde.exprToProto
import org.apache.comet.shims.ShimCometWindowGroupLimit

/**
 * Serde for Spark's `WindowGroupLimitExec` (Spark 3.5+, SPARK-37099). Handles ROW_NUMBER, RANK,
 * and DENSE_RANK natively. ROW_NUMBER without PARTITION BY collapses to a `LocalLimitExec` over
 * the Spark-sorted child. Every other combination (ROW_NUMBER partitioned, RANK/DENSE_RANK with
 * or without PARTITION BY) maps onto Comet's streaming `PartitionedRankLimitExec`.
 *
 * The Scala type parameter is `SparkPlan` (not `WindowGroupLimitExec`) so this file stays
 * compilable against Spark 3.4, where the exec class does not exist. Field extraction is
 * delegated to the per-Spark-minor `ShimCometWindowGroupLimit`.
 */
object CometWindowGroupLimitExec extends CometOperatorSerde[SparkPlan] {

  /** Fields extracted from a Spark `WindowGroupLimitExec` (Spark 3.5+). */
  case class Fields(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      rankLikeFunction: RankLikeFunction,
      limit: Int)

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED)

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // `nativeExecs` only routes here for a real `WindowGroupLimitExec` on Spark 3.5+, so the
    // shim always returns Some.
    val fields = ShimCometWindowGroupLimit.extract(op).get

    if (fields.limit <= 0) {
      // Spark's optimizer collapses limit <= 0 to an empty LocalRelation, but guard anyway.
      withFallbackReason(op, s"WindowGroupLimit: non-positive limit ${fields.limit}")
      return None
    }

    val childOutput = op.children.head.output
    val partitionProtos = fields.partitionSpec.map(e => e -> exprToProto(e, childOutput))
    val orderProtos = fields.orderSpec.map(e => e -> exprToProto(e, childOutput))

    val failing = (partitionProtos ++ orderProtos).collect { case (e, None) => e }
    if (failing.nonEmpty) {
      withFallbackReason(
        op,
        failing.map(_.sql).mkString("WindowGroupLimit: unsupported expressions: ", ", ", ""))
      return None
    }

    val wglBuilder = OperatorOuterClass.WindowGroupLimit
      .newBuilder()
      .setLimit(fields.limit)
      .setRankLikeFunction(fields.rankLikeFunction)
    wglBuilder.addAllPartitionByList(partitionProtos.map(_._2.get).asJava)
    wglBuilder.addAllOrderByList(orderProtos.map(_._2.get).asJava)
    Some(builder.setWindowGroupLimit(wglBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec = {
    val fields = ShimCometWindowGroupLimit
      .extract(op)
      .getOrElse(
        throw new IllegalStateException(
          "createExec called on a non-WindowGroupLimitExec operator: " + op.nodeName))
    CometWindowGroupLimitExec(
      nativeOp,
      op,
      op.output,
      fields.partitionSpec,
      fields.orderSpec,
      fields.limit,
      op.children.head,
      SerializedPlan(None))
  }
}

/**
 * Comet physical plan node for Spark `WindowGroupLimitExec`. The Spark Partial/Final split is
 * preserved unchanged in the Spark plan tree (each side is planned as its own native subtree), so
 * the case class doesn't carry a mode field.
 */
case class CometWindowGroupLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    limit: Int,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def nodeName: String = "CometWindowGroupLimitExec"

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(output, partitionSpec, orderSpec, limit, child)

  override def equals(obj: Any): Boolean = obj match {
    case other: CometWindowGroupLimitExec =>
      this.output == other.output &&
      this.partitionSpec == other.partitionSpec &&
      this.orderSpec == other.orderSpec &&
      this.limit == other.limit &&
      this.child == other.child &&
      this.serializedPlanOpt == other.serializedPlanOpt
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(output, partitionSpec, orderSpec, Integer.valueOf(limit), child)
}

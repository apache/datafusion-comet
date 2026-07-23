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
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto
import org.apache.comet.shims.ShimCometWindowGroupLimit

/**
 * Serde for Spark's `WindowGroupLimitExec` (Spark 3.5+, SPARK-37099). Handles the ROW_NUMBER and
 * RANK pushdown cases with a non-empty PARTITION BY -- ROW_NUMBER maps onto DataFusion's
 * `PartitionedTopKExec`, RANK maps onto Comet's streaming `PartitionedRankLimitExec`. DENSE_RANK
 * and empty PARTITION BY are rejected here and fall back to Spark; add a native implementation
 * before enabling them.
 *
 * The Scala type parameter is `SparkPlan` (not `WindowGroupLimitExec`) so this file stays
 * compilable against Spark 3.4, where the exec class does not exist. Field extraction is
 * delegated to the per-Spark-minor `ShimCometWindowGroupLimit`.
 */
object CometWindowGroupLimitExec extends CometOperatorSerde[SparkPlan] {
  import CometWindowGroupLimit._

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED)

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val fields = ShimCometWindowGroupLimit.extract(op).getOrElse {
      // Defensive: nativeExecs only routes here for a real WindowGroupLimitExec on Spark 3.5+.
      withFallbackReason(op, "WindowGroupLimit: unexpected operator shape")
      return None
    }

    // DENSE_RANK and empty (global) PARTITION BY are still Spark-only. RANK is native via the
    // streaming operator, which relies on the Spark-injected `[partition, order]` sort.
    val rankType = fields.rankLikeKind match {
      case RowNumberKind => OperatorOuterClass.WindowGroupLimit.RankType.ROW_NUMBER
      case RankKind => OperatorOuterClass.WindowGroupLimit.RankType.RANK
      case DenseRankKind =>
        withFallbackReason(op, "WindowGroupLimit: DENSE_RANK not yet supported")
        return None
    }
    if (fields.partitionSpec.isEmpty) {
      withFallbackReason(
        op,
        "WindowGroupLimit: empty PARTITION BY not yet supported (global top-K)")
      return None
    }
    if (fields.limit <= 0) {
      // Spark's optimizer collapses limit <= 0 to an empty LocalRelation, but guard anyway.
      withFallbackReason(op, s"WindowGroupLimit: non-positive limit ${fields.limit}")
      return None
    }

    val childOutput = op.children.head.output
    val partitionExprs = fields.partitionSpec.map(exprToProto(_, childOutput))
    val orderExprs = fields.orderSpec.map(exprToProto(_, childOutput))

    if (partitionExprs.forall(_.isDefined) && orderExprs.forall(_.isDefined)) {
      val wglBuilder = OperatorOuterClass.WindowGroupLimit
        .newBuilder()
        .setLimit(fields.limit)
        .setRankType(rankType)
      wglBuilder.addAllPartitionByList(partitionExprs.map(_.get).asJava)
      wglBuilder.addAllOrderByList(orderExprs.map(_.get).asJava)
      Some(builder.setWindowGroupLimit(wglBuilder).build())
    } else {
      val failing =
        fields.partitionSpec.zip(partitionExprs).collect { case (e, None) => e } ++
          fields.orderSpec.zip(orderExprs).collect { case (e, None) => e }
      withFallbackReason(op, failing: _*)
      None
    }
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
 * Comet physical plan node for Spark `WindowGroupLimitExec`. Executes as DataFusion's
 * `PartitionedTopKExec`; the Spark Partial/Final split is preserved unchanged in the Spark plan
 * tree (each side is planned as its own native subtree), so the case class doesn't carry a mode
 * field.
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

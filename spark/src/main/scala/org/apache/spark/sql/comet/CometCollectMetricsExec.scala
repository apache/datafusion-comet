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

import java.util.Objects

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.{CollectMetricsExec, SparkPlan}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometCollectMetricsExec extends CometOperatorSerde[CollectMetricsExec] {
  def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_COLLECT_METRICS_ENABLED)

  private def extractAggregates(
      metricExpressions: Seq[NamedExpression]): Seq[AggregateExpression] = {
    metricExpressions
      .flatMap(_.collect { case agg: AggregateExpression => agg })
      .distinct
  }

  private def rewriteMetricExpressions(
      metricExpressions: Seq[NamedExpression],
      aggExpressions: Seq[AggregateExpression]): (Seq[AttributeReference], Seq[Expression]) = {
    val aggMap =
      aggExpressions.zipWithIndex.map { case (agg, idx) =>
        agg -> AttributeReference(s"agg_$idx", agg.dataType, agg.nullable)()
      }.toMap
    val aggAttributes = aggExpressions.map(aggMap)
    val rewritten = metricExpressions.map { expr =>
      expr.transformDown { case agg: AggregateExpression =>
        aggMap.getOrElse(agg, agg)
      }
    }
    (aggAttributes, rewritten)
  }

  def convert(
      op: CollectMetricsExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val aggExprs = extractAggregates(op.metricExpressions)
    val aggExprsProtos = aggExprs.flatMap(
      QueryPlanSerde.aggExprToProto(_, op.child.output, binding = false, op.conf))
    if (aggExprsProtos.length != aggExprs.length) {
      return None
    }
    val (aggAttributes, rewrittenExprs) = rewriteMetricExpressions(op.metricExpressions, aggExprs)
    val metricExprProtos = rewrittenExprs.flatMap(QueryPlanSerde.exprToProto(_, aggAttributes))
    if (metricExprProtos.length != rewrittenExprs.length) {
      return None
    }
    val collectMetricsProto = OperatorOuterClass.CollectMetrics
      .newBuilder()
      .setMetricName(op.name)
      .addAllAggExprs(aggExprsProtos.asJava)
      .addAllMetricExprs(metricExprProtos.asJava)
      .build()
    Some(
      builder
        .setCollectMetrics(collectMetricsProto)
        .build())
  }

  def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: CollectMetricsExec): CometNativeExec = {
    CometCollectMetricsExec(
      nativeOp,
      op,
      op.name,
      op.metricExpressions,
      op.child,
      SerializedPlan(None))
  }
}

case class CometCollectMetricsExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    name: String,
    metricExpressions: Seq[NamedExpression],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = s"CometCollectMetrics $name"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometCollectMetricsExec =>
        this.name == other.name &&
        this.metricExpressions == other.metricExpressions &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(name, metricExpressions, child)
  }
}

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

package org.apache.comet.serde

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HllUnionAgg}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.exprToProto

// IMPORTANT: In Spark 4.0, HllUnionAgg's fields are `left` (the child binary sketch
// expression) and `right` (the allowDifferentLgConfigK boolean expression) - NOT
// `child`/`allowDifferentLgConfigKExpression`. (Verified against Spark 4.0 source.)
object CometHllUnionAgg extends CometAggregateExpressionSerde[HllUnionAgg] {

  private val incompatReason =
    "Comet uses a Rust DataSketches port; HLL sketch bytes and estimates may differ slightly " +
      "from Spark."

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: HllUnionAgg): SupportLevel = {
    if (!expr.right.foldable) {
      Unsupported(Some("The allowDifferentLgConfigK argument must be a foldable literal."))
    } else {
      Incompatible(Some(incompatReason))
    }
  }

  override def convert(
      aggExpr: AggregateExpression,
      expr: HllUnionAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val childExpr = exprToProto(expr.left, inputs, binding)
    val allow = expr.right.eval() match {
      case b: Boolean => b
      case other =>
        withFallbackReason(
          aggExpr,
          s"Unsupported allowDifferentLgConfigK literal: $other",
          expr.left)
        return None
    }
    if (childExpr.isDefined) {
      val builder = ExprOuterClass.HllUnionAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setAllowDifferentLgConfigK(allow)
      Some(ExprOuterClass.AggExpr.newBuilder().setHllUnionAgg(builder).build())
    } else {
      withFallbackReason(aggExpr, expr.left)
      None
    }
  }
}

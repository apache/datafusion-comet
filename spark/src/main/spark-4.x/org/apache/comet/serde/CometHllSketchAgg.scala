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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HllSketchAgg}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.exprToProto

// In Spark 4.0, HllSketchAgg's fields are `left` (the child value expression) and `right` (the
// lgConfigK expression), not `child`/`lgConfigKExpression`. Accepted input types are Integer,
// Long, String, and Binary (Byte/Short are not accepted by Spark's HllSketchAgg).
object CometHllSketchAgg extends CometAggregateExpressionSerde[HllSketchAgg] {

  // DataSketches valid lgConfigK range; outside this Spark itself throws, so we
  // fall back rather than forward an out-of-range value to native.
  private val MinLgConfigK = 4
  private val MaxLgConfigK = 21

  private val nonLiteralLgConfigKReason =
    "The lgConfigK argument must be a foldable literal."
  private val inputTypeReason =
    "Only int, long, string, and binary input types are supported."
  private val incompatReason =
    "Comet uses a Rust DataSketches port; HLL sketch bytes and estimates may differ " +
      "slightly from Spark."

  override def getUnsupportedReasons(): Seq[String] =
    Seq(nonLiteralLgConfigKReason, inputTypeReason)

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: HllSketchAgg): SupportLevel = {
    if (!expr.right.foldable) {
      return Unsupported(Some(nonLiteralLgConfigKReason))
    }
    val lgConfigK = expr.right.eval() match {
      case i: Int => i
      case l: Long => l.toInt
      case _ => return Unsupported(Some(nonLiteralLgConfigKReason))
    }
    if (lgConfigK < MinLgConfigK || lgConfigK > MaxLgConfigK) {
      return Unsupported(Some(s"lgConfigK must be in [$MinLgConfigK, $MaxLgConfigK]"))
    }
    expr.left.dataType match {
      case IntegerType | LongType | StringType | BinaryType =>
        Incompatible(Some(incompatReason))
      case _ => Unsupported(Some(inputTypeReason))
    }
  }

  override def convert(
      aggExpr: AggregateExpression,
      expr: HllSketchAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val childExpr = exprToProto(expr.left, inputs, binding)
    val lgConfigK = expr.right.eval() match {
      case i: Int => i
      case l: Long => l.toInt
      case other =>
        withFallbackReason(aggExpr, s"Unsupported lgConfigK literal: $other", expr.left)
        return None
    }
    if (childExpr.isDefined) {
      val builder = ExprOuterClass.HllSketchAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setLgConfigK(lgConfigK)
      Some(ExprOuterClass.AggExpr.newBuilder().setHllSketchAgg(builder).build())
    } else {
      withFallbackReason(aggExpr, expr.left)
      None
    }
  }
}

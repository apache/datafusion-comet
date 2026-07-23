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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ListAgg}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{NullType, StringType}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, hasNonDefaultStringCollation}

/**
 * Spark 4.0+ `LISTAGG` / `STRING_AGG`.
 *
 * Comet only supports the simple form: a `StringType` child with a literal delimiter and no
 * `WITHIN GROUP (ORDER BY ...)` clause. DISTINCT is handled by Spark's multi-stage plan rewrite
 * (grouping by the child before the aggregate), so the native side never sees it.
 */
object CometListAgg extends CometAggregateExpressionSerde[ListAgg] {

  private val binaryChildReason = "`BinaryType` inputs are not supported."
  private val withinGroupReason = "`WITHIN GROUP (ORDER BY ...)` is not supported."
  private val nonFoldableDelimiterReason = "Non-literal delimiters are not supported."
  private val collationReason = "Non-default string collations are not supported."

  // Note: DISTINCT is not listed here. It never reaches this serde: Comet's aggregate
  // dispatcher rejects multi-column distinct aggregates earlier in `aggExprToProto`, which
  // emits its own fallback reason.
  override def getUnsupportedReasons(): Seq[String] =
    Seq(binaryChildReason, withinGroupReason, nonFoldableDelimiterReason, collationReason)

  override def getSupportLevel(expr: ListAgg): SupportLevel = {
    // Spark's analyzer already enforces `delimiter.foldable`, so this only ever rejects
    // non-string / non-null delimiter types.
    //
    // The collation guards below are defense-in-depth: today a collated-string column falls
    // back earlier at the Comet scan (which rejects a `StringType(<collation>)` schema), so
    // this branch is not reachable via a query. It keeps `listagg` correct if Comet later
    // gains collated-scan support.
    expr.child.dataType match {
      case _: StringType if hasNonDefaultStringCollation(expr.child.dataType) =>
        Unsupported(Some(collationReason))
      case _: StringType =>
        expr.delimiter.dataType match {
          case _: StringType if hasNonDefaultStringCollation(expr.delimiter.dataType) =>
            Unsupported(Some(collationReason))
          case _: StringType | _: NullType =>
            if (!expr.delimiter.foldable) {
              Unsupported(Some(nonFoldableDelimiterReason))
            } else if (expr.orderExpressions.nonEmpty) {
              Unsupported(Some(withinGroupReason))
            } else {
              Compatible()
            }
          case other =>
            Unsupported(Some(s"Unsupported delimiter data type: $other"))
        }
      case other =>
        Unsupported(Some(s"Unsupported child data type: $other"))
    }
  }

  override def convert(
      aggExpr: AggregateExpression,
      expr: ListAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val childExpr = exprToProto(expr.child, inputs, binding)
    val delimiterExpr = exprToProto(expr.delimiter, inputs, binding)

    if (childExpr.isDefined && delimiterExpr.isDefined) {
      val builder = ExprOuterClass.ListAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDelimiter(delimiterExpr.get)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setListAgg(builder)
          .build())
    } else {
      withFallbackReason(aggExpr, expr.child, expr.delimiter)
      None
    }
  }
}

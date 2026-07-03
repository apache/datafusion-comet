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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ListAgg}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{NullType, StringType}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, hasNonDefaultStringCollation, serializeDataType}

/**
 * Spark 4.0+ `LISTAGG` / `STRING_AGG`.
 *
 * Comet only supports the simple form: a `StringType` child with a literal delimiter and no
 * `WITHIN GROUP (ORDER BY ...)` clause. DISTINCT is handled by Spark's multi-stage plan rewrite
 * (grouping by the child before the aggregate), so the native side never sees it.
 */
object CometListAgg extends CometAggregateExpressionSerde[ListAgg] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "`BinaryType` inputs are not supported.",
    "`WITHIN GROUP (ORDER BY ...)` is not supported.",
    "Non-literal delimiters are not supported.",
    "Non-default string collations are not supported.",
    "`DISTINCT` falls back to Spark because Comet rejects multi-column distinct aggregates.")

  override def getSupportLevel(expr: ListAgg): SupportLevel = {
    // Spark enforces `delimiter.foldable` at analysis time, so a non-literal delimiter would
    // fail before reaching us. Match only the two shapes we actually handle.
    if (!expr.child.dataType.isInstanceOf[StringType]) {
      return Unsupported(Some(s"Unsupported child data type: ${expr.child.dataType}"))
    }
    if (hasNonDefaultStringCollation(expr.child.dataType)) {
      return Unsupported(Some("Non-default string collations are not supported"))
    }
    expr.delimiter.dataType match {
      case _: StringType if hasNonDefaultStringCollation(expr.delimiter.dataType) =>
        return Unsupported(Some("Non-default string collations on delimiter are not supported"))
      case _: StringType | _: NullType => // ok
      case other =>
        return Unsupported(Some(s"Unsupported delimiter data type: $other"))
    }
    expr.delimiter match {
      case _: Literal => // ok
      case _ => return Unsupported(Some("Non-literal delimiters are not supported"))
    }
    if (expr.orderExpressions.nonEmpty) {
      return Unsupported(Some("`WITHIN GROUP (ORDER BY ...)` is not supported"))
    }
    Compatible()
  }

  override def convert(
      aggExpr: AggregateExpression,
      expr: ListAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val childExpr = exprToProto(expr.child, inputs, binding)
    val delimiterExpr = exprToProto(expr.delimiter, inputs, binding)
    val dataType = serializeDataType(expr.dataType)

    if (childExpr.isDefined && delimiterExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.ListAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDelimiter(delimiterExpr.get)
      builder.setDatatype(dataType.get)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setListAgg(builder)
          .build())
    } else if (dataType.isEmpty) {
      withFallbackReason(aggExpr, s"datatype ${expr.dataType} is not supported", expr.child)
      None
    } else {
      withFallbackReason(aggExpr, expr.child, expr.delimiter)
      None
    }
  }
}

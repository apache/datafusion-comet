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

import org.apache.spark.sql.catalyst.expressions.{Attribute, FromUnixTime, Literal}
import org.apache.spark.sql.catalyst.util.TimestampFormatter

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProto}

// TODO: DataFusion supports only -8334601211038 <= sec <= 8210266876799
// https://github.com/apache/datafusion/issues/16594
object CometFromUnixTime extends CometExpressionSerde[FromUnixTime] with CodegenDispatchFallback {

  private val collationReason = DatetimeCollation.reason("from_unixtime")

  // Applies even to the default format: Comet executes natively but DataFusion's valid timestamp
  // range differs from Spark, so results can differ outside that range.
  private val timestampRangeReason =
    "DataFusion's valid timestamp range differs from Spark" +
      " (https://github.com/apache/datafusion/issues/16594)"

  // The native (DataFusion) path covers only the default pattern; documented as an unsupported
  // limitation of that path rather than an incompatibility (see #4575).
  private val formatReason =
    "Only the default datetime format pattern `yyyy-MM-dd HH:mm:ss` is supported"

  override def getIncompatibleReasons(): Seq[String] =
    Seq(timestampRangeReason) ++ DatetimeCollation.incompatibleReasons("from_unixtime")

  override def getUnsupportedReasons(): Seq[String] =
    Seq(formatReason)

  // A non-default format has no native (DataFusion) path, so it is `Unsupported`. Because
  // `CodegenDispatchFallback` is mixed in, an `Unsupported` result still stays in the Comet
  // pipeline via JVM codegen dispatch (Spark's own `doGenCode`) rather than falling back to Spark.
  override def getSupportLevel(expr: FromUnixTime): SupportLevel = {
    if (expr.format != Literal(TimestampFormatter.defaultPattern)) {
      Unsupported(Some(formatReason))
    } else if (DatetimeCollation.hasNonDefaultCollation(expr)) {
      Incompatible(Some(collationReason))
    } else {
      Incompatible(Some(timestampRangeReason))
    }
  }

  override def convert(
      expr: FromUnixTime,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val secExpr = exprToProtoInternal(expr.sec, inputs, binding)
    // TODO: DataFusion toChar does not support Spark datetime pattern format
    // https://github.com/apache/datafusion/issues/16577
    // https://github.com/apache/datafusion/issues/14536
    // After fixing these issues, use provided `format` instead of the manual replacement below
    val formatExpr = exprToProtoInternal(Literal("%Y-%m-%d %H:%M:%S"), inputs, binding)
    val timeZone = exprToProtoInternal(Literal(expr.timeZoneId.orNull), inputs, binding)

    if (expr.format != Literal(TimestampFormatter.defaultPattern)) {
      withFallbackReason(expr, formatReason)
      None
    } else if (secExpr.isDefined && formatExpr.isDefined) {
      val timestampExpr =
        scalarFunctionExprToProto("from_unixtime", Seq(secExpr, timeZone): _*)
      val optExpr = scalarFunctionExprToProto("to_char", Seq(timestampExpr, formatExpr): _*)
      optExprWithFallbackReason(optExpr, expr, expr.sec, expr.format)
    } else {
      withFallbackReason(expr, expr.sec, expr.format)
      None
    }
  }
}

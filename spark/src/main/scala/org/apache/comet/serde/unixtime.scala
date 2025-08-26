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

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

// TODO: DataFusion supports only -8334601211038 <= sec <= 8210266876799
// https://github.com/apache/datafusion/issues/16594
object CometFromUnixTime
    extends CometExpressionSerde[FromUnixTime]
    with IncompatExpr[FromUnixTime] {
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
      withInfo(expr, "Datetime pattern format is unsupported")
      None
    } else if (secExpr.isDefined && formatExpr.isDefined) {
      val timestampExpr =
        scalarFunctionExprToProto("from_unixtime", Seq(secExpr, timeZone): _*)
      val optExpr = scalarFunctionExprToProto("to_char", Seq(timestampExpr, formatExpr): _*)
      optExprWithInfo(optExpr, expr, expr.sec, expr.format)
    } else {
      withInfo(expr, expr.sec, expr.format)
      None
    }
  }
}

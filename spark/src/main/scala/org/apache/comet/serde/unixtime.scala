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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, FromUnixTime, Literal}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types.StringType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.QueryPlanSerde.{castToProto, exprToProtoInternal, scalarFunctionExprToProto}

object CometFromUnixTime extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    expr match {
      case FromUnixTime(sec, format, timeZoneId) =>
        val secExpr = exprToProtoInternal(sec, inputs, binding)
        val formatExpr = exprToProtoInternal(format, inputs, binding)
        val timeZone = exprToProtoInternal(Literal(timeZoneId.orNull), inputs, binding)

        // TODO: DataFusion toChar does not support Spark format
        // https://github.com/apache/datafusion/issues/16577
        // https://github.com/apache/datafusion/issues/14536
        // TODO: DataFusion supports only -8334601211038 <= sec <= 8210266876799
        // https://github.com/apache/datafusion/issues/16594
        // if (secExpr.isDefined && formatExpr.isDefined) {
        //   val timestampExpr =
        //     scalarFunctionExprToProto("from_unixtime", Seq(secExpr, timeZone): _*)
        //   val optExpr = scalarFunctionExprToProto("to_char", Seq(timestampExpr, formatExpr): _*)
        //   optExprWithInfo(optExpr, expr, sec, format)
        if (secExpr.isDefined && formatExpr.isDefined && format == Literal(
            TimestampFormatter.defaultPattern)) {
          val optExpr = scalarFunctionExprToProto("from_unixtime", Seq(secExpr, timeZone): _*)
          castToProto(expr, timeZoneId, StringType, optExpr.get, CometEvalMode.LEGACY)
        } else {
          withInfo(expr, sec, format)
          None
        }
    }
  }
}

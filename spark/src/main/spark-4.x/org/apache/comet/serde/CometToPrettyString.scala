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

import org.apache.spark.sql.catalyst.expressions.{Attribute, ToPrettyString}
import org.apache.spark.sql.types.DataTypes

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.QueryPlanSerde.{binaryOutputStyle, exprToProtoInternal}

object CometToPrettyString extends CometExpressionSerde[ToPrettyString] {

  override def getUnsupportedReasons(): Seq[String] =
    Seq("Falls back to Spark when the input type cannot be cast to string.")

  override def getSupportLevel(expr: ToPrettyString): SupportLevel = {
    CometCast.isSupported(
      expr.child.dataType,
      DataTypes.StringType,
      expr.timeZoneId,
      CometEvalMode.TRY) match {
      case Compatible(_) | Incompatible(_) => Compatible(None)
      case Unsupported(reason) =>
        Unsupported(Some(s"Cast to string is unsupported: ${reason.getOrElse("")}"))
    }
  }

  override def convert(
      expr: ToPrettyString,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    exprToProtoInternal(expr.child, inputs, binding) match {
      case Some(p) =>
        val tps = ExprOuterClass.ToPrettyString
          .newBuilder()
          .setChild(p)
          .setTimezone(expr.timeZoneId.getOrElse("UTC"))
          .setBinaryOutputStyle(binaryOutputStyle)
          .build()
        Some(ExprOuterClass.Expr.newBuilder().setToPrettyString(tps).build())
      case _ =>
        withInfo(expr, expr.child)
        None
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Attribute, DateAdd, DateSub, Expression, Hour, Literal, Minute, Second, TruncDate, TruncTimestamp, Year}
import org.apache.spark.sql.types.{DateType, IntegerType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType, serializeDataType}

object CometYear extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val year = expr.asInstanceOf[Year]
    val periodType = exprToProtoInternal(Literal("year"), inputs, binding)
    val childExpr = exprToProtoInternal(year.child, inputs, binding)
    val optExpr = scalarFunctionExprToProto("datepart", Seq(periodType, childExpr): _*)
      .map(e => {
        Expr
          .newBuilder()
          .setCast(
            ExprOuterClass.Cast
              .newBuilder()
              .setChild(e)
              .setDatatype(serializeDataType(IntegerType).get)
              .setEvalMode(ExprOuterClass.EvalMode.LEGACY)
              .setAllowIncompat(false)
              .build())
          .build()
      })
    optExprWithInfo(optExpr, expr, year.child)
  }
}

object CometHour extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val hour = expr.asInstanceOf[Hour]
    val childExpr = exprToProtoInternal(hour.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Hour.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = hour.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setHour(builder)
          .build())
    } else {
      withInfo(expr, hour.child)
      None
    }
  }
}

object CometMinute extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val minute = expr.asInstanceOf[Minute]
    val childExpr = exprToProtoInternal(minute.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Minute.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = minute.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setMinute(builder)
          .build())
    } else {
      withInfo(expr, minute.child)
      None
    }
  }
}

object CometSecond extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val second = expr.asInstanceOf[Second]
    val childExpr = exprToProtoInternal(second.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Second.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = second.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setSecond(builder)
          .build())
    } else {
      withInfo(expr, second.child)
      None
    }
  }
}

object CometDateAdd extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val dateAdd = expr.asInstanceOf[DateAdd]
    val leftExpr = exprToProtoInternal(dateAdd.left, inputs, binding)
    val rightExpr = exprToProtoInternal(dateAdd.right, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_add", DateType, leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, dateAdd.left, dateAdd.right)
  }
}

object CometDateSub extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val dateSub = expr.asInstanceOf[DateSub]
    val leftExpr = exprToProtoInternal(dateSub.left, inputs, binding)
    val rightExpr = exprToProtoInternal(dateSub.right, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_sub", DateType, leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, dateSub.left, dateSub.right)
  }
}

object CometTruncDate extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val truncDate = expr.asInstanceOf[TruncDate]
    val childExpr = exprToProtoInternal(truncDate.date, inputs, binding)
    val formatExpr = exprToProtoInternal(truncDate.format, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_trunc", DateType, childExpr, formatExpr)
    optExprWithInfo(optExpr, expr, truncDate.date, truncDate.format)
  }
}

object CometTruncTimestamp extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val truncTimestamp = expr.asInstanceOf[TruncTimestamp]
    val childExpr = exprToProtoInternal(truncTimestamp.timestamp, inputs, binding)
    val formatExpr = exprToProtoInternal(truncTimestamp.format, inputs, binding)

    if (childExpr.isDefined && formatExpr.isDefined) {
      val builder = ExprOuterClass.TruncTimestamp.newBuilder()
      builder.setChild(childExpr.get)
      builder.setFormat(formatExpr.get)

      val timeZone = truncTimestamp.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setTruncTimestamp(builder)
          .build())
    } else {
      withInfo(expr, truncTimestamp.timestamp, truncTimestamp.format)
      None
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Attribute, DateAdd, DateSub, Hour, Literal, Minute, Second, TruncDate, TruncTimestamp, Year}
import org.apache.spark.sql.types.{DateType, IntegerType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType, serializeDataType}

object CometYear extends CometExpressionSerde[Year] {
  override def convert(
      expr: Year,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val periodType = exprToProtoInternal(Literal("year"), inputs, binding)
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
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
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometHour extends CometExpressionSerde[Hour] {
  override def convert(
      expr: Hour,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Hour.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = expr.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setHour(builder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}

object CometMinute extends CometExpressionSerde[Minute] {
  override def convert(
      expr: Minute,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Minute.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = expr.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setMinute(builder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}

object CometSecond extends CometExpressionSerde[Second] {
  override def convert(
      expr: Second,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.Second.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = expr.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setSecond(builder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}

object CometDateAdd extends CometExpressionSerde[DateAdd] {
  override def convert(
      expr: DateAdd,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val rightExpr = exprToProtoInternal(expr.right, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_add", DateType, leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, expr.left, expr.right)
  }
}

object CometDateSub extends CometExpressionSerde[DateSub] {
  override def convert(
      expr: DateSub,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val rightExpr = exprToProtoInternal(expr.right, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_sub", DateType, leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, expr.left, expr.right)
  }
}

object CometTruncDate extends CometExpressionSerde[TruncDate] {
  override def convert(
      expr: TruncDate,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.date, inputs, binding)
    val formatExpr = exprToProtoInternal(expr.format, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("date_trunc", DateType, childExpr, formatExpr)
    optExprWithInfo(optExpr, expr, expr.date, expr.format)
  }
}

object CometTruncTimestamp extends CometExpressionSerde[TruncTimestamp] {
  override def convert(
      expr: TruncTimestamp,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.timestamp, inputs, binding)
    val formatExpr = exprToProtoInternal(expr.format, inputs, binding)

    if (childExpr.isDefined && formatExpr.isDefined) {
      val builder = ExprOuterClass.TruncTimestamp.newBuilder()
      builder.setChild(childExpr.get)
      builder.setFormat(formatExpr.get)

      val timeZone = expr.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setTruncTimestamp(builder)
          .build())
    } else {
      withInfo(expr, expr.timestamp, expr.format)
      None
    }
  }
}

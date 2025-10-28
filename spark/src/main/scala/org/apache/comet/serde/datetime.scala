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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Attribute, DateAdd, DateSub, DayOfMonth, DayOfWeek, DayOfYear, GetDateField, Hour, Literal, Minute, Month, Quarter, Second, TruncDate, TruncTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.CometGetDateField.CometGetDateField
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde._

private object CometGetDateField extends Enumeration {
  type CometGetDateField = Value

  // See: https://datafusion.apache.org/user-guide/sql/scalar_functions.html#date-part
  val Year: Value = Value("year")
  val Month: Value = Value("month")
  val DayOfMonth: Value = Value("day")
  // Datafusion: day of the week where Sunday is 0, but spark sunday is 1 (1 = Sunday,
  // 2 = Monday, ..., 7 = Saturday).
  val DayOfWeek: Value = Value("dow")
  val DayOfYear: Value = Value("doy")
  val WeekDay: Value = Value("isodow") // day of the week where Monday is 0
  val WeekOfYear: Value = Value("week")
  val Quarter: Value = Value("quarter")
}

/**
 * Convert spark [[org.apache.spark.sql.catalyst.expressions.GetDateField]] expressions to
 * Datafusion
 * [[https://datafusion.apache.org/user-guide/sql/scalar_functions.html#date-part datepart]]
 * function.
 */
trait CometExprGetDateField[T <: GetDateField] {
  def getDateField(
      expr: T,
      field: CometGetDateField,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val periodType = exprToProtoInternal(Literal(field.toString), inputs, binding)
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

object CometYear extends CometExpressionSerde[Year] with CometExprGetDateField[Year] {
  override def convert(
      expr: Year,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.Year, inputs, binding)
  }
}

object CometMonth extends CometExpressionSerde[Month] with CometExprGetDateField[Month] {
  override def convert(
      expr: Month,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.Month, inputs, binding)
  }
}

object CometDayOfMonth
    extends CometExpressionSerde[DayOfMonth]
    with CometExprGetDateField[DayOfMonth] {
  override def convert(
      expr: DayOfMonth,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.DayOfMonth, inputs, binding)
  }
}

object CometDayOfWeek
    extends CometExpressionSerde[DayOfWeek]
    with CometExprGetDateField[DayOfWeek] {
  override def convert(
      expr: DayOfWeek,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Datafusion: day of the week where Sunday is 0, but spark sunday is 1 (1 = Sunday,
    // 2 = Monday, ..., 7 = Saturday). So we need to add 1 to the result of datepart(dow, ...)
    val optExpr = getDateField(expr, CometGetDateField.DayOfWeek, inputs, binding)
      .zip(exprToProtoInternal(Literal(1), inputs, binding))
      .map { case (left, right) =>
        Expr
          .newBuilder()
          .setAdd(
            ExprOuterClass.MathExpr
              .newBuilder()
              .setLeft(left)
              .setRight(right)
              .setEvalMode(ExprOuterClass.EvalMode.LEGACY)
              .setReturnType(serializeDataType(IntegerType).get)
              .build())
          .build()
      }
      .headOption
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometWeekDay extends CometExpressionSerde[WeekDay] with CometExprGetDateField[WeekDay] {
  override def convert(
      expr: WeekDay,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.WeekDay, inputs, binding)
  }
}

object CometDayOfYear
    extends CometExpressionSerde[DayOfYear]
    with CometExprGetDateField[DayOfYear] {
  override def convert(
      expr: DayOfYear,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.DayOfYear, inputs, binding)
  }
}

object CometWeekOfYear
    extends CometExpressionSerde[WeekOfYear]
    with CometExprGetDateField[WeekOfYear] {
  override def convert(
      expr: WeekOfYear,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.WeekOfYear, inputs, binding)
  }
}

object CometQuarter extends CometExpressionSerde[Quarter] with CometExprGetDateField[Quarter] {
  override def convert(
      expr: Quarter,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    getDateField(expr, CometGetDateField.Quarter, inputs, binding)
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

object CometDateAdd extends CometScalarFunction[DateAdd]("date_add")

object CometDateSub extends CometScalarFunction[DateSub]("date_sub")

object CometTruncDate extends CometExpressionSerde[TruncDate] {

  val supportedFormats: Seq[String] =
    Seq("year", "yyyy", "yy", "quarter", "mon", "month", "mm", "week")

  override def getSupportLevel(expr: TruncDate): SupportLevel = {
    expr.format match {
      case Literal(fmt: UTF8String, _) =>
        if (supportedFormats.contains(fmt.toString.toLowerCase(Locale.ROOT))) {
          Compatible()
        } else {
          Unsupported(Some(s"Format $fmt is not supported"))
        }
      case _ =>
        Incompatible(
          Some("Invalid format strings will throw an exception instead of returning NULL"))
    }
  }

  override def convert(
      expr: TruncDate,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.date, inputs, binding)
    val formatExpr = exprToProtoInternal(expr.format, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType(
        "date_trunc",
        DateType,
        false,
        childExpr,
        formatExpr)
    optExprWithInfo(optExpr, expr, expr.date, expr.format)
  }
}

object CometTruncTimestamp extends CometExpressionSerde[TruncTimestamp] {

  val supportedFormats: Seq[String] =
    Seq(
      "year",
      "yyyy",
      "yy",
      "quarter",
      "mon",
      "month",
      "mm",
      "week",
      "day",
      "dd",
      "hour",
      "minute",
      "second",
      "millisecond",
      "microsecond")

  override def getSupportLevel(expr: TruncTimestamp): SupportLevel = {
    expr.format match {
      case Literal(fmt: UTF8String, _) =>
        if (supportedFormats.contains(fmt.toString.toLowerCase(Locale.ROOT))) {
          Compatible()
        } else {
          Unsupported(Some(s"Format $fmt is not supported"))
        }
      case _ =>
        Incompatible(
          Some("Invalid format strings will throw an exception instead of returning NULL"))
    }
  }

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

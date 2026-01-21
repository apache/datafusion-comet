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

import org.apache.spark.sql.catalyst.expressions.{Attribute, DateAdd, DateDiff, DateFormatClass, DateSub, DayOfMonth, DayOfWeek, DayOfYear, GetDateField, Hour, LastDay, Literal, Minute, Month, Quarter, Second, TruncDate, TruncTimestamp, UnixDate, UnixTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}
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

object CometUnixTimestamp extends CometExpressionSerde[UnixTimestamp] {

  private def isSupportedInputType(expr: UnixTimestamp): Boolean = {
    // Note: TimestampNTZType is not supported because Comet incorrectly applies
    // timezone conversion to TimestampNTZ values. TimestampNTZ stores local time
    // without timezone, so no conversion should be applied.
    expr.children.head.dataType match {
      case TimestampType | DateType => true
      case _ => false
    }
  }

  override def getSupportLevel(expr: UnixTimestamp): SupportLevel = {
    if (isSupportedInputType(expr)) {
      Compatible()
    } else {
      val inputType = expr.children.head.dataType
      Unsupported(Some(s"unix_timestamp does not support input type: $inputType"))
    }
  }

  override def convert(
      expr: UnixTimestamp,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!isSupportedInputType(expr)) {
      val inputType = expr.children.head.dataType
      withInfo(expr, s"unix_timestamp does not support input type: $inputType")
      return None
    }

    val childExpr = exprToProtoInternal(expr.children.head, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.UnixTimestamp.newBuilder()
      builder.setChild(childExpr.get)

      val timeZone = expr.timeZoneId.getOrElse("UTC")
      builder.setTimezone(timeZone)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setUnixTimestamp(builder)
          .build())
    } else {
      withInfo(expr, expr.children.head)
      None
    }
  }
}

object CometDateAdd extends CometScalarFunction[DateAdd]("date_add")

object CometDateSub extends CometScalarFunction[DateSub]("date_sub")

object CometLastDay extends CometScalarFunction[LastDay]("last_day")

object CometDateDiff extends CometScalarFunction[DateDiff]("date_diff")

/**
 * Converts a date to the number of days since Unix epoch (1970-01-01). Since dates are internally
 * stored as days since epoch, this is a simple cast to integer.
 */
object CometUnixDate extends CometExpressionSerde[UnixDate] {
  override def convert(
      expr: UnixDate,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = childExpr.map { child =>
      Expr
        .newBuilder()
        .setCast(
          ExprOuterClass.Cast
            .newBuilder()
            .setChild(child)
            .setDatatype(serializeDataType(IntegerType).get)
            .setEvalMode(ExprOuterClass.EvalMode.LEGACY)
            .setAllowIncompat(false)
            .build())
        .build()
    }
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

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

/**
 * Converts Spark DateFormatClass expression to DataFusion's to_char function.
 *
 * Spark uses Java SimpleDateFormat patterns while DataFusion uses strftime patterns. This
 * implementation supports a whitelist of common format strings that can be reliably mapped
 * between the two systems.
 */
object CometDateFormat extends CometExpressionSerde[DateFormatClass] {

  /**
   * Mapping from Spark SimpleDateFormat patterns to strftime patterns. Only formats in this map
   * are supported.
   */
  val supportedFormats: Map[String, String] = Map(
    // Full date formats
    "yyyy-MM-dd" -> "%Y-%m-%d",
    "yyyy/MM/dd" -> "%Y/%m/%d",
    "yyyy-MM-dd HH:mm:ss" -> "%Y-%m-%d %H:%M:%S",
    "yyyy/MM/dd HH:mm:ss" -> "%Y/%m/%d %H:%M:%S",
    // Date components
    "yyyy" -> "%Y",
    "yy" -> "%y",
    "MM" -> "%m",
    "dd" -> "%d",
    // Time formats
    "HH:mm:ss" -> "%H:%M:%S",
    "HH:mm" -> "%H:%M",
    "HH" -> "%H",
    "mm" -> "%M",
    "ss" -> "%S",
    // Combined formats
    "yyyyMMdd" -> "%Y%m%d",
    "yyyyMM" -> "%Y%m",
    // Month and day names
    "EEEE" -> "%A",
    "EEE" -> "%a",
    "MMMM" -> "%B",
    "MMM" -> "%b",
    // 12-hour time
    "hh:mm:ss a" -> "%I:%M:%S %p",
    "hh:mm a" -> "%I:%M %p",
    "h:mm a" -> "%-I:%M %p",
    // ISO formats
    "yyyy-MM-dd'T'HH:mm:ss" -> "%Y-%m-%dT%H:%M:%S")

  override def getSupportLevel(expr: DateFormatClass): SupportLevel = {
    // Check timezone - only UTC is fully compatible
    val timezone = expr.timeZoneId.getOrElse("UTC")
    val isUtc = timezone == "UTC" || timezone == "Etc/UTC"

    expr.right match {
      case Literal(fmt: UTF8String, _) =>
        val format = fmt.toString
        if (supportedFormats.contains(format)) {
          if (isUtc) {
            Compatible()
          } else {
            Incompatible(Some(s"Non-UTC timezone '$timezone' may produce different results"))
          }
        } else {
          Unsupported(
            Some(
              s"Format '$format' is not supported. Supported formats: " +
                supportedFormats.keys.mkString(", ")))
        }
      case _ =>
        Unsupported(Some("Only literal format strings are supported"))
    }
  }

  override def convert(
      expr: DateFormatClass,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Get the format string - must be a literal for us to map it
    val strftimeFormat = expr.right match {
      case Literal(fmt: UTF8String, _) =>
        supportedFormats.get(fmt.toString)
      case _ => None
    }

    strftimeFormat match {
      case Some(format) =>
        val childExpr = exprToProtoInternal(expr.left, inputs, binding)
        val formatExpr = exprToProtoInternal(Literal(format), inputs, binding)

        val optExpr = scalarFunctionExprToProtoWithReturnType(
          "to_char",
          StringType,
          false,
          childExpr,
          formatExpr)
        optExprWithInfo(optExpr, expr, expr.left, expr.right)
      case None =>
        withInfo(expr, expr.left, expr.right)
        None
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{AddMonths, Attribute, ConvertTimezone, DateAdd, DateDiff, DateFormatClass, DateFromUnixDate, DateSub, DayOfMonth, DayOfWeek, DayOfYear, Days, FromUTCTimestamp, GetDateField, GetTimestamp, Hour, Hours, LastDay, Literal, MakeDate, MakeTimestamp, MicrosToTimestamp, MillisToTimestamp, Minute, Month, MonthsBetween, NextDay, Quarter, Second, SecondsToTimestamp, ToUnixTimestamp, ToUTCTimestamp, TruncDate, TruncTimestamp, UnixDate, UnixMicros, UnixMillis, UnixSeconds, UnixTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.expressions.{CometCast, CometEvalMode}
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
  // Datafusion `isodow` is 1..=7 with Monday=1; Spark `WeekDay` is 0..=6 with Monday=0.
  val WeekDay: Value = Value("isodow")
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
    optExprWithFallbackReason(optExpr, expr, expr.child)
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
    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

object CometWeekDay extends CometExpressionSerde[WeekDay] with CometExprGetDateField[WeekDay] {
  override def convert(
      expr: WeekDay,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Datafusion `isodow` is 1..=7 with Monday=1, but Spark `WeekDay` is 0..=6 with Monday=0,
    // so subtract 1 from the result of datepart(isodow, ...).
    // TODO: fix upstream to avoid substraction
    // https://github.com/apache/datafusion/issues/22599
    val optExpr = getDateField(expr, CometGetDateField.WeekDay, inputs, binding)
      .zip(exprToProtoInternal(Literal(1), inputs, binding))
      .map { case (left, right) =>
        Expr
          .newBuilder()
          .setSubtract(
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
    optExprWithFallbackReason(optExpr, expr, expr.child)
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

  val incompatReason: String = "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
    " (https://github.com/apache/datafusion-comet/issues/3180)"

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: Hour): SupportLevel = {
    if (expr.child.dataType == TimestampNTZType) {
      Incompatible(
        Some(
          "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
            " (https://github.com/apache/datafusion-comet/issues/3180)"))
    } else {
      Compatible()
    }
  }

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
      withFallbackReason(expr, expr.child)
      None
    }
  }
}

object CometMinute extends CometExpressionSerde[Minute] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
      " (https://github.com/apache/datafusion-comet/issues/3180)")

  override def getSupportLevel(expr: Minute): SupportLevel = {
    if (expr.child.dataType == TimestampNTZType) {
      Incompatible(
        Some(
          "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
            " (https://github.com/apache/datafusion-comet/issues/3180)"))
    } else {
      Compatible()
    }
  }

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
      withFallbackReason(expr, expr.child)
      None
    }
  }
}

object CometSecond extends CometExpressionSerde[Second] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
      " (https://github.com/apache/datafusion-comet/issues/3180)")

  override def getSupportLevel(expr: Second): SupportLevel = {
    if (expr.child.dataType == TimestampNTZType) {
      Incompatible(
        Some(
          "Incorrectly applies timezone conversion to TimestampNTZ inputs" +
            " (https://github.com/apache/datafusion-comet/issues/3180)"))
    } else {
      Compatible()
    }
  }

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
      withFallbackReason(expr, expr.child)
      None
    }
  }
}

object CometUnixTimestamp extends CometExpressionSerde[UnixTimestamp] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Only `TimestampType` and `DateType` inputs are supported." +
      " `TimestampNTZType` is not supported because Comet incorrectly applies timezone" +
      " conversion to TimestampNTZ values.")

  private def isSupportedInputType(expr: UnixTimestamp): Boolean = {
    expr.children.head.dataType match {
      case TimestampType | DateType => true
      case TimestampNTZType => true
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
      withFallbackReason(expr, s"unix_timestamp does not support input type: $inputType")
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
      withFallbackReason(expr, expr.children.head)
      None
    }
  }
}

object CometDateAdd extends CometScalarFunction[DateAdd]("date_add")

object CometDateSub extends CometScalarFunction[DateSub]("date_sub")

private object UTCTimestampSerde {
  val tzParseIncompatReason: String =
    "Comet's native timezone parser only accepts IANA zone IDs (e.g." +
      " `America/Los_Angeles`) and fixed offsets in `+HH:MM` form. Spark also" +
      " accepts forms such as `GMT+1`, `UTC+1`, or three-letter abbreviations like" +
      " `PST`; queries using those forms will throw a native parse error at" +
      " execution time. See https://github.com/apache/datafusion-comet/issues/2013."
}

object CometFromUTCTimestamp extends CometExpressionSerde[FromUTCTimestamp] {

  override def getSupportLevel(expr: FromUTCTimestamp): SupportLevel =
    Incompatible(Some(UTCTimestampSerde.tzParseIncompatReason))

  override def getIncompatibleReasons(): Seq[String] =
    Seq(UTCTimestampSerde.tzParseIncompatReason)

  override def convert(
      expr: FromUTCTimestamp,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto("from_utc_timestamp", childExprs: _*)
    optExprWithFallbackReason(optExpr, expr, expr.children: _*)
  }
}

object CometToUTCTimestamp extends CometExpressionSerde[ToUTCTimestamp] {

  override def getSupportLevel(expr: ToUTCTimestamp): SupportLevel =
    Incompatible(Some(UTCTimestampSerde.tzParseIncompatReason))

  override def getIncompatibleReasons(): Seq[String] =
    Seq(UTCTimestampSerde.tzParseIncompatReason)

  override def convert(
      expr: ToUTCTimestamp,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto("to_utc_timestamp", childExprs: _*)
    optExprWithFallbackReason(optExpr, expr, expr.children: _*)
  }
}

object CometConvertTimezone extends CometExpressionSerde[ConvertTimezone] {

  override def getSupportLevel(expr: ConvertTimezone): SupportLevel =
    Incompatible(Some(UTCTimestampSerde.tzParseIncompatReason))

  override def getIncompatibleReasons(): Seq[String] =
    Seq(UTCTimestampSerde.tzParseIncompatReason)

  override def convert(
      expr: ConvertTimezone,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val srcTz = exprToProtoInternal(expr.sourceTz, inputs, binding)
    val tgtTz = exprToProtoInternal(expr.targetTz, inputs, binding)
    val ts = exprToProtoInternal(expr.sourceTs, inputs, binding)
    val toUtc = scalarFunctionExprToProto("to_utc_timestamp", ts, srcTz)
    val fromUtc = scalarFunctionExprToProto("from_utc_timestamp", toUtc, tgtTz)
    optExprWithFallbackReason(fromUtc, expr, expr.children: _*)
  }
}

object CometNextDay extends CometScalarFunction[NextDay]("next_day")

object CometMakeDate extends CometScalarFunction[MakeDate]("make_date")

object CometSecondsToTimestamp
    extends CometScalarFunction[SecondsToTimestamp]("seconds_to_timestamp") {
  override def getSupportLevel(expr: SecondsToTimestamp): SupportLevel =
    expr.child.dataType match {
      case IntegerType | LongType | FloatType | DoubleType => Compatible()
      case dt => Unsupported(Some(s"timestamp_seconds does not support input type $dt"))
    }
}

object CometLastDay extends CometScalarFunction[LastDay]("last_day")

object CometDateFromUnixDate extends CometScalarFunction[DateFromUnixDate]("date_from_unix_date")

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
    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

object CometTruncDate extends CometExpressionSerde[TruncDate] {

  val supportedFormats: Seq[String] =
    Seq("year", "yyyy", "yy", "quarter", "mon", "month", "mm", "week")

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Non-literal format strings will throw an exception instead of returning NULL")

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Only the following formats are supported: " + supportedFormats.mkString(", "))

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
    optExprWithFallbackReason(optExpr, expr, expr.date, expr.format)
  }
}

object CometTruncTimestamp extends CometExpressionSerde[TruncTimestamp] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Produces incorrect results when used with non-UTC timezones. Compatible when timezone is" +
      " UTC. (https://github.com/apache/datafusion-comet/issues/2649)")

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
    val timezone = expr.timeZoneId.getOrElse("UTC")
    val isUtc = timezone == "UTC" || timezone == "Etc/UTC"
    expr.format match {
      case Literal(fmt: UTF8String, _) =>
        if (supportedFormats.contains(fmt.toString.toLowerCase(Locale.ROOT))) {
          if (isUtc) {
            Compatible()
          } else {
            Incompatible(
              Some(
                s"Incorrect results in non-UTC timezone '$timezone'" +
                  " (https://github.com/apache/datafusion-comet/issues/2649)"))
          }
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
      withFallbackReason(expr, expr.timestamp, expr.format)
      None
    }
  }
}

/**
 * Converts Spark `DateFormatClass` to DataFusion's `to_char` when format and timezone are
 * mappable, otherwise routes the expression through the Arrow-direct codegen dispatcher so that
 * Spark's own `DateFormatClass.doGenCode` runs inside the Comet pipeline.
 *
 * Routing:
 *   - format is a literal in `supportedFormats` AND timezone is UTC -> native `to_char`
 *   - format is a literal in `supportedFormats` AND timezone is non-UTC, with the per-expression
 *     `allowIncompatible` flag set -> native `to_char` (results may differ from Spark)
 *   - all other cases -> JVM codegen dispatcher ([[CometScalaUDF.emitJvmCodegenDispatch]]), gated
 *     by [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]]. When that flag is disabled the operator
 *     falls back to Spark.
 */
object CometDateFormat extends CometExpressionSerde[DateFormatClass] {

  /**
   * Mapping from Spark SimpleDateFormat patterns to strftime patterns. Only formats in this map
   * are supported by the native path.
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

  // Compatibility is decided inside `convert`: the native path covers a subset, and the codegen
  // dispatcher covers everything else when enabled. Plan-time tagging happens via
  // `withFallbackReason` on the path that returns None.
  override def getSupportLevel(expr: DateFormatClass): SupportLevel = Compatible()

  override def getCompatibleNotes(): Seq[String] = Seq(
    "Format strings in a curated allow-list run natively via DataFusion's `to_char` for UTC " +
      "sessions. Other format strings (including non-literal formats), as well as non-UTC " +
      "sessions, route through Spark's own `DateFormatClass.doGenCode` via the Arrow-direct " +
      "codegen dispatcher when `spark.comet.exec.scalaUDF.codegen.enabled=true`. When the " +
      "codegen dispatcher is disabled (default) the operator falls back to Spark in those " +
      "cases.")

  override def convert(
      expr: DateFormatClass,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val timezone = expr.timeZoneId.getOrElse("UTC")
    val isUtc = timezone == "UTC" || timezone == "Etc/UTC"

    val nativeFormat: Option[String] = expr.right match {
      case Literal(fmt: UTF8String, _) => supportedFormats.get(fmt.toString)
      case _ => None
    }

    val canUseNative = nativeFormat.isDefined && {
      isUtc || CometConf.isExprAllowIncompat(getExprConfigName(expr))
    }

    if (canUseNative) {
      val childExpr = exprToProtoInternal(expr.left, inputs, binding)
      val formatExpr = exprToProtoInternal(Literal(nativeFormat.get), inputs, binding)
      val optExpr = scalarFunctionExprToProtoWithReturnType(
        "to_char",
        StringType,
        false,
        childExpr,
        formatExpr)
      optExprWithFallbackReason(optExpr, expr, expr.left, expr.right)
    } else {
      // Hand the full `DateFormatClass` (with `timeZoneId` already stamped by `ResolveTimeZone`)
      // to the codegen dispatcher. It closure-serializes the bound tree, so non-UTC timezones
      // and non-whitelisted / non-literal format strings produce Spark-identical results.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

/**
 * Converts a timestamp to the number of hours since Unix epoch (1970-01-01 00:00:00 UTC). This is
 * a V2 partition transform expression.
 *
 * Both TimestampType and TimestampNTZType use direct division of the raw microsecond value
 * without applying any session timezone offset.
 */
object CometHours extends CometExpressionSerde[Hours] {
  override def convert(
      expr: Hours,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val optExpr = expr.child.dataType match {
      case TimestampType | TimestampNTZType =>
        exprToProtoInternal(expr.child, inputs, binding).map { childExpr =>
          val builder = ExprOuterClass.HoursTransform.newBuilder()
          builder.setChild(childExpr)

          ExprOuterClass.Expr
            .newBuilder()
            .setHoursTransform(builder)
            .build()
        }
      case other =>
        withFallbackReason(expr, s"Hours does not support input type: $other")
        None
    }
    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

/**
 * Converts a timestamp or date to the number of days since Unix epoch (1970-01-01). This is a V2
 * partition transform expression.
 *
 * For DateType: dates are internally stored as days since epoch, so this is a simple cast to
 * integer (same as CometUnixDate).
 *
 * For TimestampType: uses a timezone-aware Cast(Timestamp to Date) followed by Cast(Date to Int).
 * The first cast respects the session timezone to correctly determine the date boundary.
 */
object CometDays extends CometExpressionSerde[Days] {
  override def convert(
      expr: Days,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    // Normalize input to DateType (Timestamp converts to Date first)
    val dateExprOpt = expr.child.dataType match {
      case DateType => childExpr
      case TimestampType =>
        val timezone = SQLConf.get.sessionLocalTimeZone
        childExpr.flatMap { child =>
          CometCast.castToProto(expr, Some(timezone), DateType, child, CometEvalMode.LEGACY)
        }
      case other =>
        withFallbackReason(expr, s"Days does not support input type: $other")
        None
    }

    // Convert DateType to IntegerType (days since epoch)
    val optExpr = dateExprOpt.map { dateExpr =>
      Expr
        .newBuilder()
        .setCast(
          ExprOuterClass.Cast
            .newBuilder()
            .setChild(dateExpr)
            .setDatatype(serializeDataType(IntegerType).get)
            .setEvalMode(ExprOuterClass.EvalMode.LEGACY)
            .setAllowIncompat(false)
            .build())
        .build()
    }

    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

object CometAddMonths extends CometCodegenDispatch[AddMonths]

object CometMonthsBetween extends CometCodegenDispatch[MonthsBetween]

object CometMakeTimestamp extends CometCodegenDispatch[MakeTimestamp]

object CometMicrosToTimestamp extends CometCodegenDispatch[MicrosToTimestamp]

object CometMillisToTimestamp extends CometCodegenDispatch[MillisToTimestamp]

object CometUnixSeconds extends CometCodegenDispatch[UnixSeconds]

object CometUnixMillis extends CometCodegenDispatch[UnixMillis]

object CometUnixMicros extends CometCodegenDispatch[UnixMicros]

object CometToUnixTimestamp extends CometCodegenDispatch[ToUnixTimestamp]

object CometGetTimestamp extends CometCodegenDispatch[GetTimestamp]

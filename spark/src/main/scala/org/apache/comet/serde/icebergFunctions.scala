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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType}

/**
 * Native support for Iceberg's Spark system functions (`bucket`, `truncate`, `years`, `months`,
 * `days`, `hours`).
 *
 * Iceberg exposes each of these through Spark's static magic method, so
 * `V2ExpressionUtils.resolveScalarFunction` binds them as `StaticInvoke(cls, "invoke", args)`
 * where `cls` is one of the per-type implementations under `org.apache.iceberg.spark.functions`
 * (e.g. `BucketFunction$BucketInt`). The same expressions appear in the hash distribution and
 * local sort that Iceberg requests in front of a partitioned write, and in predicates and
 * projections that users write against hidden partitioning, so routing them through
 * [[CometStaticInvoke]] covers shuffle, sort, filter, and projection at once.
 *
 * Iceberg is not on Comet's compile classpath, so the handlers are keyed on class name rather
 * than `Class[_]`. The list of classes is Iceberg's; `IcebergVersionFunction` is a zero-argument
 * constant and is deliberately left out.
 */
object CometIcebergSystemFunctions {

  private val FunctionsPackage = "org.apache.iceberg.spark.functions."

  private def implementations(
      outer: String,
      handler: CometExpressionSerde[StaticInvoke],
      inner: String*): Seq[(String, CometExpressionSerde[StaticInvoke])] =
    inner.map(name => s"$FunctionsPackage$outer$$$name" -> handler)

  /**
   * Handlers keyed by the fully qualified name of the Iceberg class that `StaticInvoke` calls.
   */
  val staticInvokeHandlers: Map[String, CometExpressionSerde[StaticInvoke]] = (
    implementations(
      "BucketFunction",
      CometIcebergBucket,
      "BucketInt",
      "BucketLong",
      "BucketString",
      "BucketBinary",
      "BucketDecimal") ++
      implementations(
        "TruncateFunction",
        CometIcebergTruncate,
        "TruncateTinyInt",
        "TruncateSmallInt",
        "TruncateInt",
        "TruncateBigInt",
        "TruncateString",
        "TruncateBinary",
        "TruncateDecimal") ++
      implementations(
        "YearsFunction",
        CometIcebergYears,
        "DateToYearsFunction",
        "TimestampToYearsFunction",
        "TimestampNtzToYearsFunction") ++
      implementations(
        "MonthsFunction",
        CometIcebergMonths,
        "DateToMonthsFunction",
        "TimestampToMonthsFunction",
        "TimestampNtzToMonthsFunction") ++
      implementations(
        "DaysFunction",
        CometIcebergDays,
        "DateToDaysFunction",
        "TimestampToDaysFunction",
        "TimestampNtzToDaysFunction") ++
      implementations(
        "HoursFunction",
        CometIcebergHours,
        "TimestampToHoursFunction",
        "TimestampNtzToHoursFunction")
  ).toMap

  /**
   * The `numBuckets` / `width` argument as a positive int, if it is a literal. Iceberg declares
   * the parameter as `IntegerType`, so a tinyint or smallint literal arrives already cast and
   * folded; the narrower literal types are matched anyway in case folding did not run.
   */
  private[serde] def positiveIntLiteral(expr: Expression): Option[Int] = expr match {
    case Literal(v: Int, IntegerType) if v > 0 => Some(v)
    case Literal(v: Short, ShortType) if v > 0 => Some(v.toInt)
    case Literal(v: Byte, ByteType) if v > 0 => Some(v.toInt)
    case _ => None
  }
}

/**
 * Shared shape of `bucket(numBuckets, value)` and `truncate(width, value)`: a positive integer
 * parameter followed by the value. The parameter has to be a literal because the native kernel
 * takes it as a constant, and it has to be positive because Iceberg's Java implementation divides
 * by it (zero throws, which the fallback preserves by leaving the expression to Spark).
 */
abstract class CometIcebergParameterizedTransform(
    nativeName: String,
    parameterName: String,
    valueTypeSupported: DataType => Boolean)
    extends CometExpressionSerde[StaticInvoke] {

  override def getSupportLevel(expr: StaticInvoke): SupportLevel = expr.arguments match {
    case Seq(parameter, value) =>
      if (CometIcebergSystemFunctions.positiveIntLiteral(parameter).isEmpty) {
        Unsupported(Some(s"$parameterName must be a positive integer literal, got $parameter"))
      } else if (!valueTypeSupported(value.dataType)) {
        Unsupported(Some(s"$nativeName does not support input type ${value.dataType}"))
      } else {
        Compatible()
      }
    case other =>
      Unsupported(Some(s"expected ($parameterName, value) arguments, got ${other.size}"))
  }

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = expr.arguments match {
    case Seq(parameter, value) =>
      // Normalize to an int literal so the native side always sees an Int32 scalar.
      val parameterProto = CometIcebergSystemFunctions
        .positiveIntLiteral(parameter)
        .flatMap(n => exprToProtoInternal(Literal(n, IntegerType), inputs, binding))
      val valueProto = exprToProtoInternal(value, inputs, binding)
      scalarFunctionExprToProtoWithReturnType(
        nativeName,
        expr.dataType,
        failOnError = false,
        parameterProto,
        valueProto)
    case _ => None
  }
}

/** `bucket(numBuckets, value)` over the types `BucketFunction.bind` accepts. */
object CometIcebergBucket
    extends CometIcebergParameterizedTransform(
      "iceberg_bucket",
      "numBuckets",
      {
        case ByteType | ShortType | IntegerType | LongType | DateType |
            TimestampType | TimestampNTZType | StringType | BinaryType =>
          true
        case _: DecimalType => true
        case _ => false
      })

/** `truncate(width, value)` over the types `TruncateFunction.bind` accepts. */
object CometIcebergTruncate
    extends CometIcebergParameterizedTransform(
      "iceberg_truncate",
      "width",
      {
        case ByteType | ShortType | IntegerType | LongType | StringType | BinaryType => true
        case _: DecimalType => true
        case _ => false
      })

/** Shared shape of the single-argument `years`, `months`, `days`, and `hours` transforms. */
abstract class CometIcebergTemporalTransform(
    nativeName: String,
    valueTypeSupported: DataType => Boolean)
    extends CometExpressionSerde[StaticInvoke] {

  override def getSupportLevel(expr: StaticInvoke): SupportLevel = expr.arguments match {
    case Seq(value) if valueTypeSupported(value.dataType) => Compatible()
    case Seq(value) =>
      Unsupported(Some(s"$nativeName does not support input type ${value.dataType}"))
    case other => Unsupported(Some(s"expected one argument, got ${other.size}"))
  }

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val valueProto = exprToProtoInternal(expr.arguments.head, inputs, binding)
    scalarFunctionExprToProtoWithReturnType(
      nativeName,
      expr.dataType,
      failOnError = false,
      valueProto)
  }
}

private object IcebergTemporalTypes {
  val dateOrTimestamp: DataType => Boolean = {
    case DateType | TimestampType | TimestampNTZType => true
    case _ => false
  }
  val timestampOnly: DataType => Boolean = {
    case TimestampType | TimestampNTZType => true
    case _ => false
  }
}

object CometIcebergYears
    extends CometIcebergTemporalTransform("iceberg_years", IcebergTemporalTypes.dateOrTimestamp)

object CometIcebergMonths
    extends CometIcebergTemporalTransform("iceberg_months", IcebergTemporalTypes.dateOrTimestamp)

object CometIcebergDays
    extends CometIcebergTemporalTransform("iceberg_days", IcebergTemporalTypes.dateOrTimestamp)

object CometIcebergHours
    extends CometIcebergTemporalTransform("iceberg_hours", IcebergTemporalTypes.timestampOnly)

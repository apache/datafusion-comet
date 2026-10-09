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

import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType}

/**
 * Native support for Iceberg's Spark system functions (`bucket`, `truncate`, `years`, `months`,
 * `days`, `hours`).
 *
 * Iceberg exposes each of these through Spark's DataSourceV2 catalog function API. Spark's
 * `V2ExpressionUtils.resolveScalarFunction` resolves them one of two ways: when the per-type
 * implementation class exposes a static `invoke` "magic method" with a matching signature the
 * call is lowered to `StaticInvoke(cls, "invoke", args)`; otherwise the call is lowered to
 * `ApplyFunctionExpression(function, args)`. The magic method is an optional performance opt-in
 * in the DSv2 API, not a requirement, so any Iceberg release (or third-party V2 catalog) that
 * omits it lands on the second path. Both paths carry the same class as their identity, so a
 * single set of handlers keyed by that class name covers them. See [[CometStaticInvoke]] for the
 * `StaticInvoke` entry point and [[CometApplyFunctionExpression]] for the
 * `ApplyFunctionExpression` entry point.
 *
 * The list of classes is Iceberg's; `IcebergVersionFunction` is a zero-argument constant and is
 * deliberately left out.
 */
object CometIcebergSystemFunctions {

  private val FunctionsPackage = "org.apache.iceberg.spark.functions."

  /** Every Iceberg system function exposes its static magic method under this name. */
  private[serde] val MagicMethod = "invoke"

  private def implementations(
      outer: String,
      handler: CometExpressionSerde[Expression],
      inner: String*): Seq[(String, CometExpressionSerde[Expression])] =
    inner.map(name => s"$FunctionsPackage$outer$$$name" -> handler)

  /**
   * Handlers keyed by the Iceberg implementation class name that both `StaticInvoke` and
   * `ApplyFunctionExpression` carry as their identity. Iceberg is not on Comet's compile
   * classpath, which is why the key is a class name rather than a class.
   */
  val handlers: Map[String, CometExpressionSerde[Expression]] = (
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

  /**
   * Extracts `(arguments, dataType)` from either wrapping expression. Kept together so a future
   * shape (e.g. Spark introduces yet another lowering) has a single seam to extend.
   */
  private[serde] def unwrap(expr: Expression): Option[(Seq[Expression], DataType)] = expr match {
    case si: StaticInvoke => Some((si.arguments, si.dataType))
    case afe: ApplyFunctionExpression => Some((afe.children, afe.dataType))
    case _ => None
  }
}

/**
 * Shared shape of `bucket(numBuckets, value)` and `truncate(width, value)`: a positive integer
 * parameter followed by the value. The parameter has to be a literal because the native kernel
 * takes it as a constant, and it has to be positive because Iceberg's Java implementation divides
 * by it (zero throws, which the fallback preserves by leaving the expression to Spark).
 *
 * Accepts either `StaticInvoke` or `ApplyFunctionExpression` since the same handler is registered
 * under both entry points; the extractor in [[CometIcebergSystemFunctions.unwrap]] normalizes
 * both to `(arguments, dataType)`.
 */
abstract class CometIcebergParameterizedTransform(
    nativeName: String,
    parameterName: String,
    valueTypeSupported: DataType => Boolean)
    extends CometExpressionSerde[Expression] {

  override def getSupportLevel(expr: Expression): SupportLevel =
    CometIcebergSystemFunctions.unwrap(expr) match {
      case Some((Seq(parameter, value), _)) =>
        if (CometIcebergSystemFunctions.positiveIntLiteral(parameter).isEmpty) {
          Unsupported(Some(s"$parameterName must be a positive integer literal, got $parameter"))
        } else if (!valueTypeSupported(value.dataType)) {
          Unsupported(Some(s"$nativeName does not support input type ${value.dataType}"))
        } else {
          Compatible()
        }
      case Some((other, _)) =>
        Unsupported(Some(s"expected ($parameterName, value) arguments, got ${other.size}"))
      case None =>
        Unsupported(Some(s"unrecognized wrapping expression: ${expr.getClass.getName}"))
    }

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] =
    CometIcebergSystemFunctions.unwrap(expr) match {
      case Some((Seq(parameter, value), returnType)) =>
        // Normalize to an int literal so the native side always sees an Int32 scalar.
        val parameterProto = CometIcebergSystemFunctions
          .positiveIntLiteral(parameter)
          .flatMap(n => exprToProtoInternal(Literal(n, IntegerType), inputs, binding))
        val valueProto = exprToProtoInternal(value, inputs, binding)
        scalarFunctionExprToProtoWithReturnType(
          nativeName,
          returnType,
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
        case ByteType | ShortType | IntegerType | LongType | DateType | TimestampType |
            TimestampNTZType | StringType | BinaryType | _: DecimalType =>
          true
        case _ => false
      })

/**
 * `truncate(width, value)` over the types `TruncateFunction.bind` accepts, minus decimals.
 *
 * Decimals are declined for a semantic reason rather than a missing kernel; see
 * [[CometIcebergTruncate.DecimalNote]].
 */
object CometIcebergTruncate
    extends CometIcebergParameterizedTransform(
      "iceberg_truncate",
      "width",
      {
        case ByteType | ShortType | IntegerType | LongType | StringType | BinaryType => true
        case _ => false
      }) {

  /**
   * Why decimal `truncate` stays with Spark. Truncating a negative decimal grows its magnitude,
   * so the result can need one more digit than the column's precision allows. Iceberg's
   * `TruncateDecimal.invoke` hands that oversized `Decimal` back unchanged and Spark turns it
   * into null only when the row is materialized, whereas an Arrow `Decimal128(precision, scale)`
   * array has no encoding for it -- a native kernel would have to null it during evaluation,
   * changing what an enclosing predicate or hash sees.
   */
  val DecimalNote: String =
    "Iceberg's TruncateDecimal returns a Decimal that can exceed the column's declared " +
      "precision, and Spark only turns that into null when the row is materialized. An Arrow " +
      "Decimal128(precision, scale) array cannot carry that intermediate, so a native kernel " +
      "would null it during evaluation and change what an enclosing predicate or hash sees."

  // Ordered after the parameter check so that `truncate(0, decimal_col)` still reports the width
  // problem, which is the one that changes whether Iceberg's own ArithmeticException is raised.
  override def getSupportLevel(expr: Expression): SupportLevel =
    CometIcebergSystemFunctions.unwrap(expr) match {
      case Some((Seq(parameter, value), _))
          if value.dataType.isInstanceOf[DecimalType] &&
            CometIcebergSystemFunctions.positiveIntLiteral(parameter).isDefined =>
        Unsupported(Some(DecimalNote))
      case _ => super.getSupportLevel(expr)
    }

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Iceberg's `truncate(width, value)` system function on a `decimal` column. " + DecimalNote +
      " Truncating `-99999999999999.9999` in a `decimal(18,4)` column by a width of 10 is one " +
      "such value: the result has 19 digits. The other `truncate` input types, and `bucket` on " +
      "decimals, are unaffected.")
}

/**
 * Shared shape of the single-argument `years`, `months`, `days`, and `hours` transforms. Accepts
 * either wrapping expression via [[CometIcebergSystemFunctions.unwrap]].
 */
abstract class CometIcebergTemporalTransform(
    nativeName: String,
    valueTypeSupported: DataType => Boolean)
    extends CometExpressionSerde[Expression] {

  override def getSupportLevel(expr: Expression): SupportLevel =
    CometIcebergSystemFunctions.unwrap(expr) match {
      case Some((Seq(value), _)) if valueTypeSupported(value.dataType) => Compatible()
      case Some((Seq(value), _)) =>
        Unsupported(Some(s"$nativeName does not support input type ${value.dataType}"))
      case Some((other, _)) =>
        Unsupported(Some(s"expected one argument, got ${other.size}"))
      case None =>
        Unsupported(Some(s"unrecognized wrapping expression: ${expr.getClass.getName}"))
    }

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] =
    CometIcebergSystemFunctions.unwrap(expr) match {
      case Some((Seq(value), returnType)) =>
        val valueProto = exprToProtoInternal(value, inputs, binding)
        scalarFunctionExprToProtoWithReturnType(
          nativeName,
          returnType,
          failOnError = false,
          valueProto)
      case _ => None
    }
}

object CometIcebergYears
    extends CometIcebergTemporalTransform(
      "iceberg_years",
      Set(DateType, TimestampType, TimestampNTZType))

object CometIcebergMonths
    extends CometIcebergTemporalTransform(
      "iceberg_months",
      Set(DateType, TimestampType, TimestampNTZType))

object CometIcebergDays
    extends CometIcebergTemporalTransform(
      "iceberg_days",
      Set(DateType, TimestampType, TimestampNTZType))

object CometIcebergHours
    extends CometIcebergTemporalTransform("iceberg_hours", Set(TimestampType, TimestampNTZType))

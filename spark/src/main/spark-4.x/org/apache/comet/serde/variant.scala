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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.{ArrayExtraction, ObjectExtraction, VariantGet}
import org.apache.spark.sql.types._

import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

object CometVariantGet extends CometExpressionSerde[VariantGet] {
  private val pathReason = "Variant extraction requires a non-null foldable path."
  private val invalidPathReason =
    "Invalid Variant paths require Spark's null and evaluation semantics."
  private val inputReason = "Variant extraction requires a top-level Variant column or literal."
  private val targetReason =
    "Variant extraction supports Boolean, numeric, binary, date and timestamp targets only."
  private val stringReason =
    "Variant STRING extraction requires Spark-compatible JSON and scalar formatting " +
      "(https://github.com/apache/datafusion-comet/issues/5424)."
  private val decimalReason =
    "Floating-point to decimal rounding can differ from Spark on JDK 17 " +
      "(https://github.com/apache/datafusion-comet/issues/5424)."
  private val temporalReason =
    "Date/time parsing and timezone conversion support a narrower year range than Spark " +
      "(https://github.com/apache/datafusion-comet/issues/5424)."

  override def getUnsupportedReasons(): Seq[String] =
    Seq(
      pathReason,
      invalidPathReason,
      inputReason,
      targetReason,
      stringReason,
      "Timezones with second-resolution offsets cannot be represented in native code.")
  override def getIncompatibleReasons(): Seq[String] = Seq(decimalReason, temporalReason)

  override def getSupportLevel(expr: VariantGet): SupportLevel = {
    if (!expr.path.foldable || expr.path.eval() == null) {
      Unsupported(Some(pathReason))
    } else if (!hasValidPath(expr)) {
      Unsupported(Some(invalidPathReason))
    } else if (!(expr.child.isInstanceOf[AttributeReference] || expr.child
        .isInstanceOf[Literal]) ||
      expr.child.dataType != VariantType) {
      Unsupported(Some(inputReason))
    } else if (CometTimeZone.nativeId(expr.timeZoneId).isEmpty) {
      CometTimeZone.supportLevel(expr.timeZoneId)
    } else {
      expr.dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
            DoubleType | BinaryType =>
          Compatible()
        case _: DecimalType => Incompatible(Some(decimalReason))
        case DateType | TimestampType | TimestampNTZType => Incompatible(Some(temporalReason))
        case StringType => Unsupported(Some(stringReason))
        case _ => Unsupported(Some(targetReason))
      }
    }
  }

  private def hasValidPath(expr: VariantGet): Boolean = {
    try {
      VariantGet.getParsedPath(expr.path.eval().toString, expr.prettyName)
      true
    } catch {
      case e: SparkThrowable if e.getCondition == "INVALID_VARIANT_GET_PATH" => false
    }
  }

  override def convert(
      expr: VariantGet,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    // Invalid paths stay in Spark because error timing depends on nulls and code generation.
    val path = VariantGet.getParsedPath(expr.path.eval().toString, expr.prettyName)
    CometVariantInput.convert(expr.child, inputs, binding).map { child =>
      val builder = ExprOuterClass.VariantGet
        .newBuilder()
        .setChild(child)
        .setDatatype(serializeDataType(expr.dataType).get)
        .setTargetSql(expr.dataType.sql)
        .setPathSql(expr.path.eval().toString)
        .setFailOnError(expr.failOnError)
        .setTimezone(CometTimeZone.nativeId(expr.timeZoneId).get)
        .setSizeLimit(org.apache.spark.types.variant.VariantUtil.SIZE_LIMIT)
      path.foreach {
        case ObjectExtraction(key) =>
          builder.addPath(ExprOuterClass.VariantPathSegment.newBuilder().setKey(key))
        case ArrayExtraction(index) =>
          builder.addPath(ExprOuterClass.VariantPathSegment.newBuilder().setIndex(index))
      }
      Expr.newBuilder().setVariantGet(builder).build()
    }
  }
}

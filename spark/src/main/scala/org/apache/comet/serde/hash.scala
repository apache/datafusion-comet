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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Murmur3Hash, Sha1, Sha2, XxHash64}
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegerType, LongType, MapType, StringType, StructType}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, isTimeType, scalarFunctionExprToProtoWithReturnType, serializeDataType, supportedDataType}

// Native-unsupported, Spark-codegen-compatible cases (`DecimalType` precision > 18, including
// nested, and `sha2` with a non-foldable `numBits`) stay in the Comet pipeline via
// `CodegenDispatchFallback` on the four hash serdes below. `TimeType` is out of scope for that
// dispatcher enrollment: `getSupportLevel` reports `Compatible` so the mixin does not intercept,
// and `convert` declines the native path so the projection falls back to Spark.
object CometXxHash64 extends CometExpressionSerde[XxHash64] with CodegenDispatchFallback {

  override def getUnsupportedReasons(): Seq[String] = HashUtils.unsupportedReasons

  override def getSupportLevel(expr: XxHash64): SupportLevel =
    HashUtils.supportLevelForChildren(expr)

  override def convert(
      expr: XxHash64,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    HashUtils.convertNativeOrSparkFallback(expr) {
      val exprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
      val seedBuilder = LiteralOuterClass.Literal
        .newBuilder()
        .setDatatype(serializeDataType(LongType).get)
        .setLongVal(expr.seed)
      val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
      // the seed is put at the end of the arguments
      scalarFunctionExprToProtoWithReturnType("xxhash64", LongType, false, exprs :+ seedExpr: _*)
    }
  }
}

object CometMurmur3Hash extends CometExpressionSerde[Murmur3Hash] with CodegenDispatchFallback {

  override def getUnsupportedReasons(): Seq[String] = HashUtils.unsupportedReasons

  override def getSupportLevel(expr: Murmur3Hash): SupportLevel =
    HashUtils.supportLevelForChildren(expr)

  override def convert(
      expr: Murmur3Hash,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    HashUtils.convertNativeOrSparkFallback(expr) {
      val exprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
      val seedBuilder = LiteralOuterClass.Literal
        .newBuilder()
        .setDatatype(serializeDataType(IntegerType).get)
        .setIntVal(expr.seed)
      val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
      // the seed is put at the end of the arguments
      scalarFunctionExprToProtoWithReturnType(
        "murmur3_hash",
        IntegerType,
        false,
        exprs :+ seedExpr: _*)
    }
  }
}

object CometSha2 extends CometExpressionSerde[Sha2] with CodegenDispatchFallback {

  private val nonFoldableNumBitsReason =
    "The `numBits` argument must be a foldable literal value"

  override def getUnsupportedReasons(): Seq[String] =
    HashUtils.unsupportedReasons :+ nonFoldableNumBitsReason

  override def getSupportLevel(expr: Sha2): SupportLevel = {
    // TimeType is not enrolled in the dispatcher; check it before the non-foldable `numBits`
    // `Unsupported` so a mixed tree does not take the mixin path.
    if (HashUtils.containsTimeTypeInChildren(expr)) {
      Compatible()
    } else if (!expr.right.foldable) {
      Unsupported(Some(nonFoldableNumBitsReason))
    } else {
      HashUtils.supportLevelForChildren(expr)
    }
  }

  override def convert(
      expr: Sha2,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    HashUtils.convertNativeOrSparkFallback(expr) {
      val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
      val numBitsExpr = exprToProtoInternal(expr.right, inputs, binding)
      scalarFunctionExprToProtoWithReturnType("sha2", StringType, false, leftExpr, numBitsExpr)
    }
  }
}

object CometSha1 extends CometExpressionSerde[Sha1] with CodegenDispatchFallback {

  override def getUnsupportedReasons(): Seq[String] = HashUtils.unsupportedReasons

  override def getSupportLevel(expr: Sha1): SupportLevel =
    HashUtils.supportLevelForChildren(expr)

  override def convert(
      expr: Sha1,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    HashUtils.convertNativeOrSparkFallback(expr) {
      val childExpr = exprToProtoInternal(expr.child, inputs, binding)
      scalarFunctionExprToProtoWithReturnType("sha1", StringType, false, childExpr)
    }
  }
}

private object HashUtils {

  private val unsupportedDecimalReason =
    "`DecimalType` with precision > 18 is not supported (Spark hashes via Java `BigDecimal`)"
  private val unsupportedTimeTypeReason = "`TimeType` is not supported"

  // `TimeType` is omitted: `CodegenDispatchFallback` documents this list as JVM-dispatch cases.
  val unsupportedReasons: Seq[String] =
    Seq(unsupportedDecimalReason, "Unsupported child data type")

  def containsTimeTypeInChildren(expr: Expression): Boolean =
    expr.children.exists(c => containsTimeType(c.dataType))

  def supportLevelForChildren(expr: Expression): SupportLevel = {
    // Compatible (not Unsupported) so `CodegenDispatchFallback` does not enroll TimeType.
    if (containsTimeTypeInChildren(expr)) {
      Compatible()
    } else {
      expr.children.iterator
        .flatMap(c => unsupportedReasonFor(c.dataType).iterator)
        .toSeq
        .headOption match {
        case Some(reason) => Unsupported(Some(reason))
        case None => Compatible()
      }
    }
  }

  def convertNativeOrSparkFallback(expr: Expression)(
      native: => Option[ExprOuterClass.Expr]): Option[ExprOuterClass.Expr] = {
    if (containsTimeTypeInChildren(expr)) {
      withFallbackReason(expr, unsupportedTimeTypeReason)
      None
    } else {
      native
    }
  }

  private def containsTimeType(dt: DataType): Boolean = dt match {
    case t if isTimeType(t) => true
    case s: StructType => s.fields.exists(f => containsTimeType(f.dataType))
    case a: ArrayType => containsTimeType(a.elementType)
    case m: MapType => containsTimeType(m.keyType) || containsTimeType(m.valueType)
    case _ => false
  }

  private def unsupportedReasonFor(dt: DataType): Option[String] = dt match {
    case d: DecimalType if d.precision > 18 => Some(unsupportedDecimalReason)
    case s: StructType =>
      s.fields.iterator.flatMap(f => unsupportedReasonFor(f.dataType).iterator).toSeq.headOption
    case a: ArrayType => unsupportedReasonFor(a.elementType)
    case m: MapType =>
      unsupportedReasonFor(m.keyType).orElse(unsupportedReasonFor(m.valueType))
    case _ if !supportedDataType(dt, allowComplex = true) =>
      Some(s"Unsupported child data type: $dt")
    case _ => None
  }
}

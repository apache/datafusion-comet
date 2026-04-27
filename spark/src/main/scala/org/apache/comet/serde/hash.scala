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

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType, serializeDataType, supportedDataType}

object CometXxHash64 extends CometExpressionSerde[XxHash64] {
  override def convert(
      expr: XxHash64,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!HashUtils.isSupportedType(expr)) {
      return None
    }
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

object CometMurmur3Hash extends CometExpressionSerde[Murmur3Hash] {
  override def convert(
      expr: Murmur3Hash,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!HashUtils.isSupportedType(expr)) {
      return None
    }
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

object CometSha2 extends CometExpressionSerde[Sha2] {
  override def convert(
      expr: Sha2,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!HashUtils.isSupportedType(expr)) {
      return None
    }

    // It's possible for spark to dynamically compute the number of bits from input
    // expression, however DataFusion does not support that yet.
    if (!expr.right.foldable) {
      withInfo(expr, "For Sha2, non literal numBits is not supported")
      return None
    }

    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val numBitsExpr = exprToProtoInternal(expr.right, inputs, binding)
    scalarFunctionExprToProtoWithReturnType("sha2", StringType, false, leftExpr, numBitsExpr)
  }
}

object CometSha1 extends CometExpressionSerde[Sha1] {
  override def convert(
      expr: Sha1,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!HashUtils.isSupportedType(expr)) {
      withInfo(expr, s"HashUtils doesn't support dataType: ${expr.child.dataType}")
      return None
    }
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    scalarFunctionExprToProtoWithReturnType("sha1", StringType, false, childExpr)
  }
}

private object HashUtils {
  def isSupportedType(expr: Expression): Boolean = {
    for (child <- expr.children) {
      if (!isSupportedDataType(expr, child.dataType)) {
        return false
      }
    }
    true
  }

  private def isSupportedDataType(expr: Expression, dt: DataType): Boolean = {
    dt match {
      case d: DecimalType if d.precision > 18 =>
        // Spark converts decimals with precision > 18 into
        // Java BigDecimal before hashing
        withInfo(expr, s"Unsupported datatype: $dt (precision > 18)")
        false
      case s: StructType =>
        s.fields.forall(f => isSupportedDataType(expr, f.dataType))
      case a: ArrayType =>
        isSupportedDataType(expr, a.elementType)
      case m: MapType =>
        isSupportedDataType(expr, m.keyType) && isSupportedDataType(expr, m.valueType)
      case _ if !supportedDataType(dt, allowComplex = true) =>
        withInfo(expr, s"Unsupported datatype $dt")
        false
      case _ =>
        true
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Murmur3Hash, Sha2, XxHash64}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType}

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
    val seedBuilder = ExprOuterClass.Literal
      .newBuilder()
      .setDatatype(serializeDataType(LongType).get)
      .setLongVal(expr.seed)
    val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
    // the seed is put at the end of the arguments
    scalarFunctionExprToProtoWithReturnType("xxhash64", LongType, exprs :+ seedExpr: _*)
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
    val seedBuilder = ExprOuterClass.Literal
      .newBuilder()
      .setDatatype(serializeDataType(IntegerType).get)
      .setIntVal(expr.seed)
    val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
    // the seed is put at the end of the arguments
    scalarFunctionExprToProtoWithReturnType("murmur3_hash", IntegerType, exprs :+ seedExpr: _*)
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
      withInfo(expr, "For Sha2, non-foldable right argument is not supported")
      return None
    }

    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val numBitsExpr = exprToProtoInternal(expr.right, inputs, binding)
    scalarFunctionExprToProtoWithReturnType("sha2", StringType, leftExpr, numBitsExpr)
  }
}

private object HashUtils {
  def isSupportedType(expr: Expression): Boolean = {
    for (child <- expr.children) {
      child.dataType match {
        case dt: DecimalType if dt.precision > 18 =>
          // Spark converts decimals with precision > 18 into
          // Java BigDecimal before hashing
          withInfo(expr, s"Unsupported datatype: $dt (precision > 18)")
          return false
        case dt if !supportedDataType(dt) =>
          withInfo(expr, s"Unsupported datatype $dt")
          return false
        case _ =>
      }
    }
    true
  }
}

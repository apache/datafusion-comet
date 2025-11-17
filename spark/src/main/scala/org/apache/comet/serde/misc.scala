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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BloomFilterMightContain, KnownFloatingPointNormalized, MakeDecimal, UnscaledValue}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types.{DecimalType, LongType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType, serializeDataType}

object CometUnscaledValue extends CometExpressionSerde[UnscaledValue] {
  override def convert(
      expr: UnscaledValue,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("unscaled_value", LongType, false, childExpr)
    optExprWithInfo(optExpr, expr, expr.child)

  }
}
object CometKnownFloatingPointNormalized
    extends CometExpressionSerde[KnownFloatingPointNormalized] {

  override def getSupportLevel(expr: KnownFloatingPointNormalized): SupportLevel = {
    expr.child match {
      case _: NormalizeNaNAndZero => Compatible()
      case _ => Unsupported()
    }
  }

  override def convert(
      expr: KnownFloatingPointNormalized,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    val wrapped = expr.child.asInstanceOf[NormalizeNaNAndZero].child

    val dataType = serializeDataType(wrapped.dataType)
    if (dataType.isEmpty) {
      withInfo(wrapped, s"Unsupported datatype ${wrapped.dataType}")
      return None
    }
    val ex = exprToProtoInternal(wrapped, inputs, binding)
    ex.map { child =>
      val builder = ExprOuterClass.NormalizeNaNAndZero
        .newBuilder()
        .setChild(child)
        .setDatatype(dataType.get)
      ExprOuterClass.Expr.newBuilder().setNormalizeNanAndZero(builder).build()
    }
  }
}

object CometMakeDecimal extends CometExpressionSerde[MakeDecimal] {
  override def convert(
      expr: MakeDecimal,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "make_decimal",
      DecimalType(expr.precision, expr.scale),
      failOnError = !expr.nullOnOverflow,
      childExpr)
    optExprWithInfo(optExpr, expr, expr.child)

  }
}

object CometBloomFilterMightContain extends CometExpressionSerde[BloomFilterMightContain] {

  override def convert(
      expr: BloomFilterMightContain,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    val bloomFilter = expr.left
    val value = expr.right
    val bloomFilterExpr = exprToProtoInternal(bloomFilter, inputs, binding)
    val valueExpr = exprToProtoInternal(value, inputs, binding)
    if (bloomFilterExpr.isDefined && valueExpr.isDefined) {
      val builder = ExprOuterClass.BloomFilterMightContain.newBuilder()
      builder.setBloomFilter(bloomFilterExpr.get)
      builder.setValue(valueExpr.get)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setBloomFilterMightContain(builder)
          .build())
    } else {
      withInfo(expr, bloomFilter, value)
      None
    }
  }
}

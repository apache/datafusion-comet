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

import org.apache.spark.sql.catalyst.expressions.{Attribute, HllUnion}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProtoWithReturnType}

// Spark 4.0 HllUnion is a TernaryExpression: `first`, `second` (binary sketches),
// `third` (allowDifferentLgConfigK boolean, default Literal(false)). All three are
// passed to the native `hll_union`, which enforces the flag and unions with min lgConfigK.
object CometHllUnion extends CometExpressionSerde[HllUnion] {
  private val incompatReason =
    "Comet uses a Rust DataSketches port; HLL sketch bytes and estimates may differ slightly from Spark."

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: HllUnion): SupportLevel = {
    if (!expr.third.foldable) {
      Unsupported(Some("The allowDifferentLgConfigK argument must be a foldable literal."))
    } else {
      Incompatible(Some(incompatReason))
    }
  }
  override def convert(
      expr: HllUnion,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val first = exprToProtoInternal(expr.first, inputs, binding)
    val second = exprToProtoInternal(expr.second, inputs, binding)
    val third = exprToProtoInternal(expr.third, inputs, binding)
    val unionExpr = scalarFunctionExprToProtoWithReturnType(
      "hll_union",
      BinaryType,
      failOnError = false,
      first,
      second,
      third)
    optExprWithFallbackReason(unionExpr, expr, expr.first, expr.second, expr.third)
  }
}

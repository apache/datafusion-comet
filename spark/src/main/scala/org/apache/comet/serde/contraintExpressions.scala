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

import org.apache.spark.sql.catalyst.expressions.{Attribute, KnownFloatingPointNormalized, KnownNullable}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, serializeDataType}

object CometKnownFloatingPointNormalized
    extends CometExpressionSerde[KnownFloatingPointNormalized] {

  override def convert(
      expr: KnownFloatingPointNormalized,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    expr.child match {
      case normalize: NormalizeNaNAndZero =>
        // Scalar float/double normalization: unwrap and emit the native NormalizeNaNAndZero node.
        val wrapped = normalize.child
        val dataType = serializeDataType(wrapped.dataType)
        if (dataType.isEmpty) {
          withFallbackReason(wrapped, s"Unsupported datatype ${wrapped.dataType}")
          return None
        }
        val ex = exprToProtoInternal(wrapped, inputs, binding)
        val optExpr = ex.map { child =>
          val builder = ExprOuterClass.NormalizeNaNAndZero
            .newBuilder()
            .setChild(child)
            .setDatatype(dataType.get)
          ExprOuterClass.Expr.newBuilder().setNormalizeNanAndZero(builder).build()
        }
        optExprWithFallbackReason(optExpr, expr, wrapped)

      case child =>
        // Nested normalization (array / struct / map). Spark 4.2 normalizes the inputs to
        // array_distinct and the array set operations by wrapping them as, e.g.,
        // `KnownFloatingPointNormalized(ArrayTransform(arr, x -> NormalizeNaNAndZero(x)))`.
        // `KnownFloatingPointNormalized` is a runtime no-op tag, so serialize the child directly
        // and let its serde (e.g. the ArrayTransform codegen dispatcher) carry the normalization.
        val optExpr = exprToProtoInternal(child, inputs, binding)
        optExprWithFallbackReason(optExpr, expr, child)
    }
  }
}

/**
 * `KnownNullable` is a tagging expression that only marks its child as nullable; it is a runtime
 * no-op (`eval` returns the child's value unchanged). Spark's time-window resolution wraps window
 * bounds in `KnownNullable`, so supporting it lets those grouping queries run natively. We simply
 * serialize the child and drop the tag.
 */
object CometKnownNullable extends CometExpressionSerde[KnownNullable] {
  override def convert(
      expr: KnownNullable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val optExpr = exprToProtoInternal(expr.child, inputs, binding)
    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

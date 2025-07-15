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
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GreaterThan}

import scala.collection.JavaConverters._

object CometGreaterThan extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val greaterThan = expr.asInstanceOf[GreaterThan]

    createBinaryExpr(
      expr,
      greaterThan.left,
      greaterThan.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGt(binaryExpr))
  }
}

sealed trait ComparisonBase {

  /**
   * Creates a UnaryExpr by calling exprToProtoInternal for the provided child expression and then
   * invokes the supplied function to wrap this UnaryExpr in a top-level Expr.
   *
   * @param child
   *   Spark expression
   * @param inputs
   *   Inputs to the expression
   * @param f
   *   Function that accepts an Expr.Builder and a UnaryExpr and builds the specific top-level
   *   Expr
   * @return
   *   Some(Expr) or None if not supported
   */
  def createUnaryExpr(
      expr: Expression,
      child: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(child, inputs, binding) // TODO review
    if (childExpr.isDefined) {
      // create the generic UnaryExpr message
      val inner = ExprOuterClass.UnaryExpr
        .newBuilder()
        .setChild(childExpr.get)
        .build()
      // call the user-supplied function to wrap UnaryExpr in a top-level Expr
      // such as Expr.IsNull or Expr.IsNotNull
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, child)
      None
    }
  }

  def createBinaryExpr(
      expr: Expression,
      left: Expression,
      right: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)
    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic BinaryExpr message
      val inner = ExprOuterClass.BinaryExpr
        .newBuilder()
        .setLeft(leftExpr.get)
        .setRight(rightExpr.get)
        .build()
      // call the user-supplied function to wrap BinaryExpr in a top-level Expr
      // such as Expr.And or Expr.Or
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, left, right)
      None
    }
  }
}

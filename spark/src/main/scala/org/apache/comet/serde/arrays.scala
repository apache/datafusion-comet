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

import org.apache.spark.sql.catalyst.expressions.{ArrayRemove, Attribute, Expression}
import org.apache.spark.sql.types.DataType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.createBinaryExpr

object CometArrayContains extends CometExpressionSerde {

  /** Exposed for unit testing */
  def isTypeSupported(dt: DataType): Boolean = {
    isPrimitiveType(dt)
  }

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val ar = expr.asInstanceOf[ArrayRemove]
    val inputTypes: Set[DataType] = ar.children.map(_.dataType).toSet
    for (dt <- inputTypes) {
      if (!isTypeSupported(dt)) {
        withInfo(expr, s"data type not supported: $dt")
        return None
      }
    }
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayContains(binaryExpr))
  }
}

object CometArrayRemove extends CometExpressionSerde {

  /** Exposed for unit testing */
  def isTypeSupported(dt: DataType): Boolean = {
    isPrimitiveType(dt)
  }

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val ar = expr.asInstanceOf[ArrayRemove]
    val inputTypes: Set[DataType] = ar.children.map(_.dataType).toSet
    for (dt <- inputTypes) {
      if (!isTypeSupported(dt)) {
        withInfo(expr, s"data type not supported: $dt")
        return None
      }
    }
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayRemove(binaryExpr))
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

/** Serde for scalar function. */
case class CometScalarFunction[T <: Expression](name: String) extends CometExpressionSerde[T] {
  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto(name, childExpr: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}

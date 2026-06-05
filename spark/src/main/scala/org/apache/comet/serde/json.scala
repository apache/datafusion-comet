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

import org.apache.spark.sql.catalyst.expressions.{Attribute, LengthOfJsonArray}

import org.apache.comet.CometConf
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProto}

/**
 * `json_array_length` runs Spark's own implementation through the codegen dispatcher by default,
 * for byte-exact results. The native (rust) path is faster but incompatible with Spark for
 * single-quoted JSON, unescaped control characters, and trailing content, so it is opt-in via
 * `spark.comet.expression.LengthOfJsonArray.allowIncompatible`; otherwise it rides the codegen
 * dispatcher via [[CometCodegenDispatch]].
 */
object CometLengthOfJsonArray extends CometCodegenDispatch[LengthOfJsonArray] {

  override def convert(
      expr: LengthOfJsonArray,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] =
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
      val optExpr = scalarFunctionExprToProto("json_array_length", childExpr: _*)
      optExprWithFallbackReason(optExpr, expr, expr.children: _*)
    } else {
      super.convert(expr, inputs, binding)
    }
}

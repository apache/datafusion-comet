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

import org.apache.spark.sql.catalyst.expressions.{AesDecrypt, Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke

import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

private object CometAesDecryptHelper {
  def convertToAesDecryptExpr[T <: Expression](
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "aes_decrypt",
      expr.dataType,
      failOnError = false,
      childExpr: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}

object CometAesDecrypt extends CometExpressionSerde[AesDecrypt] {
  override def convert(
      expr: AesDecrypt,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    CometAesDecryptHelper.convertToAesDecryptExpr(expr, inputs, binding)
  }
}

object CometAesDecryptStaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    CometAesDecryptHelper.convertToAesDecryptExpr(expr, inputs, binding)
  }
}

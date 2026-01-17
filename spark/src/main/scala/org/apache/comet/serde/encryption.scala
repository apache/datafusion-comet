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

import org.apache.spark.sql.catalyst.expressions.{AesEncrypt, Attribute, Expression}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType}

object CometAesEncrypt extends CometExpressionSerde[AesEncrypt] {
  override def convert(
      expr: AesEncrypt,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    val children = expr.children

    val inputExpr = exprToProtoInternal(children(0), inputs, binding)
    val keyExpr = exprToProtoInternal(children(1), inputs, binding)
    val modeExpr = exprToProtoInternal(children(2), inputs, binding)
    val paddingExpr = exprToProtoInternal(children(3), inputs, binding)
    val ivExpr = exprToProtoInternal(children(4), inputs, binding)
    val aadExpr = exprToProtoInternal(children(5), inputs, binding)

    if (inputExpr.isDefined && keyExpr.isDefined && modeExpr.isDefined &&
      paddingExpr.isDefined && ivExpr.isDefined && aadExpr.isDefined) {
      scalarFunctionExprToProtoWithReturnType(
        "aes_encrypt",
        BinaryType,
        false,
        inputExpr,
        keyExpr,
        modeExpr,
        paddingExpr,
        ivExpr,
        aadExpr)
    } else {
      None
    }
  }
}

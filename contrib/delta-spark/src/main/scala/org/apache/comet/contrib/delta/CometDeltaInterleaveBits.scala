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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.expressions.InterleaveBits
import org.apache.spark.sql.types.{BinaryType, IntegerType}

import org.apache.comet.serde.{CometExpressionSerde, Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType}

/**
 * Delta's `InterleaveBits`, the Z-order key `OPTIMIZE ... ZORDER BY` sorts on, through the native
 * `delta_interleave_bits` kernel. The kernel takes one or two int inputs; other shapes fall back.
 *
 * A null input counts as 0 and the result is never null, so the call is not wrapped in a null
 * short circuit.
 */
object CometDeltaInterleaveBits extends CometExpressionSerde[InterleaveBits] {

  private val MaxInputs = 2

  override def getSupportLevel(expr: InterleaveBits): SupportLevel = {
    val arity = expr.children.length
    if (arity < 1 || arity > MaxInputs) {
      Unsupported(Some(s"InterleaveBits supports 1 or $MaxInputs inputs, got $arity"))
    } else {
      expr.children.find(_.dataType != IntegerType) match {
        case Some(child) =>
          Unsupported(
            Some(s"InterleaveBits supports only int inputs, got ${child.dataType.catalogString}"))
        case None => Compatible(None)
      }
    }
  }

  override def convert(
      expr: InterleaveBits,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val args = expr.children.map(exprToProtoInternal(_, inputs, binding))
    scalarFunctionExprToProtoWithReturnType("delta_interleave_bits", BinaryType, false, args: _*)
  }
}

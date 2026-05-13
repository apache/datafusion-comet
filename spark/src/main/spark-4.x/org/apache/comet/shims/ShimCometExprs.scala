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

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{BinaryType, BooleanType}

import org.apache.comet.serde.CommonStringExprs
import org.apache.comet.serde.ExprOuterClass.Expr

trait ShimCometExprs extends CommonStringExprs {

  protected def sparkExprToProto(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      // encode(str, 'utf-8') -> cast(string AS binary) — Arrow's Utf8->Binary
      // is a zero-copy reinterpret, matching Spark's UTF8String.getBytes() exactly.
      case s: StaticInvoke
          if s.staticObject == classOf[Encode] &&
            s.dataType.isInstanceOf[BinaryType] &&
            s.functionName == "encode" &&
            s.arguments.size == 4 &&
            s.inputTypes == Seq(
              StringTypeWithCollation(supportsTrimCollation = true),
              StringTypeWithCollation(supportsTrimCollation = true),
              BooleanType,
              BooleanType) =>
        val Seq(value, charset, _, _) = s.arguments
        stringEncode(expr, charset, value, inputs, binding)

      case _ => None
    }
  }
}

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

import org.apache.comet.serde.CommonStringExprs
import org.apache.comet.serde.ExprOuterClass.Expr

trait CometExprShimCommon extends CommonStringExprs {

  protected def sparkExprToProto(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      // For valid UTF-8, encode(str, 'utf-8') -> cast(string AS binary) is a zero-copy
      // reinterpret. Malformed UTF-8 differs as described in CommonStringExprs.stringEncode.
      case s: StaticInvoke
          if s.staticObject == classOf[Encode] &&
            s.functionName == "encode" =>
        s.arguments match {
          case value +: charset +: _ =>
            stringEncode(expr, charset, value, inputs, binding)
          case _ => None
        }

      case _ => None
    }
  }
}

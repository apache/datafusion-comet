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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Reverse}
import org.apache.spark.sql.types.ArrayType

import org.apache.comet.serde.ExprOuterClass.Expr

object CometReverse extends CometScalarFunction[Reverse]("reverse") {

  override def getSupportLevel(expr: Reverse): SupportLevel = {
    if (expr.child.dataType.isInstanceOf[ArrayType]) {
      CometArrayReverse.getSupportLevel(expr)
    } else {
      Compatible()
    }
  }

  override def convert(expr: Reverse, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (expr.child.dataType.isInstanceOf[ArrayType]) {
      CometArrayReverse.convert(expr, inputs, binding)
    } else {
      super.convert(expr, inputs, binding)
    }
  }
}

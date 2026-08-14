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

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProto}

/** Serde for scalar function. */
case class CometScalarFunction[T <: Expression](name: String) extends CometExpressionSerde[T] {
  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (CometScalarFunction.isAnsiSensitive(expr)) {
      withFallbackReason(
        expr,
        s"${expr.nodeName} carries failOnError/evalMode/nullOnOverflow and cannot use " +
          s"CometScalarFunction('$name'). Prefer name-based ANSI/try variants " +
          "(e.g. parse_url / try_parse_url), or a custom serde with " +
          "scalarFunctionExprToProtoWithReturnType plus a native match arm that " +
          "consumes fail_on_error.")
      return None
    }
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto(name, childExpr: _*)
    optExpr
  }
}

object CometScalarFunction {

  /** Product field names that indicate ANSI / eval-mode sensitive Spark expressions. */
  private val AnsiSensitiveFields: Set[String] =
    Set("failOnError", "evalMode", "nullOnOverflow")

  /**
   * True when the Spark expression case class declares an ANSI-related constructor field. Used to
   * reject miswiring via plain [[CometScalarFunction]].
   */
  private[serde] def isAnsiSensitive(expr: Expression): Boolean = {
    expr match {
      case p: Product =>
        p.productElementNames.exists(AnsiSensitiveFields.contains)
      case _ =>
        isAnsiSensitive(expr.getClass)
    }
  }

  /**
   * Class-level check used by registration audits: the Spark expression type carries an
   * ANSI-related field regardless of any particular instance's flag value.
   */
  private[serde] def isAnsiSensitive(clazz: Class[_]): Boolean = {
    clazz.getDeclaredFields.exists(f => AnsiSensitiveFields.contains(f.getName))
  }
}

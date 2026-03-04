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

/**
 * Serializer contract for Spark `Invoke` expressions that wrap a private evaluator object.
 *
 * In Spark 4.0 several built-in functions (e.g. `parse_url`) became `RuntimeReplaceable` and are
 * rewritten by the analyser into an `Invoke(evaluator, arg, ...)` node whose first child is a
 * `Literal` of `ObjectType` holding a private evaluator instance. The Spark expression class that
 * Comet normally dispatches on (e.g. `ParseUrl`) is therefore never seen at serde time on Spark
 * 4.0.
 *
 * Implementors expose:
 *   - [[invokeTargetClassName]] - the fully-qualified name of the evaluator class embedded in the
 *     first `Literal(_, ObjectType)` child of the `Invoke` node. This is the key used by
 *     [[QueryPlanSerde]] to route the node to the correct handler.
 *   - [[convertFromInvoke]] - the actual serialization logic, receiving the raw `Invoke`
 *     expression (unerased, with all children including the evaluator literal).
 *
 * To register a new handler, add the object to [[QueryPlanSerde.invokeSerdeByTargetClassName]].
 */
trait CometInvokeExpressionSerde[T <: Expression] {

  /**
   * Fully-qualified class name of the private evaluator object held in the first child
   * `Literal(_, ObjectType(...))` of the `Invoke` node.
   *
   * Example: `"org.apache.spark.sql.catalyst.expressions.url.ParseUrlEvaluator"`
   */
  def invokeTargetClassName: String

  /**
   * Serialize the `Invoke` expression into a Comet proto `Expr`.
   *
   * @param expr
   *   The raw `Invoke` expression node (first child is the evaluator literal).
   * @param inputs
   *   Resolved input attributes for the enclosing operator.
   * @param binding
   *   Whether attributes are bound (relevant for aggregate expressions).
   * @return
   *   `Some(Expr)` on success, `None` if the expression cannot be handled (the implementor is
   *   responsible for calling `withInfo` to record the reason).
   */
  def convertFromInvoke(
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr]

  /**
   * Unchecked entry point used by [[QueryPlanSerde]] to dispatch through an existential
   * `CometInvokeExpressionSerde[_]`. Casts `expr` to `T` before delegating to
   * [[convertFromInvoke]]. Only call this when you have already verified (via
   * [[invokeTargetClassName]]) that the handler owns this `Invoke` node.
   */
  final def convertFromInvokeUnchecked(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] =
    convertFromInvoke(expr.asInstanceOf[T], inputs, binding)
}

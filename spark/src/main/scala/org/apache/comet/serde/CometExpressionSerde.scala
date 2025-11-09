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
 * Trait for providing serialization logic for expressions.
 */
trait CometExpressionSerde[T <: Expression] {

  /**
   * Get a short name for the expression that can be used as part of a config key related to the
   * expression, such as enabling or disabling that expression.
   *
   * @param expr
   *   The Spark expression.
   * @return
   *   Short name for the expression, defaulting to the Spark class name
   */
  def getExprConfigName(expr: T): String = expr.getClass.getSimpleName

  /**
   * Determine the support level of the expression based on its attributes.
   *
   * @param expr
   *   The Spark expression.
   * @return
   *   Support level (Compatible, Incompatible, or Unsupported).
   */
  def getSupportLevel(expr: T): SupportLevel = Compatible(None)

  /**
   * Convert a Spark expression into a protocol buffer representation that can be passed into
   * native code.
   *
   * @param expr
   *   The Spark expression.
   * @param inputs
   *   The input attributes.
   * @param binding
   *   Whether the attributes are bound (this is only relevant in aggregate expressions).
   * @return
   *   Protocol buffer representation, or None if the expression could not be converted. In this
   *   case it is expected that the input expression will have been tagged with reasons why it
   *   could not be converted.
   */
  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr]
}

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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.internal.SQLConf

/**
 * Trait for providing serialization logic for aggregate expressions.
 */
trait CometAggregateExpressionSerde[T <: AggregateFunction] {

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
   * Get documentation notes about ways this expression may differ from Spark that do not require
   * the user to opt in via `spark.comet.expr.allowIncompatible`. Use this for differences that
   * are always present, such as non-determinism or locale-specific behavior. This is called from
   * GenerateDocs when generating the Compatibility Guide. Each note should be written in Markdown
   * and may span multiple lines.
   *
   * @return
   *   List of notes, defaulting to an empty list.
   */
  def getCompatibleNotes(): Seq[String] = Seq.empty

  /**
   * Get documentation for usages where this expression may be incompatible with Spark. This is
   * called from GenerateDocs when generating the Compatibility Guide. Each reason should be
   * written in Markdown and may span multiple lines.
   *
   * @return
   *   List of reasons, defaulting to an empty list.
   */
  def getIncompatibleReasons(): Seq[String] = Seq.empty

  /**
   * Get documentation for usages where this expression is unsupported with Spark. This is called
   * from GenerateDocs when generating the Compatibility Guide. Each reason should be written in
   * Markdown and may span multiple lines.
   *
   * @return
   *   List of reasons, defaulting to an empty list.
   */
  def getUnsupportedReasons(): Seq[String] = Seq.empty

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
   * Whether this aggregate's intermediate buffer format is compatible between Spark and Comet,
   * making it safe to run the Partial in one engine and the Final in the other. Aggregates with
   * simple single-value buffers (MIN, MAX, bitwise) are safe; those with complex or
   * differently-encoded buffers (AVG, SUM with decimals, CollectSet, Variance) are not. COUNT is
   * intentionally excluded: mixed COUNT partial/final regressed AQE's
   * PropagateEmptyRelationAfterAQE pattern (which matches BaseAggregateExec only) and the Spark
   * 4.0 count-bug decorrelation for correlated IN subqueries.
   */
  def supportsMixedPartialFinal: Boolean = false

  /**
   * Convert a Spark expression into a protocol buffer representation that can be passed into
   * native code.
   *
   * @param aggExpr
   *   The aggregate expression.
   * @param expr
   *   The aggregate function.
   * @param inputs
   *   The input attributes.
   * @param binding
   *   Whether the attributes are bound (this is only relevant in aggregate expressions).
   * @param conf
   *   SQLConf
   * @return
   *   Protocol buffer representation, or None if the expression could not be converted. In this
   *   case it is expected that the input expression will have been tagged with reasons why it
   *   could not be converted.
   */
  def convert(
      aggExpr: AggregateExpression,
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr]
}

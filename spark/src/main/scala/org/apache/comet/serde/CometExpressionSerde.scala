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

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

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

  /**
   * Tag the expression with an informational hint that a native implementation exists but is
   * gated behind a config the user has not enabled, so a slower path (the JVM codegen dispatcher,
   * or Spark fallback) is being used instead. The hint surfaces in verbose extended explain
   * output as `[COMET-INFO: ...]` and does NOT cause fallback. Defaults to the standard
   * per-expression `allowIncompatible` config key derived from `getExprConfigName`; use the
   * two-arg overload when the gating config is something else.
   */
  def withNativeAvailableInfo(expr: T): T =
    withNativeAvailableInfo(
      expr,
      CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))

  /**
   * Like the single-arg overload but takes the gating config key explicitly. Use when the native
   * path is gated by a config other than the per-expression `allowIncompatible` flag.
   */
  def withNativeAvailableInfo(expr: T, configKey: String): T = {
    CometSparkSessionExtensions.withInfo(
      expr,
      s"A native implementation of ${getExprConfigName(expr)} is available but needs to be " +
        s"enabled with $configKey. See compatibility guide for more information.")
    expr
  }
}

/**
 * Opt-in marker for expression serdes that have a native implementation which is `Incompatible`
 * with Spark for some inputs. When such an expression reports `Incompatible` and the user has not
 * enabled `allowIncompatible` for it, mixing in this trait routes it through the JVM codegen
 * dispatcher (running Spark's own `doGenCode` inside the Comet pipeline) instead of falling the
 * projection back to Spark, so it stays native while still matching Spark exactly.
 *
 * Enrollment is opt-in: only serdes that explicitly mix this in are routed through the
 * dispatcher. Every other `Incompatible` expression falls back to Spark.
 */
trait CodegenDispatchFallback { self: CometExpressionSerde[_] => }

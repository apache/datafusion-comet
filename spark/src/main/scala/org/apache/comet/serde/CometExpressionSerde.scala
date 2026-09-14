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
   * Get documentation for ways the native implementation of this expression may differ from
   * Spark. These differences describe the native path specifically. For expressions that have a
   * native opt-in (see `NativeOptInAvailable`), the default codegen-dispatch path is always
   * Spark-compatible, so these differences apply only after the user opts into the native path.
   * This is called from GenerateDocs when generating the Compatibility Guide. Each reason should
   * be written in Markdown and may span multiple lines.
   *
   * @return
   *   List of reasons, defaulting to an empty list.
   */
  def getIncompatibleReasons(): Seq[String] = Seq.empty

  /**
   * Get documentation for usages of this expression that Comet's native implementation does not
   * support. Cases listed here normally fall back to Spark, regardless of any `allowIncompatible`
   * setting. When the serde mixes in `CodegenDispatchFallback` they are instead routed through
   * the JVM codegen dispatcher (Spark's own `doGenCode` inside the Comet pipeline), so they stay
   * in the Comet pipeline while still matching Spark exactly. This is called from GenerateDocs
   * when generating the Compatibility Guide. Each reason should be written in Markdown and may
   * span multiple lines.
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
}

/**
 * Mixin for serdes whose native implementation cannot match Spark for some inputs. When
 * `getSupportLevel` returns `Incompatible` and the user has not enabled `allowIncompatible`, the
 * expression routes through the JVM codegen dispatcher (Spark's own `doGenCode` inside the Comet
 * pipeline) instead of falling the projection back to Spark. When `getSupportLevel` returns
 * `Unsupported`, the expression always routes through the dispatcher -- the serde is declaring
 * "no native path exists for this case; run Spark's code in-pipeline." Spark fallback is reserved
 * for the case where the dispatcher itself cannot handle the expression (e.g. the global codegen
 * flag is off, or the kernel rejects the bound tree).
 *
 * Contract for `Unsupported` reasons on a `CodegenDispatchFallback` serde: the case must be
 * something `Expression.doGenCode` can compile. If you mark something `Unsupported` because Spark
 * also rejects it, that is fine -- the dispatcher will surface the same error Spark would have.
 *
 * Enrollment is opt-in: only serdes that explicitly mix this in are routed through the
 * dispatcher. Every other `Incompatible` expression falls back to Spark, and every other
 * `Unsupported` expression falls back to Spark.
 */
trait CodegenDispatchFallback extends NativeOptInAvailable { self: CometExpressionSerde[_] => }

/**
 * Doc-facing marker for serdes that run a Spark-compatible path by default but have a faster
 * native implementation the user can opt into. `GenerateDocs` uses this to render the
 * compatible-by-default header, and the gating config key is the per-expression
 * `allowIncompatible` key unless `nativeOptInConfigKeyOverride` supplies a different one.
 */
trait NativeOptInAvailable { self: CometExpressionSerde[_] =>
  def nativeOptInConfigKeyOverride: Option[String] = None
}

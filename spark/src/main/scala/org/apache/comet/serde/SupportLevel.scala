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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/**
 * @see
 *   [[SupportLevelKind]]
 */
sealed trait SupportLevel

/**
 * Comet either supports this feature with full compatibility with Spark, or may have known
 * differences in some specific edge cases that are unlikely to be an issue for most users.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Compatible(notes: Option[String] = None) extends SupportLevel

/**
 * Comet supports this feature but results can be different from Spark.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Incompatible(notes: Option[String] = None) extends SupportLevel

/** Comet does not support this feature */
case class Unsupported(notes: Option[String] = None) extends SupportLevel

object SupportLevel {

  /**
   * Returns true if the given data type contains FloatType or DoubleType at any nesting level.
   */
  def containsFloatingPoint(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case ArrayType(elementType, _) => containsFloatingPoint(elementType)
    case StructType(fields) => fields.exists(f => containsFloatingPoint(f.dataType))
    case MapType(keyType, valueType, _) =>
      containsFloatingPoint(keyType) || containsFloatingPoint(valueType)
    case _ => false
  }
}

/**
 * The kind of support outcome produced by a [[SupportCondition]].
 *
 * The member names mirror the [[SupportLevel]] case classes on purpose so that a condition's
 * `level` reads the same as the resulting `SupportLevel`. To disambiguate references in this file
 * or in wildcard imports of the `serde` package, qualify as `SupportLevelKind.Compatible` etc.
 */
sealed trait SupportLevelKind
object SupportLevelKind {
  case object Compatible extends SupportLevelKind
  case object Incompatible extends SupportLevelKind
  case object Unsupported extends SupportLevelKind
}

/**
 * A single support condition: a predicate that, when matched against an expression, yields a
 * [[SupportLevel]] with an optional message.
 *
 * Conditions are declared statically per serde so that they can be enumerated at build time for
 * documentation and tests. They evaluate at runtime by calling `fires(expr)`.
 *
 * Ordering within a serde's `conditions` list is significant: the first condition whose `fires`
 * predicate matches determines the outcome. If no condition matches, the expression is treated as
 * `Compatible(None)`.
 */
trait SupportCondition[-T <: Expression] {

  /** Stable, machine-readable id, unique per serde. Used in docs and tests. */
  def id: String

  /** Static prose describing when this fires. For example, "Child is BinaryType". */
  def description: String

  /** The outcome if this condition matches. */
  def level: SupportLevelKind

  /** Runtime predicate. May consult `CometConf` or other dynamic state. */
  def fires(expr: T): Boolean

  /** Runtime message, usually constant. May interpolate from the expression. */
  def message(expr: T): String

  /** Optional issue links for doc output. */
  def issues: Seq[String] = Nil
}

object SupportCondition {

  private final case class Impl[T <: Expression](
      id: String,
      description: String,
      level: SupportLevelKind,
      firesFn: T => Boolean,
      messageFn: T => String,
      override val issues: Seq[String])
      extends SupportCondition[T] {
    override def fires(expr: T): Boolean = firesFn(expr)
    override def message(expr: T): String = messageFn(expr)
  }

  /** Generic builder. Use this when `message` depends on the expression. */
  def apply[T <: Expression](
      id: String,
      description: String,
      level: SupportLevelKind,
      fires: T => Boolean,
      message: T => String,
      issues: Seq[String] = Nil): SupportCondition[T] =
    Impl(id, description, level, fires, message, issues)

  /** Convenience: unsupported with a static message. */
  def unsupported[T <: Expression](
      id: String,
      description: String,
      fires: T => Boolean,
      message: String,
      issues: Seq[String] = Nil): SupportCondition[T] =
    Impl(id, description, SupportLevelKind.Unsupported, fires, (_: T) => message, issues)

  /** Convenience: incompatible with a static message. */
  def incompatible[T <: Expression](
      id: String,
      description: String,
      fires: T => Boolean,
      message: String,
      issues: Seq[String] = Nil): SupportCondition[T] =
    Impl(id, description, SupportLevelKind.Incompatible, fires, (_: T) => message, issues)

  /** Convenience: compatible-with-note (a caveat on an otherwise supported path). */
  def compatibleWithNote[T <: Expression](
      id: String,
      description: String,
      fires: T => Boolean,
      message: String,
      issues: Seq[String] = Nil): SupportCondition[T] =
    Impl(id, description, SupportLevelKind.Compatible, fires, (_: T) => message, issues)
}

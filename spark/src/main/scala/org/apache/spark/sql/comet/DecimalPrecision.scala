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

package org.apache.spark.sql.comet

import scala.math.max

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DecimalType

/**
 * Wraps decimal binary arithmetic expressions in [[CheckOverflow]] where the native expression
 * cannot reproduce Spark's overflow semantics on its own.
 *
 * Decimal `+`, `-` and `*` are left unwrapped when the native planner takes its narrow
 * `Decimal128` path (`addSubFitsDecimal128` / `multiplyFitsDecimal128`). There the Arrow kernel
 * computes the same result type as Spark, and no value that respects its declared input type can
 * overflow the declared result precision, so the check can only fire for malformed input. The
 * wide path is also unwrapped, by the native planner, because `WideDecimalBinaryExpr` reproduces
 * Spark's precision adjustment and overflow handling. `Divide` and `Remainder` stay wrapped:
 * native decimal division uses `i128::MAX` as an overflow sentinel, so the wrapper must turn it
 * into null or an ANSI error.
 *
 * Spark itself stopped wrapping these in `CheckOverflow` in 3.4 (SPARK-39316), but Comet's native
 * `CheckOverflow` only validates precision (it does not rescale), so where a wrapper is still
 * emitted its target type must equal the child's actual `dataType`. Always using `expr.dataType`
 * is the safe choice: on Spark 3.5 - 4.0 it equals the value the rule would otherwise recompute
 * from `SQLConf`, and on Spark 4.1+ (SPARK-53968) it preserves the per-expression
 * `allowDecimalPrecisionLoss` captured at view creation time. Recomputing from the live `SQLConf`
 * would re-label a stored DEC(38, 17) result as DEC(38, 18) (or vice versa) and shift values by
 * 10x (issue #4124).
 *
 * The `CheckOverflow` behavior also follows each expression's captured `evalMode`, since TRY
 * arithmetic can be nested inside ANSI arithmetic (or vice versa) independently of the live
 * session ANSI setting.
 */
object DecimalPrecision {

  /**
   * Mirrors the native planner's narrow-path decision for decimal `+` and `-`
   * (`create_binary_expr_with_options` in `native/core/src/execution/planner.rs`, and
   * `decimal128_add_sub_width` in `native/spark-expr/src/math_funcs/modulo_expr.rs`): the
   * operation stays on `Decimal128` when `max(s1, s2) + max(p1 - s1, p2 - s2) < 38`. Spark's
   * result precision is that width plus one, so it is at most 38 and `adjustPrecisionScale` /
   * `bounded` leave it unchanged. The Arrow kernel's natural result type is then identical to
   * `expr.dataType`.
   */
  private def addSubFitsDecimal128(p1: Int, s1: Int, p2: Int, s2: Int): Boolean =
    max(s1, s2) + max(p1 - s1, p2 - s2) < DecimalType.MAX_PRECISION

  /**
   * Mirrors the planner's narrow-path decision for decimal `*`: `p1 + p2 < 38` (the planner
   * ignores scale for multiply). Spark's result precision is `p1 + p2 + 1`, so it is at most 38
   * and unchanged; the Arrow kernel's natural result type is then identical to `expr.dataType`.
   */
  private def multiplyFitsDecimal128(p1: Int, p2: Int): Boolean =
    p1 + p2 < DecimalType.MAX_PRECISION

  def promote(expr: Expression): Expression = {
    // `transformUp` walks the whole tree and calls `mapChildren` at every node, which allocates
    // through `children.map` / `mapProductIterator` even when no case matches (the specialized
    // `mapChildren` overrides do skip the node copy via `fastEquals`, so the cost is the traversal
    // and its allocations, not a rebuild). The serde calls this once per expression it converts,
    // so skip it when there is nothing to rewrite.
    if (containsDecimalArithmetic(expr)) rewrite(expr) else expr
  }

  /**
   * Whether [[promote]] could rewrite anything in this tree. Deliberately a superset of the rule:
   * it ignores the operands, so any arithmetic node the rule handles that produces a decimal
   * qualifies. Wrapping a child in `CheckOverflow` does not change that child's data type, so a
   * tree without such a node cannot grow one part way through the rewrite.
   */
  private def containsDecimalArithmetic(expr: Expression): Boolean = expr.exists {
    case e @ (_: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder) =>
      e.dataType.isInstanceOf[DecimalType]
    case _ => false
  }

  private def rewrite(expr: Expression): Expression = {
    expr.transformUp {
      // Recursive exprToProto calls can re-promote every decimal binary operator below. Collapse
      // only equivalent wrappers so the shared promotion rule remains idempotent.
      case outer @ CheckOverflow(
            inner @ CheckOverflow(child: BinaryArithmetic, _, _),
            dataType,
            nullOnOverflow)
          if inner.dataType == dataType && inner.nullOnOverflow == nullOnOverflow =>
        outer.copy(child = child)

      case add @ Add(DecimalExpression(p1, s1), DecimalExpression(p2, s2), _)
          if add.dataType.isInstanceOf[DecimalType] &&
            !addSubFitsDecimal128(p1, s1, p2, s2) =>
        CheckOverflow(add, add.dataType.asInstanceOf[DecimalType], add.evalMode != EvalMode.ANSI)

      case sub @ Subtract(DecimalExpression(p1, s1), DecimalExpression(p2, s2), _)
          if sub.dataType.isInstanceOf[DecimalType] &&
            !addSubFitsDecimal128(p1, s1, p2, s2) =>
        CheckOverflow(sub, sub.dataType.asInstanceOf[DecimalType], sub.evalMode != EvalMode.ANSI)

      case mul @ Multiply(DecimalExpression(p1, _), DecimalExpression(p2, _), _)
          if mul.dataType.isInstanceOf[DecimalType] && !multiplyFitsDecimal128(p1, p2) =>
        CheckOverflow(mul, mul.dataType.asInstanceOf[DecimalType], mul.evalMode != EvalMode.ANSI)

      // Native decimal division uses i128::MAX as an overflow sentinel, so this wrapper must
      // convert it to null or an ANSI error according to the expression's eval mode.
      case div @ Divide(DecimalExpression(_, _), DecimalExpression(_, _), _)
          if div.dataType.isInstanceOf[DecimalType] =>
        CheckOverflow(div, div.dataType.asInstanceOf[DecimalType], div.evalMode != EvalMode.ANSI)

      case rem @ Remainder(DecimalExpression(_, _), DecimalExpression(_, _), _)
          if rem.dataType.isInstanceOf[DecimalType] =>
        CheckOverflow(rem, rem.dataType.asInstanceOf[DecimalType], rem.evalMode != EvalMode.ANSI)

      case e => e
    }
  }

  // TODO: consider to use `org.apache.spark.sql.types.DecimalExpression` for Spark 3.5+
  object DecimalExpression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => Some((t.precision, t.scale))
      case _ => None
    }
  }
}

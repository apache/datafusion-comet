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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DecimalType

/**
 * Wraps decimal binary arithmetic expressions in [[CheckOverflow]] so the native side has an
 * explicit target type for the result.
 *
 * Spark itself stopped wrapping these in `CheckOverflow` in 3.4 (SPARK-39316), but Comet's native
 * `CheckOverflow` only validates precision (it does not rescale), so the target type must equal
 * the child's actual `dataType`. Always using `expr.dataType` is the safe choice: on Spark 3.4 -
 * 4.0 it equals the value the rule would otherwise recompute from `SQLConf`, and on Spark 4.1+
 * (SPARK-53968) it preserves the per-expression `allowDecimalPrecisionLoss` captured at view
 * creation time. Recomputing from the live `SQLConf` would re-label a stored DEC(38, 17) result
 * as DEC(38, 18) (or vice versa) and shift values by 10x (issue #4124).
 *
 * The `CheckOverflow` behavior also follows each expression's captured `evalMode`, since TRY
 * arithmetic can be nested inside ANSI arithmetic (or vice versa) independently of the live
 * session ANSI setting.
 */
object DecimalPrecision {
  def promote(expr: Expression): Expression = {
    // `transformUp` walks and rebuilds every node even when no case matches, and the serde calls
    // this once per expression it converts, so skip it when there is nothing to rewrite.
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
      // This means the binary expression is already optimized with the rule in Spark. This can
      // happen if the Spark version is < 3.4
      case e: BinaryArithmetic if e.left.prettyName == "promote_precision" => e

      case add @ Add(DecimalExpression(_, _), DecimalExpression(_, _), _)
          if add.dataType.isInstanceOf[DecimalType] =>
        CheckOverflow(add, add.dataType.asInstanceOf[DecimalType], add.evalMode != EvalMode.ANSI)

      case sub @ Subtract(DecimalExpression(_, _), DecimalExpression(_, _), _)
          if sub.dataType.isInstanceOf[DecimalType] =>
        CheckOverflow(sub, sub.dataType.asInstanceOf[DecimalType], sub.evalMode != EvalMode.ANSI)

      case mul @ Multiply(DecimalExpression(_, _), DecimalExpression(_, _), _)
          if mul.dataType.isInstanceOf[DecimalType] =>
        CheckOverflow(mul, mul.dataType.asInstanceOf[DecimalType], mul.evalMode != EvalMode.ANSI)

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

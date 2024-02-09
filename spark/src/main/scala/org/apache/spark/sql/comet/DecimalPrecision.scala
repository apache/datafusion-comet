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

import scala.math.{max, min}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DecimalType

/**
 * This is mostly copied from the `decimalAndDecimal` method in Spark's [[DecimalPrecision]] which
 * existed before Spark 3.4.
 *
 * In Spark 3.4 and up, the method `decimalAndDecimal` is removed from Spark, and for binary
 * expressions with different decimal precisions from children, the difference is handled in the
 * expression evaluation instead (see SPARK-39316).
 *
 * However in Comet, we still have to rely on the type coercion to ensure the decimal precision is
 * the same for both children of a binary expression, since our arithmetic kernels do not yet
 * handle the case where precision is different. Therefore, this re-apply the logic in the
 * original rule, and rely on `Cast` and `CheckOverflow` for decimal binary operation.
 *
 * TODO: instead of relying on this rule, it's probably better to enhance arithmetic kernels to
 * handle different decimal precisions
 */
object DecimalPrecision {
  def promote(
      allowPrecisionLoss: Boolean,
      expr: Expression,
      nullOnOverflow: Boolean): Expression = {
    expr.transformUp {
      // This means the binary expression is already optimized with the rule in Spark. This can
      // happen if the Spark version is < 3.4
      case e: BinaryArithmetic if e.left.prettyName == "promote_precision" => e

      case add @ Add(DecimalExpression(p1, s1), DecimalExpression(p2, s2), _) =>
        val resultScale = max(s1, s2)
        val resultType = if (allowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        } else {
          DecimalType.bounded(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        }
        CheckOverflow(add, resultType, nullOnOverflow)

      case sub @ Subtract(DecimalType.Expression(p1, s1), DecimalType.Expression(p2, s2), _) =>
        val resultScale = max(s1, s2)
        val resultType = if (allowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        } else {
          DecimalType.bounded(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        }
        CheckOverflow(sub, resultType, nullOnOverflow)

      case mul @ Multiply(DecimalType.Expression(p1, s1), DecimalType.Expression(p2, s2), _) =>
        val resultType = if (allowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(p1 + p2 + 1, s1 + s2)
        } else {
          DecimalType.bounded(p1 + p2 + 1, s1 + s2)
        }
        CheckOverflow(mul, resultType, nullOnOverflow)

      case div @ Divide(DecimalType.Expression(p1, s1), DecimalType.Expression(p2, s2), _) =>
        val resultType = if (allowPrecisionLoss) {
          // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
          // Scale: max(6, s1 + p2 + 1)
          val intDig = p1 - s1 + s2
          val scale = max(DecimalType.MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1)
          val prec = intDig + scale
          DecimalType.adjustPrecisionScale(prec, scale)
        } else {
          var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
          var decDig = min(DecimalType.MAX_SCALE, max(6, s1 + p2 + 1))
          val diff = (intDig + decDig) - DecimalType.MAX_SCALE
          if (diff > 0) {
            decDig -= diff / 2 + 1
            intDig = DecimalType.MAX_SCALE - decDig
          }
          DecimalType.bounded(intDig + decDig, decDig)
        }
        CheckOverflow(div, resultType, nullOnOverflow)

      case rem @ Remainder(DecimalType.Expression(p1, s1), DecimalType.Expression(p2, s2), _) =>
        val resultType = if (allowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        } else {
          DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        }
        CheckOverflow(rem, resultType, nullOnOverflow)

      case e => e
    }
  }

  object DecimalExpression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => Some((t.precision, t.scale))
      case _ => None
    }
  }
}

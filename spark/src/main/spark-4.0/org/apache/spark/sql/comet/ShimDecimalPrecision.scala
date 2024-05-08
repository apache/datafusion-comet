package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.expressions.Expression

trait ShimDecimalPrecision {
  object DecimalExpression {
    def unapply(e: Expression): Option[(Int, Int)] =
      org.apache.spark.sql.types.DecimalExpression.unapply(e)
  }
}

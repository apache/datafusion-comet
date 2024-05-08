package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DecimalType

trait ShimDecimalPrecision {
  // `org.apache.spark.sql.types.DecimalExpression` is added in Spark 3.5
  object DecimalExpression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => Some((t.precision, t.scale))
      case _ => None
    }
  }
}

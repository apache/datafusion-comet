package org.apache.comet.cost

import org.apache.comet.DataTypeSupport
import org.apache.spark.sql.catalyst.expressions.{Add, BinaryArithmetic, Expression}
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometPlan, CometProjectExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

case class CometCostEstimate(acceleration: Double)

trait CometCostModel {

  /** Estimate the relative cost of one operator */
  def estimateCost(plan: SparkPlan): CometCostEstimate
}

class DefaultComeCostModel extends CometCostModel {

  // optimistic default of 2x acceleration
  private val defaultAcceleration = 2.0

  override def estimateCost(plan: SparkPlan): CometCostEstimate = {

    plan match {
      case op: CometShuffleExchangeExec =>
        op.shuffleType match {
          case CometNativeShuffle => CometCostEstimate(1.5)
          case CometColumnarShuffle =>
          if (DataTypeSupport.hasComplexTypes(op.schema)) {
            CometCostEstimate(0.8)
          } else {
            CometCostEstimate(1.1)
          }
        }
      case _: CometColumnarToRowExec =>
        CometCostEstimate(1.0)
      case op: CometProjectExec =>
        val total: Double = op.expressions.map(estimateCost).sum
        CometCostEstimate(total / op.expressions.length.toDouble)
      case _: CometPlan =>
        CometCostEstimate(defaultAcceleration)
      case _ =>
        // Spark operator
        CometCostEstimate(1.0)
    }
  }

  /** Estimate the cost of an expression */
  def estimateCost(expr: Expression): Double = {
    expr match {
      case _: BinaryArithmetic =>
        2.0
      case _ => defaultAcceleration
    }
  }
}

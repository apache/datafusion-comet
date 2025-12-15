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

package org.apache.comet.cost

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometPlan, CometProjectExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.DataTypeSupport
import org.apache.comet.serde.{ExprOuterClass, OperatorOuterClass}

case class CometCostEstimate(acceleration: Double)

trait CometCostModel {

  /** Estimate the relative cost of one operator */
  def estimateCost(plan: SparkPlan): CometCostEstimate
}

class DefaultCometCostModel extends CometCostModel {

  // optimistic default of 2x acceleration
  private val defaultAcceleration = 2.0

  override def estimateCost(plan: SparkPlan): CometCostEstimate = {
    // Walk the entire plan tree and accumulate costs
    var totalAcceleration = 0.0
    var operatorCount = 0

    def collectOperatorCosts(node: SparkPlan): Unit = {
      val operatorCost = estimateOperatorCost(node)
      // scalastyle:off println
      println(
        s"[CostModel] Operator: ${node.getClass.getSimpleName}, " +
          s"Cost: ${operatorCost.acceleration}")
      // scalastyle:on println
      totalAcceleration += operatorCost.acceleration
      operatorCount += 1

      // Recursively process children
      node.children.foreach(collectOperatorCosts)
    }

    collectOperatorCosts(plan)

    // Calculate average acceleration across all operators
    // This is crude but gives us a starting point
    val averageAcceleration = if (operatorCount > 0) {
      totalAcceleration / operatorCount.toDouble
    } else {
      1.0 // No acceleration if no operators
    }

    // scalastyle:off println
    println(
      s"[CostModel] Plan: ${plan.getClass.getSimpleName}, Total operators: $operatorCount, " +
        s"Average acceleration: $averageAcceleration")
    // scalastyle:on println

    CometCostEstimate(averageAcceleration)
  }

  /** Estimate the cost of a single operator */
  private def estimateOperatorCost(plan: SparkPlan): CometCostEstimate = {
    val result = plan match {
      case op: CometProjectExec =>
        // scalastyle:off println
        println(s"[CostModel] CometProjectExec found - evaluating expressions")
        // scalastyle:on println
        // Cast nativeOp to Operator and extract projection expressions
        val operator = op.nativeOp.asInstanceOf[OperatorOuterClass.Operator]
        val projection = operator.getProjection
        val expressions = projection.getProjectListList.asScala
        // scalastyle:off println
        println(s"[CostModel] Found ${expressions.length} expressions in projection")
        // scalastyle:on println

        val costs = expressions.map { expr =>
          val cost = estimateCometExpressionCost(expr)
          // scalastyle:off println
          println(s"[CostModel] Expression cost: $cost")
          // scalastyle:on println
          cost
        }
        val total = costs.sum
        val average = total / expressions.length.toDouble
        // scalastyle:off println
        println(s"[CostModel] CometProjectExec total cost: $total, average: $average")
        // scalastyle:on println
        CometCostEstimate(average)

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
      case _: CometPlan =>
        // scalastyle:off println
        println(s"[CostModel] Generic CometPlan: ${plan.getClass.getSimpleName}")
        // scalastyle:on println
        CometCostEstimate(defaultAcceleration)
      case _ =>
        // scalastyle:off println
        println(s"[CostModel] Non-Comet operator: ${plan.getClass.getSimpleName}")
        // scalastyle:on println
        // Spark operator
        CometCostEstimate(1.0)
    }

    // scalastyle:off println
    println(s"[CostModel] ${plan.getClass.getSimpleName} -> acceleration: ${result.acceleration}")
    // scalastyle:on println
    result
  }

  /** Estimate the cost of a Comet protobuf expression */
  private def estimateCometExpressionCost(expr: ExprOuterClass.Expr): Double = {
    val result = expr.getExprStructCase match {
      // Handle specialized expression types
      case ExprOuterClass.Expr.ExprStructCase.SUBSTRING =>
        // scalastyle:off println
        println(s"[CostModel] Expression: SUBSTRING -> 6.3")
        // scalastyle:on println
        6.3

      // Handle generic scalar functions
      case ExprOuterClass.Expr.ExprStructCase.SCALARFUNC =>
        val funcName = expr.getScalarFunc.getFunc
        val cost = funcName match {
          // String expression numbers from CometStringExpressionBenchmark
          case "ascii" => 0.6
          case "octet_length" => 0.6
          case "lower" => 3.0
          case "upper" => 3.0
          case "char" => 0.6
          case "initcap" => 0.9
          case "trim" => 0.4
          case "concat_ws" => 0.5
          case "length" => 9.1
          case "repeat" => 0.4
          case "reverse" => 6.9
          case "instr" => 0.6
          case "replace" => 1.3
          case "string_space" => 0.8
          case "translate" => 0.8
          case _ => defaultAcceleration
        }
        // scalastyle:off println
        println(s"[CostModel] Expression: SCALARFUNC($funcName) -> $cost")
        // scalastyle:on println
        cost

      case _ =>
        // scalastyle:off println
        println(
          s"[CostModel] Expression: Unknown type ${expr.getExprStructCase} -> " +
            s"$defaultAcceleration")
        // scalastyle:on println
        defaultAcceleration
    }
    result
  }

}

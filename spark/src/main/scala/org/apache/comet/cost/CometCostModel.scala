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

import org.apache.spark.internal.Logging
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

class DefaultCometCostModel extends CometCostModel with Logging {

  // optimistic default of 2x acceleration
  private val defaultAcceleration = 2.0

  override def estimateCost(plan: SparkPlan): CometCostEstimate = {

    logTrace(s"estimateCost for $plan")

    // Walk the entire plan tree and accumulate costs
    var totalAcceleration = 0.0
    var operatorCount = 0

    def collectOperatorCosts(node: SparkPlan): Unit = {
      val operatorCost = estimateOperatorCost(node)
      logTrace(
        s"Operator: ${node.getClass.getSimpleName}, " +
          s"Cost: ${operatorCost.acceleration}")
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

    logTrace(
      s"Plan: ${plan.getClass.getSimpleName}, Total operators: $operatorCount, " +
        s"Average acceleration: $averageAcceleration")

    CometCostEstimate(averageAcceleration)
  }

  /** Estimate the cost of a single operator */
  private def estimateOperatorCost(plan: SparkPlan): CometCostEstimate = {
    val result = plan match {
      case op: CometProjectExec =>
        logTrace("CometProjectExec found - evaluating expressions")
        // Cast nativeOp to Operator and extract projection expressions
        val operator = op.nativeOp.asInstanceOf[OperatorOuterClass.Operator]
        val projection = operator.getProjection
        val expressions = projection.getProjectListList.asScala
        logTrace(s"Found ${expressions.length} expressions in projection")

        val costs = expressions.map { expr =>
          val cost = estimateCometExpressionCost(expr)
          logTrace(s"Expression cost: $cost")
          cost
        }
        val total = costs.sum
        val average = total / expressions.length.toDouble
        logTrace(s"CometProjectExec total cost: $total, average: $average")
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
        logTrace(s"Generic CometPlan: ${plan.getClass.getSimpleName}")
        CometCostEstimate(defaultAcceleration)
      case _ =>
        logTrace(s"Non-Comet operator: ${plan.getClass.getSimpleName}")
        // Spark operator
        CometCostEstimate(1.0)
    }

    logTrace(s"${plan.getClass.getSimpleName} -> acceleration: ${result.acceleration}")
    result
  }

  /** Estimate the cost of a Comet protobuf expression */
  private def estimateCometExpressionCost(expr: ExprOuterClass.Expr): Double = {
    val result = expr.getExprStructCase match {
      // Handle specialized expression types
      case ExprOuterClass.Expr.ExprStructCase.SUBSTRING => 6.3

      // Handle generic scalar functions
      case ExprOuterClass.Expr.ExprStructCase.SCALARFUNC =>
        val funcName = expr.getScalarFunc.getFunc
        funcName match {
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

      case _ =>
        logTrace(
          s"Expression: Unknown type ${expr.getExprStructCase} -> " +
            s"$defaultAcceleration")
        defaultAcceleration
    }
    result
  }

}

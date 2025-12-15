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
import org.apache.comet.serde.ExprOuterClass

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

    CometCostEstimate(averageAcceleration)
  }

  /** Estimate the cost of a single operator */
  private def estimateOperatorCost(plan: SparkPlan): CometCostEstimate = {
    plan match {
      case op: CometProjectExec =>
        val expressions = op.nativeOp.getProjection.getProjectListList.asScala
        val total: Double = expressions.map(estimateCometExpressionCost).sum
        CometCostEstimate(total / expressions.length.toDouble)
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
        CometCostEstimate(defaultAcceleration)
      case _ =>
        // Spark operator
        CometCostEstimate(1.0)
    }
  }

  /** Estimate the cost of a Comet protobuf expression */
  private def estimateCometExpressionCost(expr: ExprOuterClass.Expr): Double = {
    expr.getExprStructCase match {
      // Handle specialized expression types
      case ExprOuterClass.Expr.ExprStructCase.SUBSTRING => 6.3

      // Handle generic scalar functions
      case ExprOuterClass.Expr.ExprStructCase.SCALARFUNC =>
        expr.getScalarFunc.getFunc match {
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

      case _ => defaultAcceleration
    }
  }

}

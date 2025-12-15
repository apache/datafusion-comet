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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{Cost, CostEvaluator}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Simple Cost implementation for Comet cost evaluator.
 */
case class CometCost(value: Double) extends Cost {
  override def compare(that: Cost): Int = that match {
    case CometCost(thatValue) => java.lang.Double.compare(value, thatValue)
    case _ => 0 // If we can't compare, assume equal
  }

  override def toString: String = s"CometCost($value)"
}

/**
 * Comet implementation of Spark's CostEvaluator for adaptive query execution.
 *
 * This evaluator uses the configured CometCostModel to estimate costs for query plans, allowing
 * Spark's adaptive query execution to make informed decisions about whether to use Comet or Spark
 * operators based on estimated performance.
 */
class CometCostEvaluator extends CostEvaluator with Logging {

  @transient private lazy val costModel: CometCostModel = {
    val conf = SQLConf.get
    val costModelClass = CometConf.COMET_COST_MODEL_CLASS.get(conf)

    try {
      // scalastyle:off classforname
      val clazz = Class.forName(costModelClass)
      // scalastyle:on classforname
      val constructor = clazz.getConstructor()
      constructor.newInstance().asInstanceOf[CometCostModel]
    } catch {
      case e: Exception =>
        logWarning(
          s"Failed to instantiate cost model class '$costModelClass', " +
            s"falling back to DefaultCometCostModel. Error: ${e.getMessage}")
        new DefaultCometCostModel()
    }
  }

  /**
   * Evaluates the cost of executing the given SparkPlan.
   *
   * This method uses the configured CometCostModel to estimate the acceleration factor for the
   * plan, then converts it to a Cost object that Spark's adaptive query execution can use for
   * decision making.
   *
   * @param plan
   *   The SparkPlan to evaluate
   * @return
   *   A Cost representing the estimated execution cost
   */
  override def evaluateCost(plan: SparkPlan): Cost = {
    val estimate = costModel.estimateCost(plan)

    // Convert acceleration factor to cost
    // Lower cost means better performance, so we use the inverse of acceleration factor
    // For example:
    //   - 2.0x acceleration -> cost = 0.5 (half the cost)
    //   - 0.8x acceleration -> cost = 1.25 (25% more cost)
    val costValue = 1.0 / estimate.acceleration

    // scalastyle:off println
    println(
      s"[CostEvaluator] Plan: ${plan.getClass.getSimpleName}, " +
        s"acceleration=${estimate.acceleration}, cost=$costValue")
    // scalastyle:on println

    // Create Cost object with the calculated value
    CometCost(costValue)
  }
}

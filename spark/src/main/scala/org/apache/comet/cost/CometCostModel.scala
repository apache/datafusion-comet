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

import org.apache.spark.sql.execution.SparkPlan

/**
 * The estimated cost of running one query stage with Comet and with Spark. The units are
 * arbitrary: only the ratio between the two costs is meaningful, and only within one cost model.
 */
case class CometCostEstimate(cometCost: Double, sparkCost: Double) {

  /** How many times faster the stage is estimated to run with Comet than with Spark. */
  def speedup: Double = if (cometCost > 0) sparkCost / cometCost else Double.PositiveInfinity
}

/**
 * Experimental: estimates whether a query stage is worth running with Comet. Enabled by
 * `spark.comet.exec.costModel.enabled` and selected by `spark.comet.exec.costModel.class`.
 * Implementations need a no-argument constructor and must not retain the plans they are given.
 */
trait CometCostModel {

  /**
   * Estimates the cost of one query stage. Both plans cover the same stage and produce the same
   * result. Any `QueryStageExec`, shuffle exchange or broadcast exchange inside them is an input
   * of the stage that belongs to another stage, and is the same in both plans.
   *
   * @param cometPlan
   *   the stage as Comet would run it, including the transitions between Comet and Spark
   *   operators
   * @param sparkPlan
   *   the stage with its Comet operators reverted to the Spark operators they replaced
   */
  def estimate(cometPlan: SparkPlan, sparkPlan: SparkPlan): CometCostEstimate
}

object CometCostModel {

  def load(className: String): CometCostModel = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    // scalastyle:off classforname
    val cls = Class.forName(className, true, loader)
    // scalastyle:on classforname
    cls.getConstructor().newInstance().asInstanceOf[CometCostModel]
  }
}

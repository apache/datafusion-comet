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

package org.apache.comet.planner

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * State threaded through CometPlanner's three phases. Holds session-scoped data (configs, the
 * active SparkSession), a few plan-wide flags computed once at the top of `apply` so per-node
 * phases do not re-walk the whole tree, plus the broadcast consumer index computed in Phase 1 and
 * consulted during Phase 2's broadcast decision.
 *
 * Constructed once per CometPlanner.apply invocation. Not shared across invocations because AQE
 * re-entries reset session state.
 */
case class PlanningContext(
    session: SparkSession,
    conf: SQLConf,
    broadcastConsumers: BroadcastConsumerIndex,
    hasInputFileExpressions: Boolean)

/**
 * Index of BroadcastExchangeExec instances to whether a likely-Comet consumer (a
 * BroadcastHashJoinExec that would itself be LIKELY_COMET) exists in the plan. Built once during
 * Phase 1 by walking the plan looking for joins that reference broadcast outputs.
 *
 * Replaces the convertNode retry-on-self-with-new-children pattern in the old CometExecRule:
 * instead of converting children then re-asking whether to convert the parent, Phase 2 just reads
 * the index.
 */
trait BroadcastConsumerIndex {
  def isConsumedByCometCandidate(broadcast: BroadcastExchangeExec): Boolean
}

object BroadcastConsumerIndex extends Logging {

  /** Empty index. Used as a starting placeholder or in tests. */
  val Empty: BroadcastConsumerIndex = new BroadcastConsumerIndex {
    override def isConsumedByCometCandidate(broadcast: BroadcastExchangeExec): Boolean = false
  }

  /**
   * Walks `plan` looking for `BroadcastHashJoinExec` nodes that would themselves be LIKELY_COMET
   * under the current configuration. For each such join, records every `BroadcastExchangeExec` it
   * references as a consumer. Handles the AQE wrappers (`BroadcastQueryStageExec`,
   * `ReusedExchangeExec`) that hide the raw broadcast between planning and execution.
   */
  def build(plan: SparkPlan, conf: SQLConf): BroadcastConsumerIndex = {
    val consumed = new java.util.IdentityHashMap[BroadcastExchangeExec, java.lang.Boolean]()
    plan.foreach {
      case bhj: BroadcastHashJoinExec if Phase1LikelyComet.isLikelyComet(bhj, conf) =>
        bhj.children.foreach(indexBroadcast(_, consumed))
      case _ =>
    }
    logDebug(s"BroadcastConsumerIndex built size=${consumed.size}")
    new BroadcastConsumerIndex {
      override def isConsumedByCometCandidate(broadcast: BroadcastExchangeExec): Boolean =
        consumed.containsKey(broadcast)
    }
  }

  private def indexBroadcast(
      node: SparkPlan,
      consumed: java.util.IdentityHashMap[BroadcastExchangeExec, java.lang.Boolean]): Unit =
    node match {
      case b: BroadcastExchangeExec =>
        consumed.put(b, java.lang.Boolean.TRUE)
      case BroadcastQueryStageExec(_, b: BroadcastExchangeExec, _) =>
        consumed.put(b, java.lang.Boolean.TRUE)
      case BroadcastQueryStageExec(_, ReusedExchangeExec(_, b: BroadcastExchangeExec), _) =>
        consumed.put(b, java.lang.Boolean.TRUE)
      case ReusedExchangeExec(_, b: BroadcastExchangeExec) =>
        consumed.put(b, java.lang.Boolean.TRUE)
      case _ =>
    }
}

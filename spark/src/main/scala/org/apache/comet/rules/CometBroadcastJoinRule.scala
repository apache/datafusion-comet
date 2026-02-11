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

package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions._
import org.apache.comet.serde.OperatorOuterClass

/**
 * Rule that transforms BroadcastHashJoinExec to CometBroadcastHashJoinExec in AQE mode.
 *
 * This rule runs in queryStageOptimizerRules AFTER PlanAdaptiveDynamicPruningFilters, allowing
 * DPP to find the broadcast join and create SubqueryBroadcastExec before Comet transforms it.
 *
 * CometExecRule defers BroadcastHashJoinExec transformation in AQE mode, and this rule completes
 * the transformation.
 */
case class CometBroadcastJoinRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf) || !CometConf.COMET_EXEC_ENABLED.get(conf) ||
      !CometConf.COMET_AQE_DPP_ENABLED.get(conf) ||
      CometConf.COMET_NATIVE_SCAN_IMPL.get(conf) != CometConf.SCAN_NATIVE_DATAFUSION) {
      return plan
    }

    plan.transformUp {
      case plan: BroadcastHashJoinExec if hasBroadcastChild(plan) =>
        transformBroadcastHashJoin(plan)
      case other =>
        other
    }
  }

  /** Check if plan has a broadcast exchange child (direct or wrapped in QueryStage) */
  private def hasBroadcastChild(plan: SparkPlan): Boolean = {
    plan.children.exists {
      case _: BroadcastExchangeExec => true
      case b: BroadcastQueryStageExec => b.broadcast.isInstanceOf[BroadcastExchangeExec]
      case _ => false
    }
  }

  private def transformBroadcastHashJoin(plan: SparkPlan): SparkPlan = {
    val join = plan.asInstanceOf[BroadcastHashJoinExec]

    // Transform children: BroadcastExchangeExec -> CometBroadcastExchangeExec
    // For BroadcastQueryStageExec, we need to handle the wrapped exchange
    val newChildren = join.children.map {
      case b: BroadcastExchangeExec =>
        convertBroadcastExchange(b).getOrElse(b)
      case bqs: BroadcastQueryStageExec =>
        // In AQE, the broadcast is wrapped in a query stage
        // Try to convert the underlying exchange
        bqs.broadcast match {
          case b: BroadcastExchangeExec =>
            convertBroadcastExchange(b) match {
              case Some(cometExchange) =>
                // Return a sink placeholder that wraps the query stage
                CometSinkPlaceHolder(
                  cometExchange.asInstanceOf[CometNativeExec].nativeOp,
                  bqs,
                  cometExchange)
              case None =>
                bqs
            }
          case _ =>
            bqs
        }
      case other => other
    }

    // Check if all children are now native
    if (newChildren.forall(_.isInstanceOf[CometNativeExec])) {
      val joinWithNewChildren = join.copy(left = newChildren(0), right = newChildren(1))
      convertBroadcastHashJoin(joinWithNewChildren).getOrElse(plan)
    } else {
      plan
    }
  }

  private def convertBroadcastExchange(b: BroadcastExchangeExec): Option[SparkPlan] = {
    if (!CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.get(conf)) {
      return None
    }

    // Check if children are CometNativeExec (unwrap WholeStageCodegen/InputAdapter if needed)
    val actualChildren = b.children
      .map {
        case wsc: WholeStageCodegenExec => wsc.child
        case other => other
      }
      .map {
        case ia: InputAdapter => ia.child
        case other => other
      }

    // Need to find the actual native exec under CometColumnarToRow
    val nativeChildren = actualChildren.flatMap {
      case c2r: CometColumnarToRowExec =>
        // Unwrap InputAdapter if present (from WholeStageCodegen)
        val actualChild = c2r.child match {
          case ia: InputAdapter => ia.child
          case other => other
        }
        Some(actualChild)
      case c2r: CometNativeColumnarToRowExec =>
        val actualChild = c2r.child match {
          case ia: InputAdapter => ia.child
          case other => other
        }
        Some(actualChild)
      case n: CometNativeExec =>
        Some(n)
      case _ =>
        None
    }

    if (nativeChildren.isEmpty || nativeChildren.size != b.children.size) {
      return None
    }

    // All native children found
    if (!nativeChildren.forall(_.isInstanceOf[CometNativeExec])) {
      return None
    }

    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(b.id)
    val childOp = nativeChildren.map(_.asInstanceOf[CometNativeExec].nativeOp)
    childOp.foreach(builder.addChildren)

    // Use the companion object's convert method
    val nativeChild = nativeChildren.head
    CometBroadcastExchangeExec
      .convert(b, builder, childOp: _*)
      .map { nativeOp =>
        CometSinkPlaceHolder(
          nativeOp,
          b,
          CometBroadcastExchangeExec(b, b.output, b.mode, nativeChild))
      }
  }

  private def convertBroadcastHashJoin(join: BroadcastHashJoinExec): Option[SparkPlan] = {
    if (!CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(conf)) {
      return None
    }

    // All children must be CometNativeExec for the join to be converted
    if (!join.children.forall(_.isInstanceOf[CometNativeExec])) {
      return None
    }

    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(join.id)
    val childOps = join.children.map(_.asInstanceOf[CometNativeExec].nativeOp)
    childOps.foreach(builder.addChildren)

    CometBroadcastHashJoinExec
      .convert(join, builder, childOps: _*)
      .map(nativeOp => CometBroadcastHashJoinExec.createExec(nativeOp, join))
  }
}

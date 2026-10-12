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

package org.apache.comet.local

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.comet.{CometNativeExec, CometRangeExec}
import org.apache.spark.sql.execution.{CollectLimitExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Message

import org.apache.comet.CometConf
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.shims.ShimCometStreaming

/**
 * Admission of whole queries to local execution. A query is admitted only as a whole, from the
 * plan Comet's ordinary conversion produces: one native block with no JVM input, whose operators
 * then run for all partitions in one native graph. Everything else keeps ordinary execution.
 */
private[comet] object CometLocalRule {

  /** At most this many partitions run in one graph. */
  private val MaxPartitions = 1024

  /** Expressions that call back into the JVM, which a local query does not do. */
  private val JvmCallbacks = Set(Expr.ExprStructCase.SUBQUERY, Expr.ExprStructCase.JVM_SCALAR_UDF)

  /** An already admitted local query, with or without its result boundary or limit wrapper. */
  def isLocal(plan: SparkPlan): Boolean = plan match {
    case _: CometLocalExec | _: CometLocalResultExec => true
    case limit: CollectLimitExec => isLocal(limit.child)
    case _ => false
  }

  /**
   * `plan` as a local query if it is admitted, and otherwise as `convert`, Comet's ordinary
   * conversion, produces it. A root `CollectLimitExec` (from `take`, `head` or `show`) is kept:
   * it reads the result through `CometLocalResultExec.executeTake`.
   */
  def apply(
      session: SparkSession,
      plan: SparkPlan,
      convert: SparkPlan => SparkPlan): SparkPlan = {
    if (!CometConf.COMET_EXEC_LOCAL_ENABLED
        .get(SQLConf.get) || !environmentAdmits(session, plan)) {
      return convert(plan)
    }
    val limit = plan match {
      case limit: CollectLimitExec if limit.offset == 0 => Some(limit)
      case _ => None
    }
    convert(limit.map(_.child).getOrElse(plan)) match {
      case block: CometNativeExec if admits(block) =>
        val partitions = block.collectLeaves().head.outputPartitioning.numPartitions
        val spec = LocalQuerySpec(block.serializedPlanOpt.plan.get, partitions, block.output.size)
        val local = CometLocalResultExec(
          CometLocalExec(block.output, spec, block.outputOrdering, block))
        limit.map(_.copy(child = local)).getOrElse(local)
      case converted if limit.isEmpty => converted
      case _ => convert(plan)
    }
  }

  private def environmentAdmits(session: SparkSession, plan: SparkPlan): Boolean = {
    session.sparkContext.isLocal &&
    !ShimCometStreaming.isStreamingPlan(plan) &&
    // As in CometRule's plan-only reporting, this identifies subquery preparation.
    !Thread
      .currentThread()
      .getStackTrace
      .exists(frame =>
        frame.getClassName == QueryExecution.getClass.getName &&
          frame.getMethodName == "prepareExecutedPlan")
  }

  /**
   * A single native block that a Spark task could run for each partition on its own: its root
   * carries the serialized plan, every other operator is native and part of the same block, its
   * only leaf is a range, and its expressions are deterministic and do not call back into the
   * JVM. Running all partitions in one graph then produces what the Spark tasks would.
   */
  private def admits(block: CometNativeExec): Boolean = {
    block.serializedPlanOpt.isDefined &&
    block.output.nonEmpty &&
    block.collectLeaves().map(_.getClass) == Seq(classOf[CometRangeExec]) &&
    block.collectFirst {
      case node: CometNativeExec if (node ne block) && node.serializedPlanOpt.isDefined => node
      case node if !node.isInstanceOf[CometNativeExec] => node
    }.isEmpty &&
    !block.exists(_.expressions.exists(e =>
      !e.deterministic || e.exists(_.isInstanceOf[SubqueryExpression]))) && {
      val partitions = block.collectLeaves().head.outputPartitioning.numPartitions
      partitions >= 1 && partitions <= MaxPartitions
    } &&
    withoutJvmCallbacks(block.nativeOp)
  }

  private def withoutJvmCallbacks(message: Message): Boolean = {
    val accepted = message match {
      case expression: Expr => !JvmCallbacks.contains(expression.getExprStructCase)
      case _ => true
    }
    accepted && message.getAllFields.asScala.forall { case (field, value) =>
      if (field.getJavaType != JavaType.MESSAGE) {
        true
      } else if (field.isRepeated) {
        value.asInstanceOf[java.util.List[Message]].asScala.forall(withoutJvmCallbacks)
      } else {
        withoutJvmCallbacks(value.asInstanceOf[Message])
      }
    }
  }
}

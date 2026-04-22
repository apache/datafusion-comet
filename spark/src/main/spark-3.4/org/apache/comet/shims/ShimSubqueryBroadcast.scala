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

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression}
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec}

trait ShimSubqueryBroadcast {

  def getSubqueryBroadcastIndices(sab: SubqueryAdaptiveBroadcastExec): Seq[Int] = {
    Seq(sab.index)
  }

  /** Same version shim for SubqueryBroadcastExec. */
  def getSubqueryBroadcastExecIndices(sub: SubqueryBroadcastExec): Seq[Int] = {
    Seq(sub.index)
  }

  /** Creates a SubqueryBroadcastExec with version-appropriate index parameter. */
  def createSubqueryBroadcastExec(
      name: String,
      indices: Seq[Int],
      buildKeys: Seq[Expression],
      child: SparkPlan): SubqueryBroadcastExec = {
    assert(indices.length == 1, s"Multi-index DPP not supported: indices=$indices")
    SubqueryBroadcastExec(name, indices.head, buildKeys, child)
  }

  /**
   * Resolves a SubqueryAdaptiveBroadcastExec DPP filter via reflection. Required on Spark 3.4
   * where injectQueryStageOptimizerRule is unavailable, so CometPlanAdaptiveDynamicPruningFilters
   * cannot convert the SAB before execution.
   *
   * On Spark 3.5+ this is dead code — the rule converts SAB to CometSubqueryBroadcastExec before
   * serializedPartitionData runs, so the SAB case never matches.
   */
  def resolveSubqueryAdaptiveBroadcast(
      e: InSubqueryExec,
      sab: SubqueryAdaptiveBroadcastExec): Unit = {
    val rows = sab.child.executeCollect()
    val indices = getSubqueryBroadcastIndices(sab)

    assert(
      indices.length == 1,
      s"Multi-index DPP not supported: indices=$indices. See SPARK-46946.")
    val buildKeyIndex = indices.head
    val buildKey = sab.buildKeys(buildKeyIndex)

    val colIndex = buildKey match {
      case attr: Attribute =>
        sab.child.output.indexWhere(_.exprId == attr.exprId)
      case Cast(attr: Attribute, _, _, _) =>
        sab.child.output.indexWhere(_.exprId == attr.exprId)
      case _ => buildKeyIndex
    }
    if (colIndex < 0) {
      throw new IllegalStateException(
        s"DPP build key '$buildKey' not found in ${sab.child.output.map(_.name)}")
    }

    val result = rows.map(_.get(colIndex, e.child.dataType))
    setInSubqueryResult(e, result)
  }

  /**
   * Sets InSubqueryExec's private result field via reflection. Required because
   * SubqueryAdaptiveBroadcastExec.executeCollect() throws UnsupportedOperationException and
   * InSubqueryExec has no public setter for result.
   */
  private def setInSubqueryResult(e: InSubqueryExec, result: Array[_]): Unit = {
    val fields = e.getClass.getDeclaredFields
    val resultField = fields
      .find(f => f.getName.endsWith("$result") && !f.getName.contains("Broadcast"))
      .getOrElse {
        throw new IllegalStateException(
          s"Cannot find 'result' field in ${e.getClass.getName}. " +
            "Spark version may be incompatible with Comet's DPP implementation.")
      }
    resultField.setAccessible(true)
    resultField.set(e, result)
  }
}

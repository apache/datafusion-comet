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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast}
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}

trait ShimSubqueryBroadcast {

  /**
   * Gets the build key indices from SubqueryAdaptiveBroadcastExec. Spark 3.x has `index: Int`,
   * Spark 4.x has `indices: Seq[Int]`.
   */
  def getSubqueryBroadcastIndices(sab: SubqueryAdaptiveBroadcastExec): Seq[Int] = {
    sab.indices
  }

  /**
   * Resolves DPP values from SubqueryAdaptiveBroadcastExec and sets them on InSubqueryExec.
   *
   * SubqueryAdaptiveBroadcastExec.executeCollect() throws, so we call child.executeCollect()
   * directly and use SAB's index to find the buildKey.
   */
  def resolveSubqueryAdaptiveBroadcast(
      sab: SubqueryAdaptiveBroadcastExec,
      e: InSubqueryExec): Unit = {
    val rows = sab.child.executeCollect()
    val indices = getSubqueryBroadcastIndices(sab)

    // SPARK-46946 changed index: Int to indices: Seq[Int] for future features
    // (null-safe equality, multiple predicates). Currently always one element.
    assert(
      indices.length == 1,
      s"Multi-index DPP not supported: indices=$indices. See SPARK-46946.")
    val buildKeyIndex = indices.head
    val buildKey = sab.buildKeys(buildKeyIndex)

    val colIndex = buildKey match {
      case attr: Attribute =>
        sab.child.output.indexWhere(_.exprId == attr.exprId)
      // DPP may cast partition column to match join key type
      case Cast(attr: Attribute, _, _, _) =>
        sab.child.output.indexWhere(_.exprId == attr.exprId)
      case _ => buildKeyIndex
    }
    if (colIndex < 0) {
      throw new IllegalStateException(
        s"DPP build key '$buildKey' not found in ${sab.child.output.map(_.name)}")
    }

    setInSubqueryResult(e, rows.map(_.get(colIndex, e.child.dataType)))
  }

  /**
   * Sets InSubqueryExec's private result field via reflection.
   *
   * Reflection is required because SubqueryAdaptiveBroadcastExec.executeCollect() throws,
   * InSubqueryExec has no public setter for result, and we can't replace e.plan since it's a val.
   */
  def setInSubqueryResult(e: InSubqueryExec, result: Array[_]): Unit = {
    val fields = e.getClass.getDeclaredFields
    // Field name is mangled by Scala compiler, e.g. "org$apache$...$InSubqueryExec$$result"
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

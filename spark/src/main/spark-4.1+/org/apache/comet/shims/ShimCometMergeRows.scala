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

import org.apache.spark.sql.comet.{CometExec, CometMergeRowsExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{MergeRowsExec, V2TableWriteExec}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.operator.CometMergeRows

/**
 * Spark 4.1+ derives row-level write summaries from semantic action counters. Native MergeRows is
 * safe when the enclosing Comet write path consumes those counters. Stock V2 writers still locate
 * the concrete Spark MergeRowsExec, so this shim restores that node before such a writer runs.
 */
object ShimCometMergeRows {
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[MergeRowsExec] -> CometMergeRows)

  private val summaryFallbackReason =
    "Spark 4.1+ stock V2 writer requires Spark MergeRowsExec to build MergeSummary"

  def preserveV2WriteMergeSummary(plan: SparkPlan): SparkPlan = plan match {
    case writer: V2TableWriteExec =>
      val (query, restored) = restoreMergeRows(writer.child)
      if (restored) writer.withNewChildren(Seq(query)) else writer
    case other => other
  }

  private def restoreMergeRows(plan: SparkPlan): (SparkPlan, Boolean) = plan match {
    case merge: CometMergeRowsExec =>
      val restored = merge.originalPlan.withNewChildren(merge.children)
      withFallbackReason(restored, summaryFallbackReason)
      restored -> true
    case other =>
      val restoredChildren = other.children.map(restoreMergeRows)
      if (!restoredChildren.exists(_._2)) {
        other -> false
      } else {
        val children = restoredChildren.map(_._1)
        val restored = other match {
          case comet: CometExec => comet.originalPlan.withNewChildren(children)
          case _ => other.withNewChildren(children)
        }
        restored -> true
      }
  }
}

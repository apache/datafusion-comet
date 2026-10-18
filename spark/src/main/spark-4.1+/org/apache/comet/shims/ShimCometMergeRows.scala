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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeRows}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.operator.CometMergeRows

/** Spark 4.1+ native MERGE requires a writer that consumes its semantic counters. */
object ShimCometMergeRows {
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[MergeRowsExec] -> CometMergeRows)

  private val summaryAware = TreeNodeTag[Boolean]("comet.mergeRows.summaryAware")

  // Clone before tagging: tag-only transforms can discard structurally equal copies. The tag
  // follows logical links through AQE replanning, where the enclosing writer is no longer present.
  def withNativeMergeSummary(query: LogicalPlan): LogicalPlan = {
    val tagged = query.clone()
    tagged.foreach {
      case merge: MergeRows => merge.setTagValue(summaryAware, true)
      case _ =>
    }
    tagged
  }

  def hasNativeMergeSummary(op: MergeRowsExec): Boolean =
    op.logicalLink.exists(_.getTagValue(summaryAware).contains(true))
}

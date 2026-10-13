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

import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Insert, Keep}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

/**
 * Spark 4.2 rewrites MERGE statements containing only NOT MATCHED actions as InsertOnlyMergeExec.
 * For multiple clauses its query contains a MergeRowsExec, but the outer write owns MergeSummary
 * independently of that child's metrics. Only that exact shape may run without native summary
 * support; general MergeRowsExec plans still require a summary-aware writer.
 */
object ShimCometInsertOnlyMerge {
  def canRunWithoutMergeSummary(op: MergeRowsExec): Boolean =
    !op.checkCardinality &&
      op.matchedInstructions.isEmpty &&
      op.notMatchedBySourceInstructions.isEmpty &&
      op.notMatchedInstructions.size > 1 &&
      op.notMatchedInstructions.forall {
        case Keep(Insert, _, _) => true
        case _ => false
      } &&
      op.isSourceRowPresent.semanticEquals(TrueLiteral) &&
      op.isTargetRowPresent.semanticEquals(FalseLiteral)
}

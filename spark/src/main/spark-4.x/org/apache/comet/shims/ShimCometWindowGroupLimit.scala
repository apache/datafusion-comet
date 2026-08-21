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

import org.apache.spark.sql.catalyst.expressions.{DenseRank, Rank, RowNumber}
import org.apache.spark.sql.comet.CometWindowGroupLimitExec.Fields
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowGroupLimitExec

import org.apache.comet.serde.OperatorOuterClass.RankLikeFunction

/**
 * Spark 4.x shim exposing `WindowGroupLimitExec` to the shared Comet code. Content mirrors the
 * Spark 3.5 shim; the split exists because the per-Spark-minor `spark-3.5/` and `spark-4.x/`
 * source dirs are activated by disjoint Maven profiles.
 */
object ShimCometWindowGroupLimit {

  def windowGroupLimitClass: Option[Class[_ <: SparkPlan]] = Some(classOf[WindowGroupLimitExec])

  def extract(op: SparkPlan): Option[Fields] = op match {
    case w: WindowGroupLimitExec =>
      val fn = w.rankLikeFunction match {
        case _: RowNumber => RankLikeFunction.RowNumber
        case _: Rank => RankLikeFunction.Rank
        case _: DenseRank => RankLikeFunction.DenseRank
        case _ =>
          // Future Spark releases could add a fourth rank-like function to
          // `InferWindowGroupLimit.support`. Return None so `convert` records a fallback
          // reason instead of throwing, keeping a working query working across upgrades.
          return None
      }
      Some(Fields(w.partitionSpec, w.orderSpec, fn, w.limit, w.mode.toString))
    case _ =>
      None
  }
}

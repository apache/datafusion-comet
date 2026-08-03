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
import org.apache.spark.sql.comet.CometWindowGroupLimit.{DenseRankKind, Fields, RankKind, RowNumberKind}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowGroupLimitExec

/**
 * Spark 3.5+ shim exposing `WindowGroupLimitExec` (SPARK-37099) to the shared Comet code without
 * causing the 3.4 build to fail on a missing class reference.
 */
object ShimCometWindowGroupLimit {

  def windowGroupLimitClass: Option[Class[_ <: SparkPlan]] = Some(classOf[WindowGroupLimitExec])

  def extract(op: SparkPlan): Option[Fields] = op match {
    case w: WindowGroupLimitExec =>
      val kind = w.rankLikeFunction match {
        case _: RowNumber => RowNumberKind
        case _: Rank => RankKind
        case _: DenseRank => DenseRankKind
        case other =>
          throw new IllegalStateException(
            s"Unexpected rank-like function in WindowGroupLimitExec: ${other.getClass.getName}")
      }
      Some(Fields(w.partitionSpec, w.orderSpec, kind, w.limit))
    case _ =>
      None
  }
}

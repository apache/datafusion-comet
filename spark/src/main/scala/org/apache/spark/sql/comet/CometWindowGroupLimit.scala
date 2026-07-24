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

package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}

/**
 * Common types used by [[CometWindowGroupLimitExec]] and the per-Spark-version
 * [[org.apache.comet.shims.ShimCometWindowGroupLimit]] shims. Kept out of the shim to avoid a
 * sealed-trait-per-shim class-identity mismatch.
 */
object CometWindowGroupLimit {

  /** Which rank-like function drives the top-K semantics. Mirrors Spark's supported set. */
  sealed trait RankLikeKind
  case object RowNumberKind extends RankLikeKind
  case object RankKind extends RankLikeKind
  case object DenseRankKind extends RankLikeKind

  /** Fields extracted from a Spark `WindowGroupLimitExec` (Spark 3.5+). */
  case class Fields(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      rankLikeKind: RankLikeKind,
      limit: Int)
}

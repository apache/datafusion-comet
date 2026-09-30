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
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AQEShuffleReadRule, CoalesceShufflePartitions, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleOrigin
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}

/**
 * Coalesces the shuffle partitions below a Comet operator that Spark's CoalesceShufflePartitions
 * coalesces child by child but does not recognize.
 *
 * Spark coalesces each child of a `UnionExec` as a group of its own, and from Spark 4.0 each
 * child of a `CartesianProductExec`, `BroadcastHashJoinExec` or `BroadcastNestedLoopJoinExec`
 * too. It matches those classes, and the Comet operators that replace them are other classes, so
 * it falls through to the case that coalesces only when every leaf below the operator is an
 * exchange stage. A union with a scan or a table-cache stage in one branch then keeps every
 * partition of the shuffles in the others: `spark.sql.shuffle.partitions` tasks for a query that
 * needs a few.
 *
 * Comet replaces these operators while AQE prepares a stage, before its optimizer rules run, and
 * plans the operators above them against the Comet versions. So this runs after Spark's rule
 * instead, on each such operator whose shuffle stages that rule left untouched. It rebuilds the
 * Spark operator each Comet one replaced over the Comet children, has Spark's own rule coalesce
 * that, and swaps the Comet operators back in. The partitions come out as Spark would have
 * coalesced them, down to which operators count, since it is Spark's code deciding. The one
 * difference is that Spark divides its minimum partition count among the coalesce groups of the
 * whole plan, and this among those below the Comet operator, which are usually all of them.
 *
 * When every leaf below such an operator is an exchange stage, Spark's rule already coalesces its
 * shuffles, together rather than child by child, and this leaves them as they are.
 *
 * Extending `AQEShuffleReadRule` gets this the same treatment from AQE as Spark's rule: it is
 * skipped for the final stage when that stage's shuffle optimizations are off, and its result is
 * discarded if it breaks a distribution required above it.
 */
case object CometCoalesceShufflePartitions extends AQEShuffleReadRule {

  // The Comet operator that a stand-in Spark operator was rebuilt from.
  private val COMET_OPERATOR = TreeNodeTag[SparkPlan]("cometCoalesceShufflePartitions")

  // Required by the trait. Which shuffles are coalesced is decided by Spark's rule, which applies
  // its own list.
  override protected def supportedShuffleOrigins: Seq[ShuffleOrigin] =
    CoalesceShufflePartitions(SparkSession.active).supportedShuffleOrigins

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled || !plan.exists(replaced(_).isDefined)) {
      return plan
    }
    plan.transformDown {
      case p if replaced(p).isDefined && untouched(p) => coalesceBelow(p)
    }
  }

  // The Spark operator a Comet operator replaced, if Spark's rule coalesces its children one by
  // one. The class match mirrors Spark's, and Spark's rule decides, for its version, which of
  // these it actually treats that way.
  private def replaced(plan: SparkPlan): Option[SparkPlan] = plan match {
    case comet: CometExec =>
      comet.originalPlan match {
        case original @ (_: UnionExec | _: CartesianProductExec | _: BroadcastHashJoinExec |
            _: BroadcastNestedLoopJoinExec)
            if original.children.length == comet.children.length =>
          Some(original)
        case _ => None
      }
    case _ => None
  }

  // No AQE rule has put a read over any shuffle stage below `plan`: Spark's rule coalesced none
  // of them, and none is a skew-split or local read that coalescing now could disturb.
  private def untouched(plan: SparkPlan): Boolean =
    plan.exists(_.isInstanceOf[ShuffleQueryStageExec]) &&
      !plan.exists(_.isInstanceOf[AQEShuffleReadExec])

  private def coalesceBelow(plan: SparkPlan): SparkPlan = {
    val asSpark = plan.transformUp { case p =>
      replaced(p) match {
        case Some(original) =>
          val standIn = original.withNewChildren(p.children)
          // `withNewChildren` hands back the original itself when the children are the same ones,
          // and the tag must not land on the operator that the Comet one keeps.
          if (standIn eq original) {
            p
          } else {
            standIn.setTagValue(COMET_OPERATOR, p)
            standIn
          }
        case None => p
      }
    }
    val coalesced = CoalesceShufflePartitions(SparkSession.active).apply(asSpark)
    if (coalesced eq asSpark) plan else restore(coalesced)
  }

  // Put each Comet operator back over the children of its stand-in. Rebuilt by hand rather than
  // with transformUp, which copies a replaced node's tags onto a replacement that has none, and so
  // could leave the stand-in's tag on the Comet operator.
  private def restore(plan: SparkPlan): SparkPlan = {
    val children = plan.children.map(restore)
    plan.getTagValue(COMET_OPERATOR) match {
      case Some(comet) => comet.withNewChildren(children)
      case None => plan.withNewChildren(children)
    }
  }
}

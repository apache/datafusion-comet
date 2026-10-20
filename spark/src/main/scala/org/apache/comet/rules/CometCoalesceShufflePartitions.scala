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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AQEShuffleReadRule, CoalesceShufflePartitions, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleOrigin
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/**
 * Coalesces the shuffle partitions of a query stage that Spark's CoalesceShufflePartitions leaves
 * alone because Comet operators stand where it looks for Spark ones.
 *
 * Spark coalesces each child of a `UnionExec` as a group of its own, and from Spark 3.5 each
 * child of a `CartesianProductExec`, `BroadcastHashJoinExec` or `BroadcastNestedLoopJoinExec`
 * too. It matches those classes, and the Comet operators that replace them are other classes, so
 * it falls through to the case that coalesces only when every leaf below the operator is an
 * exchange stage. A union with a scan or a table-cache stage in one branch then keeps every
 * partition of the shuffles in the others: `spark.sql.shuffle.partitions` tasks for a query that
 * needs a few.
 *
 * Comet replaces these operators while AQE prepares a stage, before its optimizer rules run, so
 * this runs after Spark's rule instead. It rebuilds the Spark operator that each such Comet
 * operator replaced, over the Comet children, has Spark's own rule coalesce the whole stage, and
 * swaps the Comet operators back in. Spark's code makes every decision, for its version, so the
 * partitions come out as Spark would have coalesced them, including the smaller target size it
 * uses below a Cartesian product or a nested loop join from Spark 4.0. A shuffle that Spark's
 * rule has coalesced already, or that another AQE rule reads its own way, stays as it is.
 *
 * It rebuilds only operators whose output partitioning is unknown. Spark can coalesce the
 * children of a union differently, and from Spark 4.1 a union whose children share a partitioning
 * reports it, so an aggregate above can rely on it instead of a shuffle. AQE discards a
 * coalescing that breaks a distribution an operator requires, but Comet's operators state none,
 * so the aggregate would read one key from more than one partition. A broadcast join reports the
 * partitioning of its streamed side, so the same applies to it.
 *
 * When every leaf below such an operator is an exchange stage, Spark's rule already coalesces its
 * shuffles, together rather than child by child, and this leaves them as they are. It steps in
 * there only when Spark's rule cannot coalesce them together, as when they differ in partition
 * count or one of them is a single-partition shuffle.
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
    if (!conf.coalesceShufflePartitionsEnabled ||
      !plan.exists(_.isInstanceOf[ShuffleQueryStageExec])) {
      return plan
    }
    val asSpark = plan.transformDown { case comet @ Replaced(original) =>
      standIn(comet, original)
    }
    if (asSpark eq plan) {
      return plan
    }
    // A read over a shuffle stage means an AQE rule has already decided how to read it: Spark's
    // rule just coalesced it, below a Cartesian product say, or it is a skew-split or local read.
    // Spark's rule expects to coalesce only reads that split a skewed partition, so hide each read
    // behind a leaf that is not an exchange stage. Spark's rule then leaves alone every shuffle it
    // would coalesce together with a read one, and coalesces the others. The hidden groups no
    // longer count when it shares the minimum number of partitions among groups, so the others
    // can keep more partitions than on Spark, never fewer.
    val withDecidedReads = asSpark.transformUp { case read: AQEShuffleReadExec =>
      DecidedRead(read)
    }
    val coalesced = CoalesceShufflePartitions(SparkSession.active).apply(withDecidedReads)
    if (coalesced eq withDecidedReads) plan else restore(coalesced)
  }

  // A Comet operator whose Spark original's children Spark's rule coalesces one by one, with that
  // original. The class match mirrors Spark's, and Spark's rule decides, for its version, which
  // of these it actually treats that way. Comet has no counterpart of `CartesianProductExec`.
  private object Replaced {
    def unapply(plan: SparkPlan): Option[SparkPlan] = plan match {
      case comet: CometExec if comet.outputPartitioning.isInstanceOf[UnknownPartitioning] =>
        comet.originalPlan match {
          case original @ (_: UnionExec | _: BroadcastHashJoinExec |
              _: BroadcastNestedLoopJoinExec)
              if original.children.length == comet.children.length =>
            Some(original)
          case _ => None
        }
      case _ => None
    }
  }

  private def standIn(comet: SparkPlan, original: SparkPlan): SparkPlan = {
    val standIn = original.withNewChildren(comet.children) match {
      // `withNewChildren` hands back the original itself when its children are these already,
      // as for a union that AQE planned again over materialized stages. The tag must not land on
      // the operator that the Comet one keeps, so copy it.
      case same if same eq original =>
        original.makeCopy(original.productIterator.map(_.asInstanceOf[AnyRef]).toArray)
      case copy => copy
    }
    standIn.setTagValue(COMET_OPERATOR, comet)
    standIn
  }

  // Stands in for a read that an AQE rule has already decided on while Spark's rule runs.
  private case class DecidedRead(read: AQEShuffleReadExec) extends LeafExecNode {
    override def output: Seq[Attribute] = read.output

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException(s"$nodeName never runs")
  }

  // Put each Comet operator back over the children of its stand-in, and each decided read back in
  // place of its leaf. Rebuilt by hand rather than with transformUp, which copies a replaced
  // node's tags onto a replacement that has none, and so could leave the stand-in's tag on the
  // Comet operator.
  private def restore(plan: SparkPlan): SparkPlan = plan match {
    case DecidedRead(read) => read
    case _ =>
      val children = plan.children.map(restore)
      plan.getTagValue(COMET_OPERATOR) match {
        case Some(comet) => comet.withNewChildren(children)
        case None => plan.withNewChildren(children)
      }
  }
}

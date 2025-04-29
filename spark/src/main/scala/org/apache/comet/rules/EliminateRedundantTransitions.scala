package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometCollectLimitExec, CometColumnarToRowExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec

import scala.annotation.tailrec

// This rule is responsible for eliminating redundant transitions between row-based and
// columnar-based operators for Comet. Currently, three potential redundant transitions are:
// 1. `ColumnarToRowExec` on top of an ending `CometCollectLimitExec` operator, which is
//    redundant as `CometCollectLimitExec` already wraps a `ColumnarToRowExec` for row-based
//    output.
// 2. Consecutive operators of `CometSparkToColumnarExec` and `ColumnarToRowExec`.
// 3. AQE inserts an additional `CometSparkToColumnarExec` in addition to the one inserted in the
//    original plan.
//
// Note about the first case: The `ColumnarToRowExec` was added during
// ApplyColumnarRulesAndInsertTransitions' insertTransitions phase when Spark requests row-based
// output such as a `collect` call. It's correct to add a redundant `ColumnarToRowExec` for
// `CometExec`. However, for certain operators such as `CometCollectLimitExec` which overrides
// `executeCollect`, the redundant `ColumnarToRowExec` makes the override ineffective.
//
// Note about the second case: When `spark.comet.sparkToColumnar.enabled` is set, Comet will add
// `CometSparkToColumnarExec` on top of row-based operators first, but the downstream operator
// only takes row-based input as it's a vanilla Spark operator(as Comet cannot convert it for
// various reasons) or Spark requests row-based output such as a `collect` call. Spark will adds
// another `ColumnarToRowExec` on top of `CometSparkToColumnarExec`. In this case, the pair could
// be removed.
case class EliminateRedundantTransitions(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val eliminatedPlan = plan transformUp {
      case ColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        if (sparkToColumnar.child.supportsColumnar) {
          // For Spark Columnar to Comet Columnar, we should keep the ColumnarToRowExec
          ColumnarToRowExec(sparkToColumnar.child)
        } else {
          // For Spark Row to Comet Columnar, we should remove ColumnarToRowExec
          // and CometSparkToColumnarExec
          sparkToColumnar.child
        }
      case c @ ColumnarToRowExec(child) if hasCometNativeChild(child) =>
        val op = CometColumnarToRowExec(child)
        if (c.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
        } else {
          c.logicalLink.foreach(op.setLogicalLink)
        }
        op
      case CometColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        sparkToColumnar.child
      case CometSparkToColumnarExec(child: CometSparkToColumnarExec) => child
      // Spark adds `RowToColumnar` under Comet columnar shuffle. But it's redundant as the
      // shuffle takes row-based input.
      case s @ CometShuffleExchangeExec(
      _,
      RowToColumnarExec(child),
      _,
      _,
      CometColumnarShuffle,
      _) =>
        s.withNewChildren(Seq(child))
    }

    eliminatedPlan match {
      case ColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case CometColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case other =>
        other
    }
  }

  private def hasCometNativeChild(op: SparkPlan): Boolean = {
    op match {
      case c: QueryStageExec => hasCometNativeChild(c.plan)
      case _ => op.exists(_.isInstanceOf[CometPlan])
    }
  }
}



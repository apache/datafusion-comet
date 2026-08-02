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

import java.util.IdentityHashMap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet.{CometCollectLimitExec, CometColumnarToRowExec, CometMapInBatchExec, CometNativeColumnarToRowExec, CometNativeWriteExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.comet.shims.{MapInBatchInfo, ShimCometMapInBatch}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.NativeOptIn
import org.apache.comet.shims.ShimSQLConf

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
case class EliminateRedundantTransitions(session: SparkSession)
    extends Rule[SparkPlan]
    with ShimCometMapInBatch
    with ShimSQLConf {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    // `hasCometNativeChild` scans the subtree below every `ColumnarToRowExec` it is asked about,
    // so stacked transitions rescan the same nodes and the combined cost is quadratic in the plan
    // size. The scan stops at the first Comet operator it finds, which keeps the common case
    // cheap, but a stack of transitions over a Comet-free subtree walks all of it every time. The
    // memo below makes that linear.
    //
    // It is keyed on identity rather than equality because `SparkPlan` equality and hashing are
    // themselves subtree walks, and it is scoped to a single rule invocation because the rule
    // instance lives for the whole session and must not retain plans. `transformUp` reuses the
    // identity of subtrees it does not rewrite, so a rebuilt node still hits the memo one level
    // down.
    val containsCometPlanMemo = new IdentityHashMap[SparkPlan, java.lang.Boolean]()

    val eliminatedPlan = plan transformUp {
      case ColumnarToRowExec(shuffleExchangeExec: CometShuffleExchangeExec)
          if plan.conf.adaptiveExecutionEnabled =>
        shuffleExchangeExec
      case ColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        if (sparkToColumnar.child.supportsColumnar) {
          // For Spark Columnar to Comet Columnar, we should keep the ColumnarToRowExec
          ColumnarToRowExec(sparkToColumnar.child)
        } else {
          // For Spark Row to Comet Columnar, we should remove ColumnarToRowExec
          // and CometSparkToColumnarExec
          sparkToColumnar.child
        }
      // Remove unnecessary transition for native writes
      // Write should be final operation in the plan
      case ColumnarToRowExec(nativeWrite: CometNativeWriteExec) =>
        nativeWrite
      case c @ ColumnarToRowExec(child) if hasCometNativeChild(child, containsCometPlanMemo) =>
        val op = createColumnarToRowExec(child)
        if (c.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
        } else {
          c.logicalLink.foreach(op.setLogicalLink)
        }
        op
      case CometColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        sparkToColumnar.child
      case CometNativeColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        sparkToColumnar.child
      case CometSparkToColumnarExec(child: CometSparkToColumnarExec) => child
      // Replace MapInBatchExec (PythonMapInArrowExec / MapInArrowExec / MapInPandasExec) that has
      // a ColumnarToRow child with CometMapInBatchExec, eliminating the input and output
      // UnsafeProjection copies and keeping the stage columnar. The matchers are
      // version-shimmed: Spark 3.4 / 3.5 return None (they lack the required APIs) and Spark
      // 4.1+ matches the renamed `MapInArrowExec`.
      //
      // Falls back to vanilla Spark when `spark.sql.execution.arrow.useLargeVarTypes` is enabled:
      // CometArrowPythonRunnerBase.copyVector does raw `setBytes` on each Arrow buffer, but Comet's
      // source string/binary vectors always use 4-byte offsets while the destination root is
      // allocated with 8-byte offsets when this conf is on. The buffer counts match but the
      // offset width does not, so a direct memcpy would corrupt the offsets.
      //
      // `EligibleMapInBatch` matches whenever the operator would run natively if the feature were
      // enabled. When it is disabled (the default) we leave the vanilla Spark operator in place
      // but annotate it with a non-fallback `[COMET-INFO]` hint so the user knows the native path
      // exists behind a config flag.
      case p @ EligibleMapInBatch(info, columnarChild) =>
        if (CometConf.COMET_PYARROW_UDF_ENABLED.get()) {
          CometMapInBatchExec(
            info.func,
            info.output,
            columnarChild,
            info.isBarrier,
            info.pythonEvalType)
        } else {
          withInfo(
            p,
            NativeOptIn.message(
              "PyArrow UDFs (mapInArrow/mapInPandas)",
              CometConf.COMET_PYARROW_UDF_ENABLED.key))
        }

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
      case CometNativeColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case other =>
        other
    }
  }

  /**
   * True if the subtree rooted at `op` contains a Comet operator. `QueryStageExec` and
   * `ReusedExchangeExec` are leaves for tree traversal, so the plan they wrap is unwrapped
   * explicitly, and only at the root of the checked subtree.
   */
  private def hasCometNativeChild(
      op: SparkPlan,
      memo: IdentityHashMap[SparkPlan, java.lang.Boolean]): Boolean = {
    op match {
      case c: QueryStageExec => hasCometNativeChild(c.plan, memo)
      case c: ReusedExchangeExec => hasCometNativeChild(c.child, memo)
      case _ => containsCometPlan(op, memo)
    }
  }

  /** Memoized equivalent of `op.exists(_.isInstanceOf[CometPlan])`. */
  private def containsCometPlan(
      op: SparkPlan,
      memo: IdentityHashMap[SparkPlan, java.lang.Boolean]): Boolean = {
    val cached = memo.get(op)
    if (cached != null) {
      cached.booleanValue()
    } else {
      val result = op.isInstanceOf[CometPlan] || op.children.exists(containsCometPlan(_, memo))
      memo.put(op, result)
      result
    }
  }

  /**
   * If the given plan is a Comet ColumnarToRow transition, returns the columnar child the Python
   * UDF operator can consume directly. By the time this rule runs the earlier
   * `hasCometNativeChild` arm has already rewritten any `ColumnarToRowExec` over a Comet columnar
   * source to one of the Comet variants, so vanilla `ColumnarToRowExec` cannot reach here on a
   * Comet-driven plan and is intentionally not handled.
   */
  private def extractColumnarChild(plan: SparkPlan): Option[SparkPlan] = plan match {
    case CometColumnarToRowExec(child) => Some(child)
    case CometNativeColumnarToRowExec(child) => Some(child)
    // Chained `mapInArrow(udf1).mapInArrow(udf2)`: by the time the outer operator is visited
    // (transformUp is bottom-up) the inner one has already become a `CometMapInBatchExec`, which
    // is itself columnar. There is no row transition between them to strip, so consume its
    // columnar output directly. Its flattened output vectors are `CometVector`s, exactly what
    // `CometMapInBatchExec`'s input path expects.
    case child: CometMapInBatchExec => Some(child)
    case _ => None
  }

  /**
   * Matches the plans that could run natively as `CometMapInBatchExec`, independent of whether
   * the `spark.comet.exec.pyarrowUdf.enabled` feature flag is set. The `transformUp` arm reads
   * that flag to decide between rewriting the operator and merely annotating it with an opt-in
   * hint. Single extractor so the matchers run once per visited plan. Returns `(info,
   * columnarChild)` where `columnarChild` is the Comet columnar producer that
   * `CometMapInBatchExec` will consume directly. Returns `None` (and the arm misses) when
   * `useLargeVarTypes` forces the fallback, when the plan is not one of the version-shimmed
   * MapInArrow / MapInPandas operators, or when the child is not a Comet columnar-to-row
   * transition we can strip.
   */
  private object EligibleMapInBatch {
    def unapply(plan: SparkPlan): Option[(MapInBatchInfo, SparkPlan)] = {
      if (arrowUseLargeVarTypes(plan.conf)) {
        None
      } else {
        matchMapInArrow(plan)
          .orElse(matchMapInPandas(plan))
          .flatMap(info => extractColumnarChild(info.child).map(child => (info, child)))
      }
    }
  }

  /**
   * Creates an appropriate columnar to row transition operator.
   *
   * If native columnar to row conversion is enabled and the schema is supported, uses
   * CometNativeColumnarToRowExec. Otherwise falls back to CometColumnarToRowExec.
   */
  private def createColumnarToRowExec(child: SparkPlan): SparkPlan = {
    val schema = child.schema
    val useNative = CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.get() &&
      CometNativeColumnarToRowExec.supportsSchema(schema)

    if (useNative) {
      CometNativeColumnarToRowExec(child)
    } else {
      CometColumnarToRowExec(child)
    }
  }
}

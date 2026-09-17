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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet.{CometCollectLimitExec, CometColumnarToRowExec, CometIcebergWriteExec, CometMapInBatchExec, CometNativeColumnarToRowExec, CometNativeWriteExec, CometPlan, CometSparkToColumnarExec}
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
    val eliminatedPlan = stripIcebergWriteInputTransition(plan) transformUp {
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
      case c @ ColumnarToRowExec(child) if hasCometNativeChild(child) =>
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
      // Native Comet string/binary vectors use 4-byte offsets. The IPC schema follows these
      // vectors, so the stream is internally consistent, but the worker would receive string /
      // binary instead of the large_string / large_binary input types requested by this conf.
      // Keep the fallback to preserve Spark's input type contract.
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

  private def hasCometNativeChild(op: SparkPlan): Boolean = {
    op match {
      case c: QueryStageExec => hasCometNativeChild(c.plan)
      case c: ReusedExchangeExec => hasCometNativeChild(c.child)
      case _ => op.exists(_.isInstanceOf[CometPlan])
    }
  }

  /**
   * `CometIcebergWriteExec` is row-based (it emits the serialised Iceberg commit message) but
   * consumes Arrow batches from its child over FFI, so Spark's
   * `ApplyColumnarRulesAndInsertTransitions` inserts a columnar-to-row transition *underneath*
   * it. Strip that transition so `doExecuteColumnar` sees the columnar child directly;
   * `CometIcebergNativeWrite.requiresNativeChildren` already guarantees the child was
   * Comet-native when the write was converted.
   *
   * The write deliberately does not tag itself as a `ColumnarToRowTransition` to suppress the
   * insertion: Spark leaves such a node untouched, so the whole subtree below the write is never
   * visited and the transitions the rest of that subtree needs are never inserted
   * (https://github.com/apache/datafusion-comet/issues/5689).
   *
   * This runs as a separate pass *before* the main `transformUp`, not as an arm inside it,
   * because the write's input transition has to be removed before the generic transition
   * cancellation can consume it. `transformUp` visits children first, so the
   * `ColumnarToRowExec(CometSparkToColumnarExec)` arm would otherwise rewrite the write's child
   * before the write itself is visited, and that arm is destructive at this boundary:
   *   - over a row source it drops the `CometSparkToColumnarExec` as well, leaving the write with
   *     a row child and no Arrow producer at all;
   *   - over a Spark-columnar source it keeps a `ColumnarToRowExec` but drops the Arrow bridge,
   *     so the write would be handed Spark `ColumnarVector`s where the FFI adapter requires
   *     `CometVector`s. That shape is reachable whenever `spark.comet.sparkToColumnar.enabled`
   *     admits the write's source: `CometSparkToColumnarExec.createExec` wraps it in a
   *     `CometScanWrapper` (a `CometNativeExec`, so `requiresNativeChildren` accepts it), and
   *     `CometExecRule` then unwraps the placeholder, leaving the bridge directly beneath the
   *     write.
   */
  private def stripIcebergWriteInputTransition(plan: SparkPlan): SparkPlan = plan.transform {
    case w: CometIcebergWriteExec =>
      stripColumnarToRow(w.child).map(child => w.withNewChildren(Seq(child))).getOrElse(w)
  }

  /**
   * Unwraps a columnar-to-row transition, returning the columnar child underneath it, or `None`
   * when `plan` is not such a transition.
   *
   * The plain `ColumnarToRowExec` is what Spark's insertion pass produces and is the only form
   * observed in the suites. The two Comet variants are handled as well because this rule is part
   * of `postColumnarTransitions` and AQE applies those rules once per materialised stage plus
   * once for the final plan, so it can be handed a plan whose transitions an earlier pass already
   * rewrote. Missing a variant would not be caught at planning time -- the write would fail at
   * runtime with `requires a columnar (Comet native) child`.
   */
  private def stripColumnarToRow(plan: SparkPlan): Option[SparkPlan] = plan match {
    case CometNativeColumnarToRowExec(child) => Some(child)
    case CometColumnarToRowExec(child) => Some(child)
    case ColumnarToRowExec(child) => Some(child)
    case _ => None
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
   * the `spark.comet.exec.pyarrowUDF.enabled` feature flag is set. The `transformUp` arm reads
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

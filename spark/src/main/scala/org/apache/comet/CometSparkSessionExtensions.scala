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

package org.apache.comet

import java.nio.ByteOrder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.rules.{CometExecRule, CometPlanAdaptiveDynamicPruningFilters, CometReuseSubquery, CometScanRule, EliminateRedundantTransitions}
import org.apache.comet.shims.ShimCometSparkSessionExtensions

/**
 * CometDriverPlugin will register an instance of this class with Spark.
 *
 * Comet rules are injected into Spark's rule pipeline at several extension points. The execution
 * order differs between AQE and non-AQE paths:
 *
 * Non-AQE (QueryExecution.preparations):
 * {{{
 *   1. PlanDynamicPruningFilters    -- Spark creates non-AQE DPP (SubqueryBroadcastExec)
 *   2. PlanSubqueries               -- Spark creates SubqueryExec for scalar subqueries
 *   3. EnsureRequirements            -- Spark inserts shuffles/sorts
 *   4. ApplyColumnarRulesAndInsertTransitions:
 *      a. preColumnarTransitions:   CometScanRule, CometExecRule
 *         - CometExecRule.convertSubqueryBroadcasts converts SubqueryBroadcastExec to
 *           CometSubqueryBroadcastExec for exchange reuse with Comet broadcasts
 *      b. insertTransitions:        ColumnarToRow/RowToColumnar added
 *      c. postColumnarTransitions:  EliminateRedundantTransitions
 *   5. ReuseExchangeAndSubquery     -- Spark deduplicates subqueries (sees Comet nodes)
 * }}}
 *
 * AQE (AdaptiveSparkPlanExec, Spark 3.5+):
 * {{{
 *   Initial plan:
 *     PlanAdaptiveSubqueries:       creates SubqueryAdaptiveBroadcastExec (SAB) for AQE DPP
 *     queryStagePreparationRules:   CometScanRule, CometExecRule
 *       - CometExecRule.convertSubqueryBroadcasts wraps SABs in
 *         CometSubqueryAdaptiveBroadcastExec to prevent Spark's
 *         PlanAdaptiveDynamicPruningFilters from replacing DPP with Literal.TrueLiteral
 *
 *   Per stage (optimizeQueryStage + postStageCreationRules):
 *     1. queryStageOptimizerRules:
 *        a. PlanAdaptiveDynamicPruningFilters (Spark) -- skips wrapped SABs
 *        b. ReuseAdaptiveSubquery (Spark)
 *        c. CometPlanAdaptiveDynamicPruningFilters   -- converts wrapped SABs to
 *           CometSubqueryBroadcastExec with BroadcastQueryStageExec for broadcast reuse
 *        d. CometReuseSubquery                       -- deduplicates converted subqueries
 *     2. postStageCreationRules -> ApplyColumnarRulesAndInsertTransitions:
 *        a. preColumnarTransitions: CometScanRule, CometExecRule (no-ops, already converted)
 *        b. insertTransitions
 *        c. postColumnarTransitions: EliminateRedundantTransitions
 * }}}
 *
 * On Spark 3.4, injectQueryStageOptimizerRule is unavailable. CometExecRule does not wrap SABs,
 * and CometPlanAdaptiveDynamicPruningFilters/CometReuseSubquery are not registered. AQE DPP scans
 * fall back to Spark so that Spark's PlanAdaptiveDynamicPruningFilters handles them natively
 * (with DPP).
 */
class CometSparkSessionExtensions
    extends (SparkSessionExtensions => Unit)
    with Logging
    with ShimCometSparkSessionExtensions {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar { session => CometScanColumnar(session) }
    extensions.injectColumnar { session => CometExecColumnar(session) }
    extensions.injectQueryStagePrepRule { session => CometScanRule(session) }
    extensions.injectQueryStagePrepRule { session => CometExecRule(session) }
    injectQueryStageOptimizerRuleShim(extensions, CometPlanAdaptiveDynamicPruningFilters)
    injectQueryStageOptimizerRuleShim(extensions, CometReuseSubquery)
  }

  case class CometScanColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometScanRule(session)
  }

  case class CometExecColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometExecRule(session)

    override def postColumnarTransitions: Rule[SparkPlan] =
      EliminateRedundantTransitions(session)
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)

  /**
   * Checks whether Comet extension should be loaded for Spark.
   */
  private[comet] def isCometLoaded(conf: SQLConf): Boolean = {
    if (isBigEndian) {
      logInfo("Comet extension is disabled because platform is big-endian")
      return false
    }
    if (!COMET_ENABLED.get(conf)) {
      logInfo(s"Comet extension is disabled, please turn on ${COMET_ENABLED.key} to enable it")
      return false
    }

    // We don't support INT96 timestamps written by Apache Impala in a different timezone yet
    if (conf.getConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION)) {
      logWarning(
        "Comet extension is disabled, because it currently doesn't support" +
          s" ${SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION} setting to true.")
      return false
    }

    try {
      // This will load the Comet native lib on demand, and if success, should set
      // `NativeBase.loaded` to true
      NativeBase.isLoaded
    } catch {
      case e: Throwable =>
        if (COMET_NATIVE_LOAD_REQUIRED.get(conf)) {
          throw new CometRuntimeException(
            "Error when loading native library. Please fix the error and try again, or fallback " +
              s"to Spark by setting ${COMET_ENABLED.key} to false",
            e)
        } else {
          logWarning(
            "Comet extension is disabled because of error when loading native lib. " +
              "Falling back to Spark",
            e)
        }
        false
    }
  }

  // Check whether Comet shuffle is enabled:
  // 1. `COMET_EXEC_SHUFFLE_ENABLED` is true
  // 2. `spark.shuffle.manager` is set to `CometShuffleManager`
  // 3. Off-heap memory is enabled || Spark/Comet unit testing
  def isCometShuffleEnabled(conf: SQLConf): Boolean =
    COMET_EXEC_SHUFFLE_ENABLED.get(conf) && isCometShuffleManagerEnabled(conf)

  def isCometShuffleManagerEnabled(conf: SQLConf): Boolean = {
    conf.contains("spark.shuffle.manager") && conf.getConfString("spark.shuffle.manager") ==
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
  }

  def isCometScan(op: SparkPlan): Boolean = {
    op.isInstanceOf[CometBatchScanExec] || op.isInstanceOf[CometScanExec]
  }

  def isSpark35Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.5"
  }

  def isSpark40Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.0"
  }

  /**
   * Whether we should override Spark memory configuration for Comet. This only returns true when
   * Comet native execution is enabled and/or Comet shuffle is enabled and Comet doesn't use
   * off-heap mode (unified memory manager).
   */
  def shouldOverrideMemoryConf(conf: SparkConf): Boolean = {
    val cometEnabled = getBooleanConf(conf, CometConf.COMET_ENABLED)
    val cometShuffleEnabled = getBooleanConf(conf, CometConf.COMET_EXEC_SHUFFLE_ENABLED)
    val cometExecEnabled = getBooleanConf(conf, CometConf.COMET_EXEC_ENABLED)
    val offHeapMode = CometSparkSessionExtensions.isOffHeapEnabled(conf)
    cometEnabled && (cometShuffleEnabled || cometExecEnabled) && !offHeapMode
  }

  /**
   * Determines required memory overhead in MB per executor process for Comet when running in
   * on-heap mode.
   */
  def getCometMemoryOverheadInMiB(sparkConf: SparkConf): Long = {
    if (isOffHeapEnabled(sparkConf)) {
      // when running in off-heap mode we use unified memory management to share
      // off-heap memory with Spark so do not add overhead
      return 0
    }
    ConfigHelpers.byteFromString(
      sparkConf.get(
        COMET_ONHEAP_MEMORY_OVERHEAD.key,
        COMET_ONHEAP_MEMORY_OVERHEAD.defaultValueString),
      ByteUnit.MiB)
  }

  private def getBooleanConf(conf: SparkConf, entry: ConfigEntry[Boolean]) =
    conf.getBoolean(entry.key, entry.defaultValue.get)

  /**
   * Calculates required memory overhead in bytes per executor process for Comet when running in
   * on-heap mode.
   */
  def getCometMemoryOverhead(sparkConf: SparkConf): Long = {
    ByteUnit.MiB.toBytes(getCometMemoryOverheadInMiB(sparkConf))
  }

  /**
   * Calculates required shuffle memory size in bytes per executor process for Comet when running
   * in on-heap mode.
   */
  def getCometShuffleMemorySize(sparkConf: SparkConf, conf: SQLConf = SQLConf.get): Long = {
    assert(!isOffHeapEnabled(sparkConf))

    val cometMemoryOverhead = getCometMemoryOverheadInMiB(sparkConf)

    val overheadFactor = COMET_ONHEAP_SHUFFLE_MEMORY_FACTOR.get(conf)

    val shuffleMemorySize = (overheadFactor * cometMemoryOverhead).toLong
    if (shuffleMemorySize > cometMemoryOverhead) {
      logWarning(
        s"Configured shuffle memory size $shuffleMemorySize is larger than Comet memory overhead " +
          s"$cometMemoryOverhead, using Comet memory overhead instead.")
      ByteUnit.MiB.toBytes(cometMemoryOverhead)
    } else {
      ByteUnit.MiB.toBytes(shuffleMemorySize)
    }
  }

  def isOffHeapEnabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean("spark.memory.offHeap.enabled", false)
  }

  /**
   * Record a fallback reason on a `TreeNode` (a Spark operator or expression) explaining why
   * Comet cannot accelerate it. Reasons recorded here are surfaced in extended explain output
   * (see `ExtendedExplainInfo`) and, when `COMET_LOG_FALLBACK_REASONS` is enabled, logged as
   * warnings. The reasons are also rolled up from child nodes so that the operator that remains
   * in the Spark plan carries the reasons from its converted-away subtree.
   *
   * Call this in any code path where Comet decides not to convert a given node - serde `convert`
   * methods returning `None`, unsupported data types, disabled configs, etc. Do not use this for
   * informational messages that are not fallback reasons: anything tagged here is treated by the
   * rules as a signal that the node falls back to Spark.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reason. Optional, may be null or empty - pass empty only when the call is used
   *   purely to roll up reasons from `exprs`.
   * @param exprs
   *   Child nodes whose own fallback reasons should be rolled up into `node`. Pass the
   *   sub-expressions or child operators whose failure caused `node` to fall back.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with fallback reasons attached (as a side effect on its tag map).
   */
  def withInfo[T <: TreeNode[_]](node: T, info: String, exprs: T*): T = {
    // support existing approach of passing in multiple infos in a newline-delimited string
    val infoSet = if (info == null || info.isEmpty) {
      Set.empty[String]
    } else {
      info.split("\n").toSet
    }
    withInfos(node, infoSet, exprs: _*)
  }

  /**
   * Record one or more fallback reasons on a `TreeNode` and roll up reasons from any child nodes.
   * This is the set-valued form of [[withInfo]]; see that overload for the full contract.
   *
   * Reasons are accumulated (never overwritten) on the node's `EXTENSION_INFO` tag and are
   * surfaced in extended explain output. When `COMET_LOG_FALLBACK_REASONS` is enabled, each new
   * reason is also emitted as a warning.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reasons for this node. May be empty when the call is used purely to roll up
   *   child reasons.
   * @param exprs
   *   Child nodes whose own fallback reasons should be rolled up into `node`.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with fallback reasons attached (as a side effect on its tag map).
   */
  def withInfos[T <: TreeNode[_]](node: T, info: Set[String], exprs: T*): T = {
    if (CometConf.COMET_LOG_FALLBACK_REASONS.get()) {
      for (reason <- info) {
        logWarning(s"Comet cannot accelerate ${node.getClass.getSimpleName} because: $reason")
      }
    }
    val existingNodeInfos = node.getTagValue(CometExplainInfo.EXTENSION_INFO)
    val newNodeInfo = (existingNodeInfos ++ exprs
      .flatMap(_.getTagValue(CometExplainInfo.EXTENSION_INFO))).flatten.toSet
    node.setTagValue(CometExplainInfo.EXTENSION_INFO, newNodeInfo ++ info)
    node
  }

  /**
   * Roll up fallback reasons from `exprs` onto `node` without adding a new reason of its own. Use
   * this when a parent operator is itself falling back and wants to preserve the reasons recorded
   * on its child expressions/operators so they appear together in explain output.
   *
   * @param node
   *   The parent operator or expression falling back to Spark.
   * @param exprs
   *   Child nodes whose fallback reasons should be aggregated onto `node`.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with the rolled-up reasons attached (as a side effect on its tag map).
   */
  def withInfo[T <: TreeNode[_]](node: T, exprs: T*): T = {
    withInfos(node, Set.empty, exprs: _*)
  }

  /**
   * True if any fallback reason has been recorded on `node` (via [[withInfo]] / [[withInfos]]).
   * Callers that need to short-circuit when a prior rule pass has already decided a node falls
   * back can use this as the sticky signal.
   */
  def hasExplainInfo(node: TreeNode[_]): Boolean = {
    node.getTagValue(CometExplainInfo.EXTENSION_INFO).exists(_.nonEmpty)
  }

}

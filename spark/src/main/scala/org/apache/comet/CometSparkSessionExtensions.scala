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
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometShuffleManager}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.iceberg.IcebergWriteStrategy
import org.apache.comet.rules.{CometExecRule, CometPlanAdaptiveDynamicPruningFilters, CometReuseSubquery, CometScanRule, CometSpark34AqeDppFallbackRule, EliminateRedundantTransitions, RevertNativeForTransitionHeavyStages}
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
 *      c. postColumnarTransitions:  RevertNativeForTransitionHeavyStages,
 *                                   EliminateRedundantTransitions
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
 *        c. postColumnarTransitions: RevertNativeForTransitionHeavyStages,
 *                                    EliminateRedundantTransitions
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
    // Pre-3.5 only: tag AQE DPP regions so the conversion rules below leave them Spark-native.
    // Registered before CometScanRule/CometExecRule so tags are in place when conversion runs.
    // No-op on Spark 3.5+; see CometSpark34AqeDppFallbackRule's class docstring.
    injectPreSpark35QueryStagePrepRuleShim(extensions, CometSpark34AqeDppFallbackRule)
    extensions.injectQueryStagePrepRule { session => CometScanRule(session) }
    extensions.injectQueryStagePrepRule { session => CometExecRule(session) }
    injectQueryStageOptimizerRuleShim(extensions, CometPlanAdaptiveDynamicPruningFilters)
    injectQueryStageOptimizerRuleShim(extensions, CometReuseSubquery)
    extensions.injectPlannerStrategy { session => IcebergWriteStrategy(session) }
  }

  case class CometScanColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometScanRule(session)
  }

  case class CometExecColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometExecRule(session)

    override def postColumnarTransitions: Rule[SparkPlan] = {
      val rules =
        Seq(RevertNativeForTransitionHeavyStages(session), EliminateRedundantTransitions(session))
      plan => rules.foldLeft(plan) { case (p, rule) => rule(p) }
    }
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)

  private val SHUFFLE_MANAGER_KEY = "spark.shuffle.manager"

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

    if (COMET_SHUFFLE_ENABLED.get(conf) && !isCometShuffleManagerEnabled(conf)) {
      logWarning(
        "Comet extension is disabled because spark.shuffle.manager is not set to " +
          s"${classOf[CometShuffleManager].getName} or " +
          s"${classOf[CometCelebornShuffleManager].getName}. " +
          "Comet provides limited benefit without its shuffle manager. " +
          s"Set ${COMET_SHUFFLE_ENABLED.key}=false to keep Comet enabled with " +
          "Spark's default shuffle manager.")
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

  def isCometShuffleEnabled(conf: SQLConf): Boolean =
    COMET_SHUFFLE_ENABLED.get(conf) && isCometShuffleManagerEnabled(conf) && {
      // Explicit native mode opts out of Celeborn's local fallback. Keep this restriction at the
      // common gate because CollectLimit and TakeOrdered bypass ordinary exchange planning.
      !isCometCelebornShuffleManagerEnabled(conf) ||
      (COMET_EXEC_ENABLED.get(conf) && COMET_SHUFFLE_MODE.get(conf) == "native")
    }

  def isCometCelebornShuffleManagerEnabled(conf: SQLConf): Boolean =
    conf.contains(SHUFFLE_MANAGER_KEY) &&
      conf.getConfString(SHUFFLE_MANAGER_KEY) == classOf[CometCelebornShuffleManager].getName

  def isCometShuffleManagerEnabled(conf: SQLConf): Boolean = {
    conf.contains(SHUFFLE_MANAGER_KEY) && {
      val manager = conf.getConfString(SHUFFLE_MANAGER_KEY)
      manager == classOf[CometShuffleManager].getName ||
      manager == classOf[CometCelebornShuffleManager].getName
    }
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

  def isSpark41Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.1"
  }

  def isSpark42Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.2"
  }

  /**
   * Whether we should override Spark memory configuration for Comet. This only returns true when
   * Comet native execution is enabled and/or Comet shuffle is enabled and Comet doesn't use
   * off-heap mode (unified memory manager).
   */
  def shouldOverrideMemoryConf(conf: SparkConf): Boolean = {
    val cometEnabled = getBooleanConf(conf, CometConf.COMET_ENABLED)
    val cometShuffleEnabled = getBooleanConf(conf, CometConf.COMET_SHUFFLE_ENABLED)
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

    val overheadFactor = COMET_SHUFFLE_JVM_MEMORY_FACTOR.get(conf)

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
   * (see `ExtendedExplainInfo`) and, when `COMET_EXPLAIN_FALLBACK_LOG_ENABLED` is enabled, logged
   * as warnings.
   *
   * Call this in any code path where Comet decides not to convert a given node - serde `convert`
   * methods returning `None`, unsupported data types, disabled configs, etc. Do not use this for
   * informational messages that are not fallback reasons: anything tagged here is treated by the
   * rules as a signal that the node falls back to Spark.
   *
   * Tag only the node that actually failed, and state a real reason. There is deliberately no way
   * to copy reasons from child nodes onto a parent: extended explain only walks plan nodes, so an
   * expression-level reason is lifted onto the enclosing operator centrally by
   * `CometExecRule.rollUpFallbackReasons` when that operator is left in the Spark plan. See
   * https://github.com/apache/datafusion-comet/issues/5230.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reason. Newline-delimited to record more than one reason.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with the fallback reason attached (as a side effect on its tag map).
   */
  def withFallbackReason[T <: TreeNode[_]](node: T, info: String): T = {
    // support existing approach of passing in multiple infos in a newline-delimited string
    val infoSet = if (info == null || info.isEmpty) {
      Set.empty[String]
    } else {
      info.split("\n").toSet
    }
    withFallbackReasons(node, infoSet)
  }

  /**
   * Record one or more fallback reasons on a `TreeNode`. This is the set-valued form of
   * [[withFallbackReason]]; see that overload for the full contract.
   *
   * Reasons are accumulated (never overwritten) on the node's `FALLBACK_REASONS` tag and are
   * surfaced in extended explain output. When `COMET_EXPLAIN_FALLBACK_LOG_ENABLED` is enabled,
   * each new reason is also emitted as a warning.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reasons for this node.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with fallback reasons attached (as a side effect on its tag map).
   */
  def withFallbackReasons[T <: TreeNode[_]](node: T, info: Set[String]): T = {
    if (CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.get()) {
      for (reason <- info) {
        logWarning(s"Comet cannot accelerate ${node.getClass.getSimpleName} because: $reason")
      }
    }
    val existingNodeInfos =
      node.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])
    node.setTagValue(CometExplainInfo.FALLBACK_REASONS, existingNodeInfos ++ info)
    node
  }

  /**
   * True if any fallback reason has been recorded on `node` (via [[withFallbackReason]] /
   * [[withFallbackReasons]]). Callers that need to short-circuit when a prior rule pass has
   * already decided a node falls back can use this as the sticky signal.
   *
   * This deliberately reads only the node's own tag. It is a planning control signal, not explain
   * output, so it must not observe reasons that merely exist somewhere in the node's expression
   * trees - see `CometExecRule.rollUpFallbackReasons`.
   */
  def hasFallbackReason(node: TreeNode[_]): Boolean = {
    node.getTagValue(CometExplainInfo.FALLBACK_REASONS).exists(_.nonEmpty)
  }

  /**
   * Record a purely informational message on a `TreeNode`. Unlike `withFallbackReason`, this does
   * NOT cause the node to fall back to Spark: the planning rules never read this tag. Messages
   * accumulate (never overwrite) on the node's `EXTENSION_INFO` tag and are surfaced in verbose
   * extended explain output under a `[COMET-INFO: ...]` label. Use this to point the user at a
   * faster or alternative path that is available but not currently selected, such as a native
   * implementation gated behind a config.
   */
  def withInfo[T <: TreeNode[_]](node: T, message: String): T = {
    appendTagValue(node, CometExplainInfo.EXTENSION_INFO, message)
  }

  /**
   * Record that `node` (typically an `Expression`) is routing through the JVM codegen dispatcher.
   * `CometExecRule.rollUpInfoMessages` collects the names across an operator's expression trees
   * and emits one combined `[COMET-INFO: ...]` segment.
   */
  def withCodegenDispatchExpr[T <: TreeNode[_]](node: T, name: String): T = {
    appendTagValue(node, CometExplainInfo.CODEGEN_DISPATCH_EXPRS, name)
  }

  /**
   * Record that `node` (typically an `Expression`) was lowered to a native DataFusion expression.
   * The native counterpart of [[withCodegenDispatchExpr]]: `CometExecRule.rollUpInfoMessages`
   * collects the names across an operator's expression trees onto the converted Comet plan node,
   * where extended explain reads them for expression coverage stats.
   */
  def withNativeExpr[T <: TreeNode[_]](node: T, name: String): T = {
    appendTagValue(node, CometExplainInfo.NATIVE_EXPRS, name)
  }

  /**
   * Add `value` to a `Set`-valued `TreeNodeTag`, accumulating rather than overwriting. Null and
   * empty values are dropped so callers do not have to guard. Shared by [[withInfo]] and the
   * expression coverage tags.
   */
  private def appendTagValue[T <: TreeNode[_]](
      node: T,
      tag: TreeNodeTag[Set[String]],
      value: String): T = {
    if (value != null && value.nonEmpty) {
      appendTagValues(node, tag, Set(value))
    }
    node
  }

  /** Bulk form of [[appendTagValue]], for lifting a whole name set onto another node. */
  private[comet] def appendTagValues[T <: TreeNode[_]](
      node: T,
      tag: TreeNodeTag[Set[String]],
      values: Set[String]): T = {
    if (values.nonEmpty) {
      node.setTagValue(tag, node.getTagValue(tag).getOrElse(Set.empty[String]) ++ values)
    }
    node
  }

}

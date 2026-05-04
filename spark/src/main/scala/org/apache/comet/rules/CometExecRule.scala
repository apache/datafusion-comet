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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Divide, DoubleLiteral, EqualNullSafe, EqualTo, Expression, FloatLiteral, GreaterThan, GreaterThanOrEqual, KnownFloatingPointNormalized, LessThan, LessThanOrEqual, NamedExpression, Remainder}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2CommandExec}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometExplainInfo, ExtendedExplainInfo}
import org.apache.comet.CometConf.{COMET_SPARK_TO_ARROW_ENABLED, COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST}
import org.apache.comet.CometSparkSessionExtensions._
import org.apache.comet.rules.CometExecRule.allExecs
import org.apache.comet.serde._
import org.apache.comet.serde.operator._
import org.apache.comet.shims.ShimSubqueryBroadcast

object CometExecRule {

  /**
   * Fully native operators.
   */
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(
      classOf[ProjectExec] -> CometProjectExec,
      classOf[FilterExec] -> CometFilterExec,
      classOf[LocalLimitExec] -> CometLocalLimitExec,
      classOf[GlobalLimitExec] -> CometGlobalLimitExec,
      classOf[ExpandExec] -> CometExpandExec,
      classOf[GenerateExec] -> CometExplodeExec,
      classOf[HashAggregateExec] -> CometHashAggregateExec,
      classOf[ObjectHashAggregateExec] -> CometObjectHashAggregateExec,
      classOf[BroadcastHashJoinExec] -> CometBroadcastHashJoinExec,
      classOf[ShuffledHashJoinExec] -> CometHashJoinExec,
      classOf[SortMergeJoinExec] -> CometSortMergeJoinExec,
      classOf[SortExec] -> CometSortExec,
      classOf[LocalTableScanExec] -> CometLocalTableScanExec,
      classOf[WindowExec] -> CometWindowExec)

  /**
   * Sinks that have a native plan of ScanExec.
   */
  val sinks: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(
      classOf[CoalesceExec] -> CometCoalesceExec,
      classOf[CollectLimitExec] -> CometCollectLimitExec,
      classOf[TakeOrderedAndProjectExec] -> CometTakeOrderedAndProjectExec,
      classOf[UnionExec] -> CometUnionExec)

  val allExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = nativeExecs ++ sinks

  /**
   * Tag set on a `ShuffleExchangeExec` that should be left as a plain Spark shuffle rather than
   * wrapped in `CometShuffleExchangeExec`. See `tagRedundantColumnarShuffle`.
   */
  val SKIP_COMET_SHUFFLE_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometShuffle")

  /**
   * Tag set on a `BroadcastExchangeExec` that should be left as a plain Spark broadcast rather
   * than converted to `CometBroadcastExchangeExec`. Written by [[CometSpark34AqeDppFallbackRule]]
   * on Spark < 3.5. See that rule's class docstring for the rationale.
   */
  val SKIP_COMET_BROADCAST_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometBroadcast")
}

/**
 * Spark physical optimizer rule for replacing Spark operators with Comet operators.
 */
case class CometExecRule(session: SparkSession)
    extends Rule[SparkPlan]
    with ShimSubqueryBroadcast {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  /**
   * Revert any `CometShuffleExchangeExec` with `CometColumnarShuffle` whose parent and child are
   * both non-Comet `HashAggregateExec` / `ObjectHashAggregateExec` operators back to the original
   * Spark `ShuffleExchangeExec`. This is the partial-final-aggregate pattern where Comet couldn't
   * convert either aggregate; keeping a columnar shuffle between them only adds
   * row->arrow->shuffle->arrow->row conversion overhead with no Comet consumer on either side.
   * See https://github.com/apache/datafusion-comet/issues/4004.
   *
   * The match is intentionally narrow (both sides must be row-based aggregates that remained JVM
   * after the main transform pass). Running the revert post-transform means we only fire when the
   * main conversion already decided to keep both aggregates JVM - we never create the dangerous
   * mixed mode where a Comet partial feeds a JVM final (see issue #1389).
   *
   * Correctness depends on running as part of `preColumnarTransitions`: if the revert ran after
   * Spark inserted `ColumnarToRowExec` between the aggregate and the columnar shuffle, the
   * pattern would no longer match (the shuffle would be separated from the aggregate by the
   * transition) and the unnecessary conversion could not be eliminated.
   *
   * The reverted shuffle is tagged with `SKIP_COMET_SHUFFLE_TAG` so both the AQE
   * `QueryStagePrepRule` pass and the `ColumnarRule` `preColumnarTransitions` pass leave it alone
   * on re-entry - AQE in particular re-runs the rule on each stage in isolation, where the outer
   * aggregate context is no longer visible and the shuffle would otherwise be re-wrapped as a
   * Comet columnar shuffle.
   */
  private def revertRedundantColumnarShuffle(plan: SparkPlan): SparkPlan = {
    def isAggregate(p: SparkPlan): Boolean =
      p.isInstanceOf[HashAggregateExec] || p.isInstanceOf[ObjectHashAggregateExec]

    def isRedundantShuffle(child: SparkPlan): Boolean = child match {
      case s: CometShuffleExchangeExec =>
        s.shuffleType == CometColumnarShuffle && isAggregate(s.child)
      case _ => false
    }

    plan.transform {
      case op if isAggregate(op) && op.children.exists(isRedundantShuffle) =>
        val newChildren = op.children.map {
          case s: CometShuffleExchangeExec
              if s.shuffleType == CometColumnarShuffle && isAggregate(s.child) =>
            val reverted =
              s.originalPlan.withNewChildren(Seq(s.child)).asInstanceOf[ShuffleExchangeExec]
            reverted.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
            logInfo(
              "Reverting Comet columnar shuffle to Spark shuffle between " +
                s"${op.getClass.getSimpleName} and ${s.child.getClass.getSimpleName} " +
                "(no Comet operator on either side to consume columnar output)")
            reverted
          case other => other
        }
        op.withNewChildren(newChildren)
    }
  }

  private def shouldSkipCometShuffle(s: ShuffleExchangeExec): Boolean =
    s.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined

  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s
      case s: ShuffleExchangeExec =>
        CometShuffleExchangeExec.shuffleSupported(s) match {
          case Some(CometNativeShuffle) =>
            // Switch to use Decimal128 regardless of precision, since Arrow native execution
            // doesn't support Decimal32 and Decimal64 yet.
            conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
            CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
          case Some(CometColumnarShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
          case None =>
            s
        }
    }
  }

  private def isCometNative(op: SparkPlan): Boolean = op.isInstanceOf[CometNativeExec]

  // spotless:off

  /**
   * Tries to transform a Spark physical plan into a Comet plan.
   *
   * This rule traverses bottom-up from the original Spark plan and for each plan node, there
   * are a few cases to consider:
   *
   * 1. The child(ren) of the current node `p` cannot be converted to native
   *   In this case, we'll simply return the original Spark plan, since Comet native
   *   execution cannot start from an arbitrary Spark operator (unless it is special node
   *   such as scan or sink such as shuffle exchange, union etc., which are wrapped by
   *   `CometScanWrapper` and `CometSinkPlaceHolder` respectively).
   *
   * 2. The child(ren) of the current node `p` can be converted to native
   *   There are two sub-cases for this scenario: 1) This node `p` can also be converted to
   *   native. In this case, we'll create a new native Comet operator for `p` and connect it with
   *   its previously converted child(ren); 2) This node `p` cannot be converted to native. In
   *   this case, similar to 1) above, we simply return `p` as it is. Its child(ren) would still
   *   be native Comet operators.
   *
   * After this rule finishes, we'll do another pass on the final plan to convert all adjacent
   * Comet native operators into a single native execution block. Please see where
   * `convertBlock` is called below.
   *
   * Here are a few examples:
   *
   *     Scan                       ======>             CometScan
   *      |                                                |
   *     Filter                                         CometFilter
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     Exchange                                       CometExchange
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *
   * Native execution doesn't necessarily have to start from `CometScan`:
   *
   *     Scan                       =======>            CometScan
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *      |                                                |
   *     HashAggregate                                  HashAggregate
   *      |                                                |
   *     Exchange                                       CometExchange
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *
   * A sink can also be Comet operators other than `CometExchange`, for instance `CometUnion`:
   *
   *     Scan   Scan                =======>          CometScan CometScan
   *      |      |                                       |         |
   *     Filter Filter                                CometFilter CometFilter
   *      |      |                                       |         |
   *        Union                                         CometUnion
   *          |                                               |
   *        Project                                       CometProject
   */
  // spotless:on
  private def transform(plan: SparkPlan): SparkPlan = {
    def convertNode(op: SparkPlan): SparkPlan = op match {
      // Fully native scan for V1
      case scan: CometScanExec if scan.scanImpl == CometConf.SCAN_NATIVE_DATAFUSION =>
        convertToComet(scan, CometNativeScan).getOrElse(scan)

      // Fully native Iceberg scan for V2 (iceberg-rust path)
      // Only handle scans with native metadata; other scans fall through to isCometScan
      // Config checks (COMET_ICEBERG_NATIVE_ENABLED, COMET_EXEC_ENABLED) are done in CometScanRule
      case scan: CometBatchScanExec if scan.nativeIcebergScanMetadata.isDefined =>
        convertToComet(scan, CometIcebergNativeScan).getOrElse(scan)

      case scan: CometBatchScanExec if scan.wrapped.scan.isInstanceOf[CSVScan] =>
        convertToComet(scan, CometCsvNativeScanExec).getOrElse(scan)

      // Comet JVM + native scan for V1 and V2
      case op if isCometScan(op) =>
        convertToComet(op, CometScanWrapper).getOrElse(op)

      case op if shouldApplySparkToColumnar(conf, op) =>
        convertToComet(op, CometSparkToColumnarExec).getOrElse(op)

      // AQE reoptimization looks for `DataWritingCommandExec` or `WriteFilesExec`
      // if there is none it would reinsert write nodes, and since Comet remap those nodes
      // to Comet counterparties the write nodes are twice to the plan.
      // Checking if AQE inserted another write Command on top of existing write command
      case _ @DataWritingCommandExec(_, w: WriteFilesExec)
          if w.child.isInstanceOf[CometNativeWriteExec] =>
        w.child

      case op: DataWritingCommandExec =>
        convertToComet(op, CometDataWritingCommand).getOrElse(op)

      // For AQE broadcast stage on a Comet broadcast exchange
      case s @ BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      case s @ BroadcastQueryStageExec(
            _,
            ReusedExchangeExec(_, _: CometBroadcastExchangeExec),
            _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // `CometBroadcastExchangeExec`'s broadcast output is not compatible with Spark's broadcast
      // exchange. It is only used for Comet native execution. We only transform Spark broadcast
      // exchange to Comet broadcast exchange if its downstream is a Comet native plan or if the
      // broadcast exchange is forced to be enabled by Comet config.
      case plan if plan.children.exists(_.isInstanceOf[BroadcastExchangeExec]) =>
        val newChildren = plan.children.map {
          // Tagged by CometSpark34AqeDppFallbackRule on Spark < 3.5 to keep the build-side
          // broadcast Spark-native so Spark's PlanAdaptiveDynamicPruningFilters can match it.
          case b: BroadcastExchangeExec
              if b.getTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG).isDefined =>
            b
          case b: BroadcastExchangeExec if b.children.forall(_.isInstanceOf[CometNativeExec]) =>
            convertToComet(b, CometBroadcastExchangeExec).getOrElse(b)
          case other => other
        }
        if (!newChildren.exists(_.isInstanceOf[BroadcastExchangeExec])) {
          val newPlan = convertNode(plan.withNewChildren(newChildren))
          if (isCometNative(newPlan) || CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.get(conf)) {
            newPlan
          } else {
            // copy fallback reasons to the original plan
            newPlan
              .getTagValue(CometExplainInfo.EXTENSION_INFO)
              .foreach(reasons => withInfos(plan, reasons))
            // return the original plan
            plan
          }
        } else {
          plan
        }

      // For AQE shuffle stage on a Comet shuffle exchange
      case s @ ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // For AQE shuffle stage on a reused Comet shuffle exchange
      // Note that we don't need to handle `ReusedExchangeExec` for non-AQE case, because
      // the query plan won't be re-optimized/planned in non-AQE mode.
      case s @ ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s

      case s: ShuffleExchangeExec =>
        convertToComet(s, CometShuffleExchangeExec).getOrElse(s)

      case op =>
        // if all children are native (or if this is a leaf node) then see if there is a
        // registered handler for creating a fully native plan
        if (op.children.forall(_.isInstanceOf[CometNativeExec])) {
          val handler = allExecs
            .get(op.getClass)
            .map(_.asInstanceOf[CometOperatorSerde[SparkPlan]])
          handler match {
            case Some(handler) =>
              return convertToComet(op, handler).getOrElse(op)
            case _ =>
          }
        }

        op match {
          case _: CometPlan | _: AQEShuffleReadExec | _: BroadcastExchangeExec |
              _: BroadcastQueryStageExec | _: AdaptiveSparkPlanExec | _: ExecutedCommandExec |
              _: V2CommandExec =>
            // Some execs should never be replaced. We include
            // these cases specially here so we do not add a misleading 'info' message
            op
          case _ =>
            // The operator was not converted to a Comet plan. Possible reasons for this happening:
            // 1. Comet does not support this operator.
            // 2. The operator could not be supported based on query context and current
            //    configs. In this case, it should have already been tagged with fallback
            //    reasons.
            // 3. The operator has children that could not be converted, so execution
            //    has already fallen back to Spark.
            if (op.children.forall(_.isInstanceOf[CometNativeExec]) && !hasExplainInfo(op)) {
              withInfo(op, s"${op.nodeName} is not supported")
            } else {
              op
            }
        }
    }

    plan.transformUp { case op =>
      val converted = convertNode(op)
      // Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in DPP expressions
      // when the broadcast child has a Comet plan underneath. This enables exchange reuse
      // between the DPP subquery and the join's CometBroadcastExchangeExec because both
      // will have the same CometBroadcastExchangeExec type and canonical form.
      convertSubqueryBroadcasts(converted)
    }
  }

  /**
   * Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in a node's expressions
   * (non-AQE DPP), and wrap SubqueryAdaptiveBroadcastExec in CometSubqueryAdaptiveBroadcastExec
   * (AQE DPP) to protect it from Spark's PlanAdaptiveDynamicPruningFilters.
   *
   * Non-AQE DPP: When CometExecRule converts BroadcastExchangeExec to CometBroadcastExchangeExec
   * on the join side, the DPP subquery still references the original BroadcastExchangeExec.
   * ReuseExchangeAndSubquery (which runs after Comet rules) can't match them because they have
   * different types. By replacing SubqueryBroadcastExec with CometSubqueryBroadcastExec (which
   * wraps a CometBroadcastExchangeExec), both sides have the same exchange type and reuse works.
   *
   * AQE DPP: Spark's PlanAdaptiveDynamicPruningFilters (queryStageOptimizerRule) pattern-matches
   * on SubqueryAdaptiveBroadcastExec. When it can't find BroadcastHashJoinExec (Comet replaced
   * it), it replaces DPP with Literal.TrueLiteral. We wrap SABs in
   * CometSubqueryAdaptiveBroadcastExec to prevent this. CometPlanAdaptiveDynamicPruningFilters (a
   * later queryStageOptimizerRule) unwraps and converts them with access to the materialized
   * BroadcastQueryStageExec.
   */
  private def convertSubqueryBroadcasts(plan: SparkPlan): SparkPlan = {
    // CometIcebergNativeScanExec.runtimeFilters is a top-level constructor field visible to
    // productIterator, so transformExpressionsUp rewrites it directly. The wrapped @transient
    // originalPlan still holds the pre-rewrite runtimeFilters; we don't sync it here because
    // CometIcebergNativeScanExec.serializedPartitionData rebuilds originalPlan from the
    // top-level runtimeFilters at serialization time (single source of truth).
    plan.transformExpressionsUp { case inSub: InSubqueryExec =>
      rewriteInSubqueryPlan(inSub)
    }
  }

  private def rewriteInSubqueryPlan(inSub: InSubqueryExec): Expression = {
    inSub.plan match {
      case sub: SubqueryBroadcastExec =>
        sub.child match {
          case b: BroadcastExchangeExec =>
            // The BroadcastExchangeExec child is CometNativeColumnarToRowExec wrapping
            // a Comet plan. Strip the row transition to get the columnar Comet plan.
            val cometChild = b.child match {
              case c2r: CometNativeColumnarToRowExec => c2r.child
              case other => other
            }
            if (cometChild.isInstanceOf[CometNativeExec]) {
              logInfo(
                "Converting SubqueryBroadcastExec to " +
                  "CometSubqueryBroadcastExec for DPP exchange reuse")
              val cometBroadcast = CometBroadcastExchangeExec(b, b.output, b.mode, cometChild)
              val cometSub = CometSubqueryBroadcastExec(
                sub.name,
                getSubqueryBroadcastExecIndices(sub),
                sub.buildKeys,
                cometBroadcast)
              inSub.withNewPlan(cometSub)
            } else {
              inSub
            }
          case _ => inSub
        }
      case sab: SubqueryAdaptiveBroadcastExec if isSpark35Plus =>
        // Wrap SABs to prevent Spark's PlanAdaptiveDynamicPruningFilters from
        // converting them to Literal.TrueLiteral. Spark's rule pattern-matches for
        // BroadcastHashJoinExec, which Comet replaced with CometBroadcastHashJoinExec.
        // Without wrapping, DPP is disabled for both Comet native scans and non-Comet
        // scans (e.g., V2 BatchScan). CometPlanAdaptiveDynamicPruningFilters
        // (queryStageOptimizerRule, 3.5+) unwraps and converts them later.
        //
        // On Spark 3.4, injectQueryStageOptimizerRule is unavailable. The isSpark35Plus
        // guard leaves SABs unwrapped; CometSpark34AqeDppFallbackRule then tags the
        // matching BHJ's build broadcast so Spark's rule can match it natively.
        assert(
          sab.buildKeys.nonEmpty,
          s"SubqueryAdaptiveBroadcastExec '${sab.name}' has empty buildKeys")
        logInfo(
          s"Wrapping SubqueryAdaptiveBroadcastExec '${sab.name}' in " +
            "CometSubqueryAdaptiveBroadcastExec to preserve AQE DPP")
        val indices = getSubqueryBroadcastIndices(sab)
        val wrapped = CometSubqueryAdaptiveBroadcastExec(
          sab.name,
          indices,
          sab.onlyInBroadcast,
          sab.buildPlan,
          sab.buildKeys,
          sab.child)
        inSub.withNewPlan(wrapped)
      case _ => inSub
    }
  }

  private def normalizePlan(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p: ProjectExec =>
        val newProjectList = p.projectList.map(normalize(_).asInstanceOf[NamedExpression])
        ProjectExec(newProjectList, p.child)
      case f: FilterExec =>
        val newCondition = normalize(f.condition)
        FilterExec(newCondition, f.child)
    }
  }

  // Spark will normalize NaN and zero for floating point numbers for several cases.
  // See `NormalizeFloatingNumbers` optimization rule in Spark.
  // However, one exception is for comparison operators. Spark does not normalize NaN and zero
  // because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the
  // comparison functions in arrow-rs do not normalize NaN and zero. So we need to normalize NaN
  // and zero for comparison operators in Comet.
  private def normalize(expr: Expression): Expression = {
    expr.transformUp {
      case EqualTo(left, right) =>
        EqualTo(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case EqualNullSafe(left, right) =>
        EqualNullSafe(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case GreaterThan(left, right) =>
        GreaterThan(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case GreaterThanOrEqual(left, right) =>
        GreaterThanOrEqual(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case LessThan(left, right) =>
        LessThan(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case LessThanOrEqual(left, right) =>
        LessThanOrEqual(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case Divide(left, right, evalMode) =>
        Divide(left, normalizeNaNAndZero(right), evalMode)
      case Remainder(left, right, evalMode) =>
        Remainder(left, normalizeNaNAndZero(right), evalMode)
    }
  }

  private def normalizeNaNAndZero(expr: Expression): Expression = {
    expr match {
      case _: KnownFloatingPointNormalized => expr
      case FloatLiteral(f) if !f.equals(-0.0f) => expr
      case DoubleLiteral(d) if !d.equals(-0.0d) => expr
      case _ =>
        expr.dataType match {
          case _: FloatType | _: DoubleType =>
            KnownFloatingPointNormalized(NormalizeNaNAndZero(expr))
          case _ => expr
        }
    }
  }

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
    // We shouldn't transform Spark query plan if Comet is not loaded.
    if (!isCometLoaded(conf)) return plan

    if (!CometConf.COMET_EXEC_ENABLED.get(conf)) {
      // Comet exec is disabled, but for Spark shuffle, we still can use Comet columnar shuffle
      if (isCometShuffleEnabled(conf)) {
        applyCometShuffle(plan)
      } else {
        plan
      }
    } else {
      val normalizedPlan = normalizePlan(plan)

      val planWithJoinRewritten = if (CometConf.COMET_REPLACE_SMJ.get()) {
        normalizedPlan.transformUp { case p =>
          RewriteJoin.rewrite(p)
        }
      } else {
        normalizedPlan
      }

      var newPlan = transform(planWithJoinRewritten)

      // if the plan cannot be run fully natively then explain why (when appropriate
      // config is enabled)
      if (CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get()) {
        val info = new ExtendedExplainInfo()
        if (info.extensionInfo(newPlan).nonEmpty) {
          logWarning(
            "Comet cannot execute some parts of this plan natively " +
              s"(set ${CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key}=false " +
              "to disable this logging):\n" +
              s"${info.generateExtendedInfo(newPlan)}")
        }
      }

      // Remove placeholders
      newPlan = newPlan.transform {
        case CometSinkPlaceHolder(_, _, s) => s
        case CometScanWrapper(_, s) => s
      }

      // Revert CometColumnarShuffle to Spark's ShuffleExchangeExec when both its parent and child
      // are non-Comet HashAggregate/ObjectHashAggregate operators that remained JVM after the main
      // transform pass. See https://github.com/apache/datafusion-comet/issues/4004.
      if (CometConf.COMET_EXEC_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.get()) {
        newPlan = revertRedundantColumnarShuffle(newPlan)
      }

      // Set up logical links
      newPlan = newPlan.transform {
        case op: CometExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
        case op: CometShuffleExchangeExec =>
          // Original Spark shuffle exchange operator might have empty logical link.
          // But the `setLogicalLink` call above on downstream operator of
          // `CometShuffleExchangeExec` will set its logical link to the downstream
          // operators which cause AQE behavior to be incorrect. So we need to unset
          // the logical link here.
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op

        case op: CometBroadcastExchangeExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
      }

      // Convert native execution block by linking consecutive native operators.
      var firstNativeOp = true
      newPlan.transformDown {
        case op: CometNativeExec =>
          val newPlan = if (firstNativeOp) {
            firstNativeOp = false
            op.convertBlock()
          } else {
            op
          }

          // If reaching leaf node, reset `firstNativeOp` to true
          // because it will start a new block in next iteration.
          if (op.children.isEmpty) {
            firstNativeOp = true
          }

          // CometNativeWriteExec is special: it has two separate plans:
          // 1. A protobuf plan (nativeOp) describing the write operation
          // 2. A Spark plan (child) that produces the data to write
          // The serializedPlanOpt is a def that always returns Some(...) by serializing
          // nativeOp on-demand, so it doesn't need convertBlock(). However, its child
          // (e.g., CometNativeScanExec) may need its own serialization. Reset the flag
          // so children can start their own native execution blocks.
          if (op.isInstanceOf[CometNativeWriteExec]) {
            firstNativeOp = true
          }

          newPlan
        case op =>
          firstNativeOp = true
          op
      }
    }
  }

  /** Convert a Spark plan to a Comet plan using the specified serde handler */
  private def convertToComet(op: SparkPlan, handler: CometOperatorSerde[_]): Option[SparkPlan] = {
    val serde = handler.asInstanceOf[CometOperatorSerde[SparkPlan]]
    if (isOperatorEnabled(serde, op)) {
      // For operators that require native children (like writes), check if all data-producing
      // children are CometNativeExec. This prevents runtime failures when the native operator
      // expects Arrow arrays but receives non-Arrow data (e.g., OnHeapColumnVector).
      if (serde.requiresNativeChildren && op.children.nonEmpty) {
        // Get the actual data-producing children (unwrap WriteFilesExec if present)
        val dataProducingChildren = op.children.flatMap {
          case writeFiles: WriteFilesExec => Seq(writeFiles.child)
          case other => Seq(other)
        }
        if (!dataProducingChildren.forall(_.isInstanceOf[CometNativeExec])) {
          withInfo(op, "Cannot perform native operation because input is not in Arrow format")
          return None
        }
      }

      val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
      if (op.children.nonEmpty && op.children.forall(_.isInstanceOf[CometNativeExec])) {
        val childOp = op.children.map(_.asInstanceOf[CometNativeExec].nativeOp)
        childOp.foreach(builder.addChildren)
        return serde
          .convert(op, builder, childOp: _*)
          .map(nativeOp => serde.createExec(nativeOp, op))
      } else {
        return serde
          .convert(op, builder)
          .map(nativeOp => serde.createExec(nativeOp, op))
      }
    }
    None
  }

  private def isOperatorEnabled(
      handler: CometOperatorSerde[SparkPlan],
      op: SparkPlan): Boolean = {
    val opName = op.getClass.getSimpleName
    if (handler.enabledConfig.forall(_.get(op.conf))) {
      handler.getSupportLevel(op) match {
        case Unsupported(notes) =>
          withInfo(op, notes.getOrElse(""))
          false
        case Incompatible(notes) =>
          val allowIncompat = CometConf.isOperatorAllowIncompat(opName)
          val incompatConf = CometConf.getOperatorAllowIncompatConfigKey(opName)
          if (allowIncompat) {
            if (notes.isDefined) {
              logWarning(
                s"Comet supports $opName when $incompatConf=true " +
                  s"but has notes: ${notes.get}")
            }
            true
          } else {
            val optionalNotes = notes.map(str => s" ($str)").getOrElse("")
            withInfo(
              op,
              s"$opName is not fully compatible with Spark$optionalNotes. " +
                s"To enable it anyway, set $incompatConf=true. " +
                s"${CometConf.COMPAT_GUIDE}.")
            false
          }
        case Compatible(notes) =>
          if (notes.isDefined) {
            logWarning(s"Comet supports $opName but has notes: ${notes.get}")
          }
          true
      }
    } else {
      withInfo(
        op,
        s"Native support for operator $opName is disabled. " +
          s"Set ${handler.enabledConfig.get.key}=true to enable it.")
      false
    }
  }

  private def shouldApplySparkToColumnar(conf: SQLConf, op: SparkPlan): Boolean = {
    // Only consider converting leaf nodes to columnar currently, so that all the following
    // operators can have a chance to be converted to columnar. Leaf operators that output
    // columnar batches, such as Spark's vectorized readers, will also be converted to native
    // comet batches.
    val fallbackReasons = new ListBuffer[String]()
    if (CometSparkToColumnarExec.isSchemaSupported(op.schema, fallbackReasons)) {
      op match {
        // Convert Spark DS v1 scan to Arrow format
        case scan: FileSourceScanExec =>
          scan.relation.fileFormat match {
            case _: CSVFileFormat => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonFileFormat => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetFileFormat => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // Convert Spark DS v2 scan to Arrow format
        case scan: BatchScanExec =>
          scan.scan match {
            case _: CSVScan => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonScan => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetScan => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // other leaf nodes
        case _: LeafExecNode =>
          isSparkToArrowEnabled(conf, op)
        case _ =>
          // TODO: consider converting other intermediate operators to columnar.
          false
      }
    } else {
      false
    }
  }

  private def isSparkToArrowEnabled(conf: SQLConf, op: SparkPlan) = {
    COMET_SPARK_TO_ARROW_ENABLED.get(conf) && {
      val simpleClassName = Utils.getSimpleName(op.getClass)
      val nodeName = simpleClassName.replaceAll("Exec$", "")
      COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.get(conf).contains(nodeName)
    }
  }

}

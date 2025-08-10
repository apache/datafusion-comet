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

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Contains, Divide, DoubleLiteral, EndsWith, EqualNullSafe, EqualTo, Expression, FloatLiteral, GreaterThan, GreaterThanOrEqual, KnownFloatingPointNormalized, LessThan, LessThanOrEqual, NamedExpression, Remainder, StartsWith}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometConf.COMET_ANSI_MODE_ENABLED
import org.apache.comet.CometSparkSessionExtensions._
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.serde.QueryPlanSerde.emitWarning

/**
 * Spark physical optimizer rule for replacing Spark operators with Comet operators.
 */
case class CometExecRule(session: SparkSession) extends Rule[SparkPlan] {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec
          if isCometPlan(s.child) && isCometNativeShuffleMode(conf) &&
            nativeShuffleSupported(s)._1 =>
        logInfo("Comet extension enabled for Native Shuffle")

        // Switch to use Decimal128 regardless of precision, since Arrow native execution
        // doesn't support Decimal32 and Decimal64 yet.
        conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
        CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)

      // Columnar shuffle for regular Spark operators (not Comet) and Comet operators
      // (if configured)
      case s: ShuffleExchangeExec
          if (!s.child.supportsColumnar || isCometPlan(s.child)) && isCometJVMShuffleMode(conf) &&
            columnarShuffleSupported(s)._1 &&
            !isShuffleOperator(s.child) =>
        logInfo("Comet extension enabled for JVM Columnar Shuffle")
        CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
    }
  }

  private def isCometPlan(op: SparkPlan): Boolean = op.isInstanceOf[CometPlan]

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
    def operator2Proto(op: SparkPlan): Option[Operator] = {
      if (op.children.forall(_.isInstanceOf[CometNativeExec])) {
        QueryPlanSerde.operator2Proto(
          op,
          op.children.map(_.asInstanceOf[CometNativeExec].nativeOp): _*)
      } else {
        None
      }
    }

    /**
     * Convert operator to proto and then apply a transformation to wrap the proto in a new plan.
     */
    def newPlanWithProto(op: SparkPlan, fun: Operator => SparkPlan): SparkPlan = {
      operator2Proto(op).map(fun).getOrElse(op)
    }

    plan.transformUp {
      // Fully native scan for V1
      case scan: CometScanExec if scan.scanImpl == CometConf.SCAN_NATIVE_DATAFUSION =>
        val nativeOp = QueryPlanSerde.operator2Proto(scan).get
        CometNativeScanExec(nativeOp, scan.wrapped, scan.session)

      // Comet JVM + native scan for V1 and V2
      case op if isCometScan(op) =>
        val nativeOp = QueryPlanSerde.operator2Proto(op)
        CometScanWrapper(nativeOp.get, op)

      case op if shouldApplySparkToColumnar(conf, op) =>
        val cometOp = CometSparkToColumnarExec(op)
        val nativeOp = QueryPlanSerde.operator2Proto(cometOp)
        CometScanWrapper(nativeOp.get, cometOp)

      case op: ProjectExec =>
        newPlanWithProto(
          op,
          CometProjectExec(_, op, op.output, op.projectList, op.child, SerializedPlan(None)))

      case op: FilterExec =>
        newPlanWithProto(
          op,
          CometFilterExec(_, op, op.output, op.condition, op.child, SerializedPlan(None)))

      case op: SortExec =>
        newPlanWithProto(
          op,
          CometSortExec(
            _,
            op,
            op.output,
            op.outputOrdering,
            op.sortOrder,
            op.child,
            SerializedPlan(None)))

      case op: LocalLimitExec =>
        newPlanWithProto(op, CometLocalLimitExec(_, op, op.limit, op.child, SerializedPlan(None)))

      case op: GlobalLimitExec if op.offset == 0 =>
        newPlanWithProto(
          op,
          CometGlobalLimitExec(_, op, op.limit, op.child, SerializedPlan(None)))

      case op: CollectLimitExec =>
        val fallbackReasons = new ListBuffer[String]()
        if (!CometConf.COMET_EXEC_COLLECT_LIMIT_ENABLED.get(conf)) {
          fallbackReasons += s"${CometConf.COMET_EXEC_COLLECT_LIMIT_ENABLED.key} is false"
        }
        if (!isCometShuffleEnabled(conf)) {
          fallbackReasons += "Comet shuffle is not enabled"
        }
        if (op.offset != 0) {
          fallbackReasons += "CollectLimit with non-zero offset is not supported"
        }
        if (fallbackReasons.nonEmpty) {
          withInfos(op, fallbackReasons.toSet)
        } else {
          if (!isCometNative(op.child)) {
            // no reason to report reason if child is not native
            op
          } else {
            QueryPlanSerde
              .operator2Proto(op)
              .map { nativeOp =>
                val cometOp =
                  CometCollectLimitExec(op, op.limit, op.offset, op.child)
                CometSinkPlaceHolder(nativeOp, op, cometOp)
              }
              .getOrElse(op)
          }
        }

      case op: ExpandExec =>
        newPlanWithProto(
          op,
          CometExpandExec(_, op, op.output, op.projections, op.child, SerializedPlan(None)))

      // When Comet shuffle is disabled, we don't want to transform the HashAggregate
      // to CometHashAggregate. Otherwise, we probably get partial Comet aggregation
      // and final Spark aggregation.
      case op: BaseAggregateExec
          if op.isInstanceOf[HashAggregateExec] ||
            op.isInstanceOf[ObjectHashAggregateExec] &&
            isCometShuffleEnabled(conf) =>
        val modes = op.aggregateExpressions.map(_.mode).distinct
        // In distinct aggregates there can be a combination of modes
        val multiMode = modes.size > 1
        // For a final mode HashAggregate, we only need to transform the HashAggregate
        // if there is Comet partial aggregation.
        val sparkFinalMode = modes.contains(Final) && findCometPartialAgg(op.child).isEmpty

        if (multiMode || sparkFinalMode) {
          op
        } else {
          newPlanWithProto(
            op,
            nativeOp => {
              // The aggExprs could be empty. For example, if the aggregate functions only have
              // distinct aggregate functions or only have group by, the aggExprs is empty and
              // modes is empty too. If aggExprs is not empty, we need to verify all the
              // aggregates have the same mode.
              assert(modes.length == 1 || modes.isEmpty)
              CometHashAggregateExec(
                nativeOp,
                op,
                op.output,
                op.groupingExpressions,
                op.aggregateExpressions,
                op.resultExpressions,
                op.child.output,
                modes.headOption,
                op.child,
                SerializedPlan(None))
            })
        }

      case op: ShuffledHashJoinExec
          if CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(conf) &&
            op.children.forall(isCometNative) =>
        newPlanWithProto(
          op,
          CometHashJoinExec(
            _,
            op,
            op.output,
            op.outputOrdering,
            op.leftKeys,
            op.rightKeys,
            op.joinType,
            op.condition,
            op.buildSide,
            op.left,
            op.right,
            SerializedPlan(None)))

      case op: ShuffledHashJoinExec if !CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(conf) =>
        withInfo(op, "ShuffleHashJoin is not enabled")

      case op: ShuffledHashJoinExec if !op.children.forall(isCometNative) =>
        op

      case op: BroadcastHashJoinExec
          if CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.get(conf) &&
            op.children.forall(isCometNative) =>
        newPlanWithProto(
          op,
          CometBroadcastHashJoinExec(
            _,
            op,
            op.output,
            op.outputOrdering,
            op.leftKeys,
            op.rightKeys,
            op.joinType,
            op.condition,
            op.buildSide,
            op.left,
            op.right,
            SerializedPlan(None)))

      case op: SortMergeJoinExec
          if CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) &&
            op.children.forall(isCometNative) =>
        newPlanWithProto(
          op,
          CometSortMergeJoinExec(
            _,
            op,
            op.output,
            op.outputOrdering,
            op.leftKeys,
            op.rightKeys,
            op.joinType,
            op.condition,
            op.left,
            op.right,
            SerializedPlan(None)))

      case op: CopyExec if op.children.forall(isCometNative) =>
        newPlanWithProto(
          op,
          CometCopyExec(_, op, op.output, op.copyMode, op.child, SerializedPlan(None)))

      case op: SortMergeJoinExec
          if CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) &&
            !op.children.forall(isCometNative) =>
        op

      case op: SortMergeJoinExec if !CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) =>
        withInfo(op, "SortMergeJoin is not enabled")

      case op: SortMergeJoinExec if !op.children.forall(isCometNative) =>
        op

      case c @ CoalesceExec(numPartitions, child)
          if CometConf.COMET_EXEC_COALESCE_ENABLED.get(conf)
            && isCometNative(child) =>
        QueryPlanSerde
          .operator2Proto(c)
          .map { nativeOp =>
            val cometOp = CometCoalesceExec(c, c.output, numPartitions, child)
            CometSinkPlaceHolder(nativeOp, c, cometOp)
          }
          .getOrElse(c)

      case c @ CoalesceExec(_, _) if !CometConf.COMET_EXEC_COALESCE_ENABLED.get(conf) =>
        withInfo(c, "Coalesce is not enabled")

      case op: CoalesceExec if !op.children.forall(isCometNative) =>
        op

      case s: TakeOrderedAndProjectExec
          if isCometNative(s.child) && CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED
            .get(conf)
            && isCometShuffleEnabled(conf) &&
            CometTakeOrderedAndProjectExec.isSupported(s) =>
        QueryPlanSerde
          .operator2Proto(s)
          .map { nativeOp =>
            val cometOp =
              CometTakeOrderedAndProjectExec(
                s,
                s.output,
                s.limit,
                s.sortOrder,
                s.projectList,
                s.child)
            CometSinkPlaceHolder(nativeOp, s, cometOp)
          }
          .getOrElse(s)

      case s: TakeOrderedAndProjectExec =>
        val info1 = createMessage(
          !CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED.get(conf),
          "TakeOrderedAndProject is not enabled")
        val info2 = createMessage(
          !isCometShuffleEnabled(conf),
          "TakeOrderedAndProject requires shuffle to be enabled")
        withInfo(s, Seq(info1, info2).flatten.mkString(","))

      case w: WindowExec =>
        newPlanWithProto(
          w,
          CometWindowExec(
            _,
            w,
            w.output,
            w.windowExpression,
            w.partitionSpec,
            w.orderSpec,
            w.child,
            SerializedPlan(None)))

      case u: UnionExec
          if CometConf.COMET_EXEC_UNION_ENABLED.get(conf) &&
            u.children.forall(isCometNative) =>
        newPlanWithProto(
          u, {
            val cometOp = CometUnionExec(u, u.output, u.children)
            CometSinkPlaceHolder(_, u, cometOp)
          })

      case u: UnionExec if !CometConf.COMET_EXEC_UNION_ENABLED.get(conf) =>
        withInfo(u, "Union is not enabled")

      case op: UnionExec if !op.children.forall(isCometNative) =>
        op

      // For AQE broadcast stage on a Comet broadcast exchange
      case s @ BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) =>
        newPlanWithProto(s, CometSinkPlaceHolder(_, s, s))

      case s @ BroadcastQueryStageExec(
            _,
            ReusedExchangeExec(_, _: CometBroadcastExchangeExec),
            _) =>
        newPlanWithProto(s, CometSinkPlaceHolder(_, s, s))

      // `CometBroadcastExchangeExec`'s broadcast output is not compatible with Spark's broadcast
      // exchange. It is only used for Comet native execution. We only transform Spark broadcast
      // exchange to Comet broadcast exchange if its downstream is a Comet native plan or if the
      // broadcast exchange is forced to be enabled by Comet config.
      case plan if plan.children.exists(_.isInstanceOf[BroadcastExchangeExec]) =>
        val newChildren = plan.children.map {
          case b: BroadcastExchangeExec
              if isCometNative(b.child) &&
                CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.get(conf) =>
            QueryPlanSerde.operator2Proto(b) match {
              case Some(nativeOp) =>
                val cometOp = CometBroadcastExchangeExec(b, b.output, b.mode, b.child)
                CometSinkPlaceHolder(nativeOp, b, cometOp)
              case None => b
            }
          case other => other
        }
        if (!newChildren.exists(_.isInstanceOf[BroadcastExchangeExec])) {
          val newPlan = apply(plan.withNewChildren(newChildren))
          if (isCometNative(newPlan) || isCometBroadCastForceEnabled(conf)) {
            newPlan
          } else {
            if (isCometNative(newPlan)) {
              val reason =
                getCometBroadcastNotEnabledReason(conf).getOrElse("no reason available")
              withInfo(plan, s"Broadcast is not enabled: $reason")
            }
            plan
          }
        } else {
          plan
        }

      // this case should be checked only after the previous case checking for a
      // child BroadcastExchange has been applied, otherwise that transform
      // never gets applied
      case op: BroadcastHashJoinExec if !op.children.forall(isCometNative) =>
        op

      case op: BroadcastHashJoinExec
          if !CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.get(conf) =>
        withInfo(op, "BroadcastHashJoin is not enabled")

      // For AQE shuffle stage on a Comet shuffle exchange
      case s @ ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) =>
        newPlanWithProto(s, CometSinkPlaceHolder(_, s, s))

      // For AQE shuffle stage on a reused Comet shuffle exchange
      // Note that we don't need to handle `ReusedExchangeExec` for non-AQE case, because
      // the query plan won't be re-optimized/planned in non-AQE mode.
      case s @ ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) =>
        newPlanWithProto(s, CometSinkPlaceHolder(_, s, s))

      // Native shuffle for Comet operators
      case s: ShuffleExchangeExec =>
        val nativePrecondition = isCometShuffleEnabled(conf) &&
          isCometNativeShuffleMode(conf) &&
          nativeShuffleSupported(s)._1

        val nativeShuffle: Option[SparkPlan] =
          if (nativePrecondition) {
            val newOp = operator2Proto(s)
            newOp match {
              case Some(nativeOp) =>
                // Switch to use Decimal128 regardless of precision, since Arrow native execution
                // doesn't support Decimal32 and Decimal64 yet.
                conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
                val cometOp = CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
                Some(CometSinkPlaceHolder(nativeOp, s, cometOp))
              case None =>
                None
            }
          } else {
            None
          }

        val nativeOrColumnarShuffle = if (nativeShuffle.isDefined) {
          nativeShuffle
        } else {
          // Columnar shuffle for regular Spark operators (not Comet) and Comet operators
          // (if configured).
          // If the child of ShuffleExchangeExec is also a ShuffleExchangeExec, we should not
          // convert it to CometColumnarShuffle,
          if (isCometShuffleEnabled(conf) && isCometJVMShuffleMode(conf) &&
            columnarShuffleSupported(s)._1 &&
            !isShuffleOperator(s.child)) {

            val newOp = QueryPlanSerde.operator2Proto(s)
            newOp match {
              case Some(nativeOp) =>
                s.child match {
                  case n if n.isInstanceOf[CometNativeExec] || !n.supportsColumnar =>
                    val cometOp =
                      CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
                    Some(CometSinkPlaceHolder(nativeOp, s, cometOp))
                  case _ =>
                    None
                }
              case None =>
                None
            }
          } else {
            None
          }
        }

        if (nativeOrColumnarShuffle.isDefined) {
          nativeOrColumnarShuffle.get
        } else {
          val isShuffleEnabled = isCometShuffleEnabled(conf)
          val reason = getCometShuffleNotEnabledReason(conf).getOrElse("no reason available")
          val msg1 = createMessage(!isShuffleEnabled, s"Comet shuffle is not enabled: $reason")
          val columnarShuffleEnabled = isCometJVMShuffleMode(conf)
          val msg2 = createMessage(
            isShuffleEnabled && !columnarShuffleEnabled && !nativeShuffleSupported(s)._1,
            "Native shuffle: " +
              s"${nativeShuffleSupported(s)._2}")
          val typeInfo = columnarShuffleSupported(s)._2
          val msg3 = createMessage(
            isShuffleEnabled && columnarShuffleEnabled && !columnarShuffleSupported(s)._1,
            "JVM shuffle: " +
              s"$typeInfo")
          withInfo(s, Seq(msg1, msg2, msg3).flatten.mkString(","))
        }

      case op =>
        op match {
          case _: CometExec | _: AQEShuffleReadExec | _: BroadcastExchangeExec |
              _: CometBroadcastExchangeExec | _: CometShuffleExchangeExec =>
            // Some execs should never be replaced. We include
            // these cases specially here so we do not add a misleading 'info' message
            op
          case _ =>
            // An operator that is not supported by Comet
            withInfo(op, s"${op.nodeName} is not supported")
        }
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
    if (showTransformations) {
      logInfo(s"\nINPUT: $plan\nOUTPUT: $newPlan")
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    // DataFusion doesn't have ANSI mode. For now we just disable CometExec if ANSI mode is
    // enabled.
    if (isANSIEnabled(conf)) {
      if (COMET_ANSI_MODE_ENABLED.get()) {
        if (!isSpark40Plus) {
          logWarning("Using Comet's experimental support for ANSI mode.")
        }
      } else {
        logInfo("Comet extension disabled for ANSI mode")
        return plan
      }
    }

    // We shouldn't transform Spark query plan if Comet is not loaded.
    if (!isCometLoaded(conf)) return plan

    if (!isCometExecEnabled(conf)) {
      // Comet exec is disabled, but for Spark shuffle, we still can use Comet columnar shuffle
      if (isCometShuffleEnabled(conf)) {
        applyCometShuffle(plan)
      } else {
        plan
      }
    } else {
      val normalizedPlan = if (CometConf.COMET_REPLACE_SMJ.get()) {
        normalizePlan(plan).transformUp { case p =>
          RewriteJoin.rewrite(p)
        }
      } else {
        normalizePlan(plan)
      }

      // FIXME: Should we move to separate Rule
      var newPlan = transformAndAddCopyExec(normalizedPlan)
      newPlan = transform(normalizedPlan)

      // if the plan cannot be run fully natively then explain why (when appropriate
      // config is enabled)
      if (CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get()) {
        val info = new ExtendedExplainInfo()
        if (info.extensionInfo(newPlan).nonEmpty) {
          logWarning(
            "Comet cannot execute some parts of this plan natively " +
              s"(set ${CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key}=false " +
              "to disable this logging):\n" +
              s"${info.generateVerboseExtendedInfo(newPlan)}")
        }
      }

      // Remove placeholders
      newPlan = newPlan.transform {
        case CometSinkPlaceHolder(_, _, s) => s
        case CometScanWrapper(_, s) => s
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

          newPlan
        case op =>
          firstNativeOp = true
          op
      }
    }
  }

  private def transformAndAddCopyExec(plan: SparkPlan) = plan.transform {
    case shj: ShuffledHashJoinExec =>
      val newLeft = wrapInCopyExec(shj.left)
      val newRight = wrapInCopyExec(shj.right)
      shj.copy(left = newLeft, right = newRight)
    case se: SortExec =>
      val newChild = wrapInCopyExec(se.child)
      se.copy(child = newChild)
    case ee: ExpandExec =>
      val newChild = wrapInCopyExec(ee.child)
      ee.copy(child = newChild)
    case filter @ FilterExec(condition, _) if condition.exists(expr => {
          expr.isInstanceOf[StartsWith] || expr.isInstanceOf[EndsWith] || expr
            .isInstanceOf[Contains]
        }) =>
      // Some native expressions do not support operating on dictionary-encoded arrays, so
      // wrap the child in a CopyExec to unpack dictionaries first.
      val newChild = wrapInCopyExec(filter.child)
      filter.copy(child = newChild)
  }

  // Returns true if given operator can return input array as output array without
  // modification. This is used to determine if we need to copy the input batch to avoid
  // data corruption from reusing the input batch.
  @tailrec
  private def canReuseInputBatch(plan: SparkPlan): Boolean = {
    if (plan.isInstanceOf[ProjectExec] || plan.isInstanceOf[LocalLimitExec]) {
      canReuseInputBatch(plan.children.head)
    } else {
      // FIXME
      plan.isInstanceOf[CometScanExec]
    }
  }

  private def wrapInCopyExec(plan: SparkPlan): SparkPlan = {
    if (canReuseInputBatch(plan)) {
      CopyExec(plan, UnpackOrDeepCopy)
    } else {
      CopyExec(plan, UnpackOrClone)
    }
  }

  /**
   * Find the first Comet partial aggregate in the plan. If it reaches a Spark HashAggregate with
   * partial mode, it will return None.
   */
  private def findCometPartialAgg(plan: SparkPlan): Option[CometHashAggregateExec] = {
    plan.collectFirst {
      case agg: CometHashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
        Some(agg)
      case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) => None
      case agg: ObjectHashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
        None
      case a: AQEShuffleReadExec => findCometPartialAgg(a.child)
      case s: ShuffleQueryStageExec => findCometPartialAgg(s.plan)
    }.flatten
  }

  /**
   * Returns true if a given spark plan is Comet shuffle operator.
   */
  private def isShuffleOperator(op: SparkPlan): Boolean = {
    op match {
      case op: ShuffleQueryStageExec if op.plan.isInstanceOf[CometShuffleExchangeExec] => true
      case _: CometShuffleExchangeExec => true
      case op: CometSinkPlaceHolder => isShuffleOperator(op.child)
      case _ => false
    }
  }

  /**
   * Whether the given Spark partitioning is supported by Comet native shuffle.
   */
  private def nativeShuffleSupported(s: ShuffleExchangeExec): (Boolean, String) = {

    /**
     * Determine which data types are supported as hash-partition keys in native shuffle.
     *
     * Hash Partition Key determines how data should be collocated for operations like
     * `groupByKey`, `reduceByKey` or `join`.
     */
    def supportedHashPartitionKeyDataType(dt: DataType): Boolean = dt match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
          _: FloatType | _: DoubleType | _: StringType | _: BinaryType | _: TimestampType |
          _: TimestampNTZType | _: DecimalType | _: DateType =>
        true
      case _ =>
        false
    }

    val inputs = s.child.output
    val partitioning = s.outputPartitioning
    val conf = SQLConf.get
    var msg = ""
    val supported = partitioning match {
      case HashPartitioning(expressions, _) =>
        // native shuffle currently does not support complex types as partition keys
        // due to lack of hashing support for those types
        val supported =
          expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            expressions.forall(e => supportedHashPartitionKeyDataType(e.dataType)) &&
            inputs.forall(attr => supportedShuffleDataType(attr.dataType)) &&
            CometConf.COMET_EXEC_SHUFFLE_WITH_HASH_PARTITIONING_ENABLED.get(conf)
        if (!supported) {
          msg = s"unsupported Spark partitioning: $expressions"
        }
        supported
      case SinglePartition =>
        inputs.forall(attr => supportedShuffleDataType(attr.dataType))
      case RangePartitioning(ordering, _) =>
        val supported = ordering.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
          inputs.forall(attr => supportedShuffleDataType(attr.dataType)) &&
          CometConf.COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED.get(conf)
        if (!supported) {
          msg = s"unsupported Spark partitioning: $ordering"
        }
        supported
      case _ =>
        msg = s"unsupported Spark partitioning: ${partitioning.getClass.getName}"
        false
    }

    if (!supported) {
      emitWarning(msg)
      (false, msg)
    } else {
      (true, null)
    }
  }

  /**
   * Check if the datatypes of shuffle input are supported. This is used for Columnar shuffle
   * which supports struct/array.
   */
  private def columnarShuffleSupported(s: ShuffleExchangeExec): (Boolean, String) = {
    val inputs = s.child.output
    val partitioning = s.outputPartitioning
    var msg = ""
    val supported = partitioning match {
      case HashPartitioning(expressions, _) =>
        // columnar shuffle supports the same data types (including complex types) both for
        // partition keys and for other columns
        val supported =
          expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            expressions.forall(e => supportedShuffleDataType(e.dataType)) &&
            inputs.forall(attr => supportedShuffleDataType(attr.dataType))
        if (!supported) {
          msg = s"unsupported Spark partitioning expressions: $expressions"
        }
        supported
      case SinglePartition =>
        inputs.forall(attr => supportedShuffleDataType(attr.dataType))
      case RoundRobinPartitioning(_) =>
        inputs.forall(attr => supportedShuffleDataType(attr.dataType))
      case RangePartitioning(orderings, _) =>
        val supported =
          orderings.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            orderings.forall(e => supportedShuffleDataType(e.dataType)) &&
            inputs.forall(attr => supportedShuffleDataType(attr.dataType))
        if (!supported) {
          msg = s"unsupported Spark partitioning expressions: $orderings"
        }
        supported
      case _ =>
        msg = s"unsupported Spark partitioning: ${partitioning.getClass.getName}"
        false
    }

    if (!supported) {
      emitWarning(msg)
      (false, msg)
    } else {
      (true, null)
    }
  }

  /**
   * Determine which data types are supported in a shuffle.
   */
  private def supportedShuffleDataType(dt: DataType): Boolean = dt match {
    case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
        _: FloatType | _: DoubleType | _: StringType | _: BinaryType | _: TimestampType |
        _: TimestampNTZType | _: DecimalType | _: DateType =>
      true
    case StructType(fields) =>
      fields.forall(f => supportedShuffleDataType(f.dataType)) &&
      // Java Arrow stream reader cannot work on duplicate field name
      fields.map(f => f.name).distinct.length == fields.length
    case ArrayType(ArrayType(_, _), _) => false // TODO: nested array is not supported
    case ArrayType(MapType(_, _, _), _) => false // TODO: map array element is not supported
    case ArrayType(elementType, _) =>
      supportedShuffleDataType(elementType)
    case MapType(MapType(_, _, _), _, _) => false // TODO: nested map is not supported
    case MapType(_, MapType(_, _, _), _) => false
    case MapType(StructType(_), _, _) => false // TODO: struct map key/value is not supported
    case MapType(_, StructType(_), _) => false
    case MapType(ArrayType(_, _), _, _) => false // TODO: array map key/value is not supported
    case MapType(_, ArrayType(_, _), _) => false
    case MapType(keyType, valueType, _) =>
      supportedShuffleDataType(keyType) && supportedShuffleDataType(valueType)
    case _ =>
      false
  }

}

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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{createMessage, isANSIEnabled, isCometBroadCastForceEnabled, isCometColumnarShuffleEnabled, isCometEnabled, isCometExecEnabled, isCometOperatorEnabled, isCometScan, isCometScanEnabled, isCometShuffleEnabled, isSchemaSupported, shouldApplyRowToColumnar, withInfo}
import org.apache.comet.parquet.{CometParquetScan, SupportsComet}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.shims.ShimCometSparkSessionExtensions

/**
 * The entry point of Comet extension to Spark. This class is responsible for injecting Comet
 * rules and extensions into Spark.
 *
 * CometScanRule: A rule to transform a Spark scan plan into a Comet scan plan. CometExecRule: A
 * rule to transform a Spark execution plan into a Comet execution plan.
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
  }

  case class CometScanColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometScanRule(session)
  }

  case class CometExecColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometExecRule(session)

    override def postColumnarTransitions: Rule[SparkPlan] =
      EliminateRedundantTransitions(session)
  }

  case class CometScanRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!isCometEnabled(conf) || !isCometScanEnabled(conf)) {
        if (!isCometEnabled(conf)) {
          withInfo(plan, "Comet is not enabled")
        } else if (!isCometScanEnabled(conf)) {
          withInfo(plan, "Comet Scan is not enabled")
        }
        plan
      } else {
        plan.transform {
          // data source V2
          case scanExec: BatchScanExec
              if scanExec.scan.isInstanceOf[ParquetScan] &&
                isSchemaSupported(scanExec.scan.asInstanceOf[ParquetScan].readDataSchema) &&
                isSchemaSupported(scanExec.scan.asInstanceOf[ParquetScan].readPartitionSchema) &&
                // Comet does not support pushedAggregate
                getPushedAggregate(scanExec.scan.asInstanceOf[ParquetScan]).isEmpty =>
            val cometScan = CometParquetScan(scanExec.scan.asInstanceOf[ParquetScan])
            logInfo("Comet extension enabled for Scan")
            CometBatchScanExec(
              scanExec.copy(scan = cometScan),
              runtimeFilters = scanExec.runtimeFilters)

          // If it is a `ParquetScan` but unsupported by Comet, attach the exact
          // reason to the plan.
          case scanExec: BatchScanExec if scanExec.scan.isInstanceOf[ParquetScan] =>
            val requiredSchema = scanExec.scan.asInstanceOf[ParquetScan].readDataSchema
            val info1 = createMessage(
              !isSchemaSupported(requiredSchema),
              s"Schema $requiredSchema is not supported")
            val readPartitionSchema = scanExec.scan.asInstanceOf[ParquetScan].readPartitionSchema
            val info2 = createMessage(
              !isSchemaSupported(readPartitionSchema),
              s"Partition schema $readPartitionSchema is not supported")
            // Comet does not support pushedAggregate
            val info3 = createMessage(
              getPushedAggregate(scanExec.scan.asInstanceOf[ParquetScan]).isDefined,
              "Comet does not support pushed aggregate")
            withInfo(scanExec, Seq(info1, info2, info3).flatten.mkString("\n"))
            scanExec

          // Other datasource V2 scan
          case scanExec: BatchScanExec =>
            scanExec.scan match {
              // Iceberg scan, supported cases
              case s: SupportsComet
                  if s.isCometEnabled &&
                    isSchemaSupported(scanExec.scan.readSchema()) =>
                logInfo(s"Comet extension enabled for ${scanExec.scan.getClass.getSimpleName}")
                // When reading from Iceberg, we automatically enable type promotion
                SQLConf.get.setConfString(COMET_SCHEMA_EVOLUTION_ENABLED.key, "true")
                CometBatchScanExec(
                  scanExec.clone().asInstanceOf[BatchScanExec],
                  runtimeFilters = scanExec.runtimeFilters)

              // Iceberg scan but disabled or unsupported by Comet
              case s: SupportsComet =>
                val info1 = createMessage(
                  !s.isCometEnabled,
                  "Comet extension is not enabled for " +
                    s"${scanExec.scan.getClass.getSimpleName}: not enabled on data source side")
                val info2 = createMessage(
                  !isSchemaSupported(scanExec.scan.readSchema()),
                  "Comet extension is not enabled for " +
                    s"${scanExec.scan.getClass.getSimpleName}: Schema not supported")
                withInfo(scanExec, Seq(info1, info2).flatten.mkString("\n"))

              // If it is data source V2 other than Parquet or Iceberg,
              // attach the unsupported reason to the plan.
              case _ =>
                withInfo(scanExec, "Comet Scan only supports Parquet")
                scanExec
            }

          // data source V1
          case scanExec @ FileSourceScanExec(
                HadoopFsRelation(_, partitionSchema, _, _, _: ParquetFileFormat, _),
                _: Seq[AttributeReference],
                requiredSchema,
                _,
                _,
                _,
                _,
                _,
                _) if isSchemaSupported(requiredSchema) && isSchemaSupported(partitionSchema) =>
            logInfo("Comet extension enabled for v1 Scan")
            CometScanExec(scanExec, session)

          // data source v1 not supported case
          case scanExec @ FileSourceScanExec(
                HadoopFsRelation(_, partitionSchema, _, _, _: ParquetFileFormat, _),
                _: Seq[AttributeReference],
                requiredSchema,
                _,
                _,
                _,
                _,
                _,
                _) =>
            val info1 = createMessage(
              !isSchemaSupported(requiredSchema),
              s"Schema $requiredSchema is not supported")
            val info2 = createMessage(
              !isSchemaSupported(partitionSchema),
              s"Partition schema $partitionSchema is not supported")
            withInfo(scanExec, Seq(info1, info2).flatten.mkString(","))
            scanExec
        }
      }
    }
  }

  case class CometExecRule(session: SparkSession) extends Rule[SparkPlan] {
    private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
      plan.transformUp {
        case s: ShuffleExchangeExec
            if isCometPlan(s.child) && !isCometColumnarShuffleEnabled(conf) &&
              QueryPlanSerde.supportPartitioning(s.child.output, s.outputPartitioning)._1 =>
          logInfo("Comet extension enabled for Native Shuffle")

          // Switch to use Decimal128 regardless of precision, since Arrow native execution
          // doesn't support Decimal32 and Decimal64 yet.
          conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
          CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)

        // Columnar shuffle for regular Spark operators (not Comet) and Comet operators
        // (if configured)
        case s: ShuffleExchangeExec
            if (!s.child.supportsColumnar || isCometPlan(
              s.child)) && isCometColumnarShuffleEnabled(conf) &&
              QueryPlanSerde.supportPartitioningTypes(s.child.output)._1 &&
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
      def transform1(op: SparkPlan): Option[Operator] = {
        if (op.children.forall(_.isInstanceOf[CometNativeExec])) {
          QueryPlanSerde.operator2Proto(
            op,
            op.children.map(_.asInstanceOf[CometNativeExec].nativeOp): _*)
        } else {
          None
        }
      }

      plan.transformUp {
        case op if isCometScan(op) =>
          val nativeOp = QueryPlanSerde.operator2Proto(op).get
          CometScanWrapper(nativeOp, op)

        case op if shouldApplyRowToColumnar(conf, op) =>
          val cometOp = CometRowToColumnarExec(op)
          val nativeOp = QueryPlanSerde.operator2Proto(cometOp).get
          CometScanWrapper(nativeOp, cometOp)

        case op: ProjectExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometProjectExec(
                nativeOp,
                op,
                op.projectList,
                op.output,
                op.child,
                SerializedPlan(None))
            case None =>
              op
          }

        case op: FilterExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometFilterExec(nativeOp, op, op.condition, op.child, SerializedPlan(None))
            case None =>
              op
          }

        case op: SortExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometSortExec(nativeOp, op, op.sortOrder, op.child, SerializedPlan(None))
            case None =>
              op
          }

        case op: LocalLimitExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometLocalLimitExec(nativeOp, op, op.limit, op.child, SerializedPlan(None))
            case None =>
              op
          }

        case op: GlobalLimitExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometGlobalLimitExec(nativeOp, op, op.limit, op.child, SerializedPlan(None))
            case None =>
              op
          }

        case op: CollectLimitExec
            if isCometNative(op.child) && isCometOperatorEnabled(conf, "collectLimit")
              && isCometShuffleEnabled(conf)
              && getOffset(op) == 0 =>
          QueryPlanSerde.operator2Proto(op) match {
            case Some(nativeOp) =>
              val offset = getOffset(op)
              val cometOp =
                CometCollectLimitExec(op, op.limit, offset, op.child)
              CometSinkPlaceHolder(nativeOp, op, cometOp)
            case None =>
              op
          }

        case op: ExpandExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometExpandExec(nativeOp, op, op.projections, op.child, SerializedPlan(None))
            case None =>
              op
          }

        case op @ HashAggregateExec(_, _, _, groupingExprs, aggExprs, _, _, _, child) =>
          val modes = aggExprs.map(_.mode).distinct

          if (!modes.isEmpty && modes.size != 1) {
            // This shouldn't happen as all aggregation expressions should share the same mode.
            // Fallback to Spark nevertheless here.
            op
          } else {
            val sparkFinalMode = {
              !modes.isEmpty && modes.head == Final && findPartialAgg(child).isEmpty
            }

            if (sparkFinalMode) {
              op
            } else {
              val newOp = transform1(op)
              newOp match {
                case Some(nativeOp) =>
                  val modes = aggExprs.map(_.mode).distinct
                  // The aggExprs could be empty. For example, if the aggregate functions only have
                  // distinct aggregate functions or only have group by, the aggExprs is empty and
                  // modes is empty too. If aggExprs is not empty, we need to verify all the
                  // aggregates have the same mode.
                  assert(modes.length == 1 || modes.length == 0)
                  CometHashAggregateExec(
                    nativeOp,
                    op,
                    groupingExprs,
                    aggExprs,
                    child.output,
                    if (modes.nonEmpty) Some(modes.head) else None,
                    child,
                    SerializedPlan(None))
                case None =>
                  op
              }
            }
          }

        case op: ShuffledHashJoinExec
            if isCometOperatorEnabled(conf, "hash_join") &&
              op.children.forall(isCometNative(_)) =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometHashJoinExec(
                nativeOp,
                op,
                op.leftKeys,
                op.rightKeys,
                op.joinType,
                op.condition,
                op.buildSide,
                op.left,
                op.right,
                SerializedPlan(None))
            case None =>
              op
          }

        case op: ShuffledHashJoinExec if !isCometOperatorEnabled(conf, "hash_join") =>
          withInfo(op, "ShuffleHashJoin is not enabled")
          op

        case op: ShuffledHashJoinExec if !op.children.forall(isCometNative(_)) =>
          withInfo(op, "ShuffleHashJoin disabled because not all child plans are native")
          op

        case op: BroadcastHashJoinExec
            if isCometOperatorEnabled(conf, "broadcast_hash_join") &&
              op.children.forall(isCometNative(_)) =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometBroadcastHashJoinExec(
                nativeOp,
                op,
                op.leftKeys,
                op.rightKeys,
                op.joinType,
                op.condition,
                op.buildSide,
                op.left,
                op.right,
                SerializedPlan(None))
            case None =>
              op
          }

        case op: BroadcastHashJoinExec if !isCometOperatorEnabled(conf, "broadcast_hash_join") =>
          withInfo(op, "BroadcastHashJoin is not enabled")
          op

        case op: SortMergeJoinExec
            if isCometOperatorEnabled(conf, "sort_merge_join") &&
              op.children.forall(isCometNative(_)) =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometSortMergeJoinExec(
                nativeOp,
                op,
                op.leftKeys,
                op.rightKeys,
                op.joinType,
                op.condition,
                op.left,
                op.right,
                SerializedPlan(None))
            case None =>
              op
          }
        case op: SortMergeJoinExec if !isCometOperatorEnabled(conf, "sort_merge_join") =>
          withInfo(op, "SortMergeJoin is not enabled")
          op

        case op: SortMergeJoinExec if !op.children.forall(isCometNative(_)) =>
          withInfo(op, "SortMergeJoin disabled because not all child plans are native")
          op

        case c @ CoalesceExec(numPartitions, child)
            if isCometOperatorEnabled(conf, "coalesce")
              && isCometNative(child) =>
          QueryPlanSerde.operator2Proto(c) match {
            case Some(nativeOp) =>
              val cometOp = CometCoalesceExec(c, numPartitions, child)
              CometSinkPlaceHolder(nativeOp, c, cometOp)
            case None =>
              c
          }

        case c @ CoalesceExec(_, _) if !isCometOperatorEnabled(conf, "coalesce") =>
          withInfo(c, "Coalesce is not enabled")
          c

        case op: CoalesceExec if !op.children.forall(isCometNative(_)) =>
          withInfo(op, "Coalesce disabled because not all child plans are native")
          op

        case s: TakeOrderedAndProjectExec
            if isCometNative(s.child) && isCometOperatorEnabled(conf, "takeOrderedAndProjectExec")
              && isCometShuffleEnabled(conf) &&
              CometTakeOrderedAndProjectExec.isSupported(s) =>
          QueryPlanSerde.operator2Proto(s) match {
            case Some(nativeOp) =>
              val cometOp =
                CometTakeOrderedAndProjectExec(s, s.limit, s.sortOrder, s.projectList, s.child)
              CometSinkPlaceHolder(nativeOp, s, cometOp)
            case None =>
              s
          }

        case s: TakeOrderedAndProjectExec =>
          val info1 = createMessage(
            !isCometOperatorEnabled(conf, "takeOrderedAndProjectExec"),
            "TakeOrderedAndProject is not enabled")
          val info2 = createMessage(
            !isCometShuffleEnabled(conf),
            "TakeOrderedAndProject requires shuffle to be enabled")
          withInfo(s, Seq(info1, info2).flatten.mkString(","))
          s

        case u: UnionExec
            if isCometOperatorEnabled(conf, "union") &&
              u.children.forall(isCometNative) =>
          QueryPlanSerde.operator2Proto(u) match {
            case Some(nativeOp) =>
              val cometOp = CometUnionExec(u, u.children)
              CometSinkPlaceHolder(nativeOp, u, cometOp)
            case None =>
              u
          }

        case u: UnionExec if !isCometOperatorEnabled(conf, "union") =>
          withInfo(u, "Union is not enabled")
          u

        case op: UnionExec if !op.children.forall(isCometNative(_)) =>
          withInfo(op, "Union disabled because not all child plans are native")
          op

        // For AQE broadcast stage on a Comet broadcast exchange
        case s @ BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) =>
          val newOp = transform1(s)
          newOp match {
            case Some(nativeOp) =>
              CometSinkPlaceHolder(nativeOp, s, s)
            case None =>
              s
          }

        // `CometBroadcastExchangeExec`'s broadcast output is not compatible with Spark's broadcast
        // exchange. It is only used for Comet native execution. We only transform Spark broadcast
        // exchange to Comet broadcast exchange if its downstream is a Comet native plan or if the
        // broadcast exchange is forced to be enabled by Comet config.
        case plan if plan.children.exists(_.isInstanceOf[BroadcastExchangeExec]) =>
          val newChildren = plan.children.map {
            case b: BroadcastExchangeExec
                if isCometNative(b.child) &&
                  isCometOperatorEnabled(conf, "broadcastExchangeExec") =>
              QueryPlanSerde.operator2Proto(b) match {
                case Some(nativeOp) =>
                  val cometOp = CometBroadcastExchangeExec(b, b.child)
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
              if (!isCometOperatorEnabled(
                  conf,
                  "broadcastExchangeExec") && !isCometBroadCastForceEnabled(conf)) {
                withInfo(plan, "Native Broadcast is not enabled")
              }
              plan
            }
          } else {
            plan
          }

        // this case should be checked only after the previous case checking for a
        // child BroadcastExchange has been applied, otherwise that transform
        // never gets applied
        case op: BroadcastHashJoinExec if !op.children.forall(isCometNative(_)) =>
          withInfo(op, "BroadcastHashJoin disabled because not all child plans are native")
          op

        // For AQE shuffle stage on a Comet shuffle exchange
        case s @ ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) =>
          val newOp = transform1(s)
          newOp match {
            case Some(nativeOp) =>
              CometSinkPlaceHolder(nativeOp, s, s)
            case None =>
              s
          }

        // For AQE shuffle stage on a reused Comet shuffle exchange
        // Note that we don't need to handle `ReusedExchangeExec` for non-AQE case, because
        // the query plan won't be re-optimized/planned in non-AQE mode.
        case s @ ShuffleQueryStageExec(
              _,
              ReusedExchangeExec(_, _: CometShuffleExchangeExec),
              _) =>
          val newOp = transform1(s)
          newOp match {
            case Some(nativeOp) =>
              CometSinkPlaceHolder(nativeOp, s, s)
            case None =>
              s
          }

        // Native shuffle for Comet operators
        case s: ShuffleExchangeExec
            if isCometShuffleEnabled(conf) &&
              !isCometColumnarShuffleEnabled(conf) &&
              QueryPlanSerde.supportPartitioning(s.child.output, s.outputPartitioning)._1 =>
          logInfo("Comet extension enabled for Native Shuffle")

          val newOp = transform1(s)
          newOp match {
            case Some(nativeOp) =>
              // Switch to use Decimal128 regardless of precision, since Arrow native execution
              // doesn't support Decimal32 and Decimal64 yet.
              conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
              val cometOp = CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
              CometSinkPlaceHolder(nativeOp, s, cometOp)
            case None =>
              s
          }

        // Columnar shuffle for regular Spark operators (not Comet) and Comet operators
        // (if configured).
        // If the child of ShuffleExchangeExec is also a ShuffleExchangeExec, we should not
        // convert it to CometColumnarShuffle,
        case s: ShuffleExchangeExec
            if isCometShuffleEnabled(conf) && isCometColumnarShuffleEnabled(conf) &&
              QueryPlanSerde.supportPartitioningTypes(s.child.output)._1 &&
              !isShuffleOperator(s.child) =>
          logInfo("Comet extension enabled for JVM Columnar Shuffle")

          val newOp = QueryPlanSerde.operator2Proto(s)
          newOp match {
            case Some(nativeOp) =>
              s.child match {
                case n if n.isInstanceOf[CometNativeExec] || !n.supportsColumnar =>
                  val cometOp = CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
                  CometSinkPlaceHolder(nativeOp, s, cometOp)
                case _ =>
                  s
              }
            case None =>
              s
          }

        case s: ShuffleExchangeExec =>
          val isShuffleEnabled = isCometShuffleEnabled(conf)
          val msg1 = createMessage(!isShuffleEnabled, "Native shuffle is not enabled")
          val columnarShuffleEnabled = isCometColumnarShuffleEnabled(conf)
          val msg2 = createMessage(
            isShuffleEnabled && !columnarShuffleEnabled && !QueryPlanSerde
              .supportPartitioning(s.child.output, s.outputPartitioning)
              ._1,
            "Shuffle: " +
              s"${QueryPlanSerde.supportPartitioning(s.child.output, s.outputPartitioning)._2}")
          val msg3 = createMessage(
            isShuffleEnabled && columnarShuffleEnabled && !QueryPlanSerde
              .supportPartitioningTypes(s.child.output)
              ._1,
            s"Columnar shuffle: ${QueryPlanSerde.supportPartitioningTypes(s.child.output)._2}")
          withInfo(s, Seq(msg1, msg2, msg3).flatten.mkString(","))
          s

        case op =>
          // An operator that is not supported by Comet
          op match {
            case _: CometExec | _: CometBroadcastExchangeExec | _: CometShuffleExchangeExec => op
            case o =>
              withInfo(o, s"${o.nodeName} is not supported")
              o
          }
      }
    }

    override def apply(plan: SparkPlan): SparkPlan = {
      // DataFusion doesn't have ANSI mode. For now we just disable CometExec if ANSI mode is
      // enabled.
      if (isANSIEnabled(conf)) {
        if (COMET_ANSI_MODE_ENABLED.get()) {
          logWarning("Using Comet's experimental support for ANSI mode.")
        } else {
          logInfo("Comet extension disabled for ANSI mode")
          return plan
        }
      }

      // We shouldn't transform Spark query plan if Comet is disabled.
      if (!isCometEnabled(conf)) return plan

      if (!isCometExecEnabled(conf)) {
        // Comet exec is disabled, but for Spark shuffle, we still can use Comet columnar shuffle
        if (isCometShuffleEnabled(conf)) {
          applyCometShuffle(plan)
        } else {
          plan
        }
      } else {
        var newPlan = transform(plan)

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
            if (firstNativeOp) {
              firstNativeOp = false
              op.convertBlock()
            } else {
              op
            }
          case op =>
            firstNativeOp = true
            op
        }
      }
    }

    /**
     * Find the first Comet partial aggregate in the plan. If it reaches a Spark HashAggregate
     * with partial mode, it will return None.
     */
    def findPartialAgg(plan: SparkPlan): Option[CometHashAggregateExec] = {
      plan.collectFirst {
        case agg: CometHashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
          Some(agg)
        case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) => None
        case a: AQEShuffleReadExec => findPartialAgg(a.child)
        case s: ShuffleQueryStageExec => findPartialAgg(s.plan)
      }.flatten
    }

    /**
     * Returns true if a given spark plan is Comet shuffle operator.
     */
    def isShuffleOperator(op: SparkPlan): Boolean = {
      op match {
        case op: ShuffleQueryStageExec if op.plan.isInstanceOf[CometShuffleExchangeExec] => true
        case _: CometShuffleExchangeExec => true
        case op: CometSinkPlaceHolder => isShuffleOperator(op.child)
        case _ => false
      }
    }
  }

  // This rule is responsible for eliminating redundant transitions between row-based and
  // columnar-based operators for Comet. Currently, two potential redundant transitions are:
  // 1. `ColumnarToRowExec` on top of an ending `CometCollectLimitExec` operator, which is
  //    redundant as `CometCollectLimitExec` already wraps a `ColumnarToRowExec` for row-based
  //    output.
  // 2. Consecutive operators of `CometRowToColumnarExec` and `ColumnarToRowExec`.
  //
  // Note about the first case: The `ColumnarToRowExec` was added during
  // ApplyColumnarRulesAndInsertTransitions' insertTransitions phase when Spark requests row-based
  // output such as a `collect` call. It's correct to add a redundant `ColumnarToRowExec` for
  // `CometExec`. However, for certain operators such as `CometCollectLimitExec` which overrides
  // `executeCollect`, the redundant `ColumnarToRowExec` makes the override ineffective.
  //
  // Note about the second case: When `spark.comet.rowToColumnar.enabled` is set, Comet will add
  // `CometRowToColumnarExec` on top of row-based operators first, but the downstream operator
  // only takes row-based input as it's a vanilla Spark operator(as Comet cannot convert it for
  // various reasons) or Spark requests row-based output such as a `collect` call. Spark will adds
  // another `ColumnarToRowExec` on top of `CometRowToColumnarExec`. In this case, the pair could
  // be removed.
  case class EliminateRedundantTransitions(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      val eliminatedPlan = plan transformUp {
        case ColumnarToRowExec(rowToColumnar: CometRowToColumnarExec) => rowToColumnar.child
      }

      eliminatedPlan match {
        case ColumnarToRowExec(child: CometCollectLimitExec) =>
          child
        case other =>
          other
      }
    }
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)

  private[comet] def isANSIEnabled(conf: SQLConf): Boolean = {
    conf.getConf(SQLConf.ANSI_ENABLED)
  }

  /**
   * Checks whether Comet extension should be enabled for Spark.
   */
  private[comet] def isCometEnabled(conf: SQLConf): Boolean = {
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

  private[comet] def isCometOperatorEnabled(conf: SQLConf, operator: String): Boolean = {
    val operatorFlag = s"$COMET_EXEC_CONFIG_PREFIX.$operator.enabled"
    val operatorDisabledFlag = s"$COMET_EXEC_CONFIG_PREFIX.$operator.disabled"
    conf.getConfString(operatorFlag, "false").toBoolean || isCometAllOperatorEnabled(conf) &&
    !conf.getConfString(operatorDisabledFlag, "false").toBoolean
  }

  private[comet] def isCometBroadCastForceEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_BROADCAST_FORCE_ENABLED.get(conf)
  }

  private[comet] def isCometShuffleEnabled(conf: SQLConf): Boolean =
    COMET_EXEC_SHUFFLE_ENABLED.get(conf) &&
      (conf.contains("spark.shuffle.manager") && conf.getConfString("spark.shuffle.manager") ==
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

  private[comet] def isCometScanEnabled(conf: SQLConf): Boolean = {
    COMET_SCAN_ENABLED.get(conf)
  }

  private[comet] def isCometExecEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ENABLED.get(conf)
  }

  private[comet] def isCometColumnarShuffleEnabled(conf: SQLConf): Boolean = {
    COMET_COLUMNAR_SHUFFLE_ENABLED.get(conf)
  }

  private[comet] def isCometAllOperatorEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ALL_OPERATOR_ENABLED.get(conf)
  }

  private[comet] def isCometAllExprEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ALL_EXPR_ENABLED.get(conf)
  }

  private[comet] def isSchemaSupported(schema: StructType): Boolean =
    schema.map(_.dataType).forall(isTypeSupported)

  private[comet] def isTypeSupported(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        BinaryType | StringType | _: DecimalType | DateType | TimestampType =>
      true
    // `TimestampNTZType` is private in Spark 3.2.
    case t: DataType if t.typeName == "timestamp_ntz" && !isSpark32 => true
    case dt =>
      logInfo(s"Comet extension is disabled because data type $dt is not supported")
      false
  }

  def isCometScan(op: SparkPlan): Boolean = {
    op.isInstanceOf[CometBatchScanExec] || op.isInstanceOf[CometScanExec]
  }

  private def shouldApplyRowToColumnar(conf: SQLConf, op: SparkPlan): Boolean = {
    // Only consider converting leaf nodes to columnar currently, so that all the following
    // operators can have a chance to be converted to columnar.
    // TODO: consider converting other intermediate operators to columnar.
    op.isInstanceOf[LeafExecNode] && !op.supportsColumnar && isSchemaSupported(op.schema) &&
    COMET_ROW_TO_COLUMNAR_ENABLED.get(conf) && {
      val simpleClassName = Utils.getSimpleName(op.getClass)
      val nodeName = simpleClassName.replaceAll("Exec$", "")
      COMET_ROW_TO_COLUMNAR_SUPPORTED_OPERATOR_LIST.get(conf).contains(nodeName)
    }
  }

  /** Used for operations that weren't available in Spark 3.2 */
  def isSpark32: Boolean = {
    org.apache.spark.SPARK_VERSION.matches("3\\.2\\..*")
  }

  def isSpark33Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.3"
  }

  /** Used for operations that are available in Spark 3.4+ */
  def isSpark34Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.4"
  }

  /** Calculates required memory overhead in MB per executor process for Comet. */
  def getCometMemoryOverheadInMiB(sparkConf: SparkConf): Long = {
    // `spark.executor.memory` default value is 1g
    val executorMemoryMiB = ConfigHelpers
      .byteFromString(sparkConf.get("spark.executor.memory", "1024MB"), ByteUnit.MiB)

    val minimum = ConfigHelpers
      .byteFromString(sparkConf.get(COMET_MEMORY_OVERHEAD_MIN_MIB.key, "384"), ByteUnit.MiB)
    val overheadFactor = sparkConf.getDouble(COMET_MEMORY_OVERHEAD_FACTOR.key, 0.2)

    val overHeadMemFromConf = sparkConf
      .getOption(COMET_MEMORY_OVERHEAD.key)
      .map(ConfigHelpers.byteFromString(_, ByteUnit.MiB))

    overHeadMemFromConf.getOrElse(math.max((overheadFactor * executorMemoryMiB).toLong, minimum))
  }

  /** Calculates required memory overhead in bytes per executor process for Comet. */
  def getCometMemoryOverhead(sparkConf: SparkConf): Long = {
    ByteUnit.MiB.toBytes(getCometMemoryOverheadInMiB(sparkConf))
  }

  /** Calculates required shuffle memory size in bytes per executor process for Comet. */
  def getCometShuffleMemorySize(sparkConf: SparkConf, conf: SQLConf): Long = {
    val cometMemoryOverhead = getCometMemoryOverheadInMiB(sparkConf)

    val overheadFactor = COMET_COLUMNAR_SHUFFLE_MEMORY_FACTOR.get(conf)
    val cometShuffleMemoryFromConf = COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.get(conf)

    val shuffleMemorySize =
      cometShuffleMemoryFromConf.getOrElse((overheadFactor * cometMemoryOverhead).toLong)
    if (shuffleMemorySize > cometMemoryOverhead) {
      logWarning(
        s"Configured shuffle memory size $shuffleMemorySize is larger than Comet memory overhead " +
          s"$cometMemoryOverhead, using Comet memory overhead instead.")
      ByteUnit.MiB.toBytes(cometMemoryOverhead)
    } else {
      ByteUnit.MiB.toBytes(shuffleMemorySize)
    }
  }

  /**
   * Attaches explain information to a TreeNode, rolling up the corresponding information tags
   * from any child nodes. For now, we are using this to attach the reasons why certain Spark
   * operators or expressions are disabled.
   *
   * @param node
   *   The node to attach the explain information to. Typically a SparkPlan
   * @param info
   *   Information text. Optional, may be null or empty. If not provided, then only information
   *   from child nodes will be included.
   * @param exprs
   *   Child nodes. Information attached in these nodes will be be included in the information
   *   attached to @node
   * @tparam T
   *   The type of the TreeNode. Typically SparkPlan, AggregateExpression, or Expression
   * @return
   *   The node with information (if any) attached
   */
  def withInfo[T <: TreeNode[_]](node: T, info: String, exprs: T*): T = {
    val exprInfo = exprs
      .flatMap { e => Seq(e.getTagValue(CometExplainInfo.EXTENSION_INFO)) }
      .flatten
      .mkString("\n")
    if (info != null && info.nonEmpty && exprInfo.nonEmpty) {
      node.setTagValue(CometExplainInfo.EXTENSION_INFO, Seq(exprInfo, info).mkString("\n"))
    } else if (exprInfo.nonEmpty) {
      node.setTagValue(CometExplainInfo.EXTENSION_INFO, exprInfo)
    } else if (info != null && info.nonEmpty) {
      node.setTagValue(CometExplainInfo.EXTENSION_INFO, info)
    }
    node
  }

  /**
   * Attaches explain information to a TreeNode, rolling up the corresponding information tags
   * from any child nodes
   *
   * @param node
   *   The node to attach the explain information to. Typically a SparkPlan
   * @param exprs
   *   Child nodes. Information attached in these nodes will be be included in the information
   *   attached to @node
   * @tparam T
   *   The type of the TreeNode. Typically SparkPlan, AggregateExpression, or Expression
   * @return
   *   The node with information (if any) attached
   */
  def withInfo[T <: TreeNode[_]](node: T, exprs: T*): T = {
    withInfo(node, "", exprs: _*)
  }

  // Helper to reduce boilerplate
  def createMessage(condition: Boolean, message: => String): Option[String] = {
    if (condition) {
      Some(message)
    } else {
      None
    }
  }
}

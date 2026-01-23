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
import org.apache.comet.serde.{CometOperatorSerde, Compatible, Incompatible, OperatorOuterClass, Unsupported}
import org.apache.comet.serde.operator._
import org.apache.comet.serde.operator.CometDataWritingCommand

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

}

/**
 * Spark physical optimizer rule for replacing Spark operators with Comet operators.
 */
case class CometExecRule(session: SparkSession) extends Rule[SparkPlan] {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec if CometShuffleExchangeExec.nativeShuffleSupported(s) =>
        // Switch to use Decimal128 regardless of precision, since Arrow native execution
        // doesn't support Decimal32 and Decimal64 yet.
        conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
        CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)

      case s: ShuffleExchangeExec if CometShuffleExchangeExec.columnarShuffleSupported(s) =>
        // Columnar shuffle for regular Spark operators (not Comet) and Comet operators
        // (if configured)
        CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
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
      // Only handle scans with native metadata; SupportsComet scans fall through to isCometScan
      // Config checks (COMET_ICEBERG_NATIVE_ENABLED, COMET_EXEC_ENABLED) are done in CometScanRule
      case scan: CometBatchScanExec if scan.nativeIcebergScanMetadata.isDefined =>
        convertToComet(scan, CometIcebergNativeScan).getOrElse(scan)

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
      convertNode(op)
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

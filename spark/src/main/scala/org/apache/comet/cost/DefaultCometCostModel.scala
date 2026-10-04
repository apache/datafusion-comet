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

package org.apache.comet.cost

import java.util.IdentityHashMap

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, EqualNullSafe, EqualTo, Expression, In, InSet, IsNotNull, LeafExpression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.comet.{CometExec, CometPlan}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, DataSourceScanExec, ExpandExec, FilterExec, GenerateExec, InputAdapter, LimitExec, ProjectExec, RowToColumnarTransition, SortExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StringType, StructType}

import org.apache.comet.CometConf

/**
 * Experimental: charges every operator in a stage for the rows it is estimated to process.
 *
 * An operator costs its input row count times a per-row weight that depends on the kind of
 * operator. A Comet native operator costs the weight of the Spark operator it replaces divided by
 * `spark.comet.exec.costModel.nativeSpeedup`. A conversion between columnar batches and rows
 * costs a weight per row that grows with the width of the schema, multiplied by
 * `spark.comet.exec.costModel.transitionCostFactor` unless it is Spark converting the output of
 * its own vectorized scan.
 *
 * Row counts come from the runtime statistics of materialized query stages where there are any,
 * and otherwise from the logical plan statistics of the stage's scans, pushed up through the
 * stage with fixed selectivities. Every cost is linear in the row count, so for a stage with one
 * input only the row counts relative to that input matter.
 *
 * The weights are initial estimates that have not been calibrated against benchmarks. Subclasses
 * can override them.
 */
class DefaultCometCostModel extends CometCostModel {
  import DefaultCometCostModel._

  /** How many times faster a native operator is than the Spark operator it replaces. */
  protected def nativeSpeedup: Double = CometConf.COMET_EXEC_COST_MODEL_NATIVE_SPEEDUP.get()

  /** The cost of a transition to or from Comet relative to Spark's own columnar-to-row. */
  protected def transitionCostFactor: Double =
    CometConf.COMET_EXEC_COST_MODEL_TRANSITION_COST_FACTOR.get()

  /**
   * The cost for a Spark operator to process one input row, or to produce one output row when it
   * is a leaf. Comet operators are costed through the Spark operator they replace.
   */
  protected def operatorWeight(op: SparkPlan): Double = op match {
    case _: DataSourceScanExec | _: DataSourceV2ScanExecBase | _: InMemoryTableScanExec =>
      ScanWeight * schemaWidth(op.output)
    case filter: FilterExec =>
      RowWeight + ExpressionWeight * expressionCount(Seq(filter.condition))
    case project: ProjectExec =>
      RowWeight + ExpressionWeight * expressionCount(project.projectList)
    case _: BaseAggregateExec => AggregateWeight
    case _: BaseJoinExec => JoinWeight
    case _: SortExec | _: TakeOrderedAndProjectExec => SortWeight
    case _: WindowExecBase => WindowWeight
    case _: ExpandExec | _: GenerateExec => GenerateWeight
    case _ => RowWeight
  }

  /** The cost of converting one row between columnar and row format, before any factor. */
  protected def transitionWeight(output: Seq[Attribute]): Double =
    TransitionWeight * schemaWidth(output)

  override def estimate(cometPlan: SparkPlan, sparkPlan: SparkPlan): CometCostEstimate = {
    val estimator = new StageEstimator(nativeSpeedup, transitionCostFactor)
    CometCostEstimate(estimator.stageCost(cometPlan), estimator.stageCost(sparkPlan))
  }

  /** Costs the candidate plans of one stage. The row count memo makes it single use. */
  private class StageEstimator(nativeSpeedup: Double, transitionCostFactor: Double) {

    // Keyed on identity because `SparkPlan` equality and hashing walk the subtree.
    private val rowsMemo = new IdentityHashMap[SparkPlan, java.lang.Double]()

    def stageCost(plan: SparkPlan): Double =
      if (isStageInput(plan)) 0.0 else operatorCost(plan) + plan.children.map(stageCost).sum

    private def operatorCost(node: SparkPlan): Double = node match {
      case _: WholeStageCodegenExec | _: InputAdapter | _: AQEShuffleReadExec => 0.0
      case toRow: ColumnarToRowTransition =>
        val factor = if (producesCometBatches(toRow.child)) transitionCostFactor else 1.0
        rows(toRow.child) * transitionWeight(toRow.output) * factor
      case toColumnar: RowToColumnarTransition =>
        rows(toColumnar.child) * transitionWeight(toColumnar.output) * transitionCostFactor
      case comet: CometExec if comet.originalPlan != null =>
        inputRows(comet) * operatorWeight(comet.originalPlan) / nativeSpeedup
      case comet: CometPlan =>
        inputRows(comet) * operatorWeight(comet) / nativeSpeedup
      case _ =>
        inputRows(node) * operatorWeight(node)
    }

    private def inputRows(node: SparkPlan): Double =
      if (node.children.isEmpty) rows(node) else node.children.map(rows).sum

    /** The estimated number of rows `node` produces. */
    private def rows(node: SparkPlan): Double = {
      val known = rowsMemo.get(node)
      if (known != null) {
        known
      } else {
        val estimated = math.max(estimateRows(node), 1.0)
        rowsMemo.put(node, estimated)
        estimated
      }
    }

    private def estimateRows(node: SparkPlan): Double = node match {
      case stage: QueryStageExec =>
        // Exact once the stage has materialized, which under AQE is before its consumer is
        // planned.
        stage.computeStats().map(rowsFromStats(_, stage.output)).getOrElse(rows(stage.plan))
      case reused: ReusedExchangeExec => rows(reused.child)
      case comet: CometExec
          if comet.originalPlan != null &&
            comet.originalPlan.children.size == comet.children.size =>
        outputRows(comet.originalPlan, comet)
      case _ => outputRows(node, node)
    }

    /**
     * @param op
     *   the Spark operator whose semantics apply, which for a Comet operator is its original
     * @param node
     *   the operator in the plan being costed, which supplies the children
     */
    private def outputRows(op: SparkPlan, node: SparkPlan): Double = {
      val inputs = node.children.map(rows)
      op match {
        case _ if inputs.isEmpty => leafRows(node)
        case filter: FilterExec => inputs.head * selectivity(filter.condition)
        case aggregate: BaseAggregateExec =>
          if (aggregate.groupingExpressions.isEmpty) 1.0 else inputs.head * GroupingReduction
        case limit: LimitExec if limit.limit >= 0 => math.min(inputs.head, limit.limit.toDouble)
        case topK: TakeOrderedAndProjectExec => math.min(inputs.head, topK.limit.toDouble)
        case expand: ExpandExec => inputs.head * expand.projections.size
        case join: BaseJoinExec =>
          val Seq(left, right) = inputs
          join.joinType match {
            case LeftSemi | LeftAnti => left * SemiJoinSelectivity
            case _: ExistenceJoin => left
            case _ if join.leftKeys.isEmpty && join.condition.isEmpty => left * right
            case _ => math.max(left, right)
          }
        case _: UnionExec => inputs.sum
        case _ => inputs.max
      }
    }

    private def leafRows(node: SparkPlan): Double = {
      val logical = node.logicalLink.orElse(node match {
        case comet: CometExec => Option(comet.originalPlan).flatMap(_.logicalLink)
        case _ => None
      })
      logical
        .map { plan =>
          // A scan inherits the link of the filter or project planned with it. Take the relation,
          // so that statistics which already apply the filter are not filtered again.
          val source = plan.collectLeaves() match {
            case Seq(leaf) => leaf
            case _ => plan
          }
          rowsFromStats(source.stats, source.output)
        }
        .getOrElse(UnknownRows)
    }
  }
}

object DefaultCometCostModel {

  // Per-row weights for Spark operators, relative to each other.
  private val RowWeight = 1.0
  private val ExpressionWeight = 2.0
  private val ScanWeight = 3.0
  private val TransitionWeight = 2.0
  private val GenerateWeight = 10.0
  private val AggregateWeight = 30.0
  private val JoinWeight = 40.0
  private val WindowWeight = 60.0
  private val SortWeight = 100.0

  // Selectivities for predicates with no usable statistics, after Selinger et al. (1979).
  private val EqualitySelectivity = 0.1
  private val DefaultSelectivity = 1.0 / 3
  private val SemiJoinSelectivity = 0.5

  /** The fraction of its input rows a grouping aggregate is assumed to output. */
  private val GroupingReduction = 0.5

  /** The row count assumed for a leaf with no statistics. */
  private val UnknownRows = 1000000.0

  private def isStageInput(plan: SparkPlan): Boolean = plan match {
    case _: QueryStageExec | _: ShuffleExchangeLike | _: BroadcastExchangeLike |
        _: ReusedExchangeExec =>
      true
    case _ => false
  }

  private def producesCometBatches(plan: SparkPlan): Boolean = plan match {
    case _: CometPlan => true
    case stage: QueryStageExec => producesCometBatches(stage.plan)
    case reused: ReusedExchangeExec => producesCometBatches(reused.child)
    case read: AQEShuffleReadExec => producesCometBatches(read.child)
    case _ => false
  }

  private def rowsFromStats(stats: Statistics, output: Seq[Attribute]): Double =
    stats.rowCount.map(_.toDouble).getOrElse {
      val rowSize = 8 + output.map(_.dataType.defaultSize).sum
      stats.sizeInBytes.toDouble / rowSize
    }

  private def selectivity(condition: Expression): Double = condition match {
    case And(left, right) => selectivity(left) * selectivity(right)
    // Spark infers these from other predicates and they rarely remove rows.
    case _: IsNotNull => 1.0
    case _: EqualTo | _: EqualNullSafe | _: In | _: InSet => EqualitySelectivity
    case _ => DefaultSelectivity
  }

  /** Counts the expressions that compute something, so that selecting a column is free. */
  private def expressionCount(expressions: Seq[Expression]): Int =
    expressions
      .map(_.collect {
        case e if !e.isInstanceOf[LeafExpression] && !e.isInstanceOf[Alias] => e
      }.size)
      .sum

  /** The width of a schema in columns, weighting variable-length and nested types more. */
  private def schemaWidth(output: Seq[Attribute]): Double =
    math.max(output.map(attr => typeWidth(attr.dataType)).sum, 1.0)

  private def typeWidth(dataType: DataType): Double = dataType match {
    case struct: StructType => struct.fields.map(field => typeWidth(field.dataType)).sum
    case array: ArrayType => 2 * typeWidth(array.elementType)
    case map: MapType => 2 * (typeWidth(map.keyType) + typeWidth(map.valueType))
    case _: StringType | BinaryType => 2.0
    case _ => 1.0
  }
}

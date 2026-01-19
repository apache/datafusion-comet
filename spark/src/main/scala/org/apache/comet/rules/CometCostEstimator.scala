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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Cost analysis result containing all metrics for the CBO decision.
 */
case class CostAnalysis(
    cometOperatorCount: Int,
    sparkOperatorCount: Int,
    transitionCount: Int,
    estimatedRowCount: Option[Long],
    estimatedSizeBytes: Option[Long],
    sparkCost: Double,
    cometCost: Double,
    estimatedSpeedup: Double,
    shouldUseComet: Boolean) {

  def toExplainString: String = {
    f"""CBO Analysis:
       |  Decision: ${if (shouldUseComet) "Use Comet" else "Fall back to Spark"}
       |  Estimated Speedup: $estimatedSpeedup%.2fx
       |  Comet Operators: $cometOperatorCount
       |  Spark Operators: $sparkOperatorCount
       |  Transitions: $transitionCount
       |  Estimated Rows: ${estimatedRowCount.map(_.toString).getOrElse("unknown")}
       |  Spark Cost: $sparkCost%.2f
       |  Comet Cost: $cometCost%.2f""".stripMargin
  }
}

/**
 * Statistics collected from plan traversal.
 */
case class PlanStatistics(
    cometOps: Int = 0,
    sparkOps: Int = 0,
    transitions: Int = 0,
    cometScans: Int = 0,
    cometFilters: Int = 0,
    cometProjects: Int = 0,
    cometAggregates: Int = 0,
    cometJoins: Int = 0,
    cometSorts: Int = 0,
    sparkScans: Int = 0,
    sparkFilters: Int = 0,
    sparkProjects: Int = 0,
    sparkAggregates: Int = 0,
    sparkJoins: Int = 0,
    sparkSorts: Int = 0)

/**
 * Tag for attaching CBO info to plan nodes for EXPLAIN output.
 */
object CometCBOInfo {
  val TAG: TreeNodeTag[CostAnalysis] = new TreeNodeTag[CostAnalysis]("CometCBOInfo")
}

/**
 * Cost estimator for comparing Comet vs Spark execution plans.
 *
 * The estimator uses a heuristic-based cost model with configurable weights for different
 * operator types and transition penalties. It estimates whether running a query with Comet will
 * be faster than running it with Spark.
 */
object CometCostEstimator extends Logging {

  /**
   * Analyze a Comet plan and determine if it should be used over Spark.
   */
  def analyze(cometPlan: SparkPlan, conf: SQLConf): CostAnalysis = {
    val stats = collectStats(cometPlan)
    val rowCount = extractRowCount(cometPlan)
    val sizeBytes = extractSizeBytes(cometPlan)

    val sparkCost = calculateSparkCost(stats, rowCount, conf)
    val cometCost = calculateCometCost(stats, rowCount, conf)

    val speedup = if (cometCost > 0) sparkCost / cometCost else Double.MaxValue
    val threshold = CometConf.COMET_CBO_SPEEDUP_THRESHOLD.get(conf)

    CostAnalysis(
      cometOperatorCount = stats.cometOps,
      sparkOperatorCount = stats.sparkOps,
      transitionCount = stats.transitions,
      estimatedRowCount = rowCount,
      estimatedSizeBytes = sizeBytes,
      sparkCost = sparkCost,
      cometCost = cometCost,
      estimatedSpeedup = speedup,
      shouldUseComet = speedup >= threshold)
  }

  private def collectStats(plan: SparkPlan): PlanStatistics = {
    var stats = PlanStatistics()

    plan.foreach {
      // Transitions - these are expensive
      case _: CometColumnarToRowExec | _: CometSparkToColumnarExec | _: ColumnarToRowExec |
          _: RowToColumnarExec =>
        stats = stats.copy(transitions = stats.transitions + 1)

      // Comet scans
      case _: CometScanExec | _: CometBatchScanExec | _: CometNativeScanExec |
          _: CometIcebergNativeScanExec | _: CometLocalTableScanExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1, cometScans = stats.cometScans + 1)

      // Comet filters
      case _: CometFilterExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1, cometFilters = stats.cometFilters + 1)

      // Comet projects
      case _: CometProjectExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1, cometProjects = stats.cometProjects + 1)

      // Comet aggregates
      case _: CometHashAggregateExec =>
        stats =
          stats.copy(cometOps = stats.cometOps + 1, cometAggregates = stats.cometAggregates + 1)

      // Comet joins
      case _: CometBroadcastHashJoinExec | _: CometHashJoinExec | _: CometSortMergeJoinExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1, cometJoins = stats.cometJoins + 1)

      // Comet sorts
      case _: CometSortExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1, cometSorts = stats.cometSorts + 1)

      // Other Comet operators (shuffle, window, etc.)
      case _: CometShuffleExchangeExec | _: CometBroadcastExchangeExec | _: CometWindowExec |
          _: CometCoalesceExec | _: CometCollectLimitExec | _: CometTakeOrderedAndProjectExec |
          _: CometUnionExec | _: CometExpandExec | _: CometExplodeExec | _: CometLocalLimitExec |
          _: CometGlobalLimitExec =>
        stats = stats.copy(cometOps = stats.cometOps + 1)

      // Spark scans
      case _: FileSourceScanExec | _: BatchScanExec =>
        stats = stats.copy(sparkOps = stats.sparkOps + 1, sparkScans = stats.sparkScans + 1)

      // Spark filters
      case _: FilterExec =>
        stats = stats.copy(sparkOps = stats.sparkOps + 1, sparkFilters = stats.sparkFilters + 1)

      // Spark projects
      case _: ProjectExec =>
        stats = stats.copy(sparkOps = stats.sparkOps + 1, sparkProjects = stats.sparkProjects + 1)

      // Spark aggregates
      case _: HashAggregateExec | _: ObjectHashAggregateExec =>
        stats =
          stats.copy(sparkOps = stats.sparkOps + 1, sparkAggregates = stats.sparkAggregates + 1)

      // Spark joins
      case _: BroadcastHashJoinExec | _: ShuffledHashJoinExec | _: SortMergeJoinExec =>
        stats = stats.copy(sparkOps = stats.sparkOps + 1, sparkJoins = stats.sparkJoins + 1)

      // Spark sorts
      case _: SortExec =>
        stats = stats.copy(sparkOps = stats.sparkOps + 1, sparkSorts = stats.sparkSorts + 1)

      // Ignore wrapper/internal nodes
      case _: AdaptiveSparkPlanExec | _: InputAdapter | _: QueryStageExec |
          _: WholeStageCodegenExec | _: ReusedExchangeExec | _: AQEShuffleReadExec =>
      // Don't count these

      case _ =>
      // Other operators not specifically categorized
    }
    stats
  }

  private def extractRowCount(plan: SparkPlan): Option[Long] = {
    // Try logical plan stats first
    plan.logicalLink.flatMap(_.stats.rowCount.map(_.toLong)).orElse {
      // Fallback: estimate from size (assume ~100 bytes per row)
      extractSizeBytes(plan).map(_ / 100)
    }
  }

  private def extractSizeBytes(plan: SparkPlan): Option[Long] = {
    plan.logicalLink.map(_.stats.sizeInBytes.toLong).orElse {
      // Fallback: look for scan-level size info
      plan.collectLeaves().collectFirst {
        case scan: CometScanExec => scan.relation.sizeInBytes
        case scan: FileSourceScanExec => scan.relation.sizeInBytes
      }
    }
  }

  private def calculateSparkCost(
      stats: PlanStatistics,
      rowCount: Option[Long],
      conf: SQLConf): Double = {
    val rows = rowCount.getOrElse(CometConf.COMET_CBO_DEFAULT_ROW_COUNT.get(conf)).toDouble

    // Base cost for each operator type (calculate as if all operators ran in Spark)
    val scanCost =
      (stats.sparkScans + stats.cometScans) * CometConf.COMET_CBO_SCAN_WEIGHT.get(conf) * rows
    val filterCost =
      (stats.sparkFilters + stats.cometFilters) * CometConf.COMET_CBO_FILTER_WEIGHT.get(
        conf) * rows
    val projectCost =
      (stats.sparkProjects + stats.cometProjects) * CometConf.COMET_CBO_PROJECT_WEIGHT
        .get(conf) * rows
    val aggCost =
      (stats.sparkAggregates + stats.cometAggregates) * CometConf.COMET_CBO_AGGREGATE_WEIGHT
        .get(conf) * rows
    val joinCost =
      (stats.sparkJoins + stats.cometJoins) * CometConf.COMET_CBO_JOIN_WEIGHT.get(conf) * rows
    val sortCost = (stats.sparkSorts + stats.cometSorts) * CometConf.COMET_CBO_SORT_WEIGHT.get(
      conf) * rows * Math.log(rows + 1)

    scanCost + filterCost + projectCost + aggCost + joinCost + sortCost
  }

  private def calculateCometCost(
      stats: PlanStatistics,
      rowCount: Option[Long],
      conf: SQLConf): Double = {
    val rows = rowCount.getOrElse(CometConf.COMET_CBO_DEFAULT_ROW_COUNT.get(conf)).toDouble

    // Comet operators cost less (divided by speedup factor)
    val cometScanCost = stats.cometScans * CometConf.COMET_CBO_SCAN_WEIGHT.get(
      conf) * rows / CometConf.COMET_CBO_SCAN_SPEEDUP.get(conf)
    val cometFilterCost = stats.cometFilters * CometConf.COMET_CBO_FILTER_WEIGHT.get(
      conf) * rows / CometConf.COMET_CBO_FILTER_SPEEDUP.get(conf)
    val cometProjectCost = stats.cometProjects * CometConf.COMET_CBO_PROJECT_WEIGHT.get(
      conf) * rows / CometConf.COMET_CBO_FILTER_SPEEDUP.get(conf)
    val cometAggCost = stats.cometAggregates * CometConf.COMET_CBO_AGGREGATE_WEIGHT.get(
      conf) * rows / CometConf.COMET_CBO_AGGREGATE_SPEEDUP.get(conf)
    val cometJoinCost = stats.cometJoins * CometConf.COMET_CBO_JOIN_WEIGHT.get(
      conf) * rows / CometConf.COMET_CBO_JOIN_SPEEDUP.get(conf)
    val cometSortCost = stats.cometSorts * CometConf.COMET_CBO_SORT_WEIGHT.get(conf) * rows * Math
      .log(rows + 1) / CometConf.COMET_CBO_SORT_SPEEDUP.get(conf)

    val cometOpCost = cometScanCost + cometFilterCost + cometProjectCost +
      cometAggCost + cometJoinCost + cometSortCost

    // Spark operators that couldn't be converted still have full cost
    val sparkScanCost = stats.sparkScans * CometConf.COMET_CBO_SCAN_WEIGHT.get(conf) * rows
    val sparkFilterCost = stats.sparkFilters * CometConf.COMET_CBO_FILTER_WEIGHT.get(conf) * rows
    val sparkProjectCost =
      stats.sparkProjects * CometConf.COMET_CBO_PROJECT_WEIGHT.get(conf) * rows
    val sparkAggCost =
      stats.sparkAggregates * CometConf.COMET_CBO_AGGREGATE_WEIGHT.get(conf) * rows
    val sparkJoinCost = stats.sparkJoins * CometConf.COMET_CBO_JOIN_WEIGHT.get(conf) * rows
    val sparkSortCost =
      stats.sparkSorts * CometConf.COMET_CBO_SORT_WEIGHT.get(conf) * rows * Math.log(rows + 1)

    val sparkOpCost = sparkScanCost + sparkFilterCost + sparkProjectCost +
      sparkAggCost + sparkJoinCost + sparkSortCost

    // Transition penalty
    val transitionCost = stats.transitions * CometConf.COMET_CBO_TRANSITION_COST.get(conf) * rows

    cometOpCost + sparkOpCost + transitionCost
  }
}

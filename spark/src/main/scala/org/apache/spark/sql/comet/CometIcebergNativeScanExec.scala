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

package org.apache.spark.sql.comet

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.AccumulatorV2

import com.google.common.base.Objects

import org.apache.comet.iceberg.CometIcebergNativeScanMetadata
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometIcebergNativeScan

/**
 * Native Iceberg scan operator that delegates file reading to iceberg-rust.
 *
 * Replaces Spark's Iceberg BatchScanExec to bypass the DataSource V2 API and enable native
 * execution. Iceberg's catalog and planning run in Spark to produce FileScanTasks, which are
 * serialized to protobuf for the native side to execute using iceberg-rust's FileIO and
 * ArrowReader. This provides better performance than reading through Spark's abstraction layers.
 *
 * Supports Dynamic Partition Pruning (DPP) via top-level `runtimeFilters` (mirroring Spark's
 * `BatchScanExec.runtimeFilters`). Because the field is a constructor parameter, Spark's standard
 * `expressions` walk picks up the contained `DynamicPruningExpression(InSubqueryExec(...))`, and
 * the standard `prepare -> prepareSubqueries -> waitForSubqueries` lifecycle resolves it. The
 * lifecycle is invoked via `CometLeafExec.ensureSubqueriesResolved`, called from
 * `PlanDataInjector.findAllPlanData` before `commonData` is read.
 */
case class CometIcebergNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    runtimeFilters: Seq[Expression],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    metadataLocation: String,
    scanHashCode: Int,
    @transient nativeIcebergScanMetadata: CometIcebergNativeScanMetadata)
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometIcebergNativeScan"

  /**
   * `originalPlan` rebuilt with the current top-level `runtimeFilters`. Spark's
   * PlanAdaptiveDynamicPruningFilters and our transformExpressionsUp passes rewrite the top-level
   * `runtimeFilters` (visible via productIterator), but `originalPlan` is @transient and not
   * touched by transformAllExpressions. serializePartitions reads runtime filters via `inputRDD
   * -> filteredPartitions`, so an out-of-sync originalPlan would re-translate the original
   * (unresolved) InSubqueryExec and throw "no subquery result". This makes the top-level
   * runtimeFilters the single source of truth at serialization time. Iceberg reports its planning
   * metrics on the instance whose `inputRDD` ran, so [[LazyIcebergMetric]] reads them from here.
   */
  @transient private lazy val plannedOriginalPlan: BatchScanExec = {
    // Canonicalized instances set originalPlan = null and are not meant to be executed.
    // If we ever reach this lazy val on a canonicalized form, fail loud rather than NPE
    // deep inside originalPlan.inputRDD.
    assert(
      originalPlan != null,
      "plan data accessed on a canonicalized CometIcebergNativeScanExec; " +
        "this lazy val should only execute on non-canonical instances")
    if (originalPlan.runtimeFilters != runtimeFilters) {
      originalPlan.copy(runtimeFilters = runtimeFilters)
    } else {
      originalPlan
    }
  }

  /**
   * Lazy partition serialization, deferred until execution time. Triggered from `commonData` /
   * `perPartitionData` (via `PlanDataInjector.findAllPlanData`) and from
   * `LazyIcebergMetric.value` (via Iceberg planning metrics). Lazy val semantics ensure single
   * evaluation across entry points.
   *
   * DPP InSubqueryExec values must already be resolved by the time this lazy val runs.
   * `PlanDataInjector.findAllPlanData` calls `ensureSubqueriesResolved` (which invokes Spark's
   * `prepare -> waitForSubqueries`) before reading `commonData`. The `serializePartitions` call
   * below reads `originalPlan.runtimeFilters` indirectly through `inputRDD -> filteredPartitions`
   * and applies the resolved values to Iceberg's runtime filtering. `originalPlan.runtimeFilters`
   * shares the same `InSubqueryExec` instances as the top-level `runtimeFilters` field (enforced
   * by every construction site), so values resolved through `waitForSubqueries` are visible on
   * both sides.
   */
  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) =
    CometIcebergNativeScan.serializePartitions(
      plannedOriginalPlan,
      output,
      nativeIcebergScanMetadata)

  /**
   * The SQL UI aggregates accumulator updates instead of reading driver-side values, so the
   * planning metrics reach it only as a driver metric update, posted from the first read of
   * `commonData` inside each SQL execution. Spark keeps driver updates as a list, so a second
   * post within one execution would double the displayed totals, while a re-executed Dataset
   * reuses this plan and needs its own post. `perPartitionData` does not post: it also backs
   * `numPartitions`, which planning could read before the execution is live.
   */
  @transient private lazy val planningMetricsPostedTo = ConcurrentHashMap.newKeySet[String]()

  private def postPlanningMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null && planningMetricsPostedTo.add(executionId)) {
      SQLMetrics.postDriverMetricUpdates(
        sparkContext,
        executionId,
        icebergPlanningMetrics.values.toSeq)
    }
  }

  def commonData: Array[Byte] = {
    postPlanningMetrics()
    serializedPartitionData._1
  }

  def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  // numPartitions for execution - derived from actual DPP-filtered partitions
  // Only accessed during execution, not planning
  def numPartitions: Int = perPartitionData.length

  override lazy val outputPartitioning: Partitioning = UnknownPartitioning(numPartitions)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  /**
   * Maps Iceberg V2 custom metric types to standard Spark metric types for better UI formatting.
   *
   * Iceberg uses V2 custom metrics which don't get formatted in Spark UI (they just show raw
   * numbers). By mapping to standard Spark types, we get proper formatting:
   *   - "size" metrics: formatted as KB/MB/GB (e.g., "10.3 GB" instead of "11040868925")
   *   - "timing" metrics: formatted as ms/s (e.g., "200 ms" instead of "200")
   *   - "sum" metrics: plain numbers with commas (e.g., "1,000")
   *
   * This provides better UX than vanilla Iceberg Java which shows raw numbers.
   */
  private def mapMetricType(name: String, originalType: String): String = {
    import java.util.Locale

    // Only remap V2 custom metrics; leave standard Spark metrics unchanged
    if (!originalType.startsWith("v2Custom_")) {
      return originalType
    }

    // Map based on metric name patterns from Iceberg
    val nameLower = name.toLowerCase(Locale.ROOT)
    if (nameLower.contains("size")) {
      "size" // Will format as KB/MB/GB
    } else if (nameLower.contains("duration")) {
      "timing" // Will format as ms/s (Iceberg durations are in milliseconds)
    } else {
      "sum" // Plain number formatting
    }
  }

  /**
   * Defers value computation until .value is read. SparkPlanInfo.fromSparkPlan reads the metrics
   * map (names, ids, metricType) for SQL UI events at planning time, before AQE's
   * queryStageOptimizerRules run. If we triggered serializedPartitionData during map
   * construction, DPP would be resolved against an unconverted CometSubqueryAdaptiveBroadcastExec
   * and throw at executeCollect(). Lazy value access ensures planning runs only when the value is
   * actually needed, by which time CometPlanAdaptiveDynamicPruningFilters has converted the SAB.
   *
   * Overrides merge/reset so nothing can zero out the resolved value: these are driver-side
   * planning metrics, and [[CometMetricNode.fromCometPlan]] keeps them out of the tree that tasks
   * update.
   */
  private class LazyIcebergMetric(metricType: String, metricName: String)
      extends SQLMetric(metricType, 0) {

    override def value: Long = {
      // Ensure DPP InSubqueryExec values are resolved before serializedPartitionData runs;
      // otherwise serializePartitions reads originalPlan.runtimeFilters with empty values
      // and inputRDD -> filteredPartitions skips DPP, caching an unfiltered result.
      ensureSubqueriesResolved()
      val _ = serializedPartitionData
      plannedOriginalPlan.metrics.get(metricName).map(_.value).getOrElse(0L)
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = {}

    override def reset(): Unit = {}
  }

  /**
   * Iceberg planning metrics, declared eagerly from originalPlan.metrics names/types but with
   * values resolved lazily via [[LazyIcebergMetric]]. Constructing this map enumerates only the
   * metric definitions (scan.supportedCustomMetrics), which is a metadata call that does not
   * trigger Iceberg planning. Registered under Iceberg's descriptions so the SQL UI labels them
   * as it labels `BatchScan`.
   */
  @transient private lazy val icebergPlanningMetrics: Map[String, LazyIcebergMetric] = {
    if (originalPlan == null) {
      Map.empty
    } else {
      originalPlan.metrics
        .filterNot { case (name, _) =>
          // numOutputRows and numSplits are runtime metrics incremented on the native side;
          // numDeletes has no native counterpart (iceberg-rust's ScanMetrics only count bytes).
          name == "numOutputRows" || name == "numDeletes" || name == "numSplits"
        }
        .map { case (name, metric) =>
          val mappedType = mapMetricType(name, metric.metricType)
          val lazyMetric = new LazyIcebergMetric(mappedType, name)
          // Comet formats sizes and durations, so drop Iceberg's unit hints from the labels.
          val label = metric.name.getOrElse(name).stripSuffix(" (ms)").stripSuffix(" (bytes)")
          sparkContext.register(lazyMetric, label)
          name -> lazyMetric
        }
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    val baseMetrics = Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "bytes_scanned" -> SQLMetrics.createSizeMetric(sparkContext, "number of bytes scanned"))

    // Add num_splits as a runtime metric (incremented on the native side during execution)
    val numSplitsMetric = SQLMetrics.createMetric(sparkContext, "number of file splits processed")

    baseMetrics ++ icebergPlanningMetrics + ("num_splits" -> numSplitsMetric)
  }

  /** The metrics native execution updates; the planning metrics are set on the driver only. */
  private[comet] def runtimeMetrics: Map[String, SQLMetric] =
    metrics -- icebergPlanningMetrics.keys

  /** Executes using CometExecRDD - planning data is computed lazily on first access. */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    // Key by the same (metadata_location, scan_hash_code) pair PlanDataInjector.injectPlanData
    // looks up via IcebergPlanDataInjector.getKey (see the scan_hash_code field comment in
    // operator.proto for why metadata_location alone cannot identify a scan).
    val injectorKey = IcebergPlanDataInjector.getKey(nativeOp).getOrElse(metadataLocation)
    new CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(injectorKey -> commonData),
      perPartitionByKey = Map(injectorKey -> perPartitionData),
      serializedPlan = serializedPlan,
      planFingerprint = PlanDataInjector.planFingerprint(serializedPlan),
      defaultNumPartitions = perPartitionData.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty)
  }

  /**
   * Override convertBlock to preserve @transient fields. The parent implementation uses
   * makeCopy() which loses transient fields.
   */
  override def convertBlock(): CometIcebergNativeScanExec = {
    // Serialize the native plan if not already done
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      val bytes = CometExec.serializeNativePlan(nativeOp)
      SerializedPlan(Some(bytes))
    } else {
      serializedPlanOpt
    }

    // Create new instance preserving transient fields
    CometIcebergNativeScanExec(
      nativeOp,
      output,
      runtimeFilters,
      originalPlan,
      newSerializedPlan,
      metadataLocation,
      scanHashCode,
      nativeIcebergScanMetadata)
  }

  override protected def doCanonicalize(): CometIcebergNativeScanExec = {
    CometIcebergNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(runtimeFilters),
        output),
      null, // Don't need originalPlan for canonicalization
      SerializedPlan(None),
      metadataLocation,
      // originalPlan is nulled here, so scanHashCode is the only field left that distinguishes
      // scans differing solely in pushed-down filters. Dropping it lets ReuseExchange collapse
      // them (see #4774).
      scanHashCode,
      null
    ) // Don't need metadata for canonicalization
  }

  override def stringArgs: Iterator[Any] = {
    // Use metadata task count to avoid triggering serializedPartitionData during planning
    val hasMeta = nativeIcebergScanMetadata != null && nativeIcebergScanMetadata.tasks != null
    val taskCount = if (hasMeta) nativeIcebergScanMetadata.tasks.size() else 0
    val scanDesc = if (originalPlan != null) originalPlan.scan.description() else "canonicalized"
    // Include runtime filters (DPP) in string representation
    val runtimeFiltersStr = if (runtimeFilters.nonEmpty) {
      s", runtimeFilters=${runtimeFilters.mkString("[", ", ", "]")}"
    } else {
      ""
    }
    Iterator(output, s"$metadataLocation, $scanDesc$runtimeFiltersStr", taskCount)
  }

  /**
   * Equality / hashCode include runtimeFilters so plan-tree walks (e.g. transformUp) detect DPP
   * rewrites. Without runtimeFilters in equality, replacing an `InSubqueryExec` produces a new
   * instance that compares equal to the old one and the rewrite is silently dropped.
   * `originalPlan` (`@transient`) and `nativeIcebergScanMetadata` (`@transient`) are
   * intentionally omitted: they're recoverable from `metadataLocation` + the serialized plan and
   * including them would over-constrain equality across re-planning.
   *
   * `scanHashCode` (Iceberg's `SparkScan.hashCode()`, folding in pushed filters, snapshot,
   * branch, and read schema) distinguishes scans that differ only in static pushed-down filters,
   * so that ReuseExchange does not collapse two scans that read different data (see #4774). It is
   * an `Int` and so carries a theoretical hash-collision risk, but `metadataLocation` (table +
   * snapshot path) is compared alongside it, matching how Iceberg's `SparkBatch.equals` pairs the
   * hash with `table.name()`.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometIcebergNativeScanExec =>
        this.metadataLocation == other.metadataLocation &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.runtimeFilters == other.runtimeFilters &&
        this.scanHashCode == other.scanHashCode
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      metadataLocation,
      output.asJava,
      serializedPlanOpt,
      runtimeFilters,
      // Ascribe java.lang.Integer: Scala 2.12 rejects the implicit Int -> Object conversion into
      // the Object... varargs of Guava's Objects.hashCode.
      scanHashCode: java.lang.Integer)
}

object CometIcebergNativeScanExec {

  /** Creates a CometIcebergNativeScanExec with deferred partition serialization. */
  def apply(
      nativeOp: Operator,
      scanExec: BatchScanExec,
      session: SparkSession,
      metadataLocation: String,
      nativeIcebergScanMetadata: CometIcebergNativeScanMetadata): CometIcebergNativeScanExec = {

    val exec = CometIcebergNativeScanExec(
      nativeOp,
      scanExec.output,
      scanExec.runtimeFilters,
      scanExec,
      SerializedPlan(None),
      metadataLocation,
      // Capture Iceberg's scan hash now, while the transient scan is still available; it is
      // needed for equality after canonicalization nulls originalPlan (see #4774).
      scanExec.scan.hashCode(),
      nativeIcebergScanMetadata)

    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

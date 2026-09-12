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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ReusedSubqueryExec, ScalarSubquery, SparkPlan, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.contrib.delta.DeltaSparkScanEnvelope
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native scan node for Delta Lake tables (contrib). Delta's own planning (log replay, snapshot
 * resolution, partition pruning) has already run inside delta-spark by the time this node is
 * created from the DSv1 [[FileSourceScanExec]]; file listing and split planning are delegated to
 * a [[CometScanExec]] helper, and data reads execute through Comet's native DataFusion parquet
 * machinery, inheriting row-group and page-index pruning.
 *
 * DPP: `runtimeFilters` is a constructor field included in equality, so its rewrite (via
 * [[CometScanWithPlanData]]) survives plan copies -- a transient field would be dropped by
 * `TreeNode.makeCopy` on MERGE re-planning (the CometIcebergNativeScanExec lesson).
 */
case class CometDeltaNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    requiredSchema: StructType,
    runtimeFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    @transient relation: HadoopFsRelation,
    originalPlan: FileSourceScanExec,
    override val serializedPlanOpt: SerializedPlan,
    sourceKey: String)
    extends CometLeafExec
    with CometScanWithPlanData {

  override val nodeName: String = s"CometDeltaNativeScan $relation"

  // Derived from (originalPlan, runtimeFilters), never stored: any copy of this node
  // automatically gets a helper consistent with ITS runtimeFilters, avoiding the #3510 class of
  // bug where a stored helper field desyncs from rewritten filters. Costs one extra file listing
  // per executed instance; correctness over the duplicate driver-side listing.
  //
  // Forcing invariant: this lazy val is forced by the `metrics` override below, and AQE's UI
  // plan-walk calls `.metrics` on every node MID-PLANNING, including while a DPP subquery is
  // still an adaptive placeholder or a partition filter holds an unresolved ScalarSubquery (see
  // `hasUnevaluableSubqueryFilter` below). That's safe ONLY because constructing `scanHelper` is a
  // cheap case-class build with no file listing, and core's `CometScanExec.metrics` touches only
  // `wrapped.driverMetrics` (populated by Spark's own planning) plus a static metric-node
  // constructor -- neither file listing nor subquery resolution. If core's `metrics` ever touches
  // either, forcing `scanHelper` here would resurrect the AQE mid-planning crashes this invariant
  // prevents.
  @transient private lazy val scanHelper: CometScanExec =
    CometDeltaNativeScanExec.planningHelper(originalPlan, runtimeFilters)

  // NOT lazy val: while a DPP subquery is still an adaptive placeholder, or a partition filter
  // holds an unresolved scalar subquery, this returns a temporary value that must not be
  // memoized -- after CometPlanAdaptiveDynamicPruningFilters rewrites the filters (DPP case) or
  // AQE resolves the subquery (scalar case), later reads must see the real post-pruning
  // partition count.
  override def outputPartitioning: Partitioning =
    if (hasUnevaluableSubqueryFilter) UnknownPartitioning(0)
    else UnknownPartitioning(perPartitionData.length)

  // runtimeFilters IS scanHelper.partitionFilters element-for-element, so checking runtimeFilters
  // here avoids constructing/forcing the derived scanHelper just to read partitioning. The
  // InSubqueryExec placeholder shapes mirror
  // CometPlanAdaptiveDynamicPruningFilters.extractSABData + hasWrappedSAB -- keep in sync. The
  // ScalarSubquery case is probed rather than treated as permanently unevaluable: Spark exposes no
  // public finished/updated flag on ExecSubqueryExpression, but `eval()` doubles as one -- it only
  // reads the cached `result` behind a `require(updated, ...)` guard, while the subquery is
  // actually run by `updateResult()` (invoked separately during prepare/AQE), never by `eval()`.
  // Once resolved, outputPartitioning below reports the real perPartitionData.length instead of
  // staying at zero -- a fused native parent's buildNativeContext requires that count to match.
  private def hasUnevaluableSubqueryFilter: Boolean =
    runtimeFilters.exists(_.exists {
      // Match `e: InSubqueryExec` and dispatch on e.plan rather than unapplying InSubqueryExec
      // directly: its unapply arity differs across Spark versions and this module ships no
      // version shim.
      case e: InSubqueryExec => isAdaptivePlaceholder(e.plan)
      case s: ScalarSubquery => !isScalarSubqueryResolved(s)
      case _ => false
    })

  // `eval()` never triggers the subquery's execution: on a resolved subquery it is a pure cached
  // read of `result` (verified against bytecode: `Predef.require(updated(), ...)` then a plain
  // field read), so this probe is safe to call repeatedly, including from AQE's mid-planning plan
  // walks. Pre-resolution, the ONLY throw is `require`'s `IllegalArgumentException`; catch exactly
  // that, since anything else escaping is a genuine bug we must not mask as unpartitioned.
  private def isScalarSubqueryResolved(s: ScalarSubquery): Boolean =
    try {
      s.eval()
      true
    } catch {
      case _: IllegalArgumentException => false
    }

  private def isAdaptivePlaceholder(p: SparkPlan): Boolean = p match {
    case ReusedSubqueryExec(inner) => isAdaptivePlaceholder(inner)
    case _: CometSubqueryAdaptiveBroadcastExec => true
    case _: SubqueryAdaptiveBroadcastExec => true
    case _ => false
  }

  override lazy val outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  override def dynamicPruningFilters: Seq[Expression] = runtimeFilters

  override def withDynamicPruningFilters(filters: Seq[Expression]): SparkPlan = {
    // A real copy: runtimeFilters is a constructor field included in equality, so the copy
    // survives enclosing-block rebuilds, and the derived scanHelper picks up the rewritten
    // filters automatically.
    copy(runtimeFilters = filters)
  }

  /**
   * Lazy split-mode serialization, mirroring CometNativeScanExec: common data was serialized at
   * planning; per-partition file lists serialize here, at execution time.
   */
  @transient private lazy val serializedPartitionData
      : (Array[Byte], Array[Array[Byte]], Array[Seq[String]]) = {
    // Resolve the helper's DPP subqueries: it holds its own InSubqueryExec instances that
    // Spark's expressions walk does not see (the helper is derived, not a child).
    scanHelper.partitionFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) if e.values().isEmpty =>
        e.updateResult()
      case _ =>
    }

    val commonBytes = {
      val deltaScan = DeltaSparkScanEnvelope.unpack(nativeOp)
      // Scalar subqueries in dataFilters were unresolved at planning; resolve them now and
      // append them as pushed filters, as CometNativeScanExec.serializedPartitionData does.
      // has_data_filters follows their presence, not the serialized count: a filter that fails
      // to serialize still keeps native on the safe timestamp conversion for a filtered scan.
      val resolved = org.apache.comet.contrib.delta.CometDeltaNativeScan
        .resolvedSubqueryFilters(dataFilters, output, requiredSchema, conf)
      val common = if (!resolved.hasResolvedFilters) {
        deltaScan.getCommon
      } else {
        val builder = deltaScan.getCommon.toBuilder
        builder.setHasDataFilters(true)
        resolved.protos.foreach(builder.addDataFilters)
        builder.build()
      }
      OperatorOuterClass.DeltaSparkScan
        .newBuilder()
        .setCommon(common)
        .setDeltaCommon(deltaScan.getDeltaCommon)
        .build()
        .toByteArray
    }

    val filePartitions = scanHelper.getFilePartitions()

    val tableRoot = DeltaSparkScanEnvelope.unpack(nativeOp).getDeltaCommon.getTableRoot
    val perPartitionBytes = filePartitions.map { filePartition =>
      org.apache.comet.contrib.delta.CometDeltaNativeScan
        .serializePartition(filePartition, originalPlan, tableRoot)
    }.toArray

    val perPartitionPaths = filePartitions.map(_.files.map(_.filePath.toString).toSeq).toArray

    (commonBytes, perPartitionBytes, perPartitionPaths)
  }

  override def commonData: Array[Byte] = serializedPartitionData._1

  override def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  def perPartitionFilePaths: Array[Seq[String]] = serializedPartitionData._3

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)

    new CometExecRDD(
      sparkContext,
      Seq.empty,
      Map(sourceKey -> commonData),
      Map(sourceKey -> perPartitionData),
      serializedPlan,
      perPartitionData.length,
      output.length,
      nativeMetrics,
      Seq.empty,
      None,
      Seq.empty,
      perPartitionFilePaths = perPartitionFilePaths,
      reportScanInputMetrics = true)
  }

  override def doCanonicalize(): CometDeltaNativeScanExec = {
    val canonOriginal = if (originalPlan != null) {
      val stripped = originalPlan.copy(partitionFilters =
        CometScanUtils.filterUnusedDynamicPruningExpressions(originalPlan.partitionFilters))
      stripped.doCanonicalize()
    } else {
      null
    }
    CometDeltaNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(runtimeFilters),
        output),
      QueryPlan.normalizePredicates(dataFilters, output),
      relation,
      canonOriginal,
      SerializedPlan(None),
      "")
  }

  override def stringArgs: Iterator[Any] = Iterator(output, runtimeFilters)

  override def equals(obj: Any): Boolean = obj match {
    case other: CometDeltaNativeScanExec =>
      this.originalPlan == other.originalPlan &&
      this.serializedPlanOpt == other.serializedPlanOpt &&
      this.runtimeFilters == other.runtimeFilters &&
      this.dataFilters == other.dataFilters
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(originalPlan, serializedPlanOpt, runtimeFilters, dataFilters)

  private val driverMetricKeys =
    Set(
      "numFiles",
      "filesSize",
      "numPartitions",
      "metadataTime",
      "staticFilesNum",
      "staticFilesSize",
      "pruningTime")

  // Forces `scanHelper` (see its doc above for why that -- and reading `.metrics` off it -- is
  // safe even when AQE calls `.metrics` mid-planning against an unresolved DPP/scalar subquery).
  override lazy val metrics: Map[String, SQLMetric] = {
    CometMetricNode.nativeScanMetrics(session.sparkContext) ++
      scanHelper.metrics.filter { case (k, _) => driverMetricKeys.contains(k) }
  }
}

object CometDeltaNativeScanExec {

  /** File-planning helper: reuses CometScanExec's listing/splitting/DPP machinery. */
  def planningHelper(
      scanExec: FileSourceScanExec,
      partitionFilters: Seq[Expression]): CometScanExec =
    CometScanExec(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan,
      scanExec)

  def apply(
      nativeOp: Operator,
      scanExec: FileSourceScanExec,
      subqueryDataFilters: Seq[Expression] = Seq.empty): CometDeltaNativeScanExec = {
    // subqueryDataFilters: subquery predicates harvested from the covering FilterExec at claim
    // time (Spark 3.x keeps them out of scanExec.dataFilters; see
    // CometDeltaNativeScan.subqueryFiltersFromParent). Carried in dataFilters so the
    // execution-time resolve-and-push path sees them; correctness never depends on them.
    val exec = CometDeltaNativeScanExec(
      nativeOp,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.dataFilters ++ subqueryDataFilters,
      scanExec.relation,
      scanExec,
      SerializedPlan(None),
      DeltaSparkScanEnvelope.unpack(nativeOp).getDeltaCommon.getSourceKey)
    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

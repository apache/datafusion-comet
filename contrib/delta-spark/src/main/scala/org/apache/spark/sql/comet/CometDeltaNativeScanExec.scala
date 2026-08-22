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
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ReusedSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec}
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
 * DPP: `runtimeFilters` is a constructor field included in equality, so
 * `CometPlanAdaptiveDynamicPruningFilters`'s rewrite (via [[CometScanWithPlanData]]) survives
 * plan copies, the lesson from CometIcebergNativeScanExec (a transient field is dropped by
 * `TreeNode.makeCopy` on MERGE re-planning).
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

  // Derived from (originalPlan, runtimeFilters), never stored: any copy of this node, our
  // own withDynamicPruningFilters, or a generic Catalyst expression rewrite going through
  // TreeNode.makeCopy, automatically gets a helper consistent with ITS runtimeFilters. A
  // stored helper field would desync from rewritten filters (the #3510 class of bug). The
  // cost is that file listing runs once per executed instance (planning listed separately in
  // the rule extension); correctness over the duplicate driver-side listing.
  @transient private lazy val scanHelper: CometScanExec =
    CometDeltaNativeScanExec.planningHelper(originalPlan, runtimeFilters)

  // NOT lazy val: while a DPP subquery is still an adaptive placeholder this returns a
  // temporary value that must not be memoized -- after CometPlanAdaptiveDynamicPruningFilters
  // rewrites the filters, later reads must see the real post-pruning partition count.
  override def outputPartitioning: Partitioning =
    if (hasAdaptivePlaceholderFilter) UnknownPartitioning(0)
    else UnknownPartitioning(perPartitionData.length)

  // runtimeFilters IS scanHelper.partitionFilters element-for-element (planningHelper passes
  // partitionFilters = runtimeFilters into CometScanExec's plain constructor field below), so
  // checking runtimeFilters here avoids constructing/forcing the derived scanHelper just to
  // read partitioning. The placeholder shapes mirror
  // CometPlanAdaptiveDynamicPruningFilters.extractSABData + hasWrappedSAB -- keep these two
  // sets in sync; if that rule learns to unwrap a new wrapper form, mirror it here too.
  private def hasAdaptivePlaceholderFilter: Boolean =
    runtimeFilters.exists(_.exists {
      // Match `e: InSubqueryExec` and dispatch on e.plan rather than unapplying InSubqueryExec
      // directly: its unapply arity differs across Spark versions and this module ships no
      // version shim.
      case e: InSubqueryExec => isAdaptivePlaceholder(e.plan)
      case _ => false
    })

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
      val subqueryFilters = org.apache.comet.contrib.delta.CometDeltaNativeScan
        .resolvedSubqueryFilters(dataFilters, output, requiredSchema, conf)
      val common = if (subqueryFilters.isEmpty) {
        deltaScan.getCommon
      } else {
        val builder = deltaScan.getCommon.toBuilder
        subqueryFilters.foreach(builder.addDataFilters)
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
      perPartitionFilePaths = perPartitionFilePaths)
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

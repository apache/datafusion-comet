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
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native Delta Change Data Feed scan (`readChangeFeed`).
 *
 * Unlike [[CometDeltaNativeScanExec]] (per-file task list, DPP, encryption), a CDF read has no
 * per-file tasks: the native `DeltaKernelScanExec` reconstructs delta-kernel's
 * `TableChanges(start, end)` from the `DeltaScanCommon`'s `cdf_read` + version fields and calls
 * `execute()`, emitting the data columns plus `_change_type` / `_commit_version` /
 * `_commit_timestamp` (kernel's CDF per-file API is `pub(crate)`; only `execute()` is public).
 *
 * Version-range split: the inclusive `[start, end]` range is chunked into `subRanges` contiguous
 * slices, one per Spark partition. Each partition runs an independent native `TableChanges` read of
 * its slice -- the shared `nativeOp` carries the full range (+ schema, + source key); the
 * per-partition `DeltaScan` carries only that partition's sub-range, which `DeltaPlanDataInjector`
 * splices over the shared range at execution. This stays a SINGLE `CometNativeExec` emitting N
 * partitions (not a `CometUnionExec`, which is not a `CometNativeExec` and would make a downstream
 * native shuffle / aggregation ineligible). A single-element `subRanges` is the un-split case.
 *
 * `originalPlan` is the `RowDataSourceScanExec` over `DeltaCDFRelation` we replaced; it is used only
 * for `output` and `logicalLink` (there is no `FileSourceScanExec` / `HadoopFsRelation` here).
 */
case class CometDeltaCdfScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan,
    @transient originalPlan: SparkPlan,
    tableRoot: String,
    subRanges: Seq[(Long, Option[Long])])
    extends CometLeafExec {

  override def outputPartitioning: Partitioning = UnknownPartitioning(subRanges.length)

  override def outputOrdering: Seq[SortOrder] = Nil

  override lazy val metrics: Map[String, SQLMetric] = {
    // Key under both the native-side name (`output_rows`, set by BaselineMetrics in
    // kernel_scan.rs) and the Spark streaming ProgressReporter name (`numOutputRows`).
    val outputRowsMetric = SQLMetrics.createMetric(sparkContext, "number of output rows")
    Map("output_rows" -> outputRowsMetric, "numOutputRows" -> outputRowsMetric)
  }

  private def sourceKey: String = CometDeltaNativeScanExec.computeSourceKey(nativeOp)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    // The DeltaScanCommon (carrying cdf_read + full version range + required schema) is shared; each
    // per-partition DeltaScan carries only this partition's inclusive cdf sub-range in a minimal
    // common (cdf_read marks it), which DeltaPlanDataInjector splices over the shared range. No
    // per-file tasks -- the native side reconstructs TableChanges from the (overridden) range.
    val commonData = nativeOp.getDeltaScan.getCommon.toByteArray
    val perPartition: Array[Array[Byte]] = subRanges.map { case (start, end) =>
      val pc = OperatorOuterClass.DeltaScanCommon.newBuilder()
      pc.setCdfRead(true)
      pc.setCdfStartVersion(start)
      end.foreach(pc.setCdfEndVersion)
      val b = OperatorOuterClass.DeltaScan.newBuilder().setCommon(pc.build())
      if (tableRoot != null && tableRoot.nonEmpty) b.setTableRoot(tableRoot)
      b.build().toByteArray
    }.toArray
    CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> perPartition),
      serializedPlan = serializedPlan,
      numPartitions = perPartition.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty,
      broadcastedHadoopConfForEncryption = None,
      encryptedFilePaths = Seq.empty,
      perPartitionFilePaths = Array.fill(perPartition.length)(Seq.empty))
  }

  override def convertBlock(): CometDeltaCdfScanExec = {
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      SerializedPlan(Some(CometExec.serializeNativePlan(nativeOp)))
    } else {
      serializedPlanOpt
    }
    copy(serializedPlanOpt = newSerializedPlan)
  }

  override protected def doCanonicalize(): CometDeltaCdfScanExec = {
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      serializedPlanOpt = SerializedPlan(None),
      originalPlan = null)
  }
}

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

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, DynamicPruningExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.AccumulatorV2

import com.google.common.base.Objects

import org.apache.comet.iceberg.CometIcebergNativeScanMetadata
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometIcebergNativeScan
import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Native Iceberg scan operator that delegates file reading to iceberg-rust.
 *
 * Replaces Spark's Iceberg BatchScanExec to bypass the DataSource V2 API and enable native
 * execution. Iceberg's catalog and planning run in Spark to produce FileScanTasks, which are
 * serialized to protobuf for the native side to execute using iceberg-rust's FileIO and
 * ArrowReader. This provides better performance than reading through Spark's abstraction layers.
 *
 * Supports Dynamic Partition Pruning (DPP) by deferring partition serialization to execution
 * time. The doPrepare() method waits for DPP subqueries to resolve, then lazy
 * serializedPartitionData serializes the DPP-filtered partitions from inputRDD.
 */
case class CometIcebergNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    metadataLocation: String,
    @transient nativeIcebergScanMetadata: CometIcebergNativeScanMetadata)
    extends CometLeafExec
    with ShimSubqueryBroadcast {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometIcebergNativeScan"

  /**
   * Prepare DPP subquery plans. Called by Spark's prepare() before doExecuteColumnar().
   *
   * This follows Spark's convention of preparing subqueries in doPrepare() rather than
   * doExecuteColumnar(). While the actual waiting for DPP results happens later in
   * serializedPartitionData, calling prepare() here ensures subquery plans are set up before
   * execution begins.
   */
  override protected def doPrepare(): Unit = {
    originalPlan.runtimeFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan.prepare()
      case _ =>
    }
    super.doPrepare()
  }

  /**
   * Lazy partition serialization - deferred until execution time for DPP support.
   *
   * Entry points: This lazy val may be triggered from either doExecuteColumnar() (via
   * commonData/perPartitionData) or capturedMetricValues (for Iceberg metrics). Lazy val
   * semantics ensure single evaluation regardless of entry point.
   *
   * DPP (Dynamic Partition Pruning) Flow:
   *
   * {{{
   * Planning time:
   *   CometIcebergNativeScanExec created
   *     - serializedPartitionData not evaluated (lazy)
   *     - No partition serialization yet
   *
   * Execution time:
   *   1. Spark calls prepare() on the plan tree
   *        - doPrepare() calls e.plan.prepare() for each DPP filter
   *        - Subquery plans are set up (but not yet executed)
   *
   *   2. Spark calls doExecuteColumnar() (or metrics are accessed)
   *        - Accesses perPartitionData (or capturedMetricValues)
   *        - Forces serializedPartitionData evaluation (here)
   *        - Waits for DPP values (updateResult or reflection)
   *        - Calls serializePartitions with DPP-filtered inputRDD
   *        - Only matching partitions are serialized
   * }}}
   */
  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) = {
    // Ensure DPP subqueries are resolved before accessing inputRDD.
    originalPlan.runtimeFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) if e.values().isEmpty =>
        e.plan match {
          case sab: SubqueryAdaptiveBroadcastExec =>
            // SubqueryAdaptiveBroadcastExec.executeCollect() throws, so we call
            // child.executeCollect() directly. We use the index from SAB to find the
            // right buildKey, then locate that key's column in child.output.
            val rows = sab.child.executeCollect()
            val indices = getSubqueryBroadcastIndices(sab)

            // SPARK-46946 changed index: Int to indices: Seq[Int] as a preparatory refactor
            // for future features (Null Safe Equality DPP, multiple equality predicates).
            // Currently indices always has one element. CometScanRule checks for multi-index
            // DPP and falls back, so this assertion should never fail.
            assert(
              indices.length == 1,
              s"Multi-index DPP not supported: indices=$indices. See SPARK-46946.")
            val buildKeyIndex = indices.head
            val buildKey = sab.buildKeys(buildKeyIndex)

            // Find column index in child.output by matching buildKey's exprId
            val colIndex = buildKey match {
              case attr: Attribute =>
                sab.child.output.indexWhere(_.exprId == attr.exprId)
              // DPP may cast partition column to match join key type
              case Cast(attr: Attribute, _, _, _) =>
                sab.child.output.indexWhere(_.exprId == attr.exprId)
              case _ => buildKeyIndex
            }
            if (colIndex < 0) {
              throw new IllegalStateException(
                s"DPP build key '$buildKey' not found in ${sab.child.output.map(_.name)}")
            }

            setInSubqueryResult(e, rows.map(_.get(colIndex, e.child.dataType)))
          case _ =>
            e.updateResult()
        }
      case _ =>
    }

    CometIcebergNativeScan.serializePartitions(originalPlan, output, nativeIcebergScanMetadata)
  }

  /**
   * Sets InSubqueryExec's private result field via reflection.
   *
   * Reflection is required because:
   *   - SubqueryAdaptiveBroadcastExec.executeCollect() throws UnsupportedOperationException
   *   - InSubqueryExec has no public setter for result, only updateResult() which calls
   *     executeCollect()
   *   - We can't replace e.plan since it's a val
   */
  private def setInSubqueryResult(e: InSubqueryExec, result: Array[_]): Unit = {
    val fields = e.getClass.getDeclaredFields
    // Field name is mangled by Scala compiler, e.g. "org$apache$...$InSubqueryExec$$result"
    val resultField = fields
      .find(f => f.getName.endsWith("$result") && !f.getName.contains("Broadcast"))
      .getOrElse {
        throw new IllegalStateException(
          s"Cannot find 'result' field in ${e.getClass.getName}. " +
            "Spark version may be incompatible with Comet's DPP implementation.")
      }
    resultField.setAccessible(true)
    resultField.set(e, result)
  }

  def commonData: Array[Byte] = serializedPartitionData._1
  def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  // numPartitions for execution - derived from actual DPP-filtered partitions
  // Only accessed during execution, not planning
  def numPartitions: Int = perPartitionData.length

  override lazy val outputPartitioning: Partitioning = UnknownPartitioning(numPartitions)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  // Capture metric VALUES and TYPES (not objects!) in a serializable case class
  // This survives serialization while SQLMetric objects get reset to 0
  private case class MetricValue(name: String, value: Long, metricType: String)

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
   * Captures Iceberg planning metrics for display in Spark UI.
   *
   * This lazy val intentionally triggers serializedPartitionData evaluation because Iceberg
   * populates metrics during planning (when inputRDD is accessed). Both this and
   * doExecuteColumnar() may trigger serializedPartitionData, but lazy val semantics ensure it's
   * evaluated only once.
   */
  @transient private lazy val capturedMetricValues: Seq[MetricValue] = {
    // Guard against null originalPlan (from doCanonicalize)
    if (originalPlan == null) {
      Seq.empty
    } else {
      // Trigger serializedPartitionData to ensure Iceberg planning has run and metrics are populated
      val _ = serializedPartitionData

      originalPlan.metrics
        .filterNot { case (name, _) =>
          // Filter out metrics that are now runtime metrics incremented on the native side
          name == "numOutputRows" || name == "numDeletes" || name == "numSplits"
        }
        .map { case (name, metric) =>
          val mappedType = mapMetricType(name, metric.metricType)
          MetricValue(name, metric.value, mappedType)
        }
        .toSeq
    }
  }

  /**
   * Immutable SQLMetric for planning metrics that don't change during execution.
   *
   * Regular SQLMetric extends AccumulatorV2, which means when execution completes, accumulator
   * updates from executors (which are 0 since they don't update planning metrics) get merged back
   * to the driver, overwriting the driver's values with 0.
   *
   * This class overrides the accumulator methods to make the metric truly immutable once set.
   */
  private class ImmutableSQLMetric(metricType: String) extends SQLMetric(metricType, 0) {

    override def merge(other: AccumulatorV2[Long, Long]): Unit = {}

    override def reset(): Unit = {}
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    val baseMetrics = Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

    // Create IMMUTABLE metrics with captured values AND types
    // these won't be affected by accumulator merges
    val icebergMetrics = capturedMetricValues.map { mv =>
      // Create the immutable metric with initValue = 0 (Spark 4 requires initValue <= 0)
      val metric = new ImmutableSQLMetric(mv.metricType)
      // Set the actual value after creation
      metric.set(mv.value)
      // Register it with SparkContext to assign metadata (name, etc.)
      sparkContext.register(metric, mv.name)
      mv.name -> metric
    }.toMap

    // Add num_splits as a runtime metric (incremented on the native side during execution)
    val numSplitsMetric = SQLMetrics.createMetric(sparkContext, "number of file splits processed")

    baseMetrics ++ icebergMetrics + ("num_splits" -> numSplitsMetric)
  }

  /** Executes using CometExecRDD - planning data is computed lazily on first access. */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    CometExecRDD(sparkContext, commonData, perPartitionData, output.length, nativeMetrics)
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
      originalPlan,
      newSerializedPlan,
      metadataLocation,
      nativeIcebergScanMetadata)
  }

  override protected def doCanonicalize(): CometIcebergNativeScanExec = {
    CometIcebergNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      null, // Don't need originalPlan for canonicalization
      SerializedPlan(None),
      metadataLocation,
      null
    ) // Don't need metadata for canonicalization
  }

  override def stringArgs: Iterator[Any] = {
    // Use metadata task count to avoid triggering serializedPartitionData during planning
    val hasMeta = nativeIcebergScanMetadata != null && nativeIcebergScanMetadata.tasks != null
    val taskCount = if (hasMeta) nativeIcebergScanMetadata.tasks.size() else 0
    val scanDesc = if (originalPlan != null) originalPlan.scan.description() else "canonicalized"
    // Include runtime filters (DPP) in string representation
    val runtimeFiltersStr = if (originalPlan != null && originalPlan.runtimeFilters.nonEmpty) {
      s", runtimeFilters=${originalPlan.runtimeFilters.mkString("[", ", ", "]")}"
    } else {
      ""
    }
    Iterator(output, s"$metadataLocation, $scanDesc$runtimeFiltersStr", taskCount)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometIcebergNativeScanExec =>
        this.metadataLocation == other.metadataLocation &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(metadataLocation, output.asJava, serializedPlanOpt)
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
      scanExec,
      SerializedPlan(None),
      metadataLocation,
      nativeIcebergScanMetadata)

    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

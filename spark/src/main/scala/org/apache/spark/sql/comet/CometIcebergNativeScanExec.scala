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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.AccumulatorV2

import com.google.common.base.Objects

import org.apache.comet.iceberg.CometIcebergNativeScanMetadata
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native Iceberg scan operator that delegates file reading to iceberg-rust.
 *
 * Replaces Spark's Iceberg BatchScanExec to bypass the DataSource V2 API and enable native
 * execution. Iceberg's catalog and planning run in Spark to produce FileScanTasks, which are
 * serialized to protobuf for the native side to execute using iceberg-rust's FileIO and
 * ArrowReader. This provides better performance than reading through Spark's abstraction layers.
 */
case class CometIcebergNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    metadataLocation: String,
    numPartitions: Int,
    nativeIcebergScanMetadata: CometIcebergNativeScanMetadata)
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometIcebergNativeScan"

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(numPartitions)

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

  private val capturedMetricValues: Seq[MetricValue] = {
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
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "time_elapsed_opening" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "Wall clock time elapsed for file opening"),
      "time_elapsed_scanning_until_data" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "Wall clock time elapsed for file scanning + " +
          "first record batch of decompression + decoding"),
      "time_elapsed_scanning_total" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "Total elapsed wall clock time for scanning + record batch decompression / decoding"),
      "time_elapsed_processing" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "Wall clock time elapsed for data decompression + decoding"))

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

  override protected def doCanonicalize(): CometIcebergNativeScanExec = {
    CometIcebergNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      originalPlan.doCanonicalize(),
      SerializedPlan(None),
      metadataLocation,
      numPartitions,
      nativeIcebergScanMetadata)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, s"$metadataLocation, ${originalPlan.scan.description()}", numPartitions)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometIcebergNativeScanExec =>
        this.metadataLocation == other.metadataLocation &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.numPartitions == other.numPartitions &&
        this.nativeIcebergScanMetadata == other.nativeIcebergScanMetadata
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      metadataLocation,
      output.asJava,
      serializedPlanOpt,
      numPartitions: java.lang.Integer,
      nativeIcebergScanMetadata)
}

object CometIcebergNativeScanExec {

  /**
   * Creates a CometIcebergNativeScanExec from a Spark BatchScanExec.
   *
   * Determines the number of partitions from Iceberg's output partitioning:
   *   - KeyGroupedPartitioning: Use Iceberg's partition count
   *   - Other cases: Use the number of InputPartitions from Iceberg's planning
   *
   * @param nativeOp
   *   The serialized native operator
   * @param scanExec
   *   The original Spark BatchScanExec
   * @param session
   *   The SparkSession
   * @param metadataLocation
   *   Path to table metadata file
   * @param nativeIcebergScanMetadata
   *   Pre-extracted Iceberg metadata from planning phase
   * @return
   *   A new CometIcebergNativeScanExec
   */
  def apply(
      nativeOp: Operator,
      scanExec: BatchScanExec,
      session: SparkSession,
      metadataLocation: String,
      nativeIcebergScanMetadata: CometIcebergNativeScanMetadata): CometIcebergNativeScanExec = {

    // Determine number of partitions from Iceberg's output partitioning
    val numParts = scanExec.outputPartitioning match {
      case p: KeyGroupedPartitioning =>
        p.numPartitions
      case _ =>
        scanExec.inputRDD.getNumPartitions
    }

    val exec = CometIcebergNativeScanExec(
      nativeOp,
      scanExec.output,
      scanExec,
      SerializedPlan(None),
      metadataLocation,
      numParts,
      nativeIcebergScanMetadata)

    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

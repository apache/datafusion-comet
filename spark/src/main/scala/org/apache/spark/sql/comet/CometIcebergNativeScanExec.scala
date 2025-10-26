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

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet fully native Iceberg scan node for DataSource V2.
 *
 * Replaces Spark's Iceberg BatchScanExec by extracting FileScanTasks from Iceberg's planning and
 * serializing them to protobuf for native execution. All catalog access and planning happens in
 * Spark's Iceberg integration; the Rust side uses iceberg-rust's FileIO and ArrowReader to read
 * data files based on the pre-planned FileScanTasks. Catalog properties are used to configure the
 * FileIO (for credentials, regions, etc.)
 *
 * **How FileScanTask Serialization Works:**
 *
 * This implementation follows the same pattern as CometNativeScanExec for PartitionedFiles:
 *
 *   1. **At Planning Time (on Driver):**
 *      - CometScanRule creates CometIcebergNativeScanExec with originalPlan (BatchScanExec)
 *      - originalPlan.inputRDD is a DataSourceRDD containing DataSourceRDDPartition objects
 *      - Each partition contains InputPartition objects (from Iceberg's planInputPartitions())
 *      - Each InputPartition wraps a ScanTaskGroup containing FileScanTask objects
 *
 * 2. **During Serialization (in QueryPlanSerde.operator2Proto):**
 *   - When serializing CometIcebergNativeScanExec, we iterate through ALL RDD partitions
 *   - For each partition, extract InputPartitions and their FileScanTasks using reflection
 *   - Serialize each FileScanTask (file path, start, length, delete files) into protobuf
 *   - This happens ONCE on the driver, not per-worker
 *
 * 3. **At Execution Time (on Workers):**
 *   - The serialized plan (with all FileScanTasks) is sent to workers
 *   - Standard CometNativeExec.doExecuteColumnar() flow executes the native plan
 *   - Rust receives IcebergScan operator with FileScanTasks for ALL partitions
 *   - Each worker reads only the tasks for its partition index
 *
 * All tasks are extracted at planning time because the RDD and partitions exist on the driver,
 * and Iceberg has already assigned tasks to partitions.
 *
 * **Filters:** When a filter is on top of this scan, both are serialized together and executed as
 * one unit. No special RDD or per-partition logic needed.
 */
case class CometIcebergNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    metadataLocation: String,
    catalogProperties: Map[String, String], // TODO: Extract for authentication
    numPartitions: Int)
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  // FileScanTasks are serialized at planning time in QueryPlanSerde.operator2Proto()

  override val nodeName: String = "CometIcebergNativeScan"

  // FileScanTasks are serialized at planning time and grouped by partition.
  // Rust uses the partition index to select the correct task group.
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

    // Override merge to do nothing - planning metrics are not updated during execution
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
      // Do nothing - this metric's value is immutable
    }

    // Override reset to do nothing - planning metrics should never be reset
    override def reset(): Unit = {
      // Do nothing - this metric's value is immutable
    }
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
      catalogProperties,
      numPartitions)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, s"$metadataLocation, ${originalPlan.scan.description()}", numPartitions)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometIcebergNativeScanExec =>
        this.metadataLocation == other.metadataLocation &&
        this.catalogProperties == other.catalogProperties &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.numPartitions == other.numPartitions
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      metadataLocation,
      output.asJava,
      serializedPlanOpt,
      numPartitions: java.lang.Integer)
}

object CometIcebergNativeScanExec {

  /**
   * Extracts metadata location from Iceberg table.
   *
   * @param scanExec
   *   The Spark BatchScanExec containing an Iceberg scan
   * @return
   *   Path to the table metadata file
   */
  def extractMetadataLocation(scanExec: BatchScanExec): String = {
    val scan = scanExec.scan

    // Get table via reflection (table() is protected in SparkScan, need to search up hierarchy)
    var clazz: Class[_] = scan.getClass
    var tableMethod: java.lang.reflect.Method = null
    while (clazz != null && tableMethod == null) {
      try {
        tableMethod = clazz.getDeclaredMethod("table")
        tableMethod.setAccessible(true)
      } catch {
        case _: NoSuchMethodException => clazz = clazz.getSuperclass
      }
    }
    if (tableMethod == null) {
      throw new NoSuchMethodException("Could not find table() method in class hierarchy")
    }

    val table = tableMethod.invoke(scan)

    val operationsMethod = table.getClass.getMethod("operations")
    val operations = operationsMethod.invoke(table)

    val currentMethod = operations.getClass.getMethod("current")
    val metadata = currentMethod.invoke(operations)

    val metadataFileLocationMethod = metadata.getClass.getMethod("metadataFileLocation")
    metadataFileLocationMethod.invoke(metadata).asInstanceOf[String]
  }

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
   *   Path to table metadata file from extractMetadataLocation
   * @return
   *   A new CometIcebergNativeScanExec
   */
  def apply(
      nativeOp: Operator,
      scanExec: BatchScanExec,
      session: SparkSession,
      metadataLocation: String): CometIcebergNativeScanExec = {

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
      Map.empty, // TODO: Extract catalog properties for authentication
      numParts)

    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

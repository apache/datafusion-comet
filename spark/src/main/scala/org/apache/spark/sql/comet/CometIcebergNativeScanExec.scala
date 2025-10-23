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

  override lazy val metrics: Map[String, SQLMetric] = {
    Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "time_elapsed_opening" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for FileIO initialization"),
      "time_elapsed_scanning_until_data" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for scanning + first record batch"),
      "time_elapsed_scanning_total" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Total wall clock time for scanning + decompression/decoding"),
      "time_elapsed_processing" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for data decompression + decoding"),
      "bytes_scanned" ->
        SQLMetrics.createSizeMetric(sparkContext, "Number of bytes scanned"),
      "files_scanned" ->
        SQLMetrics.createMetric(sparkContext, "Number of data files scanned"),
      "manifest_files_scanned" ->
        SQLMetrics.createMetric(sparkContext, "Number of manifest files scanned"))
  }
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

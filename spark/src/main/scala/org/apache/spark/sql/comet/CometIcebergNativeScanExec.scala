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
 * Comet fully native Iceberg scan node for DataSource V2 that delegates to iceberg-rust.
 *
 * This replaces Spark's Iceberg BatchScanExec with a native implementation that:
 *   1. Extracts catalog configuration from Spark session 2. Serializes catalog info (type,
 *      properties, namespace, table name) to protobuf 3. Extracts FileScanTasks from Iceberg's
 *      InputPartitions during planning (on driver) 4. Uses iceberg-rust's catalog implementations
 *      to load the table and read data natively
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
 * **Key Insight:** Unlike the initial approach which tried to extract tasks per-partition at
 * execution time, this approach extracts ALL tasks at planning time (just like PartitionedFiles).
 * This works because:
 *   - The RDD and its partitions exist on the driver
 *   - We don't need TaskContext to access InputPartitions
 *   - Iceberg has already done the planning and assigned tasks to partitions
 *   - We just need to serialize this information into the protobuf plan
 *
 * **Why This Works With Filters:** When a filter is on top of CometIcebergNativeScanExec:
 *   - Filter's convertBlock() serializes both filter and scan together
 *   - The scan's nativeOp (created by operator2Proto) already contains all FileScanTasks
 *   - The combined filter+scan native plan is executed as one unit
 *   - No special RDD or per-partition logic needed
 */
case class CometIcebergNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: BatchScanExec,
    override val serializedPlanOpt: SerializedPlan,
    catalogType: String,
    catalogProperties: Map[String, String],
    namespace: Seq[String],
    tableName: String,
    numPartitions: Int) // Number of Spark partitions for proper parallelism
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  // No need to override doExecuteColumnar - parent CometLeafExec handles it
  // FileScanTasks are serialized at planning time in QueryPlanSerde.operator2Proto()
  // just like PartitionedFiles are for CometNativeScanExec

  override val nodeName: String =
    s"CometIcebergNativeScan ${namespace.mkString(".")}.$tableName ($catalogType)"

  // Use the actual number of partitions from Iceberg's planning.
  // FileScanTasks are extracted and serialized at planning time (in QueryPlanSerde.operator2Proto),
  // grouped by partition. Each partition receives only its assigned tasks via the protobuf message.
  // The Rust side uses the partition index to select the correct task group.
  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(numPartitions)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  override protected def doCanonicalize(): CometIcebergNativeScanExec = {
    CometIcebergNativeScanExec(
      nativeOp,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      originalPlan.doCanonicalize(),
      SerializedPlan(None),
      catalogType,
      catalogProperties,
      namespace,
      tableName,
      numPartitions)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, catalogType, namespace, tableName, numPartitions)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometIcebergNativeScanExec =>
        this.catalogType == other.catalogType &&
        this.catalogProperties == other.catalogProperties &&
        this.namespace == other.namespace &&
        this.tableName == other.tableName &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.numPartitions == other.numPartitions
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      catalogType,
      namespace.asJava,
      tableName,
      output.asJava,
      serializedPlanOpt,
      numPartitions: java.lang.Integer)

  override lazy val metrics: Map[String, SQLMetric] = {
    Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "time_elapsed_opening" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for catalog loading and table opening"),
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
   * Information extracted from an Iceberg table scan for native execution.
   */
  case class IcebergCatalogInfo(
      catalogType: String,
      properties: Map[String, String],
      namespace: Seq[String],
      tableName: String)

  /**
   * Extracts Iceberg catalog configuration from a Spark BatchScanExec.
   *
   * This method:
   *   1. Gets the catalog name from the table identifier 2. Extracts catalog configuration from
   *      Spark session config 3. Maps Spark's catalog implementation class to iceberg-rust
   *      catalog types 4. Returns catalog info ready for serialization
   *
   * @param scanExec
   *   The Spark BatchScanExec containing an Iceberg scan
   * @param session
   *   The SparkSession to extract catalog config from
   * @return
   *   Some(IcebergCatalogInfo) if catalog is supported, None otherwise
   */
  def extractCatalogInfo(
      scanExec: BatchScanExec,
      session: SparkSession): Option[IcebergCatalogInfo] = {
    try {
      // Get the full table name from Spark's table identifier
      // Format: "catalog_name.namespace.table_name" or "namespace.table_name"
      val fullTableName = scanExec.table.name()
      val parts = fullTableName.split('.')

      if (parts.length < 2) {
        return None // Need at least namespace.table
      }

      // Determine catalog name and table path
      // If 3+ parts: parts(0) is catalog, parts(1..-2) is namespace, parts(-1) is table
      // If 2 parts: default catalog, parts(0) is namespace, parts(1) is table
      val (catalogName, namespaceParts, tableNamePart) = if (parts.length >= 3) {
        (parts.head, parts.slice(1, parts.length - 1).toSeq, parts.last)
      } else {
        // Try to get default catalog from config
        val defaultCatalog = session.conf
          .getOption("spark.sql.catalog.spark_catalog")
          .map(_ => "spark_catalog")
          .getOrElse(return None)
        (defaultCatalog, Seq(parts.head), parts.last)
      }

      // Get catalog properties from Spark session config
      val catalogPrefix = s"spark.sql.catalog.$catalogName"

      // Check both catalog-impl and type properties
      val catalogImpl = session.conf.getOption(s"$catalogPrefix.catalog-impl")
      val catalogType = session.conf.getOption(s"$catalogPrefix.type")

      // Handle Hadoop catalog specially - it uses direct metadata file access
      if (catalogType.contains("hadoop") ||
        catalogImpl.exists(impl => impl.contains("HadoopCatalog"))) {

        // Hadoop catalog is filesystem-based, need to extract metadata location
        // Try to get it from the table object via reflection
        try {
          val scan = scanExec.scan
          val scanClass = scan.getClass

          // Try to get the table via reflection
          // Iceberg's SparkBatchQueryScan extends SparkScan which has protected table() method
          // Need to search up the class hierarchy
          def findTableMethod(clazz: Class[_]): Option[java.lang.reflect.Method] = {
            if (clazz == null || clazz == classOf[Object]) {
              None
            } else {
              try {
                val method = clazz.getDeclaredMethod("table")
                method.setAccessible(true)
                Some(method)
              } catch {
                case _: NoSuchMethodException =>
                  // Try superclass
                  findTableMethod(clazz.getSuperclass)
              }
            }
          }

          val tableMethod = findTableMethod(scanClass).getOrElse {
            throw new NoSuchMethodException("Could not find table() method in class hierarchy")
          }

          val table = tableMethod.invoke(scan)

          // Get the metadata location from table.operations().current().metadataFileLocation()
          val tableClass = table.getClass
          val operationsMethod = tableClass.getMethod("operations")
          val operations = operationsMethod.invoke(table)

          val operationsClass = operations.getClass
          val currentMethod = operationsClass.getMethod("current")
          val metadata = currentMethod.invoke(operations)

          val metadataClass = metadata.getClass
          val metadataFileLocationMethod = metadataClass.getMethod("metadataFileLocation")
          val metadataLocation = metadataFileLocationMethod.invoke(metadata).asInstanceOf[String]

          // Return catalog info with actual metadata file location
          return Some(
            IcebergCatalogInfo(
              catalogType = "hadoop",
              properties = Map("metadata_location" -> metadataLocation),
              namespace = namespaceParts,
              tableName = tableNamePart))
        } catch {
          case _: Exception =>
            // If reflection fails, fall back to returning None
            return None
        }
      }

      // Get the catalog implementation class
      val implClass = catalogImpl.getOrElse(return None)

      // Map Spark's catalog implementation to iceberg-rust catalog type
      val icebergCatalogType = implClass match {
        case impl if impl.contains("RESTCatalog") || impl.contains("rest") => "rest"
        case impl if impl.contains("GlueCatalog") || impl.contains("glue") => "glue"
        case impl if impl.contains("HiveCatalog") || impl.contains("hive") => "hms"
        case impl if impl.contains("JdbcCatalog") || impl.contains("jdbc") => "sql"
        case _ => return None // Unsupported catalog type
      }

      // Extract all catalog properties with the prefix
      val catalogProps = session.conf.getAll
        .filter { case (k, _) => k.startsWith(catalogPrefix + ".") }
        .map { case (k, v) =>
          // Remove prefix: "spark.sql.catalog.mycatalog.uri" -> "uri"
          val key = k.stripPrefix(catalogPrefix + ".")
          // Skip the catalog-impl property itself
          if (key == "catalog-impl" || key == "type") None else Some((key, v))
        }
        .flatten
        .toMap

      Some(
        IcebergCatalogInfo(
          catalogType = icebergCatalogType,
          properties = catalogProps,
          namespace = namespaceParts,
          tableName = tableNamePart))
    } catch {
      case _: Exception =>
        None
    }
  }

  /**
   * Creates a CometIcebergNativeScanExec from a Spark BatchScanExec.
   *
   * This method is called on the driver to create the Comet operator. The key step is determining
   * the number of partitions to use, which affects parallelism.
   *
   * **Partition Count Strategy:**
   *   - For KeyGroupedPartitioning: Use Iceberg's partition count (data is grouped by partition
   *     keys)
   *   - For other cases: Use the number of partitions in inputRDD, which Spark computes based on
   *     the number of InputPartition objects returned by Iceberg's planInputPartitions()
   *
   * **How FileScanTasks Flow to Workers:**
   *   1. Iceberg's planInputPartitions() creates InputPartition[] (each contains ScanTaskGroup)
   *      2. Spark creates BatchScanExec.inputRDD with these InputPartitions 3. Each RDD partition
   *      wraps one InputPartition 4. Spark ships RDD partitions to workers 5. On worker:
   *      QueryPlanSerde extracts FileScanTasks from the InputPartition 6. FileScanTasks are
   *      serialized to protobuf and sent to Rust 7. Rust reads only the files specified in those
   *      tasks
   *
   * @param nativeOp
   *   The serialized native operator
   * @param scanExec
   *   The original Spark BatchScanExec
   * @param session
   *   The SparkSession
   * @param catalogInfo
   *   The extracted catalog information
   * @return
   *   A new CometIcebergNativeScanExec
   */
  def apply(
      nativeOp: Operator,
      scanExec: BatchScanExec,
      session: SparkSession,
      catalogInfo: IcebergCatalogInfo): CometIcebergNativeScanExec = {

    // Determine number of partitions from Iceberg's output partitioning
    // KeyGroupedPartitioning means Iceberg grouped data by partition keys
    // Otherwise, use the number of InputPartitions that Iceberg created
    val numParts = scanExec.outputPartitioning match {
      case p: KeyGroupedPartitioning =>
        // Use Iceberg's key-grouped partition count
        p.numPartitions
      case _ =>
        // For unpartitioned tables or other partitioning schemes,
        // use the InputPartition count from inputRDD
        // This is already computed by Spark based on Iceberg's file planning
        scanExec.inputRDD.getNumPartitions
    }

    val exec = CometIcebergNativeScanExec(
      nativeOp,
      scanExec.output,
      scanExec,
      SerializedPlan(None), // Will be serialized per-partition during execution
      catalogInfo.catalogType,
      catalogInfo.properties,
      catalogInfo.namespace,
      catalogInfo.tableName,
      numParts)

    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}

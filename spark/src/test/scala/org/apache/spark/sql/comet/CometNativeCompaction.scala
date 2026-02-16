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

import scala.collection.JavaConverters._

import org.apache.iceberg.{FileScanTask, Table}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.comet.Native

/** Configuration for Iceberg table metadata passed to native code. */
case class IcebergTableConfig(
    table_identifier: String,
    metadata_location: String,
    warehouse_location: String,
    current_snapshot_id: Option[Long],
    file_io_properties: Map[String, String])

/** File scan task configuration for native compaction. */
case class FileScanTaskConfig(
    file_path: String,
    file_size_bytes: Long,
    record_count: Long,
    partition_path: String, // e.g., "year=2024/month=01" or "" for unpartitioned
    partition_spec_id: Int,
    start: Long,
    length: Long)

/** Compaction task configuration for native execution. */
case class CompactionTaskConfig(
    table_config: IcebergTableConfig,
    file_scan_tasks: Seq[FileScanTaskConfig],
    target_file_size_bytes: Long,
    compression: String,
    data_dir: String)

/** Iceberg DataFile metadata from native compaction. */
case class IcebergDataFileMetadata(
    file_path: String,
    file_format: String,
    record_count: Long,
    file_size_in_bytes: Long,
    partition_json: String,
    column_sizes: Map[Int, Long],
    value_counts: Map[Int, Long],
    null_value_counts: Map[Int, Long],
    split_offsets: Seq[Long],
    partition_spec_id: Int)

/** Result of native Iceberg compaction. */
case class IcebergCompactionResult(
    files_to_delete: Seq[String],
    files_to_add: Seq[IcebergDataFileMetadata],
    total_rows: Long,
    total_bytes_written: Long)

/** Native compaction execution result. */
case class NativeCompactionResult(
    success: Boolean,
    error_message: Option[String],
    result: Option[IcebergCompactionResult])

/**
 * Native Iceberg compaction using Rust/DataFusion for scan+write, Java API for commit.
 */
class CometNativeCompaction(spark: SparkSession) extends Logging {

  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val native = new Native()

  val DEFAULT_TARGET_FILE_SIZE: Long = 128 * 1024 * 1024 // 128 MB
  val DEFAULT_COMPRESSION: String = "zstd"

  /** Rewrite data files using native scan+write with Iceberg Java API commit. */
  def rewriteDataFiles(
      table: Table,
      targetFileSizeBytes: Long = DEFAULT_TARGET_FILE_SIZE,
      compression: String = DEFAULT_COMPRESSION): CompactionSummary = {

    logInfo(s"Starting native compaction for table ${table.name()}")

    val currentSnapshot = Option(table.currentSnapshot())
    val currentSnapshotId = currentSnapshot.map(_.snapshotId())
    val tableConfig = buildTableConfig(table, currentSnapshotId)
    val fileGroups = planFileGroups(table, targetFileSizeBytes)

    if (fileGroups.isEmpty) {
      logInfo("No files to compact")
      return CompactionSummary(
        filesDeleted = 0,
        filesAdded = 0,
        bytesDeleted = 0,
        bytesAdded = 0,
        rowsProcessed = 0)
    }

    logInfo(s"Found ${fileGroups.size} file groups to compact")

    var totalFilesDeleted = 0
    var totalFilesAdded = 0
    var totalBytesDeleted = 0L
    var totalBytesAdded = 0L
    var totalRowsProcessed = 0L

    val allFilesToDelete = scala.collection.mutable.ArrayBuffer[String]()
    val allFilesToAdd = scala.collection.mutable.ArrayBuffer[IcebergDataFileMetadata]()

    for ((group, groupIndex) <- fileGroups.zipWithIndex) {
      logInfo(
        s"Processing file group ${groupIndex + 1}/${fileGroups.size} " +
          s"with ${group.size} files")

      val compactionConfig =
        buildCompactionConfig(tableConfig, group, targetFileSizeBytes, compression)
      val result = executeNativeCompaction(compactionConfig)

      result match {
        case NativeCompactionResult(true, _, Some(compactionResult)) =>
          allFilesToDelete ++= compactionResult.files_to_delete
          allFilesToAdd ++= compactionResult.files_to_add
          totalFilesDeleted += compactionResult.files_to_delete.size
          totalFilesAdded += compactionResult.files_to_add.size
          totalRowsProcessed += compactionResult.total_rows
          totalBytesAdded += compactionResult.total_bytes_written
          for (task <- group) {
            totalBytesDeleted += task.file().fileSizeInBytes()
          }

          logInfo(
            s"Group ${groupIndex + 1} completed: " +
              s"${compactionResult.files_to_delete.size} files deleted, " +
              s"${compactionResult.files_to_add.size} files added")

        case NativeCompactionResult(false, Some(error), _) =>
          logError(s"Native compaction failed for group ${groupIndex + 1}: $error")
          throw new RuntimeException(s"Native compaction failed: $error")

        case _ =>
          logError(s"Unexpected native compaction result for group ${groupIndex + 1}")
          throw new RuntimeException("Unexpected native compaction result")
      }
    }

    if (allFilesToAdd.nonEmpty) {
      logInfo(
        s"Committing compaction: ${allFilesToDelete.size} files to delete, " +
          s"${allFilesToAdd.size} files to add")

      val commitSuccess =
        commitCompaction(table, allFilesToDelete.toSeq, allFilesToAdd.toSeq)

      if (!commitSuccess) {
        throw new RuntimeException("Failed to commit compaction results")
      }

      logInfo("Compaction committed successfully")
    }

    CompactionSummary(
      filesDeleted = totalFilesDeleted,
      filesAdded = totalFilesAdded,
      bytesDeleted = totalBytesDeleted,
      bytesAdded = totalBytesAdded,
      rowsProcessed = totalRowsProcessed)
  }

  private def buildTableConfig(
      table: Table,
      currentSnapshotId: Option[Long]): IcebergTableConfig = {

    val tableLocation = table.location()
    val metadataLocation =
      try {
        table
          .asInstanceOf[org.apache.iceberg.BaseTable]
          .operations()
          .current()
          .metadataFileLocation()
      } catch {
        case _: Exception => s"$tableLocation/metadata/v1.metadata.json"
      }
    val warehouseLocation = tableLocation.substring(0, tableLocation.lastIndexOf('/'))
    val fileIOProperties = table.properties().asScala.toMap

    IcebergTableConfig(
      table_identifier = table.name(),
      metadata_location = metadataLocation,
      warehouse_location = warehouseLocation,
      current_snapshot_id = currentSnapshotId,
      file_io_properties = fileIOProperties)
  }

  /** Plan file groups using bin-pack strategy. */
  private def planFileGroups(table: Table, targetFileSizeBytes: Long): Seq[Seq[FileScanTask]] = {

    val currentSnapshot = table.currentSnapshot()
    if (currentSnapshot == null) {
      return Seq.empty
    }

    val scanTasks = table
      .newScan()
      .planFiles()
      .iterator()
      .asScala
      .toSeq

    val smallFiles = scanTasks.filter(_.file().fileSizeInBytes() < targetFileSizeBytes)

    if (smallFiles.size < 2) {
      return Seq.empty
    }

    val groups = scala.collection.mutable.ArrayBuffer[Seq[FileScanTask]]()
    var currentGroup = scala.collection.mutable.ArrayBuffer[FileScanTask]()
    var currentGroupSize = 0L

    for (task <- smallFiles.sortBy(_.file().fileSizeInBytes())) {
      if (currentGroupSize + task.file().fileSizeInBytes() > targetFileSizeBytes * 2) {
        if (currentGroup.size >= 2) {
          groups += currentGroup.toSeq
        }
        currentGroup = scala.collection.mutable.ArrayBuffer[FileScanTask]()
        currentGroupSize = 0L
      }

      currentGroup += task
      currentGroupSize += task.file().fileSizeInBytes()
    }

    if (currentGroup.size >= 2) {
      groups += currentGroup.toSeq
    }

    groups.toSeq
  }

  private def buildCompactionConfig(
      tableConfig: IcebergTableConfig,
      tasks: Seq[FileScanTask],
      targetFileSizeBytes: Long,
      compression: String): CompactionTaskConfig = {

    val fileScanTaskConfigs = tasks.map { task =>
      val partitionPath = task.spec().partitionToPath(task.file().partition())

      FileScanTaskConfig(
        file_path = task.file().path().toString,
        file_size_bytes = task.file().fileSizeInBytes(),
        record_count = task.file().recordCount(),
        partition_path = partitionPath,
        partition_spec_id = task.spec().specId(),
        start = task.start(),
        length = task.length())
    }

    CompactionTaskConfig(
      table_config = tableConfig,
      file_scan_tasks = fileScanTaskConfigs,
      target_file_size_bytes = targetFileSizeBytes,
      compression = compression,
      data_dir = "data")
  }

  /** Execute native compaction via JNI. */
  private def executeNativeCompaction(config: CompactionTaskConfig): NativeCompactionResult = {
    val configJson = objectMapper.writeValueAsString(config)

    logDebug(s"Executing native compaction with config: $configJson")

    val resultJson = native.executeIcebergCompaction(configJson)

    logDebug(s"Native compaction result: $resultJson")

    objectMapper.readValue(resultJson, classOf[NativeCompactionResult])
  }

  /** Commit using Iceberg's Java RewriteFiles API. */
  private def commitCompaction(
      table: Table,
      filesToDelete: Seq[String],
      filesToAdd: Seq[IcebergDataFileMetadata]): Boolean = {

    import org.apache.iceberg.{DataFile, DataFiles, FileFormat, PartitionSpec}

    try {
      val specs = table.specs()
      val deleteFiles: java.util.Set[DataFile] = new java.util.HashSet[DataFile]()
      val snapshot = table.currentSnapshot()
      if (snapshot != null) {
        val deletePathSet = filesToDelete.toSet
        val fileScanTasks = table.newScan().planFiles().iterator()
        while (fileScanTasks.hasNext) {
          val task = fileScanTasks.next()
          val dataFile = task.file()
          if (deletePathSet.contains(dataFile.path().toString)) {
            deleteFiles.add(dataFile)
          }
        }
      }

      val addFiles: java.util.Set[DataFile] = new java.util.HashSet[DataFile]()
      filesToAdd.foreach { m =>
        val spec: PartitionSpec = specs.get(m.partition_spec_id)
        val builder = DataFiles
          .builder(spec)
          .withPath(m.file_path)
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(m.file_size_in_bytes)
          .withRecordCount(m.record_count)
        if (m.partition_json != null && m.partition_json.nonEmpty && m.partition_json != "{}") {
          builder.withPartitionPath(m.partition_json)
        }

        addFiles.add(builder.build())
      }

      table.newRewrite().rewriteFiles(deleteFiles, addFiles).commit()

      logInfo(
        s"Committed compaction: ${filesToDelete.size} files deleted, ${filesToAdd.size} files added")
      true
    } catch {
      case e: Exception =>
        logError(s"Failed to commit compaction: ${e.getMessage}", e)
        false
    }
  }
}

/** Summary of compaction results. */
case class CompactionSummary(
    filesDeleted: Int,
    filesAdded: Int,
    bytesDeleted: Long,
    bytesAdded: Long,
    rowsProcessed: Long) {

  def compactionRatio: Double = {
    if (bytesDeleted > 0) {
      (bytesDeleted - bytesAdded).toDouble / bytesDeleted
    } else {
      0.0
    }
  }

  override def toString: String = {
    f"CompactionSummary(files: $filesDeleted -> $filesAdded, " +
      f"bytes: ${bytesDeleted / 1024 / 1024}%.1f MB -> ${bytesAdded / 1024 / 1024}%.1f MB, " +
      f"rows: $rowsProcessed, ratio: ${compactionRatio * 100}%.1f%%)"
  }
}

object CometNativeCompaction {
  def apply(spark: SparkSession): CometNativeCompaction = new CometNativeCompaction(spark)

  def isAvailable: Boolean = {
    try {
      val version = new Native().getIcebergCompactionVersion()
      version != null && version.nonEmpty
    } catch {
      case _: UnsatisfiedLinkError | _: Exception => false
    }
  }
}

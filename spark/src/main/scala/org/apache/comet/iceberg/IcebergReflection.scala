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

package org.apache.comet.iceberg

import org.apache.spark.internal.Logging

/**
 * Shared reflection utilities for Iceberg operations.
 *
 * This object provides common reflection methods used across Comet for interacting with Iceberg
 * classes. These are needed because many Iceberg methods are protected or package-private.
 */
object IcebergReflection extends Logging {

  /**
   * Iceberg class names used throughout Comet.
   */
  object ClassNames {
    val CONTENT_SCAN_TASK = "org.apache.iceberg.ContentScanTask"
    val FILE_SCAN_TASK = "org.apache.iceberg.FileScanTask"
    val CONTENT_FILE = "org.apache.iceberg.ContentFile"
    val STRUCT_LIKE = "org.apache.iceberg.StructLike"
    val PARTITION_SCAN_TASK = "org.apache.iceberg.PartitionScanTask"
    val DELETE_FILE = "org.apache.iceberg.DeleteFile"
    val LITERAL = "org.apache.iceberg.expressions.Literal"
    val SCHEMA_PARSER = "org.apache.iceberg.SchemaParser"
    val SCHEMA = "org.apache.iceberg.Schema"
    val PARTITION_SPEC_PARSER = "org.apache.iceberg.PartitionSpecParser"
    val PARTITION_SPEC = "org.apache.iceberg.PartitionSpec"
    val PARTITION_FIELD = "org.apache.iceberg.PartitionField"
  }

  /**
   * Iceberg content types.
   */
  object ContentTypes {
    val POSITION_DELETES = "POSITION_DELETES"
    val EQUALITY_DELETES = "EQUALITY_DELETES"
  }

  /**
   * Iceberg file formats.
   */
  object FileFormats {
    val PARQUET = "PARQUET"
  }

  /**
   * Iceberg transform types.
   */
  object Transforms {
    val IDENTITY = "identity"
  }

  /**
   * Searches through class hierarchy to find a method (including protected methods).
   */
  def findMethodInHierarchy(
      clazz: Class[_],
      methodName: String): Option[java.lang.reflect.Method] = {
    var current: Class[_] = clazz
    while (current != null) {
      try {
        val method = current.getDeclaredMethod(methodName)
        method.setAccessible(true)
        return Some(method)
      } catch {
        case _: NoSuchMethodException => current = current.getSuperclass
      }
    }
    None
  }

  /**
   * Extracts file location from Iceberg ContentFile, handling both location() and path().
   *
   * Different Iceberg versions expose file paths differently:
   *   - Newer versions: location() returns String
   *   - Older versions: path() returns CharSequence
   */
  def extractFileLocation(contentFileClass: Class[_], file: Any): Option[String] = {
    try {
      val locationMethod = contentFileClass.getMethod("location")
      Some(locationMethod.invoke(file).asInstanceOf[String])
    } catch {
      case _: NoSuchMethodException =>
        try {
          val pathMethod = contentFileClass.getMethod("path")
          Some(pathMethod.invoke(file).asInstanceOf[CharSequence].toString)
        } catch {
          case _: Exception => None
        }
      case _: Exception => None
    }
  }

  /**
   * Extracts file location from ContentFile instance using dynamic class lookup.
   */
  def extractFileLocation(file: Any): Option[String] = {
    try {
      // scalastyle:off classforname
      val contentFileClass = Class.forName(ClassNames.CONTENT_FILE)
      // scalastyle:on classforname
      extractFileLocation(contentFileClass, file)
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Gets the Iceberg Table from a SparkScan.
   *
   * The table() method is protected in SparkScan, requiring reflection to access.
   */
  def getTable(scan: Any): Option[Any] = {
    findMethodInHierarchy(scan.getClass, "table").flatMap { tableMethod =>
      try {
        Some(tableMethod.invoke(scan))
      } catch {
        case e: Exception =>
          logError(
            s"Iceberg reflection failure: Failed to get table from SparkScan: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Gets the tasks from a SparkScan.
   *
   * The tasks() method is protected in SparkScan, requiring reflection to access.
   */
  def getTasks(scan: Any): Option[java.util.List[_]] = {
    try {
      val tasksMethod = scan.getClass.getSuperclass
        .getDeclaredMethod("tasks")
      tasksMethod.setAccessible(true)
      Some(tasksMethod.invoke(scan).asInstanceOf[java.util.List[_]])
    } catch {
      case e: Exception =>
        logError(
          s"Iceberg reflection failure: Failed to get tasks from SparkScan: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the filter expressions from a SparkScan.
   *
   * The filterExpressions() method is protected in SparkScan.
   */
  def getFilterExpressions(scan: Any): Option[java.util.List[_]] = {
    try {
      val filterExpressionsMethod = scan.getClass.getSuperclass.getSuperclass
        .getDeclaredMethod("filterExpressions")
      filterExpressionsMethod.setAccessible(true)
      Some(filterExpressionsMethod.invoke(scan).asInstanceOf[java.util.List[_]])
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: Failed to get filter expressions from SparkScan: " +
            s"${e.getMessage}")
        None
    }
  }

  /**
   * Gets the Iceberg table format version.
   *
   * Tries to get formatVersion() directly from table, falling back to
   * operations().current().formatVersion() for older Iceberg versions.
   */
  def getFormatVersion(table: Any): Option[Int] = {
    try {
      val formatVersionMethod = table.getClass.getMethod("formatVersion")
      Some(formatVersionMethod.invoke(table).asInstanceOf[Int])
    } catch {
      case _: NoSuchMethodException =>
        try {
          // If not directly available, access via operations/metadata
          val opsMethod = table.getClass.getMethod("operations")
          val ops = opsMethod.invoke(table)
          val currentMethod = ops.getClass.getMethod("current")
          val metadata = currentMethod.invoke(ops)
          val formatVersionMethod = metadata.getClass.getMethod("formatVersion")
          Some(formatVersionMethod.invoke(metadata).asInstanceOf[Int])
        } catch {
          case e: Exception =>
            logError(s"Iceberg reflection failure: Failed to get format version: ${e.getMessage}")
            None
        }
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get format version: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the file format from a ContentFile (e.g., "PARQUET", "ORC", "AVRO").
   */
  def getFileFormat(contentFile: Any): Option[String] = {
    try {
      val formatMethod = contentFile.getClass.getMethod("format")
      Some(formatMethod.invoke(contentFile).toString)
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get file format: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the FileIO from an Iceberg table.
   */
  def getFileIO(table: Any): Option[Any] = {
    try {
      val ioMethod = table.getClass.getMethod("io")
      Some(ioMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get FileIO from table: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the schema from an Iceberg table.
   */
  def getSchema(table: Any): Option[Any] = {
    try {
      val schemaMethod = table.getClass.getMethod("schema")
      Some(schemaMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get schema from table: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the partition spec from an Iceberg table.
   */
  def getPartitionSpec(table: Any): Option[Any] = {
    try {
      val specMethod = table.getClass.getMethod("spec")
      Some(specMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(
          s"Iceberg reflection failure: Failed to get partition spec from table: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the table metadata from an Iceberg table.
   *
   * @param table
   *   The Iceberg table instance
   * @return
   *   The TableMetadata object from table.operations().current()
   */
  def getTableMetadata(table: Any): Option[Any] = {
    try {
      val operationsMethod = table.getClass.getMethod("operations")
      val operations = operationsMethod.invoke(table)

      val currentMethod = operations.getClass.getMethod("current")
      Some(currentMethod.invoke(operations))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get table metadata: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the metadata file location from an Iceberg table.
   *
   * @param table
   *   The Iceberg table instance
   * @return
   *   Path to the table metadata file
   */
  def getMetadataLocation(table: Any): Option[String] = {
    getTableMetadata(table).flatMap { metadata =>
      try {
        val metadataFileLocationMethod = metadata.getClass.getMethod("metadataFileLocation")
        Some(metadataFileLocationMethod.invoke(metadata).asInstanceOf[String])
      } catch {
        case e: Exception =>
          logError(
            s"Iceberg reflection failure: Failed to get metadata location: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Gets the properties map from an Iceberg table's metadata.
   *
   * @param table
   *   The Iceberg table instance
   * @return
   *   Map of table properties
   */
  def getTableProperties(table: Any): Option[java.util.Map[String, String]] = {
    getTableMetadata(table).flatMap { metadata =>
      try {
        val propertiesMethod = metadata.getClass.getMethod("properties")
        Some(propertiesMethod.invoke(metadata).asInstanceOf[java.util.Map[String, String]])
      } catch {
        case e: Exception =>
          logError(s"Iceberg reflection failure: Failed to get table properties: ${e.getMessage}")
          None
      }
    }
  }
}

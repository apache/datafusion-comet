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

import java.lang.reflect.Method

import org.apache.spark.internal.Logging

/** Cached Iceberg classes and methods to avoid repeated reflection lookups. */
case class ReflectionCache(
    contentScanTaskClass: Class[_],
    fileScanTaskClass: Class[_],
    contentFileClass: Class[_],
    deleteFileClass: Class[_],
    schemaParserClass: Class[_],
    schemaClass: Class[_],
    partitionSpecParserClass: Class[_],
    partitionSpecClass: Class[_],
    structLikeClass: Class[_],
    fileMethod: Method,
    startMethod: Method,
    lengthMethod: Method,
    partitionMethod: Method,
    residualMethod: Method,
    taskSchemaMethod: Method,
    deletesMethod: Method,
    specMethod: Method,
    schemaToJsonMethod: Method,
    specToJsonMethod: Method,
    deleteContentMethod: Method,
    deleteSpecIdMethod: Method,
    deleteEqualityIdsMethod: Method)

object ReflectionCache extends Logging {

  def create(): ReflectionCache = {
    // scalastyle:off classforname
    val contentScanTaskClass = Class.forName(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    val fileScanTaskClass = Class.forName(IcebergReflection.ClassNames.FILE_SCAN_TASK)
    val contentFileClass = Class.forName(IcebergReflection.ClassNames.CONTENT_FILE)
    val deleteFileClass = Class.forName(IcebergReflection.ClassNames.DELETE_FILE)
    val schemaParserClass = Class.forName(IcebergReflection.ClassNames.SCHEMA_PARSER)
    val schemaClass = Class.forName(IcebergReflection.ClassNames.SCHEMA)
    val partitionSpecParserClass =
      Class.forName(IcebergReflection.ClassNames.PARTITION_SPEC_PARSER)
    val partitionSpecClass = Class.forName(IcebergReflection.ClassNames.PARTITION_SPEC)
    val structLikeClass = Class.forName(IcebergReflection.ClassNames.STRUCT_LIKE)
    // scalastyle:on classforname

    val fileMethod = contentScanTaskClass.getMethod("file")
    val startMethod = contentScanTaskClass.getMethod("start")
    val lengthMethod = contentScanTaskClass.getMethod("length")
    val partitionMethod = contentScanTaskClass.getMethod("partition")
    val residualMethod = contentScanTaskClass.getMethod("residual")
    val taskSchemaMethod = fileScanTaskClass.getMethod("schema")
    val deletesMethod = fileScanTaskClass.getMethod("deletes")
    val specMethod = fileScanTaskClass.getMethod("spec")
    val schemaToJsonMethod = schemaParserClass.getMethod("toJson", schemaClass)
    schemaToJsonMethod.setAccessible(true)
    val specToJsonMethod = partitionSpecParserClass.getMethod("toJson", partitionSpecClass)
    val deleteContentMethod = deleteFileClass.getMethod("content")
    val deleteSpecIdMethod = deleteFileClass.getMethod("specId")
    val deleteEqualityIdsMethod = deleteFileClass.getMethod("equalityFieldIds")

    ReflectionCache(
      contentScanTaskClass = contentScanTaskClass,
      fileScanTaskClass = fileScanTaskClass,
      contentFileClass = contentFileClass,
      deleteFileClass = deleteFileClass,
      schemaParserClass = schemaParserClass,
      schemaClass = schemaClass,
      partitionSpecParserClass = partitionSpecParserClass,
      partitionSpecClass = partitionSpecClass,
      structLikeClass = structLikeClass,
      fileMethod = fileMethod,
      startMethod = startMethod,
      lengthMethod = lengthMethod,
      partitionMethod = partitionMethod,
      residualMethod = residualMethod,
      taskSchemaMethod = taskSchemaMethod,
      deletesMethod = deletesMethod,
      specMethod = specMethod,
      schemaToJsonMethod = schemaToJsonMethod,
      specToJsonMethod = specToJsonMethod,
      deleteContentMethod = deleteContentMethod,
      deleteSpecIdMethod = deleteSpecIdMethod,
      deleteEqualityIdsMethod = deleteEqualityIdsMethod)
  }
}

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
    val UNBOUND_PREDICATE = "org.apache.iceberg.expressions.UnboundPredicate"
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
   * Iceberg type names.
   */
  object TypeNames {
    val UNKNOWN = "unknown"
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
          val opsMethod = table.getClass.getDeclaredMethod("operations")
          opsMethod.setAccessible(true)
          val ops = opsMethod.invoke(table)
          val currentMethod = ops.getClass.getDeclaredMethod("current")
          currentMethod.setAccessible(true)
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
   * Gets storage properties from an Iceberg table's FileIO.
   *
   * This extracts credentials from the FileIO implementation, which is critical for REST catalog
   * credential vending. The REST catalog returns temporary S3 credentials per-table via the
   * loadTable response, stored in the table's FileIO (typically ResolvingFileIO).
   *
   * The properties() method is not on the FileIO interface -- it exists on specific
   * implementations like ResolvingFileIO and S3FileIO. Returns None gracefully when unavailable.
   */
  def getFileIOProperties(table: Any): Option[Map[String, String]] = {
    import scala.jdk.CollectionConverters._
    getFileIO(table).flatMap { fileIO =>
      findMethodInHierarchy(fileIO.getClass, "properties").flatMap { propsMethod =>
        propsMethod.invoke(fileIO) match {
          case javaMap: java.util.Map[_, _] =>
            val scalaMap = javaMap.asScala.collect { case (k: String, v: String) =>
              k -> v
            }.toMap
            if (scalaMap.nonEmpty) Some(scalaMap) else None
          case _ => None
        }
      }
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
      val operationsMethod = table.getClass.getDeclaredMethod("operations")
      operationsMethod.setAccessible(true)
      val operations = operationsMethod.invoke(table)

      val currentMethod = operations.getClass.getDeclaredMethod("current")
      currentMethod.setAccessible(true)
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

  /**
   * Gets delete files from a single FileScanTask.
   *
   * @param task
   *   An Iceberg FileScanTask object
   * @param fileScanTaskClass
   *   The FileScanTask class (can be obtained via classforname or passed in if already loaded)
   * @return
   *   List of delete files for this task
   * @throws Exception
   *   if reflection fails (callers must handle appropriately based on context)
   */
  def getDeleteFilesFromTask(task: Any, fileScanTaskClass: Class[_]): java.util.List[_] = {
    val deletesMethod = fileScanTaskClass.getMethod("deletes")
    val deletes = deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
    if (deletes == null) new java.util.ArrayList[Any]() else deletes
  }

  /**
   * Gets equality field IDs from a delete file.
   *
   * @param deleteFile
   *   An Iceberg DeleteFile object
   * @return
   *   List of field IDs used in equality deletes, or empty list for position deletes
   */
  def getEqualityFieldIds(deleteFile: Any): java.util.List[_] = {
    try {
      // scalastyle:off classforname
      val deleteFileClass = Class.forName(ClassNames.DELETE_FILE)
      // scalastyle:on classforname
      val equalityFieldIdsMethod = deleteFileClass.getMethod("equalityFieldIds")
      val ids = equalityFieldIdsMethod.invoke(deleteFile).asInstanceOf[java.util.List[_]]
      if (ids == null) new java.util.ArrayList[Any]() else ids
    } catch {
      case _: Exception =>
        // Position delete files return null/empty for equalityFieldIds
        new java.util.ArrayList[Any]()
    }
  }

  /**
   * Gets field name and type from schema by field ID.
   *
   * @param schema
   *   Iceberg Schema object
   * @param fieldId
   *   Field ID to look up
   * @return
   *   Tuple of (field name, field type string)
   */
  def getFieldInfo(schema: Any, fieldId: Int): Option[(String, String)] = {
    try {
      val findFieldMethod = schema.getClass.getMethod("findField", classOf[Int])
      val field = findFieldMethod.invoke(schema, fieldId.asInstanceOf[AnyRef])
      if (field != null) {
        val nameMethod = field.getClass.getMethod("name")
        val typeMethod = field.getClass.getMethod("type")
        val fieldName = nameMethod.invoke(field).toString
        val fieldType = typeMethod.invoke(field).toString
        Some((fieldName, fieldType))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: Failed to get field info for ID " +
            s"$fieldId: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the expected schema from a SparkScan.
   *
   * The expectedSchema() method is protected in SparkScan and returns the Iceberg Schema for this
   * scan (which is the snapshot schema for VERSION AS OF queries).
   *
   * @param scan
   *   The SparkScan object
   * @return
   *   The expected Iceberg Schema, or None if reflection fails
   */
  def getExpectedSchema(scan: Any): Option[Any] = {
    findMethodInHierarchy(scan.getClass, "expectedSchema").flatMap { schemaMethod =>
      try {
        Some(schemaMethod.invoke(scan))
      } catch {
        case e: Exception =>
          logError(s"Failed to get expectedSchema from SparkScan: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Builds a field ID mapping from an Iceberg schema.
   *
   * Extracts the mapping of column names to Iceberg field IDs from the schema's columns. This is
   * used for schema evolution support where we need to map between column names and their
   * corresponding field IDs.
   *
   * @param schema
   *   Iceberg Schema object
   * @return
   *   Map from column name to field ID
   */
  def buildFieldIdMapping(schema: Any): Map[String, Int] = {
    import scala.jdk.CollectionConverters._
    try {
      val columnsMethod = schema.getClass.getMethod("columns")
      val columns = columnsMethod.invoke(schema).asInstanceOf[java.util.List[_]]

      columns.asScala.flatMap { column =>
        try {
          val nameMethod = column.getClass.getMethod("name")
          val name = nameMethod.invoke(column).asInstanceOf[String]

          val fieldIdMethod = column.getClass.getMethod("fieldId")
          val fieldId = fieldIdMethod.invoke(column).asInstanceOf[Int]

          Some(name -> fieldId)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to extract field ID from column: ${e.getMessage}")
            None
        }
      }.toMap
    } catch {
      case e: Exception =>
        logWarning(s"Failed to build field ID mapping from schema: ${e.getMessage}")
        Map.empty[String, Int]
    }
  }

  /**
   * Validates partition column types for compatibility with iceberg-rust.
   *
   * iceberg-rust's Literal::try_from_json() has incomplete type support: - Binary/fixed types:
   * unimplemented - Decimals: limited to precision <= 28 (rust_decimal crate limitation)
   *
   * @param partitionSpec
   *   The Iceberg PartitionSpec
   * @param schema
   *   The Iceberg Schema to look up field types
   * @return
   *   List of unsupported partition types (empty if all supported). Each entry is (fieldName,
   *   typeStr, reason)
   */
  def validatePartitionTypes(partitionSpec: Any, schema: Any): List[(String, String, String)] = {
    import scala.jdk.CollectionConverters._

    val fieldsMethod = partitionSpec.getClass.getMethod("fields")
    val fields = fieldsMethod.invoke(partitionSpec).asInstanceOf[java.util.List[_]]

    // scalastyle:off classforname
    val partitionFieldClass = Class.forName(ClassNames.PARTITION_FIELD)
    // scalastyle:on classforname
    val sourceIdMethod = partitionFieldClass.getMethod("sourceId")
    val findFieldMethod = schema.getClass.getMethod("findField", classOf[Int])

    val unsupportedTypes = scala.collection.mutable.ListBuffer[(String, String, String)]()

    fields.asScala.foreach { field =>
      val sourceId = sourceIdMethod.invoke(field).asInstanceOf[Int]
      val column = findFieldMethod.invoke(schema, sourceId.asInstanceOf[Object])

      if (column != null) {
        val nameMethod = column.getClass.getMethod("name")
        val fieldName = nameMethod.invoke(column).asInstanceOf[String]

        val typeMethod = column.getClass.getMethod("type")
        val icebergType = typeMethod.invoke(column)
        val typeStr = icebergType.toString

        // iceberg-rust/crates/iceberg/src/spec/values.rs Literal::try_from_json()
        if (typeStr.startsWith("decimal(")) {
          val precisionStr = typeStr.substring(8, typeStr.indexOf(','))
          val precision = precisionStr.toInt
          // rust_decimal crate maximum precision
          if (precision > 28) {
            unsupportedTypes += ((
              fieldName,
              typeStr,
              s"High-precision decimal (precision=$precision) exceeds maximum of 28 " +
                "(rust_decimal limitation)"))
          }
        } else if (typeStr == "binary" || typeStr.startsWith("fixed[")) {
          unsupportedTypes += ((
            fieldName,
            typeStr,
            "Binary/fixed types not yet supported (Literal::try_from_json todo!())"))
        }
      }
    }

    unsupportedTypes.toList
  }
}

/**
 * Pre-extracted Iceberg metadata for native scan execution.
 *
 * This class holds all metadata extracted from Iceberg during the planning/validation phase in
 * CometScanRule. By extracting all metadata once during validation (where reflection failures
 * trigger fallback to Spark), we avoid redundant reflection during serialization (where failures
 * would be fatal runtime errors).
 *
 * @param table
 *   The Iceberg Table object
 * @param metadataLocation
 *   Path to the table metadata file
 * @param nameMapping
 *   Optional name mapping from table properties (for schema evolution)
 * @param tasks
 *   List of FileScanTask objects from Iceberg planning
 * @param scanSchema
 *   The expectedSchema from the SparkScan (for schema evolution / VERSION AS OF)
 * @param globalFieldIdMapping
 *   Mapping from column names to Iceberg field IDs (built from scanSchema)
 * @param catalogProperties
 *   Catalog properties for FileIO (S3 credentials, regions, etc.)
 */
case class CometIcebergNativeScanMetadata(
    table: Any,
    metadataLocation: String,
    nameMapping: Option[String],
    @transient tasks: java.util.List[_],
    scanSchema: Any,
    tableSchema: Any,
    globalFieldIdMapping: Map[String, Int],
    catalogProperties: Map[String, String],
    fileFormat: String)

object CometIcebergNativeScanMetadata extends Logging {

  /**
   * Extracts all Iceberg metadata needed for native scan execution.
   *
   * This method performs all reflection operations once during planning/validation. If any
   * reflection operation fails, returns None to trigger fallback to Spark.
   *
   * @param scan
   *   The Spark BatchScanExec.scan (SparkBatchQueryScan)
   * @param metadataLocation
   *   Path to the table metadata file (already extracted)
   * @param catalogProperties
   *   Catalog properties for FileIO (already extracted)
   * @return
   *   Some(metadata) if all reflection succeeds, None to trigger fallback
   */
  def extract(
      scan: Any,
      metadataLocation: String,
      catalogProperties: Map[String, String]): Option[CometIcebergNativeScanMetadata] = {
    import org.apache.comet.iceberg.IcebergReflection._

    for {
      table <- getTable(scan)
      tasks <- getTasks(scan)
      scanSchema <- getExpectedSchema(scan)
      tableSchema <- getSchema(table)
    } yield {
      // nameMapping is optional - if it fails we just use None
      val nameMapping = getTableProperties(table).flatMap { properties =>
        val nameMappingKey = "schema.name-mapping.default"
        if (properties.containsKey(nameMappingKey)) {
          Some(properties.get(nameMappingKey))
        } else {
          None
        }
      }

      val globalFieldIdMapping = buildFieldIdMapping(scanSchema)

      // File format is always PARQUET,
      // validated in CometScanRule.validateIcebergFileScanTasks()
      // Hardcoded here for extensibility (future ORC/Avro support would add logic here)
      CometIcebergNativeScanMetadata(
        table = table,
        metadataLocation = metadataLocation,
        nameMapping = nameMapping,
        tasks = tasks,
        scanSchema = scanSchema,
        tableSchema = tableSchema,
        globalFieldIdMapping = globalFieldIdMapping,
        catalogProperties = catalogProperties,
        fileFormat = FileFormats.PARQUET)
    }
  }
}

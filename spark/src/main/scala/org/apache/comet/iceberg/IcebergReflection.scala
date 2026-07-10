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
import org.apache.spark.sql.SparkSession

import org.apache.comet.util.ClassLoaders

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
    val SPARK_BATCH_QUERY_SCAN = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
    val SPARK_STAGED_SCAN = "org.apache.iceberg.spark.source.SparkStagedScan"
    val SPARK_SCHEMA_UTIL = "org.apache.iceberg.spark.SparkSchemaUtil"
  }

  /**
   * SparkScan implementations that Comet recognises as Iceberg data scans.
   *
   * `SparkStagedScan` also backs reads against Iceberg metadata tables (e.g. `POSITION_DELETES`),
   * but the gate for that lives in `getMetadataLocation`, which returns None for metadata-table
   * instances.
   */
  val ICEBERG_SCAN_CLASSES: Set[String] =
    Set(ClassNames.SPARK_BATCH_QUERY_SCAN, ClassNames.SPARK_STAGED_SCAN)

  def isIcebergScanClass(name: String): Boolean = ICEBERG_SCAN_CLASSES.contains(name)

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
    val PUFFIN = "PUFFIN"
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
   * Loads a class using the thread context classloader first, then falls back to the system
   * classloader.
   *
   * @param className
   *   Fully qualified class name to load
   * @return
   *   The loaded Class object
   */
  def loadClass(className: String): Class[_] = ClassLoaders.loadClass(className)

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
      val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
      extractFileLocation(contentFileClass, file)
    } catch {
      case _: Exception => None
    }
  }

  /** The file format of a ContentFile (data or delete file), e.g. "PARQUET", "AVRO", "ORC". */
  def getFileFormat(file: Any): Option[String] = {
    try {
      // Resolve format() on the public ContentFile interface. Iceberg's concrete file impls are
      // package-private, so a method resolved on the concrete class throws IllegalAccessException
      // when invoked.
      // TODO callers in a loop (e.g. validateIcebergFileScanTasks) already hold a cached
      // contentFileClass; add an overload that takes it to avoid reloading per file.
      val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
      Some(contentFileClass.getMethod("format").invoke(file).toString)
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

  private lazy val sparkStagedScanClass: Class[_] = loadClass(ClassNames.SPARK_STAGED_SCAN)

  private def isStagedScan(scan: Any): Boolean = sparkStagedScanClass.isInstance(scan)

  /**
   * Gets the tasks from a SparkScan.
   *
   * Most Iceberg scans (e.g. SparkBatchQueryScan) inherit a `tasks()` accessor from
   * SparkPartitioningAwareScan. SparkStagedScan extends SparkScan directly and only declares
   * `taskGroups()`, so for staged scans we flatten the groups instead. Both methods are protected
   * and require reflection.
   */
  def getTasks(scan: Any): Option[java.util.List[_]] =
    if (isStagedScan(scan)) tasksFromTaskGroups(scan) else tasksFromTasksAccessor(scan)

  private def tasksFromTasksAccessor(scan: Any): Option[java.util.List[_]] =
    findMethodInHierarchy(scan.getClass, "tasks") match {
      case Some(method) =>
        Some(method.invoke(scan).asInstanceOf[java.util.List[_]])
      case None =>
        logError(
          "Iceberg reflection failure: Failed to get tasks from SparkScan: " +
            s"tasks() not found on ${scan.getClass.getName}")
        None
    }

  private def tasksFromTaskGroups(scan: Any): Option[java.util.List[_]] =
    findMethodInHierarchy(scan.getClass, "taskGroups") match {
      case Some(method) =>
        try {
          val groups = method.invoke(scan).asInstanceOf[java.util.List[_]]
          if (groups.isEmpty) {
            Some(new java.util.ArrayList[AnyRef]())
          } else {
            // All task groups in a stage share the same concrete class, so the per-group
            // `tasks()` lookup can be cached once instead of done N times.
            val groupTasksMethod = groups.get(0).getClass.getMethod("tasks")
            val flat = new java.util.ArrayList[AnyRef]()
            groups.forEach { group =>
              val groupTasks =
                groupTasksMethod.invoke(group).asInstanceOf[java.util.Collection[_ <: AnyRef]]
              flat.addAll(groupTasks)
            }
            Some(flat)
          }
        } catch {
          case e: ReflectiveOperationException =>
            logError(
              "Iceberg reflection failure: Failed to flatten tasks from SparkStagedScan: " +
                s"${e.getMessage}")
            None
        }
      case None =>
        logError(
          "Iceberg reflection failure: Failed to flatten tasks from SparkStagedScan: " +
            s"taskGroups() not found on ${scan.getClass.getName}")
        None
    }

  /**
   * Gets the filter expressions from a SparkScan.
   *
   * `filterExpressions()` is declared on SparkPartitioningAwareScan but absent from plain
   * SparkScan. SparkStagedScan (used by RewriteDataFiles) extends SparkScan directly and never
   * pushes filters, so we short-circuit with an empty list rather than reflectively probing for a
   * method we know isn't there.
   */
  def getFilterExpressions(scan: Any): Option[java.util.List[_]] =
    if (isStagedScan(scan)) {
      Some(java.util.Collections.emptyList[AnyRef]())
    } else {
      // Iceberg 1.11 renamed SparkScan.filterExpressions() to filters(); 1.8-1.10 use the old name.
      findMethodInHierarchy(scan.getClass, "filters")
        .orElse(findMethodInHierarchy(scan.getClass, "filterExpressions")) match {
        case Some(method) =>
          Some(method.invoke(scan).asInstanceOf[java.util.List[_]])
        case None =>
          logError(
            "Iceberg reflection failure: Failed to get filter expressions from SparkScan: " +
              s"filters()/filterExpressions() not found on ${scan.getClass.getName}")
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
          findMethodInHierarchy(ops.getClass, "current")
            .flatMap { currentMethod =>
              val metadata = currentMethod.invoke(ops)
              val formatVersionMethod = metadata.getClass.getMethod("formatVersion")
              Some(formatVersionMethod.invoke(metadata).asInstanceOf[Int])
            }
            .orElse {
              logError(
                "Iceberg reflection failure: Failed to get format version: " +
                  "current() method not found in operations class hierarchy")
              None
            }
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
   * All schema versions a table has had (table.schemas().values()), for resolving field ids of
   * columns that have since been dropped -- mirrors Iceberg-Java's FieldLookup. table.schemas()
   * is stable across Iceberg 1.5-1.11.
   */
  def getAllSchemas(table: Any): Seq[Any] = {
    import scala.jdk.CollectionConverters._
    try {
      table.getClass
        .getMethod("schemas")
        .invoke(table)
        .asInstanceOf[java.util.Map[_, _]]
        .values()
        .asScala
        .toSeq
    } catch {
      case e: Exception =>
        logDebug(s"Iceberg reflection: table.schemas() not available: ${e.getMessage}")
        Seq.empty
    }
  }

  /** Returns the `Types.NestedField` for `fieldId` in `schema`, or None. */
  def findFieldObject(schema: Any, fieldId: Int): Option[Any] = {
    try {
      val findFieldMethod = schema.getClass.getMethod("findField", classOf[Int])
      Option(findFieldMethod.invoke(schema, fieldId.asInstanceOf[AnyRef]))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Returns a schema equal to `baseSchema` but guaranteed to contain `requiredFieldIds`. Any id
   * not already present is resolved from the table's schema history (`table.schemas()`) and
   * appended.
   *
   * This mirrors Iceberg-Java's `DeleteFilter.fileProjection`: an equality delete may be keyed on
   * a column that has since been dropped from the current schema, and iceberg-rust needs that
   * column in the task schema to read and apply the delete. Called at serialization time, so it
   * throws on failure (a required id that cannot be resolved, or any reflection error) rather
   * than silently degrading; CometScanRule is responsible for falling back before we get here.
   */
  def schemaWithRequiredFields(baseSchema: Any, table: Any, requiredFieldIds: Seq[Int]): Any = {
    val existingIds = buildFieldIdMapping(baseSchema).values.toSet
    val missingIds = requiredFieldIds.distinct.filterNot(existingIds.contains)
    if (missingIds.isEmpty) {
      baseSchema
    } else {
      logDebug(
        s"Iceberg equality delete references field id(s) ${missingIds.mkString(",")} absent from " +
          "the task schema; resolving from table schema history to build the native scan schema")
      val history = getAllSchemas(table)
      val resolvedFields = missingIds.map { id =>
        history.iterator
          .flatMap(s => findFieldObject(s, id))
          .toSeq
          .headOption
          .getOrElse(throw new IllegalStateException(
            s"Cannot resolve equality-delete field id $id in table schema history"))
      }
      val existing =
        baseSchema.getClass
          .getMethod("columns")
          .invoke(baseSchema)
          .asInstanceOf[java.util.List[_]]
      val newColumns = new java.util.ArrayList[Any](existing)
      resolvedFields.foreach(newColumns.add)
      baseSchema.getClass
        .getConstructor(classOf[java.util.List[_]])
        .newInstance(newColumns)
        .asInstanceOf[AnyRef]
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

      findMethodInHierarchy(operations.getClass, "current").map(_.invoke(operations)).orElse {
        logError(
          "Iceberg reflection failure: Failed to get table metadata: " +
            "current() method not found in operations class hierarchy")
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get table metadata: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the metadata file location from an Iceberg table.
   *
   * Returns None for Iceberg metadata-table instances (e.g. POSITION_DELETES, the table that
   * `RewritePositionDeleteFiles` reads via `SparkStagedScan`). This is the gate that keeps Comet
   * from accelerating metadata-table reads, which have a different schema from the parent data
   * table and aren't supported by the iceberg-rust-driven native path. `CometScanRule` falls back
   * to Spark when this returns None; `CometIcebergRewriteActionSuite` pins the behaviour.
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
      val deleteFileClass = loadClass(ClassNames.DELETE_FILE)
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
    // Iceberg 1.11 renamed SparkScan.expectedSchema() to projection() (the projected read
    // schema); 1.8-1.10 still expose expectedSchema(). Try the new name first, then fall back.
    findMethodInHierarchy(scan.getClass, "projection")
      .orElse(findMethodInHierarchy(scan.getClass, "expectedSchema"))
      .flatMap { schemaMethod =>
        try {
          Some(schemaMethod.invoke(scan))
        } catch {
          case e: Exception =>
            logError(s"Failed to get projection/expectedSchema from SparkScan: ${e.getMessage}")
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

    val partitionFieldClass = loadClass(ClassNames.PARTITION_FIELD)
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

  /**
   * True if the table has table-level encryption configured. Encrypted Parquet footers cannot be
   * read by Comet's native reader, so such tables must fall back to Spark.
   */
  def hasTableEncryption(table: Any): Boolean =
    getTableProperties(table).exists(_.containsKey("encryption.key-id"))

  /**
   * Returns the names of schema columns (including nested struct/list/map fields) that declare a
   * V3 initial-default value. iceberg-rust does not synthesize default values for columns absent
   * from a data file, so reads projecting such columns must fall back. Throws on reflection
   * failure so the caller can fall back rather than risk a native crash.
   */
  def columnsWithInitialDefault(schema: Any): List[String] = {
    import scala.jdk.CollectionConverters._
    val columns =
      schema.getClass.getMethod("columns").invoke(schema).asInstanceOf[java.util.List[_]]
    columns.asScala.flatMap(walkFieldForDefault).toList
  }

  private def walkFieldForDefault(field: Any): List[String] = {
    import scala.jdk.CollectionConverters._
    val name = field.getClass.getMethod("name").invoke(field).asInstanceOf[String]
    val here =
      if (field.getClass.getMethod("initialDefault").invoke(field) != null) List(name) else Nil
    val fieldType = field.getClass.getMethod("type").invoke(field)
    val nested =
      if (fieldType.getClass.getMethod("isNestedType").invoke(fieldType).asInstanceOf[Boolean]) {
        val nestedType = fieldType.getClass.getMethod("asNestedType").invoke(fieldType)
        val fields =
          nestedType.getClass
            .getMethod("fields")
            .invoke(nestedType)
            .asInstanceOf[java.util.List[_]]
        fields.asScala.flatMap(walkFieldForDefault).toList
      } else {
        Nil
      }
    here ++ nested
  }

  /**
   * Converts an Iceberg `Schema` to the Spark `StructType` it reads as, via
   * `SparkSchemaUtil.convert`. Comet serializes the whole table/scan schema to native (not just
   * projected columns), so callers use this to run the schema through Comet's existing type
   * allow-list and fall back if any column is a type the native reader does not support (e.g.
   * variant). Throws on reflection failure so the caller can fall back.
   */
  def toSparkSchema(schema: Any): org.apache.spark.sql.types.StructType = {
    val sparkSchemaUtil = loadClass(ClassNames.SPARK_SCHEMA_UTIL)
    val schemaClass = loadClass(ClassNames.SCHEMA)
    val convert = sparkSchemaUtil.getMethod("convert", schemaClass)
    convert.invoke(null, schema).asInstanceOf[org.apache.spark.sql.types.StructType]
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
 * @param catalogName
 *   Spark V2 catalog name forwarded as `dispatchKey` to CometS3CredentialBridge. `None` when the
 *   table has no catalog identity (e.g. HadoopTables loaded by raw path).
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
    catalogName: Option[String],
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
        catalogName = deriveCatalogName(table),
        fileFormat = FileFormats.PARQUET)
    }
  }

  /**
   * Extracts the Spark V2 catalog name from an Iceberg `Table`. `Table.name()` returns
   * `catalog.namespace.table` for tables loaded through a catalog; we intersect against the
   * registered V2 catalogs so a value like `s3.foo` is not mistaken for a catalog `s3`. Returns
   * `None` for HadoopTables loaded by raw path or when reflection fails.
   */
  private[iceberg] def deriveCatalogName(table: Any): Option[String] =
    deriveCatalogName(table, registeredCatalogNames _)

  /**
   * Test seam that lets tests inject a fixed catalog set without bootstrapping a SparkSession.
   */
  private[iceberg] def deriveCatalogName(
      table: Any,
      knownCatalogNames: () => Iterable[String]): Option[String] = {
    if (table == null) return None
    invokeTableName(table).flatMap { name =>
      if (name.isEmpty || name == "null") {
        None
      } else {
        knownCatalogNames()
          .find(c => name == c || name.startsWith(c + "."))
          .orElse {
            val idx = name.indexOf('.')
            if (idx > 0) Some(name.substring(0, idx)) else None
          }
      }
    }
  }

  private def invokeTableName(table: Any): Option[String] = {
    try {
      table.getClass.getMethod("name").invoke(table) match {
        case s: String => Some(s)
        case other if other != null => Some(other.toString)
        case null => None
      }
    } catch {
      case e: Exception =>
        logWarning(
          s"Iceberg reflection: Table.name() not callable on ${table.getClass.getName}. " +
            "Native S3 credential dispatch will fall back to bucket-keyed isolation: " +
            s"${e.getMessage}")
        None
    }
  }

  private def registeredCatalogNames(): Iterable[String] =
    try {
      SparkSession.active.sessionState.catalogManager.listCatalogs(None)
    } catch {
      case e: Exception =>
        logDebug(s"Could not list V2 catalogs from SparkSession: ${e.getMessage}")
        Nil
    }
}

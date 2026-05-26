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
    val SPARK_WRITE = "org.apache.iceberg.spark.source.SparkWrite"
    val TABLE_PROPERTIES = "org.apache.iceberg.TableProperties"
    val TYPE_UTIL = "org.apache.iceberg.types.TypeUtil"
    val INMEMORY_INPUT_FILE = "org.apache.iceberg.inmemory.InMemoryInputFile"
    val INMEMORY_FILE_IO = "org.apache.iceberg.inmemory.InMemoryFileIO"
    val INPUT_FILE = "org.apache.iceberg.io.InputFile"
    val FILE_IO = "org.apache.iceberg.io.FileIO"
    val GENERIC_MANIFEST_FILE = "org.apache.iceberg.GenericManifestFile"
    val MANIFEST_FILE = "org.apache.iceberg.ManifestFile"
    val MANIFEST_FILES = "org.apache.iceberg.ManifestFiles"
    val DATA_FILE = "org.apache.iceberg.DataFile"

    // Iceberg 1.5.2 (Spark 3.4 profile) ships its own `ReplaceIcebergData` logical write node via
    // the iceberg-spark-extensions module instead of using Spark's stock `ReplaceData`. Iceberg
    // 1.8+ dropped it in favour of Spark 3.5's native row-level operation support, so this class
    // name will only resolve on the 3.4 + Iceberg 1.5.2 combo (where SQL UPDATE / MERGE on V2
    // tables require the Iceberg extension).
    val REPLACE_ICEBERG_DATA = "org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData"
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
   * Probes a class via reflection once, caches the outcome. Returns `None` when Iceberg is not on
   * the classpath so Comet stays buildable in non-Iceberg deployments.
   */
  private def tryLoadClass(name: String): Option[Class[_]] =
    try Some(loadClass(name))
    catch { case _: ClassNotFoundException => None }

  private lazy val sparkWriteClassOpt: Option[Class[_]] = tryLoadClass(ClassNames.SPARK_WRITE)

  /**
   * Whether `write` is an Iceberg `SparkWrite` (or subclass). Returns false if Iceberg is not on
   * the classpath. Used by the write strategy to decide whether to intercept a V2 logical write.
   */
  def isIcebergSparkWrite(write: Any): Boolean =
    sparkWriteClassOpt.exists(_.isInstance(write))

  /**
   * Whether `batchWrite` is one of Iceberg's `SparkWrite` inner `BatchWrite` impls
   * (`BatchAppend`, `OverwriteByFilter`, `DynamicOverwrite`, `CopyOnWriteOperation`,
   * `RewriteFiles`). These are private inner classes of `SparkWrite`; we detect them by name
   * prefix because they are not exposed as a public superclass.
   */
  def isIcebergBatchWrite(batchWrite: Any): Boolean = {
    if (batchWrite == null) return false
    batchWrite.getClass.getName.startsWith(ClassNames.SPARK_WRITE + "$")
  }

  /**
   * Extract the outer `SparkWrite` instance from an Iceberg `BatchWrite` inner class via the
   * synthetic `this$0` reference Java compilers emit for non-static inner classes. The `Table`
   * and table-property accessors live on the outer class.
   */
  def getOuterSparkWrite(batchWrite: Any): Option[Any] = {
    if (batchWrite == null) None
    else {
      try {
        val field = batchWrite.getClass.getDeclaredField("this$0")
        field.setAccessible(true)
        Option(field.get(batchWrite))
      } catch {
        case _: NoSuchFieldException =>
          // batchWrite may already be the outer class (static inner) -- fall back to itself.
          Some(batchWrite)
        case e: Exception =>
          logError(
            s"Iceberg reflection failure: outer SparkWrite from BatchWrite: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Whether `plan` is Iceberg's `ReplaceIcebergData` logical node (Iceberg 1.5.2's UPDATE / MERGE
   * rewrite target on Spark 3.4). Matched by FQCN so the main module doesn't compile-depend on
   * iceberg-spark-extensions.
   */
  def isReplaceIcebergData(plan: Any): Boolean =
    plan != null && plan.getClass.getName == ClassNames.REPLACE_ICEBERG_DATA

  /**
   * Read a declared (or inherited) field on `plan` reflectively. Used for extracting fields off
   * Iceberg 1.5.2's `ReplaceIcebergData` logical node when iceberg-spark-extensions isn't on the
   * main classpath.
   */
  private def reflectField(plan: Any, fieldName: String): Option[AnyRef] =
    try {
      val field = plan.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      Option(field.get(plan))
    } catch {
      case e: Exception =>
        logError(
          s"Iceberg reflection failure: $fieldName on ${plan.getClass.getName}: ${e.getMessage}")
        None
    }

  /**
   * Pull `(table, query, originalTable, write)` out of a `ReplaceIcebergData` instance. Iceberg's
   * 1.5.2 case class shape matches Spark 3.5+'s stock `ReplaceData` exactly, so the extracted
   * tuple can feed our `matchedSparkWrite` dispatcher unchanged.
   */
  def extractReplaceIcebergDataFields(plan: Any): Option[(AnyRef, AnyRef, AnyRef, AnyRef)] = {
    if (!isReplaceIcebergData(plan)) return None
    for {
      table <- reflectField(plan, "table")
      query <- reflectField(plan, "query")
      originalTable <- reflectField(plan, "originalTable")
      write <- reflectField(
        plan,
        "write"
      ) // Option[Write]; field can be Some(null) so kept AnyRef
    } yield (table, query, originalTable, write)
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
      findMethodInHierarchy(scan.getClass, "filterExpressions") match {
        case Some(method) =>
          Some(method.invoke(scan).asInstanceOf[java.util.List[_]])
        case None =>
          logError(
            "Iceberg reflection failure: Failed to get filter expressions from SparkScan: " +
              s"filterExpressions() not found on ${scan.getClass.getName}")
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
   * Reads a `private final` field from a `SparkWrite` instance via reflection. The field names
   * referenced by callers (`queryId`, `targetFileSize`, `useFanoutWriter`, `outputSpecId`,
   * `writeSchema`) are present on `SparkWrite` across the Iceberg 1.5.2 / 1.8.1 / 1.10.0 versions
   * Comet supports.
   */
  private def getSparkWriteField(sparkWrite: Any, fieldName: String): Option[Any] =
    sparkWriteClassOpt.flatMap { cls =>
      try {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        Option(field.get(sparkWrite))
      } catch {
        case _: NoSuchFieldException =>
          // Field may have been renamed across Iceberg versions. Callers that probe multiple
          // candidate names (e.g. `useFanoutWriter` / `partitionedFanoutEnabled`) should expect
          // this and `.orElse` onto the alternative.
          None
        case e: Exception =>
          logError(
            s"Iceberg reflection failure: Failed to read SparkWrite.$fieldName: ${e.getMessage}")
          None
      }
    }

  /** Operation id used in data-file names; sourced from `SparkWrite.queryId`. */
  def getOperationIdFromSparkWrite(sparkWrite: Any): Option[String] =
    getSparkWriteField(sparkWrite, "queryId").map(_.asInstanceOf[String])

  /** Target data file size in bytes; sourced from `SparkWrite.targetFileSize`. */
  def getTargetFileSizeFromSparkWrite(sparkWrite: Any): Option[Long] =
    getSparkWriteField(sparkWrite, "targetFileSize")
      .map(_.asInstanceOf[java.lang.Long].longValue())

  /**
   * Whether the planner would use a fanout writer.
   *
   * Field name changed between Iceberg releases:
   *   - 1.5.2 (Spark 3.4 profile): `partitionedFanoutEnabled`
   *   - 1.8.1+ (Spark 3.5 / 4.0 profiles): `useFanoutWriter`
   *
   * Same semantic in both versions. Try the newer name first; fall back to the older one so the
   * helper resolves across all supported Iceberg versions without a per-version shim.
   */
  def getUseFanoutWriterFromSparkWrite(sparkWrite: Any): Option[Boolean] =
    getSparkWriteField(sparkWrite, "useFanoutWriter")
      .orElse(getSparkWriteField(sparkWrite, "partitionedFanoutEnabled"))
      .map(_.asInstanceOf[java.lang.Boolean].booleanValue())

  /** Output partition spec id; sourced from `SparkWrite.outputSpecId`. */
  def getOutputSpecIdFromSparkWrite(sparkWrite: Any): Option[Int] =
    getSparkWriteField(sparkWrite, "outputSpecId")
      .map(_.asInstanceOf[java.lang.Integer].intValue())

  /** Iceberg `Schema` the write was planned against; sourced from `SparkWrite.writeSchema`. */
  def getWriteSchemaFromSparkWrite(sparkWrite: Any): Option[Any] =
    getSparkWriteField(sparkWrite, "writeSchema")

  /**
   * Effective output file format resolved by Iceberg via `SparkWriteConf.dataFileFormat()`. Java
   * consults the `write-format` write option BEFORE the `write.format.default` table property, so
   * a per-write option override must win - gating only on table properties produces false-pass
   * and false-fall-back outcomes when the two disagree.
   *
   * `SparkWrite.format` is a `FileFormat` enum (`PARQUET`/`ORC`/`AVRO`); returned lower-cased.
   */
  def getFormatFromSparkWrite(sparkWrite: Any): Option[String] =
    getSparkWriteField(sparkWrite, "format")
      .map(_.toString.toLowerCase(java.util.Locale.ROOT))

  /**
   * Extracts the Spark V2 catalog name from an Iceberg `Table`. `Table.name()` returns
   * `catalog.namespace.table` for tables loaded through a catalog; we intersect against the
   * registered V2 catalogs so a value like `s3.foo` is not mistaken for a catalog `s3`. Returns
   * `None` for HadoopTables loaded by raw path or when reflection fails. Shared by the scan and
   * write paths so both derive the credential dispatch key identically.
   */
  def deriveCatalogName(table: Any): Option[String] =
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

  /**
   * Resolved write properties from `SparkWriteConf.writeProperties()`, snapshotted on the
   * `SparkWrite` at planning time. Java's `RegistryBasedFileWriterFactory` merges these over the
   * table's properties before constructing the per-file writer; build-time native settings must
   * mirror that merge or per-write options (codec / level / variant-shredding / ...) silently
   * lose their effect.
   */
  def getWritePropertiesFromSparkWrite(sparkWrite: Any): Option[Map[String, String]] =
    getSparkWriteField(sparkWrite, "writeProperties").map { raw =>
      val javaMap = raw.asInstanceOf[java.util.Map[String, String]]
      val iter = javaMap.entrySet().iterator()
      val builder = Map.newBuilder[String, String]
      while (iter.hasNext) {
        val entry = iter.next()
        builder += (entry.getKey -> entry.getValue)
      }
      builder.result()
    }

  /**
   * Output sort order id resolved by Iceberg's
   * `SparkWriteConf.outputSortOrderId(writeRequirements)`: a per-write `output-sort-order-id`
   * option wins, else the table's sort order when an ordering is required, else `0` (unsorted).
   * Stamping the table sort order id unconditionally (the previous behaviour) writes the wrong
   * value when Java would have used `unsorted`.
   *
   * Iceberg 1.5.2 (Spark 3.4 profile) lacks this method on `SparkWriteConf`; we return `None` and
   * let the caller fall back to `Table.sortOrder().orderId()`.
   */
  def getOutputSortOrderIdFromSparkWrite(sparkWrite: Any): Option[Int] = {
    val writeConf =
      getSparkWriteField(sparkWrite, "writeConf").map(_.asInstanceOf[AnyRef]).getOrElse {
        return None
      }
    val writeRequirements =
      getSparkWriteField(sparkWrite, "writeRequirements").map(_.asInstanceOf[AnyRef]).getOrElse {
        return None
      }
    try {
      val method = writeConf.getClass.getDeclaredMethods
        .find(m => m.getName == "outputSortOrderId" && m.getParameterCount == 1)
        .getOrElse(return None)
      method.setAccessible(true)
      val result = method.invoke(writeConf, writeRequirements)
      Some(result.asInstanceOf[java.lang.Integer].intValue())
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: SparkWriteConf.outputSortOrderId failed " +
            s"(${e.getMessage}); falling back to table.sortOrder().orderId()")
        None
    }
  }

  /**
   * Reads the `private final` `table` field from a `SparkWrite`. Same setAccessible-required
   * pattern as the other private-final accessors.
   */
  def getTableFromSparkWrite(sparkWrite: Any): Option[Any] =
    getSparkWriteField(sparkWrite, "table")

  /**
   * Looks up a `PartitionSpec` from `Table.specs()` by its id. Used to retrieve the spec the
   * write was planned against (`outputSpecId`), which may differ from the table's current spec
   * for evolution scenarios.
   */
  def getPartitionSpecById(table: Any, specId: Int): Option[Any] =
    try {
      val method = table.getClass.getMethod("specs")
      val specs = method.invoke(table).asInstanceOf[java.util.Map[java.lang.Integer, _]]
      Option(specs.get(java.lang.Integer.valueOf(specId)))
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: Failed to look up partition spec " +
            s"$specId: ${e.getMessage}")
        None
    }

  /**
   * Base directory under which the writer should place data files. Resolved by invoking
   * `table.locationProvider().newDataLocation("")` and trimming the trailing slash, matching the
   * data location Iceberg-Java itself uses for `OutputFileFactory`.
   */
  def getDataLocation(table: Any): Option[String] =
    try {
      val locationProviderMethod =
        findMethodInHierarchy(table.getClass, "locationProvider").getOrElse(
          throw new NoSuchMethodException(
            s"locationProvider() not found on ${table.getClass.getName}"))
      val provider = locationProviderMethod.invoke(table)
      // `LocationProviders$DefaultLocationProvider` is package-private with a public
      // `newDataLocation(String)`. Without `setAccessible(true)` JDK 11+ rejects the invoke.
      val newDataLocMethod = provider.getClass.getMethod("newDataLocation", classOf[String])
      newDataLocMethod.setAccessible(true)
      val location = newDataLocMethod.invoke(provider, "").asInstanceOf[String]
      Some(location.stripSuffix("/"))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get data location: ${e.getMessage}", e)
        None
    }

  /** Renders an Iceberg `Schema` to its JSON wire format via `SchemaParser.toJson(Schema)`. */
  def schemaToJson(schema: Any): Option[String] =
    try {
      val parserClass = loadClass(ClassNames.SCHEMA_PARSER)
      val schemaClass = loadClass(ClassNames.SCHEMA)
      val method = parserClass.getMethod("toJson", schemaClass)
      Some(method.invoke(null, schema.asInstanceOf[AnyRef]).asInstanceOf[String])
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: SchemaParser.toJson: ${e.getMessage}")
        None
    }

  /**
   * Renders an Iceberg `PartitionSpec` to its JSON wire format via
   * `PartitionSpecParser.toJson(PartitionSpec)`.
   */
  def partitionSpecToJson(spec: Any): Option[String] =
    try {
      val parserClass = loadClass(ClassNames.PARTITION_SPEC_PARSER)
      val specClass = loadClass(ClassNames.PARTITION_SPEC)
      val method = parserClass.getMethod("toJson", specClass)
      Some(method.invoke(null, spec.asInstanceOf[AnyRef]).asInstanceOf[String])
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: PartitionSpecParser.toJson: ${e.getMessage}")
        None
    }

  private lazy val tablePropertiesClassOpt: Option[Class[_]] =
    tryLoadClass(ClassNames.TABLE_PROPERTIES)

  /**
   * Reads a static `String` constant from Iceberg's `org.apache.iceberg.TableProperties` by field
   * name (e.g. `"DEFAULT_FILE_FORMAT"` -> `"write.format.default"`). Throws when Iceberg is
   * absent or the constant has been renamed/removed; both indicate a version we have not vetted.
   */
  def tablePropertyConstant(fieldName: String): String =
    readTablePropertiesField(fieldName).asInstanceOf[String]

  /**
   * Reads a static `int` constant from `TableProperties` (e.g. one of the `*_DEFAULT` numerics).
   */
  def tablePropertyIntConstant(fieldName: String): Int =
    readTablePropertiesField(fieldName).asInstanceOf[Integer].intValue()

  /**
   * Like [[tablePropertyConstant]] but returns `None` when the constant is absent in the Iceberg
   * version on the classpath rather than throwing. Used to gate behaviour that only some Iceberg
   * versions implement -- e.g. `PARQUET_COLUMN_STATS_ENABLED_PREFIX`, added in 1.10.0; on 1.5.2 /
   * 1.8.1 the corresponding property is silently ignored by Iceberg-Java, so there is nothing to
   * gate.
   */
  def tablePropertyConstantOpt(fieldName: String): Option[String] =
    tablePropertiesClassOpt.flatMap { cls =>
      try Some(cls.getField(fieldName).get(null).asInstanceOf[String])
      catch { case _: NoSuchFieldException => None }
    }

  private def readTablePropertiesField(fieldName: String): Any = {
    val cls = tablePropertiesClassOpt.getOrElse(
      throw new IllegalStateException(s"${ClassNames.TABLE_PROPERTIES} is not on the classpath"))
    try cls.getField(fieldName).get(null)
    catch {
      case e: NoSuchFieldException =>
        throw new IllegalStateException(
          s"${ClassNames.TABLE_PROPERTIES}.$fieldName not found " +
            "(unsupported Iceberg version?)",
          e)
    }
  }

  /**
   * Returns the top-level column names of an Iceberg `Schema`, in declared order. Used by the
   * native write serde to project Spark 4.x `ReplaceData` row streams (which carry an
   * `__row_operation` column plus optional file-metadata columns) down to just the data columns
   * the native iceberg-rust writer expects.
   */
  def getSchemaFieldNames(schema: Any): Option[Seq[String]] =
    try {
      val cols = schema.getClass
        .getMethod("columns")
        .invoke(schema)
        .asInstanceOf[java.util.List[_]]
      val names = new scala.collection.mutable.ArrayBuffer[String](cols.size())
      val it = cols.iterator()
      while (it.hasNext) {
        val col = it.next().asInstanceOf[AnyRef]
        names += col.getClass.getMethod("name").invoke(col).asInstanceOf[String]
      }
      Some(names.toSeq)
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Schema.columns(): ${e.getMessage}")
        None
    }

  /**
   * Returns the count of projected field IDs in `schema` -- mirrors Iceberg-Java's
   * `TypeUtil.getProjectedIds(Schema).size()` used to gate
   * `write.metadata.metrics.max-inferred-column-defaults`.
   */
  def getProjectedFieldIdCount(schema: Any): Option[Int] = {
    for {
      typeUtilClass <- tryLoadClass(ClassNames.TYPE_UTIL)
      schemaClass <- tryLoadClass(ClassNames.SCHEMA)
    } yield {
      val method = typeUtilClass.getMethod("getProjectedIds", schemaClass)
      val ids = method.invoke(null, schema.asInstanceOf[AnyRef]).asInstanceOf[java.util.Set[_]]
      ids.size()
    }
  }

  /**
   * Sum `recordCount` and `fileSizeInBytes` across `dataFiles` for SQL-metric reporting. The
   * concrete `DataFile` impl (`BaseFile`) is package-private in Iceberg, so look the accessors up
   * on the public `DataFile` interface instead; virtual dispatch still hits the concrete
   * implementation at invoke time.
   */
  def sumDataFileMetrics(dataFiles: java.util.List[_]): (Long, Long) = {
    if (dataFiles.isEmpty) return (0L, 0L)
    val dataFileClass = loadClass(ClassNames.DATA_FILE)
    val recordCountMethod = dataFileClass.getMethod("recordCount")
    val fileSizeMethod = dataFileClass.getMethod("fileSizeInBytes")
    var rows = 0L
    var bytes = 0L
    val it = dataFiles.iterator()
    while (it.hasNext) {
      val df = it.next().asInstanceOf[AnyRef]
      rows += recordCountMethod.invoke(df).asInstanceOf[java.lang.Long].longValue()
      bytes += fileSizeMethod.invoke(df).asInstanceOf[java.lang.Long].longValue()
    }
    (rows, bytes)
  }

  /**
   * Stamp `sortOrderId` on every `DataFile` in `dataFiles`. iceberg-rust's writer doesn't expose
   * the field (it's `pub(crate)` on `DataFile`), so the manifest comes back with sort_order_id
   * unset. iceberg-java's `BaseFile` declares a private `sortOrderId: Integer` field that the
   * normal `SparkWrite` path populates from the table's `outputSortOrderId`; we mirror that here
   * by writing the same value via reflection before handing files to the committer.
   */
  def stampSortOrderId(dataFiles: java.util.List[_], sortOrderId: Int): Unit = {
    if (dataFiles.isEmpty) return
    val baseFileClass = loadClass("org.apache.iceberg.BaseFile")
    val field = baseFileClass.getDeclaredField("sortOrderId")
    field.setAccessible(true)
    val boxed = Integer.valueOf(sortOrderId)
    val it = dataFiles.iterator()
    while (it.hasNext) {
      field.set(it.next().asInstanceOf[AnyRef], boxed)
    }
  }

  /**
   * Construct a `SparkWrite$TaskCommit(DataFile[])` instance for the native commit path. The
   * constructor is package-private; `setAccessible(true)` is required on every Iceberg version.
   */
  def buildTaskCommit(dataFiles: java.util.List[_]): AnyRef = {
    val taskCommitClass = loadClass("org.apache.iceberg.spark.source.SparkWrite$TaskCommit")
    val dataFileClass = loadClass("org.apache.iceberg.DataFile")
    val arrayClass = java.lang.reflect.Array.newInstance(dataFileClass, 0).getClass
    val ctor = taskCommitClass.getDeclaredConstructor(arrayClass)
    ctor.setAccessible(true)
    val array = java.lang.reflect.Array.newInstance(dataFileClass, dataFiles.size())
    for (i <- 0 until dataFiles.size()) {
      java.lang.reflect.Array.set(array, i, dataFiles.get(i))
    }
    ctor.newInstance(array.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
  }

  /**
   * Returns the runtime Iceberg version (`IcebergBuild.version()`) when available. Used to stamp
   * the Parquet `created_by` field so files written natively can be traced back to the Iceberg
   * release whose property defaults Comet mirrored.
   */
  def icebergVersion(): String =
    try {
      val cls = loadClass("org.apache.iceberg.IcebergBuild")
      cls.getMethod("loadBuildInfo").invoke(null)
      cls.getMethod("version").invoke(null).asInstanceOf[String]
    } catch {
      case e: Exception =>
        logWarning(s"Iceberg reflection failure: IcebergBuild.version: ${e.getMessage}")
        "unknown"
    }

  /**
   * Decode the per-task manifest bytes the native Iceberg writer emits into an `Iterable` of
   * Iceberg `DataFile` snapshots. The native operator writes one V2 data manifest per task
   * (Avro-encoded) via iceberg-rust's `ManifestWriter`; this helper builds an
   * `InMemoryFileIO`/`InMemoryInputFile` pair so the manifest stays in process, then reads it via
   * the standard `ManifestFiles.read` entry point. Each entry is `copy()`-ed so the returned
   * `DataFile`s outlive the reader. The 3-arg `GenericManifestFile` constructor is
   * package-private and requires `setAccessible(true)` on every supported Iceberg version (1.5.2
   * / 1.8.1 / 1.10.0 verified).
   *
   * `specId` is stamped onto the synthesised `ManifestFile` because the manifest reader uses it
   * to pick the correct partition spec when materialising partition data on each `DataFile`.
   */
  def decodeManifestToDataFiles(bytes: Array[Byte], specId: Int): java.util.List[AnyRef] = {
    // Iceberg's `ManifestReader.open` infers format from the file extension; the in-memory path
    // must end in `.avro` for the v2 data-manifest path to be picked. The "memory:" scheme just
    // namespaces the location so it can't collide with on-disk paths in the same FileIO.
    val location = s"memory:comet-manifest-${java.util.UUID.randomUUID()}.avro"
    val fileIO = newInMemoryFileIO(location, bytes)
    val inputFile = newInMemoryInputFile(location, bytes)
    val manifestFile = newDataManifestFile(inputFile, specId)
    readDataFilesFromManifest(manifestFile, fileIO)
  }

  private def newInMemoryFileIO(location: String, bytes: Array[Byte]): AnyRef = {
    val cls = loadClass(ClassNames.INMEMORY_FILE_IO)
    val instance = cls.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    cls
      .getMethod("addFile", classOf[String], classOf[Array[Byte]])
      .invoke(instance, location, bytes)
    instance
  }

  private def newInMemoryInputFile(location: String, bytes: Array[Byte]): AnyRef = {
    val cls = loadClass(ClassNames.INMEMORY_INPUT_FILE)
    cls
      .getConstructor(classOf[String], classOf[Array[Byte]])
      .newInstance(location, bytes)
      .asInstanceOf[AnyRef]
  }

  /**
   * Construct a `GenericManifestFile` pointing at an in-memory data manifest. Two version-skew
   * issues to handle:
   *
   *   1. Constructor shape changed in Iceberg 1.6 when V3's `first_row_id` field was added:
   *      - 1.5.2 (Spark 3.4 profile): `(InputFile, int)` -- 2-arg
   *      - 1.6+ (Spark 3.5 / 4.0 profiles): `(InputFile, int, long)` -- 3-arg with `firstRowId`
   *        Both forms are package-private. We pass `firstRowId = 0` for the V3 variant because
   *        all our data manifests are V2 (V3 row-lineage is gated as Unsupported in
   *        `checkTriggers`).
   *
   * 2. `ManifestFiles.read` on Iceberg 1.5.2 refuses to read a `ManifestFile` whose
   * `snapshotId()` is `null` (`InheritableMetadataFactory.fromManifest` throws "Cannot read from
   * ManifestFile with null (unassigned) snapshot ID"). Iceberg 1.8+ relaxed that check. We set
   * `snapshotId = 0L` via reflection so the read path is happy across versions; the real snapshot
   * id is stamped onto the embedded `DataFile`s later by `BatchWrite.commit(messages)`, so the
   * placeholder never reaches storage.
   */
  private def newDataManifestFile(inputFile: AnyRef, specId: Int): AnyRef = {
    val inputFileClass = loadClass(ClassNames.INPUT_FILE)
    val cls = loadClass(ClassNames.GENERIC_MANIFEST_FILE)
    val (ctor, args): (java.lang.reflect.Constructor[_], Array[Object]) =
      try {
        val c = cls.getDeclaredConstructor(inputFileClass, classOf[Int], classOf[Long])
        (c, Array[Object](inputFile, Integer.valueOf(specId), java.lang.Long.valueOf(0L)))
      } catch {
        case _: NoSuchMethodException =>
          val c = cls.getDeclaredConstructor(inputFileClass, classOf[Int])
          (c, Array[Object](inputFile, Integer.valueOf(specId)))
      }
    ctor.setAccessible(true)
    val manifest = ctor.newInstance(args: _*).asInstanceOf[AnyRef]
    try {
      val snapshotIdField = cls.getDeclaredField("snapshotId")
      snapshotIdField.setAccessible(true)
      snapshotIdField.set(manifest, java.lang.Long.valueOf(0L))
    } catch {
      case _: NoSuchFieldException => () // field renamed in a future release; soft-fail
    }
    manifest
  }

  private def readDataFilesFromManifest(
      manifestFile: AnyRef,
      fileIO: AnyRef): java.util.List[AnyRef] = {
    val manifestFileClass = loadClass(ClassNames.MANIFEST_FILE)
    val fileIOClass = loadClass(ClassNames.FILE_IO)
    val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
    val readMethod = loadClass(ClassNames.MANIFEST_FILES)
      .getMethod("read", manifestFileClass, fileIOClass)
    val reader = readMethod.invoke(null, manifestFile, fileIO)
    try {
      val iterator = reader.getClass
        .getMethod("iterator")
        .invoke(reader)
        .asInstanceOf[java.util.Iterator[AnyRef]]
      val result = new java.util.ArrayList[AnyRef]()
      val copyMethod = contentFileClass.getMethod("copy")
      while (iterator.hasNext) {
        result.add(copyMethod.invoke(iterator.next()))
      }
      result
    } finally {
      try reader.getClass.getMethod("close").invoke(reader)
      catch {
        case e: Exception => logWarning(s"Failed to close ManifestReader: ${e.getMessage}")
      }
    }
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
        catalogName = IcebergReflection.deriveCatalogName(table),
        fileFormat = FileFormats.PARQUET)
    }
  }
}

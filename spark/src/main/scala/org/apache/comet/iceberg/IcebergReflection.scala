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
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

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
    val TABLE = "org.apache.iceberg.Table"
    val PARTITIONING = "org.apache.iceberg.Partitioning"
    val SPARK_WRITE = "org.apache.iceberg.spark.source.SparkWrite"
    val TABLE_PROPERTIES = "org.apache.iceberg.TableProperties"
    val INMEMORY_INPUT_FILE = "org.apache.iceberg.inmemory.InMemoryInputFile"
    val INMEMORY_FILE_IO = "org.apache.iceberg.inmemory.InMemoryFileIO"
    val INPUT_FILE = "org.apache.iceberg.io.InputFile"
    val FILE_IO = "org.apache.iceberg.io.FileIO"
    val GENERIC_MANIFEST_FILE = "org.apache.iceberg.GenericManifestFile"
    val MANIFEST_FILE = "org.apache.iceberg.ManifestFile"
    val MANIFEST_FILES = "org.apache.iceberg.ManifestFiles"
    val DATA_FILE = "org.apache.iceberg.DataFile"

    // Iceberg 1.5.2 uses its own `ReplaceIcebergData` due to lack of `ReplaceData` in Spark 3.4.
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

  // Iceberg FileIO implementations whose backing storage Comet's native reader can reach.
  // Custom/test FileIO classes (e.g. CustomFileIO in TestSparkExecutorCache) are not compatible
  // because Comet's native reader bypasses Java FileIO entirely.
  val COMPATIBLE_FILE_IO_CLASSES: Set[String] = Set(
    "org.apache.iceberg.hadoop.HadoopFileIO",
    "org.apache.iceberg.aws.s3.S3FileIO",
    "org.apache.iceberg.gcp.gcs.GCSFileIO",
    "org.apache.iceberg.io.ResolvingFileIO",
    "org.apache.iceberg.spark.SparkFileIO",
    "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
    "org.apache.iceberg.CachingFileIO")

  // Prefix of the EncryptingFileIO family. An encrypted table's io() is not the bare
  // EncryptingFileIO but a nested variant chosen from the wrapped delegate's capabilities
  // (e.g. EncryptingFileIO$WithSupportsPrefixOperations when the delegate is HadoopFileIO), so
  // an exact class-name match misses it. Comet forwards each file's key_metadata to iceberg-rust
  // and reads the ciphertext through iceberg-rust's own storage layer, so any EncryptingFileIO
  // variant is compatible.
  private val ENCRYPTING_FILE_IO_PREFIX = "org.apache.iceberg.encryption.EncryptingFileIO"

  /**
   * True if `fileIO` is a FileIO whose backing storage Comet's native reader can reach. Matches
   * on `fileIO`'s own class hierarchy (via [[classNameInHierarchy]]) rather than its exact leaf
   * class, so a subclass that only adds metrics/retry/credential-routing on top of a known-
   * compatible FileIO (e.g. a custom S3FileIO subclass) still matches, instead of silently
   * falling back to Spark.
   */
  def isCompatibleFileIO(fileIO: Any): Boolean =
    classNameInHierarchy(fileIO.getClass, COMPATIBLE_FILE_IO_CLASSES) ||
      fileIO.getClass.getName.startsWith(ENCRYPTING_FILE_IO_PREFIX)

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

  /** Loads a class, returning `None` when it's absent (e.g. Iceberg not on the classpath). */
  private def tryLoadClass(name: String): Option[Class[_]] =
    try Some(loadClass(name))
    catch { case _: ClassNotFoundException => None }

  private lazy val sparkWriteClassOpt: Option[Class[_]] = tryLoadClass(ClassNames.SPARK_WRITE)

  /** Whether `write` is an Iceberg `SparkWrite` (false if Iceberg isn't on the classpath). */
  def isIcebergSparkWrite(write: Any): Boolean =
    sparkWriteClassOpt.exists(_.isInstance(write))

  def isIcebergBatchWrite(batchWrite: Any): Boolean = {
    if (batchWrite == null) return false
    batchWrite.getClass.getName.startsWith(ClassNames.SPARK_WRITE + "$")
  }

  def getOuterSparkWrite(batchWrite: Any): Option[Any] = {
    if (batchWrite == null) None
    else {
      try {
        val field = batchWrite.getClass.getDeclaredField("this$0")
        field.setAccessible(true)
        Option(field.get(batchWrite))
      } catch {
        case _: NoSuchFieldException =>
          None
        case e: Exception =>
          logError(
            s"Iceberg reflection failure: outer SparkWrite from BatchWrite: ${e.getMessage}")
          None
      }
    }
  }

  def isReplaceIcebergData(plan: Any): Boolean =
    plan != null && plan.getClass.getName == ClassNames.REPLACE_ICEBERG_DATA

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
   * Methods resolved by [[findMethod]], [[getDeclaredMethod]] and [[findMethodInHierarchy]],
   * keyed by the class the lookup started from and then by the lookup itself.
   *
   * `Class.getMethod` linearly scans the class's public methods and returns a fresh defensive
   * copy of the `Method` on every call. Comet resolves the same handful of Iceberg accessors once
   * per file scan task, and again per partition field and per delete file, so planning a scan
   * over a table with many files does O(files) reflective lookups that all resolve to the same
   * few methods, repeated for every AQE stage.
   *
   * Misses are cached too, which matters most for [[extractFileLocation]]: it probes for
   * `location()` on every file, and Iceberg versions that only have `path()` would otherwise
   * construct a `NoSuchMethodException`, stack trace and all, per file.
   *
   * A `ClassValue` keys the cache on the class object itself, so entries are reclaimed with the
   * class and a cached method never pins a classloader that Spark has discarded.
   */
  private val methodCache: ClassValue[ConcurrentHashMap[String, Option[Method]]] =
    new ClassValue[ConcurrentHashMap[String, Option[Method]]] {
      override protected def computeValue(
          clazz: Class[_]): ConcurrentHashMap[String, Option[Method]] =
        new ConcurrentHashMap[String, Option[Method]]()
    }

  private def cachedLookup(clazz: Class[_], key: String)(
      resolve: => Option[Method]): Option[Method] = {
    val perClass = methodCache.get(clazz)
    // Read first: computeIfAbsent allocates the mapping function and can lock the bin even for a
    // hit, and these lookups are almost always hits.
    val cached = perClass.get(key)
    if (cached != null) cached else perClass.computeIfAbsent(key, _ => resolve)
  }

  private def lookupKey(methodName: String, paramTypes: Seq[Class[_]]): String =
    if (paramTypes.isEmpty) methodName
    else paramTypes.map(_.getName).mkString(methodName + "(", ",", ")")

  private def missing(clazz: Class[_], methodName: String, paramTypes: Seq[Class[_]]): Nothing =
    throw new NoSuchMethodException(s"${clazz.getName}.${lookupKey(methodName, paramTypes)}")

  /**
   * Suppresses access checks so the method can be invoked when its declaring class is
   * package-private, as Iceberg's concrete file/task/term implementations are. Runs once, when
   * the method is first resolved. A JVM that refuses (a class in a module that is exported but
   * not open) leaves the method usable for the public-class case, so the refusal is not fatal
   * here.
   */
  private def makeAccessible(method: Method): Method = {
    try method.setAccessible(true)
    catch { case _: RuntimeException => }
    method
  }

  private def declaredMethod(clazz: Class[_], methodName: String): Option[Method] =
    try Some(makeAccessible(clazz.getDeclaredMethod(methodName)))
    catch { case _: NoSuchMethodException => None }

  /**
   * Cached `Class.getMethod`, returning None instead of throwing when the method is absent. The
   * resolved method has access checks suppressed (see [[makeAccessible]]).
   */
  def findMethod(clazz: Class[_], methodName: String, paramTypes: Class[_]*): Option[Method] =
    cachedLookup(clazz, lookupKey(methodName, paramTypes)) {
      try Some(makeAccessible(clazz.getMethod(methodName, paramTypes: _*)))
      catch { case _: NoSuchMethodException => None }
    }

  /**
   * Cached `Class.getMethod`, throwing `NoSuchMethodException` when the method is absent, like
   * the JDK call it replaces.
   */
  def getMethod(clazz: Class[_], methodName: String, paramTypes: Class[_]*): Method =
    findMethod(clazz, methodName, paramTypes: _*).getOrElse(
      missing(clazz, methodName, paramTypes))

  /**
   * Cached `Class.getDeclaredMethod` with access checks suppressed, throwing
   * `NoSuchMethodException` when the method is absent, like the JDK call it replaces.
   */
  def getDeclaredMethod(clazz: Class[_], methodName: String): Method =
    cachedLookup(clazz, "declared:" + methodName)(declaredMethod(clazz, methodName))
      .getOrElse(missing(clazz, methodName, Nil))

  /**
   * Searches through class hierarchy to find a method (including protected methods).
   */
  def findMethodInHierarchy(clazz: Class[_], methodName: String): Option[Method] =
    cachedLookup(clazz, "hierarchy:" + methodName) {
      var current: Class[_] = clazz
      var found: Option[Method] = None
      while (found.isEmpty && current != null) {
        found = declaredMethod(current, methodName)
        if (found.isEmpty) current = current.getSuperclass
      }
      found
    }

  /**
   * True if `clazz` or any of its superclasses has a name in `names`. Walks the already-loaded
   * class object's own hierarchy, so unlike [[loadClass]] it never risks a
   * `ClassNotFoundException` for a candidate name that isn't on this JVM's classpath (e.g.
   * checking for a GCS/Azure FileIO class when only iceberg-aws is bundled).
   */
  def classNameInHierarchy(clazz: Class[_], names: Set[String]): Boolean = {
    var current: Class[_] = clazz
    while (current != null) {
      if (names.contains(current.getName)) {
        return true
      }
      current = current.getSuperclass
    }
    false
  }

  /**
   * Extracts file location from Iceberg ContentFile, handling both location() and path().
   *
   * Different Iceberg versions expose file paths differently:
   *   - Newer versions: location() returns String
   *   - Older versions: path() returns CharSequence
   *
   * `None` means neither accessor is declared; a genuine invoke failure propagates instead.
   */
  def extractFileLocation(contentFileClass: Class[_], file: Any): Option[String] =
    findMethod(contentFileClass, "location") match {
      case Some(locationMethod) => Some(locationMethod.invoke(file).asInstanceOf[String])
      case None =>
        findMethod(contentFileClass, "path")
          .map(_.invoke(file).asInstanceOf[CharSequence].toString)
    }

  /**
   * Extracts file location from ContentFile instance using dynamic class lookup.
   */
  def extractFileLocation(file: Any): Option[String] =
    tryLoadClass(ClassNames.CONTENT_FILE).flatMap(extractFileLocation(_, file))

  /**
   * The file format of a ContentFile (data or delete file), e.g. "PARQUET", "AVRO", "ORC".
   *
   * `contentFileClass` is the public ContentFile interface, which callers already hold: Iceberg's
   * concrete file impls are package-private, so `format()` resolved on the concrete class throws
   * IllegalAccessException when invoked.
   *
   * `None` means `format()` isn't declared; a genuine invoke failure propagates instead.
   */
  def getFileFormat(contentFileClass: Class[_], file: Any): Option[String] =
    findMethod(contentFileClass, "format").map(_.invoke(file).toString)

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
            val groupTasksMethod = getMethod(groups.get(0).getClass, "tasks")
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
      val formatVersionMethod = getMethod(table.getClass, "formatVersion")
      Some(formatVersionMethod.invoke(table).asInstanceOf[Int])
    } catch {
      case _: NoSuchMethodException =>
        try {
          // If not directly available, access via operations/metadata
          val ops = getDeclaredMethod(table.getClass, "operations").invoke(table)
          findMethodInHierarchy(ops.getClass, "current")
            .flatMap { currentMethod =>
              val metadata = currentMethod.invoke(ops)
              val formatVersionMethod = getMethod(metadata.getClass, "formatVersion")
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
      val ioMethod = getMethod(table.getClass, "io")
      Some(ioMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get FileIO from table: ${e.getMessage}")
        None
    }
  }

  /**
   * The table's `EncryptionManager` (`table.encryption()`). Unlike the `encryption.*` property
   * prefix, this reflects what the table's `TableOperations` actually installed, so it also
   * covers custom operations that enable encryption without any table property. Returns `None` on
   * reflection failure -- callers gate on the concrete manager and must fail closed.
   */
  def getEncryptionManager(table: Any): Option[AnyRef] = {
    try {
      val encryptionMethod = getMethod(table.getClass, "encryption")
      Option(encryptionMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: Failed to get EncryptionManager from table: " +
            s"${e.getMessage}")
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
      val schemaMethod = getMethod(table.getClass, "schema")
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
      getMethod(table.getClass, "schemas")
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
      val findFieldMethod = getMethod(schema.getClass, "findField", classOf[Int])
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
        getMethod(baseSchema.getClass, "columns")
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
      val specMethod = getMethod(table.getClass, "spec")
      Some(specMethod.invoke(table))
    } catch {
      case e: Exception =>
        logError(
          s"Iceberg reflection failure: Failed to get partition spec from table: ${e.getMessage}")
        None
    }
  }

  /**
   * Validates that the table's unified partition type can be computed -- the merge of every
   * historical partition spec, which is what the `_partition` metadata column projects.
   *
   * Iceberg Java's `Partitioning.partitionType(table)` runs the same cross-spec compatibility
   * check that iceberg-rust does natively: a V1 table does not guarantee partition field ids are
   * unique across specs, so two specs can bind the same id to incompatible source/transform
   * pairs, which cannot be merged into one struct field. iceberg-rust returns a DataInvalid error
   * in that case, but only at scan time -- too late for Comet to fall back. Calling the Java
   * check here, at plan time, lets `CometScanRule` fall back to Spark instead of failing inside
   * the native reader.
   *
   * Returns None when the unified type is computable, or Some(reason) when it is not -- either
   * the specs conflict or the reflection call itself failed. Both mean Comet cannot safely serve
   * `_partition`, so both map to a fallback.
   */
  def validateUnifiedPartitionType(table: Any): Option[String] = {
    try {
      val tableClass = loadClass(ClassNames.TABLE)
      val partitioningClass = loadClass(ClassNames.PARTITIONING)
      getMethod(partitioningClass, "partitionType", tableClass)
        .invoke(null, table.asInstanceOf[AnyRef])
      None
    } catch {
      // A conflict surfaces as the ValidationException thrown by partitionType(), wrapped by
      // reflection in InvocationTargetException; unwrap it for a meaningful reason.
      case e: java.lang.reflect.InvocationTargetException =>
        Some(Option(e.getCause).getOrElse(e).getMessage)
      case e: Exception =>
        Some(e.getMessage)
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
      val operations = getDeclaredMethod(table.getClass, "operations").invoke(table)

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
        val metadataFileLocationMethod = getMethod(metadata.getClass, "metadataFileLocation")
        // Option(...) not Some(...): a brand-new table (CTAS/RTAS before the first commit) has
        // no metadata file yet and the reflected value is null.
        Option(metadataFileLocationMethod.invoke(metadata).asInstanceOf[String])
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
        val propertiesMethod = getMethod(metadata.getClass, "properties")
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
    val deletesMethod = getMethod(fileScanTaskClass, "deletes")
    val deletes = deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
    if (deletes == null) new java.util.ArrayList[Any]() else deletes
  }

  /**
   * Gets equality field IDs from a delete file.
   *
   * @param deleteFileClass
   *   The DeleteFile interface, which callers in a loop already hold
   * @param deleteFile
   *   An Iceberg DeleteFile object
   * @return
   *   List of field IDs used in equality deletes, or empty list for position deletes
   *
   * Empty means either `equalityFieldIds()` isn't declared, or it returned `null` (Iceberg's
   * normal contract for a position-delete file). A genuine invoke failure propagates instead of
   * collapsing into empty.
   */
  def getEqualityFieldIds(deleteFileClass: Class[_], deleteFile: Any): java.util.List[_] =
    findMethod(deleteFileClass, "equalityFieldIds") match {
      case None => new java.util.ArrayList[Any]()
      case Some(method) =>
        val ids = method.invoke(deleteFile).asInstanceOf[java.util.List[_]]
        if (ids == null) new java.util.ArrayList[Any]() else ids
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
      val findFieldMethod = getMethod(schema.getClass, "findField", classOf[Int])
      val field = findFieldMethod.invoke(schema, fieldId.asInstanceOf[AnyRef])
      if (field != null) {
        val nameMethod = getMethod(field.getClass, "name")
        val typeMethod = getMethod(field.getClass, "type")
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
      val columnsMethod = getMethod(schema.getClass, "columns")
      val columns = columnsMethod.invoke(schema).asInstanceOf[java.util.List[_]]

      columns.asScala.flatMap { column =>
        try {
          val nameMethod = getMethod(column.getClass, "name")
          val name = nameMethod.invoke(column).asInstanceOf[String]

          val fieldIdMethod = getMethod(column.getClass, "fieldId")
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
   * Top-level column names whose Iceberg type iceberg-rust's page-index evaluator cannot prune
   * over, so callers must not push a residual predicate on them, not even the IS NOT NULL that
   * Iceberg adds for every filtered column. Two physical layouts fail (page_index_evaluator.rs):
   *   - FIXED_LEN_BYTE_ARRAY (decimal, uuid, fixed): rejected outright as an unsupported index
   *     type, which fails the native scan.
   *   - BYTE_ARRAY backing a binary column: the evaluator decodes column-index min/max as UTF-8
   *     (String::from_utf8(..).unwrap()) before the predicate closure runs, so non-UTF-8 bounds
   *     panic the native scan even for a bare IS [NOT] NULL. Extend this set as Iceberg adds
   *     types with either layout (e.g. geometry).
   */
  def pageIndexUnsupportedColumns(schema: Any): Set[String] = {
    import scala.jdk.CollectionConverters._
    try {
      val columns = getMethod(schema.getClass, "columns")
        .invoke(schema)
        .asInstanceOf[java.util.List[_]]
      columns.asScala.flatMap { column =>
        val name = getMethod(column.getClass, "name").invoke(column).asInstanceOf[String]
        val typeStr = getMethod(column.getClass, "type").invoke(column).toString
        if (typeStr.startsWith("decimal(") || typeStr == "uuid" || typeStr.startsWith("fixed[") ||
          typeStr == "binary") {
          Some(name)
        } else {
          None
        }
      }.toSet
    } catch {
      case e: Exception =>
        logWarning(
          s"Failed to inspect schema for page-index-unsupported columns: ${e.getMessage}")
        Set.empty[String]
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

    val fieldsMethod = getMethod(partitionSpec.getClass, "fields")
    val fields = fieldsMethod.invoke(partitionSpec).asInstanceOf[java.util.List[_]]

    val partitionFieldClass = loadClass(ClassNames.PARTITION_FIELD)
    val sourceIdMethod = getMethod(partitionFieldClass, "sourceId")
    val findFieldMethod = getMethod(schema.getClass, "findField", classOf[Int])

    val unsupportedTypes = scala.collection.mutable.ListBuffer[(String, String, String)]()

    fields.asScala.foreach { field =>
      val sourceId = sourceIdMethod.invoke(field).asInstanceOf[Int]
      val column = findFieldMethod.invoke(schema, sourceId.asInstanceOf[Object])

      if (column != null) {
        val nameMethod = getMethod(column.getClass, "name")
        val fieldName = nameMethod.invoke(column).asInstanceOf[String]

        val typeMethod = getMethod(column.getClass, "type")
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
   * Returns the names of schema columns (including nested struct/list/map fields) that declare a
   * V3 initial-default value. iceberg-rust does not synthesize default values for columns absent
   * from a data file, so reads projecting such columns must fall back. Throws on reflection
   * failure so the caller can fall back rather than risk a native crash.
   */
  def columnsWithInitialDefault(schema: Any): List[String] = {
    import scala.jdk.CollectionConverters._
    val columns =
      getMethod(schema.getClass, "columns").invoke(schema).asInstanceOf[java.util.List[_]]
    columns.asScala.flatMap(walkFieldForDefault).toList
  }

  private def walkFieldForDefault(field: Any): List[String] = {
    import scala.jdk.CollectionConverters._
    val name = getMethod(field.getClass, "name").invoke(field).asInstanceOf[String]
    val here =
      if (getMethod(field.getClass, "initialDefault").invoke(field) != null) List(name) else Nil
    val fieldType = getMethod(field.getClass, "type").invoke(field)
    val nested =
      if (getMethod(fieldType.getClass, "isNestedType").invoke(fieldType).asInstanceOf[Boolean]) {
        val nestedType = getMethod(fieldType.getClass, "asNestedType").invoke(fieldType)
        val fields =
          getMethod(nestedType.getClass, "fields")
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
    val convert = getMethod(sparkSchemaUtil, "convert", schemaClass)
    convert
      .invoke(null, schema.asInstanceOf[AnyRef])
      .asInstanceOf[org.apache.spark.sql.types.StructType]
  }

  /**
   * The configured AES data-key length in bytes for an encrypted table (Iceberg's
   * `encryption.data-key-length`, default 16), or None if the table is not encrypted. Comet's
   * native Parquet reader supports 128-bit (16-byte) and 256-bit (32-byte) keys but not 192-bit
   * (the underlying crypto has no AES-192-GCM), so callers fall back for anything else. Throws on
   * reflection failure so the caller can fall back.
   */
  def encryptionDataKeyLength(table: Any): Option[Int] =
    getTableProperties(table).filter(_.containsKey("encryption.key-id")).map { props =>
      Option(props.get("encryption.data-key-length")).map(_.toInt).getOrElse(16)
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

  def getTableFromSparkWrite(sparkWrite: Any): Option[Any] =
    getSparkWriteField(sparkWrite, "table")

  def getWritePropertiesFromSparkWrite(sparkWrite: Any): Option[Map[String, String]] = {
    import scala.jdk.CollectionConverters._
    getSparkWriteField(sparkWrite, "writeProperties")
      .map(_.asInstanceOf[java.util.Map[String, String]].asScala.toMap)
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

  def tablePropertyConstant(fieldName: String): String =
    readTablePropertiesField(fieldName).asInstanceOf[String]

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

  def getDataLocation(table: Any): Option[String] =
    try {
      val locationProviderMethod =
        findMethodInHierarchy(table.getClass, "locationProvider").getOrElse(
          throw new NoSuchMethodException(
            s"locationProvider() not found on ${table.getClass.getName}"))
      val provider = locationProviderMethod.invoke(table)
      val newDataLocMethod = provider.getClass.getMethod("newDataLocation", classOf[String])
      newDataLocMethod.setAccessible(true)
      val location = newDataLocMethod.invoke(provider, "").asInstanceOf[String]
      Some(location.stripSuffix("/"))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: Failed to get data location: ${e.getMessage}", e)
        None
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
   * Finds the first field -- nested struct/list/map fields included -- whose Iceberg type's
   * `TypeID` name is in `typeIds` (e.g. `Set("UUID")`), returning its `(name, typeId)`. Used by
   * write detection to decline schemas containing types the native writer cannot reproduce.
   * Reflection failures are deliberately not swallowed: the caller's detection wrapper turns them
   * into a fall-back, so a failed walk reads as "cannot verify" rather than "supported".
   */
  def findFieldWithTypeIds(schema: Any, typeIds: Set[String]): Option[(String, String)] = {
    val queue = new java.util.ArrayDeque[AnyRef]()
    queue.addAll(
      schema.getClass.getMethod("columns").invoke(schema).asInstanceOf[java.util.List[AnyRef]])
    while (!queue.isEmpty) {
      val field = queue.poll()
      val fieldType = field.getClass.getMethod("type").invoke(field)
      val typeId = fieldType.getClass.getMethod("typeId").invoke(fieldType)
      val typeIdName = typeId.asInstanceOf[Enum[_]].name()
      if (typeIds.contains(typeIdName)) {
        val name = field.getClass.getMethod("name").invoke(field).asInstanceOf[String]
        return Some((name, typeIdName))
      }
      val isNested =
        fieldType.getClass.getMethod("isNestedType").invoke(fieldType).asInstanceOf[Boolean]
      if (isNested) {
        val nested = fieldType.getClass.getMethod("asNestedType").invoke(fieldType)
        queue.addAll(
          nested.getClass
            .getMethod("fields")
            .invoke(nested)
            .asInstanceOf[java.util.List[AnyRef]])
      }
    }
    None
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
   * Looks up a `SortOrder` from `Table.sortOrders()` by its id. Used to recover the sort order
   * the write was planned against (`outputSortOrderId`) so it can be re-applied to the decoded
   * `DataFile`s through the public `DataFiles.Builder.withSortOrder` -- iceberg-rust's writer
   * doesn't expose the field, so the manifest comes back with `sort_order_id` unset. `SortOrder`
   * is `Serializable`, so the result can ship in a task closure.
   *
   * Id 0 falls back to `SortOrder.unsorted()` when absent from the map: a write whose
   * `outputSortOrderId` resolves to unsorted (no ordering required) may run against a table whose
   * metadata only records its non-trivial sort orders.
   */
  def getSortOrderById(table: Any, sortOrderId: Int): Option[AnyRef] =
    try {
      val method = table.getClass.getMethod("sortOrders")
      val orders = method.invoke(table).asInstanceOf[java.util.Map[java.lang.Integer, AnyRef]]
      Option(orders.get(java.lang.Integer.valueOf(sortOrderId))).orElse {
        if (sortOrderId == 0) {
          val sortOrderClass = loadClass("org.apache.iceberg.SortOrder")
          Some(sortOrderClass.getMethod("unsorted").invoke(null))
        } else {
          None
        }
      }
    } catch {
      case e: Exception =>
        logError(
          "Iceberg reflection failure: Failed to look up sort order " +
            s"$sortOrderId: ${e.getMessage}")
        None
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
   * Eagerly resolves every class, method, and constructor the executor-side commit-message
   * assembly reflects on (`decodeManifestToDataFiles`, `rebuildDataFilesWithJavaMetrics`,
   * `sumDataFileMetrics`, `buildTaskCommit`). That code runs after iceberg-rust has already
   * written the task's data files, so a reflection miss there is a task failure; probing the full
   * surface from the eligibility gate turns an Iceberg release that moves any of it into a
   * plan-time fallback instead. Executors share the driver's classpath, so driver-side resolution
   * is representative.
   *
   * Memoized per Iceberg class loader, not per JVM: dynamic classpath changes (`ADD JAR`,
   * REPL-style `--jars` additions) can shift what `loadClass` resolves to mid-run, and a result
   * cached against the old Iceberg -- in either direction -- would be wrong for the new one. The
   * loader of the resolved `ContentFile` class identifies the Iceberg the probe ran against.
   *
   * Returns a description of the first unresolvable member, or `None` when the whole executor
   * surface resolves.
   */
  def executorReflectionUnresolved: Option[String] = {
    val loader =
      try {
        loadClass(ClassNames.CONTENT_FILE).getClassLoader
      } catch {
        case NonFatal(e) =>
          return Some(s"Iceberg is not on the classpath: ${e.getMessage}")
      }
    executorReflectionProbeCache.get() match {
      case Some((cachedLoader, result)) if cachedLoader eq loader => result
      case _ =>
        val result = probeExecutorReflection()
        executorReflectionProbeCache.set(Some((loader, result)))
        result
    }
  }

  private val executorReflectionProbeCache =
    new java.util.concurrent.atomic.AtomicReference[Option[(ClassLoader, Option[String])]](None)

  private def probeExecutorReflection(): Option[String] =
    try {
      // decodeManifestToDataFiles
      val inMemoryFileIO = loadClass(ClassNames.INMEMORY_FILE_IO)
      inMemoryFileIO.getDeclaredConstructor()
      inMemoryFileIO.getMethod("addFile", classOf[String], classOf[Array[Byte]])
      loadClass(ClassNames.INMEMORY_INPUT_FILE)
        .getConstructor(classOf[String], classOf[Array[Byte]])
      val inputFileClass = loadClass(ClassNames.INPUT_FILE)
      val genericManifestFile = loadClass(ClassNames.GENERIC_MANIFEST_FILE)
      try {
        genericManifestFile.getDeclaredConstructor(inputFileClass, classOf[Int], classOf[Long])
      } catch {
        case _: NoSuchMethodException =>
          genericManifestFile.getDeclaredConstructor(inputFileClass, classOf[Int])
      }
      val fileIOClass = loadClass(ClassNames.FILE_IO)
      val manifestReadMethod = loadClass(ClassNames.MANIFEST_FILES)
        .getMethod("read", loadClass(ClassNames.MANIFEST_FILE), fileIOClass)
      manifestReadMethod.getReturnType.getMethod("iterator")
      manifestReadMethod.getReturnType.getMethod("close")
      val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
      contentFileClass.getMethod("copy")

      // sumDataFileMetrics
      val dataFileClass = loadClass(ClassNames.DATA_FILE)
      dataFileClass.getMethod("recordCount")
      dataFileClass.getMethod("fileSizeInBytes")

      // rebuildDataFilesWithJavaMetrics
      if (findMethodInHierarchy(contentFileClass, "location").isEmpty) {
        getMethod(contentFileClass, "path")
      }
      fileIOClass.getMethod("newInputFile", classOf[String], classOf[Long])
      // readParquetFooter, incl. the shading-sensitive ParquetFileReader derivation
      val parquet = resolveParquetFooterReflection()
      parquet.parquetFileReaderOpen.getReturnType.getMethod("getFooter")
      val builderClass = loadClass("org.apache.iceberg.DataFiles")
        .getMethod("builder", loadClass(ClassNames.PARTITION_SPEC))
        .getReturnType
      builderClass.getMethod("copy", dataFileClass)
      builderClass.getMethod("withSortOrder", loadClass("org.apache.iceberg.SortOrder"))
      builderClass.getMethod("withMetrics", parquet.footerMetrics.getReturnType)
      builderClass.getMethod("build")

      // buildFloatFieldMetrics
      Seq("valueCounts", "nullValueCounts", "nanValueCounts", "lowerBounds", "upperBounds")
        .foreach(contentFileClass.getMethod(_))
      loadClass(ClassNames.SCHEMA).getMethod("findType", classOf[Int])
      val typeClass = loadClass("org.apache.iceberg.types.Type")
      typeClass.getMethod("typeId")
      loadClass("org.apache.iceberg.types.Conversions")
        .getMethod("fromByteBuffer", typeClass, classOf[java.nio.ByteBuffer])
      loadClass("org.apache.iceberg.FieldMetrics").getConstructor(
        classOf[Int],
        classOf[Long],
        classOf[Long],
        classOf[Long],
        classOf[Object],
        classOf[Object])

      // buildTaskCommit
      loadClass("org.apache.iceberg.spark.source.SparkWrite$TaskCommit")
        .getDeclaredConstructor(java.lang.reflect.Array.newInstance(dataFileClass, 0).getClass)

      None
    } catch {
      case NonFatal(e) =>
        Some(
          s"executor-side Iceberg reflection did not resolve: ${e.getClass.getSimpleName}: " +
            s"${e.getMessage}")
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

  /**
   * Best-effort deletion of `locations` through the table's `FileIO`, for a task that failed
   * after iceberg-rust had already written them (the JVM-side counterpart of iceberg-java's
   * `SparkCleanupUtil.deleteTaskFiles`). Uses `SupportsBulkOperations.deleteFiles` when the
   * `FileIO` offers it and `FileIO.deleteFile(String)` per path otherwise. Never throws: the
   * original task failure must stay the one Spark reports. Returns the number of locations
   * deleted, or handed to the bulk delete. `context` identifies the task in the log lines.
   */
  def deleteFilesQuietly(io: AnyRef, locations: Seq[String], context: String): Int = {
    import scala.jdk.CollectionConverters._
    if (locations.isEmpty) return 0
    val deleted = findMethod(io.getClass, "deleteFiles", classOf[java.lang.Iterable[_]]) match {
      case Some(bulkDelete) =>
        try {
          bulkDelete.invoke(io, locations.asJava)
          locations.size
        } catch {
          case NonFatal(e) =>
            logWarning(s"Bulk delete of ${locations.size} data file(s) failed ($context)", e)
            0
        }
      case None =>
        findMethod(io.getClass, "deleteFile", classOf[String]) match {
          case None =>
            logWarning(
              s"FileIO ${io.getClass.getName} has no deleteFile(String); leaving " +
                s"${locations.size} data file(s) for remove_orphan_files ($context)")
            0
          case Some(deleteFile) =>
            locations.count { location =>
              try {
                deleteFile.invoke(io, location)
                true
              } catch {
                case NonFatal(e) =>
                  logWarning(s"Failed to delete data file $location ($context)", e)
                  false
              }
            }
        }
    }
    logInfo(s"Deleted $deleted of ${locations.size} data file(s) ($context)")
    deleted
  }

  /**
   * The locations of the data files carried by a `SparkWrite$TaskCommit` message (its
   * package-private `files()`), or empty when `message` is not one. Used to clean up after a
   * write job that failed before any commit was attempted.
   */
  def taskCommitFileLocations(message: AnyRef): Seq[String] =
    findMethodInHierarchy(message.getClass, "files") match {
      case Some(files) =>
        files.invoke(message) match {
          case array: Array[_] => array.toSeq.flatMap(f => extractFileLocation(f))
          case _ => Seq.empty
        }
      case None => Seq.empty
    }

  /** The table's `FileIO` (`table.io()`). Iceberg requires `FileIO` to be `Serializable`. */
  def getTableIO(table: Any): Option[AnyRef] =
    findMethodInHierarchy(table.getClass, "io").map(_.invoke(table))

  /**
   * `MetricsConfig.forTable(table)` -- the same resolved config (metrics modes, sorted-column
   * promotion, inferred-column cap) iceberg-java's own writer consults. `Serializable`.
   */
  def metricsConfigForTable(table: Any): Option[AnyRef] =
    try {
      val cls = loadClass("org.apache.iceberg.MetricsConfig")
      val method = cls.getMethod("forTable", loadClass(ClassNames.TABLE))
      Some(method.invoke(null, table.asInstanceOf[AnyRef]))
    } catch {
      case e: Exception =>
        logError(s"Iceberg reflection failure: MetricsConfig.forTable: ${e.getMessage}")
        None
    }

  /**
   * Replace each native-written `DataFile`'s metrics with the metrics iceberg-java's own writer
   * would have committed, so manifest metadata is Java's decision by construction.
   *
   * The base metrics come from re-reading the written parquet footer through the version-matched
   * `ParquetUtil.footerMetrics(footer, fieldMetrics, metricsConfig, nameMapping)` -- the exact
   * entry point `SparkWrite`'s parquet writer uses. Java sources float/double bounds and NaN
   * counts from writer-tracked `FieldMetrics` rather than the footer, so we synthesise one
   * `FieldMetrics` per float/double leaf from the counts and bounds the iceberg-rust writer
   * recorded on the incoming `DataFile` (rust tracks NaN counts per batch; its bounds are the
   * untruncated footer values, which are NaN-free by parquet-rs construction). Version-specific
   * decisions -- metrics modes, truncate(N) bound adjustment, list/map bound suppression, the
   * inferred-column cap -- all run inside the linked Iceberg's own code.
   *
   * Rebuilding runs through `DataFiles.builder(spec).copy(file)`, which also gives us a public
   * seam to re-apply the write's `SortOrder` (`withSortOrder`) -- iceberg-rust's writer doesn't
   * expose the field, so decoded files carry `sort_order_id` unset. `sortOrder` may be `null`
   * (unresolvable), in which case the copied value is kept.
   *
   * Runs on executors; every argument must be `Serializable` (`FileIO`, `MetricsConfig`,
   * `PartitionSpec`, `Schema`, and `SortOrder` all are). Any failure is a task failure: at this
   * point the plan is committed to the native path and silently keeping rust-computed metrics
   * could change query results via manifest pruning.
   */
  def rebuildDataFilesWithJavaMetrics(
      dataFiles: java.util.List[AnyRef],
      io: AnyRef,
      metricsConfig: AnyRef,
      spec: AnyRef,
      schema: AnyRef,
      sortOrder: AnyRef): java.util.List[AnyRef] = {
    if (dataFiles.isEmpty) return dataFiles
    val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
    val dataFileClass = loadClass(ClassNames.DATA_FILE)
    // location() replaced the deprecated path() in newer Iceberg releases; prefer it.
    val pathMethod = findMethodInHierarchy(contentFileClass, "location")
      .getOrElse(getMethod(contentFileClass, "path"))
    val sizeMethod = getMethod(contentFileClass, "fileSizeInBytes")
    val newInputFile =
      loadClass(ClassNames.FILE_IO).getMethod("newInputFile", classOf[String], classOf[Long])
    val builderFactory =
      loadClass("org.apache.iceberg.DataFiles")
        .getMethod("builder", loadClass(ClassNames.PARTITION_SPEC))
    val parquet = resolveParquetFooterReflection()

    val result = new java.util.ArrayList[AnyRef](dataFiles.size())
    val it = dataFiles.iterator()
    while (it.hasNext) {
      val dataFile = it.next().asInstanceOf[AnyRef]
      val path = pathMethod.invoke(dataFile).toString
      val length = sizeMethod.invoke(dataFile).asInstanceOf[java.lang.Long]
      val inputFile = newInputFile.invoke(io, path, length)
      val footer = readParquetFooter(parquet, inputFile)
      val fieldMetrics = buildFloatFieldMetrics(dataFile, schema)
      val metrics = parquet.footerMetrics.invoke(null, footer, fieldMetrics, metricsConfig, null)
      val builder = builderFactory.invoke(null, spec)
      val copied = getMethod(builder.getClass, "copy", dataFileClass).invoke(builder, dataFile)
      if (sortOrder != null) {
        getMethod(copied.getClass, "withSortOrder", loadClass("org.apache.iceberg.SortOrder"))
          .invoke(copied, sortOrder)
      }
      val withMetrics = getMethod(copied.getClass, "withMetrics", metrics.getClass)
        .invoke(copied, metrics)
      val built = getMethod(withMetrics.getClass, "build").invoke(withMetrics)
      result.add(built.asInstanceOf[AnyRef])
    }
    result
  }

  /**
   * The three parquet-mr-bridging members the footer re-read depends on. Resolved together, per
   * call, rather than memoized in JVM-lifetime lazy vals: `executorReflectionUnresolved` keys its
   * verdict on the current Iceberg class loader, and a probe re-run after a classpath change must
   * not be satisfied by `Method`s resolved against a previous Iceberg. Resolution is a per-task
   * cost (once per `rebuildDataFilesWithJavaMetrics` call), not per-file.
   */
  private case class ParquetFooterReflection(
      footerMetrics: Method,
      parquetIOFile: Method,
      parquetFileReaderOpen: Method)

  private def resolveParquetFooterReflection(): ParquetFooterReflection = {
    // `ParquetUtil.footerMetrics(ParquetMetadata, Stream[FieldMetrics], MetricsConfig,
    // NameMapping)`. In `iceberg-spark-runtime` jars parquet-mr is shaded, so the
    // `ParquetMetadata` parameter type is the relocated class -- resolved from the method itself
    // rather than by name.
    val footerMetrics = loadClass("org.apache.iceberg.parquet.ParquetUtil").getMethods
      .filter(_.getName == "footerMetrics")
      .find(_.getParameterCount == 4)
      .getOrElse(throw new IllegalStateException(
        "ParquetUtil.footerMetrics(footer, fieldMetrics, metricsConfig, nameMapping) not found"))
    // `ParquetIO.file` bridges Iceberg's `InputFile` to the same (possibly shaded) parquet-mr
    // classes `footerMetrics` expects; it is package-private, hence `setAccessible`.
    val parquetIOFile = loadClass("org.apache.iceberg.parquet.ParquetIO")
      .getDeclaredMethod("file", loadClass(ClassNames.INPUT_FILE))
    parquetIOFile.setAccessible(true)
    val parquetMetadataClass = footerMetrics.getParameterTypes()(0)
    val readerClass = loadClass(
      parquetMetadataClass.getName
        .replace("hadoop.metadata.ParquetMetadata", "hadoop.ParquetFileReader"))
    val parquetFileReaderOpen = readerClass.getMethod("open", parquetIOFile.getReturnType)
    ParquetFooterReflection(footerMetrics, parquetIOFile, parquetFileReaderOpen)
  }

  private def readParquetFooter(parquet: ParquetFooterReflection, inputFile: AnyRef): AnyRef = {
    val parquetInputFile = parquet.parquetIOFile.invoke(null, inputFile)
    val reader = parquet.parquetFileReaderOpen.invoke(null, parquetInputFile)
    try {
      getMethod(reader.getClass, "getFooter").invoke(reader)
    } finally {
      reader.asInstanceOf[java.io.Closeable].close()
    }
  }

  /**
   * One synthesised `FieldMetrics` per float/double leaf column, mirroring what the JVM writer's
   * `FloatFieldMetrics`/`DoubleFieldMetrics` would have tracked. Counts and bounds come from the
   * rust-written `DataFile`: value/null counts are footer-derived on both sides, NaN counts are
   * rust writer-tracked, and bounds are the untruncated footer min/max (NaN-skipping like Java;
   * parquet-rs additionally normalises zero bounds to -0.0/+0.0, a strictly conservative widening
   * documented as an accepted divergence). An all-NaN column has no footer bounds, so both bound
   * arguments are null -- exactly the shape `FloatFieldMetrics.build()` produces.
   */
  private def buildFloatFieldMetrics(dataFile: AnyRef, schema: AnyRef): AnyRef = {
    val contentFileClass = loadClass(ClassNames.CONTENT_FILE)
    def intLongMap(method: String): java.util.Map[Integer, java.lang.Long] = {
      val value = getMethod(contentFileClass, method).invoke(dataFile)
      if (value == null) java.util.Collections.emptyMap()
      else value.asInstanceOf[java.util.Map[Integer, java.lang.Long]]
    }
    def boundsMap(method: String): java.util.Map[Integer, java.nio.ByteBuffer] = {
      val value = getMethod(contentFileClass, method).invoke(dataFile)
      if (value == null) java.util.Collections.emptyMap()
      else value.asInstanceOf[java.util.Map[Integer, java.nio.ByteBuffer]]
    }
    val valueCounts = intLongMap("valueCounts")
    val nullCounts = intLongMap("nullValueCounts")
    val nanCounts = intLongMap("nanValueCounts")
    val lowerBounds = boundsMap("lowerBounds")
    val upperBounds = boundsMap("upperBounds")

    val schemaClass = loadClass(ClassNames.SCHEMA)
    val findType = getMethod(schemaClass, "findType", classOf[Int])
    val conversions = loadClass("org.apache.iceberg.types.Conversions")
    val typeClass = loadClass("org.apache.iceberg.types.Type")
    val fromByteBuffer =
      conversions.getMethod("fromByteBuffer", typeClass, classOf[java.nio.ByteBuffer])
    // Resolve by exact parameter types: Iceberg 1.10+ declares a SECOND public 6-arg
    // constructor `(int, long, long, T, T, Type)`, so arity alone is ambiguous and can bind
    // the NaN count to a bound argument.
    val fieldMetricsCtor = loadClass("org.apache.iceberg.FieldMetrics").getConstructor(
      classOf[Int],
      classOf[Long],
      classOf[Long],
      classOf[Long],
      classOf[Object],
      classOf[Object])

    val fieldMetrics = new java.util.ArrayList[AnyRef]()
    val ids = valueCounts.keySet().iterator()
    while (ids.hasNext) {
      val id = ids.next()
      val fieldType = findType.invoke(schema, Integer.valueOf(id.intValue()))
      val typeName =
        if (fieldType == null) ""
        else getMethod(fieldType.getClass, "typeId").invoke(fieldType).toString
      if (typeName == "FLOAT" || typeName == "DOUBLE") {
        def bound(map: java.util.Map[Integer, java.nio.ByteBuffer]): AnyRef =
          Option(map.get(id))
            .map(buf => fromByteBuffer.invoke(null, fieldType, buf))
            .orNull
        val nan = Option(nanCounts.get(id)).getOrElse(java.lang.Long.valueOf(0L))
        fieldMetrics.add(
          fieldMetricsCtor
            .newInstance(
              Integer.valueOf(id.intValue()),
              valueCounts.get(id),
              Option(nullCounts.get(id)).getOrElse(java.lang.Long.valueOf(0L)),
              nan,
              bound(lowerBounds),
              bound(upperBounds))
            .asInstanceOf[AnyRef])
      }
    }
    fieldMetrics.stream().asInstanceOf[AnyRef]
  }

  /**
   * Extracts the Spark V2 catalog name from an Iceberg `Table`. `Table.name()` returns
   * `catalog.namespace.table` for tables loaded through a catalog; we intersect against the
   * registered V2 catalogs so a value like `s3.foo` is not mistaken for a catalog `s3`. Returns
   * `None` for HadoopTables loaded by raw path or when reflection fails.
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
      IcebergReflection.getMethod(table.getClass, "name").invoke(table) match {
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

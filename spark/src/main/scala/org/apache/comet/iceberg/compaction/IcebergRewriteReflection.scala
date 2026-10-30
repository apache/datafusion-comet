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

package org.apache.comet.iceberg.compaction

import java.lang.reflect.{InvocationTargetException, Method}
import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.iceberg.IcebergReflection.{getMethod, loadClass}

/**
 * Everything the compaction calls on Iceberg, reached by reflection because Comet does not link
 * Iceberg. Each call is one a version of iceberg-java's own `RewriteDataFilesSparkAction` and
 * `RewriteDataFilesCommitManager` make, so it exists in every Iceberg release the Spark profile
 * that uses it runs against.
 *
 * The objects passed around are Iceberg's: a `Table`, `DataFile`s, `FileScanTask`s and an
 * `Expression`. They are typed `AnyRef` here.
 */
private[compaction] object IcebergRewriteReflection extends Logging {

  private val TABLE = "org.apache.iceberg.Table"
  private val SNAPSHOT = "org.apache.iceberg.Snapshot"
  private val PARTITION_SPEC = "org.apache.iceberg.PartitionSpec"
  private val FILE_SCAN_TASK = "org.apache.iceberg.FileScanTask"
  private val SCAN_TASK = "org.apache.iceberg.ScanTask"
  private val SPLITTABLE_SCAN_TASK = "org.apache.iceberg.SplittableScanTask"
  private val DELETE_FILE = "org.apache.iceberg.DeleteFile"
  private val CONTENT_FILE = "org.apache.iceberg.ContentFile"
  private val STRUCT_LIKE = "org.apache.iceberg.StructLike"
  private val TABLE_SCAN = "org.apache.iceberg.TableScan"
  private val SCAN = "org.apache.iceberg.Scan"
  private val EXPRESSION = "org.apache.iceberg.expressions.Expression"
  private val REWRITE_FILES = "org.apache.iceberg.RewriteFiles"
  private val PENDING_UPDATE = "org.apache.iceberg.PendingUpdate"
  private val FILE_IO = "org.apache.iceberg.io.FileIO"
  private val CONTENT_FILE_UTIL = "org.apache.iceberg.util.ContentFileUtil"
  private val TABLE_SCAN_UTIL = "org.apache.iceberg.util.TableScanUtil"
  private val SCAN_TASK_SET_MANAGER = "org.apache.iceberg.spark.ScanTaskSetManager"
  private val SPARK_TABLE_CACHE = "org.apache.iceberg.spark.SparkTableCache"
  private val FILE_REWRITE_COORDINATOR = "org.apache.iceberg.spark.FileRewriteCoordinator"
  private val SPARK_3_UTIL = "org.apache.iceberg.spark.Spark3Util"
  private val SPARK_TABLE = "org.apache.iceberg.spark.source.SparkTable"
  private val EXPRESSION_CONVERTER =
    "org.apache.spark.sql.execution.datasources.SparkExpressionConverter"

  private def boxedLong(value: Long): AnyRef = java.lang.Long.valueOf(value)

  /** Invokes `method`, and throws what the callee threw instead of the reflection wrapper. */
  private def call(method: Method, target: AnyRef, args: AnyRef*): AnyRef =
    try method.invoke(target, args: _*)
    catch {
      case e: InvocationTargetException if e.getCause != null => throw e.getCause
    }

  private def longResult(value: AnyRef): Long = value.asInstanceOf[java.lang.Long].longValue()

  private def intResult(value: AnyRef): Int = value.asInstanceOf[java.lang.Integer].intValue()

  // -- Table facts ------------------------------------------------------------------------

  def isSparkTable(table: Any): Boolean =
    loadClass(SPARK_TABLE).isInstance(table)

  /** The Iceberg `Table` behind a Spark `SparkTable`. */
  def icebergTable(sparkTable: AnyRef): AnyRef =
    call(getMethod(loadClass(SPARK_TABLE), "table"), sparkTable)

  def tableName(table: AnyRef): String =
    call(getMethod(loadClass(TABLE), "name"), table).asInstanceOf[String]

  def tableProperties(table: AnyRef): Map[String, String] =
    call(getMethod(loadClass(TABLE), "properties"), table)
      .asInstanceOf[java.util.Map[String, String]]
      .asScala
      .toMap

  /** The id of the table's current snapshot, or None for a table that has no data yet. */
  def currentSnapshotId(table: AnyRef): Option[Long] = {
    val snapshot = call(getMethod(loadClass(TABLE), "currentSnapshot"), table)
    if (snapshot == null) {
      None
    } else {
      Some(longResult(call(getMethod(loadClass(SNAPSHOT), "snapshotId"), snapshot)))
    }
  }

  def snapshotSequenceNumber(table: AnyRef, snapshotId: Long): Long = {
    val snapshot =
      call(
        getMethod(loadClass(TABLE), "snapshot", java.lang.Long.TYPE),
        table,
        boxedLong(snapshotId))
    longResult(call(getMethod(loadClass(SNAPSHOT), "sequenceNumber"), snapshot))
  }

  private def currentSpec(table: AnyRef): AnyRef =
    call(getMethod(loadClass(TABLE), "spec"), table)

  def currentSpecId(table: AnyRef): Int =
    intResult(call(getMethod(loadClass(PARTITION_SPEC), "specId"), currentSpec(table)))

  private def currentSpecFieldCount(table: AnyRef): Int =
    call(getMethod(loadClass(PARTITION_SPEC), "fields"), currentSpec(table))
      .asInstanceOf[java.util.List[_]]
      .size()

  // -- Reading a call's arguments -----------------------------------------------------------

  /**
   * Resolves the `table` argument against the procedure's catalog, as Iceberg's `ProcedureInput`
   * does, and returns the identifier when the table belongs to that catalog.
   */
  def resolveIdentifier(
      session: SparkSession,
      catalog: AnyRef,
      tableArgument: String): Option[AnyRef] = {
    val util = loadClass(SPARK_3_UTIL)
    val method = util.getMethods
      .find(m => m.getName == "catalogAndIdentifier" && m.getParameterCount == 4)
      .getOrElse(throw new NoSuchMethodException(s"$SPARK_3_UTIL.catalogAndIdentifier/4"))
    val resolved =
      call(method, null, "identifier for parameter 'table'", session, tableArgument, catalog)
    val resolvedCatalog = call(getMethod(resolved.getClass, "catalog"), resolved)
    if (resolvedCatalog == catalog) {
      Some(call(getMethod(resolved.getClass, "identifier"), resolved))
    } else {
      None
    }
  }

  /** Converts the `where` argument to an Iceberg expression, as Iceberg's procedure does. */
  def whereExpression(
      session: SparkSession,
      catalogName: String,
      identifier: AnyRef,
      where: String): AnyRef = {
    val util = loadClass(SPARK_3_UTIL)
    val quote = util.getMethods
      .find(m => m.getName == "quotedFullIdentifier" && m.getParameterCount == 2)
      .getOrElse(throw new NoSuchMethodException(s"$SPARK_3_UTIL.quotedFullIdentifier/2"))
    val name = call(quote, null, catalogName, identifier).asInstanceOf[String]
    val converter = loadClass(EXPRESSION_CONVERTER)
    val collect = converter.getMethods
      .find(m => m.getName == "collectResolvedSparkExpression" && m.getParameterCount == 3)
      .getOrElse(throw new NoSuchMethodException(s"$EXPRESSION_CONVERTER.collect/3"))
    val convert = converter.getMethods
      .find(m => m.getName == "convertToIcebergExpression" && m.getParameterCount == 1)
      .getOrElse(throw new NoSuchMethodException(s"$EXPRESSION_CONVERTER.convert/1"))
    call(convert, null, call(collect, null, session, name, where))
  }

  // -- Planning ----------------------------------------------------------------------------

  /**
   * Plans the table's data files the way iceberg-java's `BinPackRewriteFilePlanner` does: a scan
   * of the starting snapshot with the call's filter, residuals ignored, so that a file is
   * selected whole whenever any of its rows may match.
   */
  def planFiles(
      session: SparkSession,
      table: AnyRef,
      snapshotId: Long,
      filter: Option[AnyRef],
      currentSpecId: Int): Seq[CompactionFile] = {
    val scanClass = loadClass(SCAN)
    val expressionClass = loadClass(EXPRESSION)
    var scan = call(getMethod(loadClass(TABLE), "newScan"), table)
    filter.foreach { expression =>
      scan = call(getMethod(scanClass, "filter", expressionClass), scan, expression)
    }
    val caseSensitive =
      java.lang.Boolean.parseBoolean(session.conf.get("spark.sql.caseSensitive"))
    scan = call(
      getMethod(scanClass, "caseSensitive", java.lang.Boolean.TYPE),
      scan,
      java.lang.Boolean.valueOf(caseSensitive))
    scan = call(getMethod(scanClass, "ignoreResiduals"), scan)
    scan = call(
      getMethod(loadClass(TABLE_SCAN), "useSnapshot", java.lang.Long.TYPE),
      scan,
      boxedLong(snapshotId))

    val tasks = call(getMethod(scanClass, "planFiles"), scan)
    try {
      val reader = new FileFacts(currentSpecId, currentSpecFieldCount(table))
      tasks.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map(reader.read).toVector
    } finally {
      tasks.asInstanceOf[java.io.Closeable].close()
    }
  }

  /** Reads the planner's facts off scan tasks, holding the reflective handles it needs. */
  private class FileFacts(currentSpecId: Int, currentSpecFieldCount: Int) {
    private val taskClass = loadClass(FILE_SCAN_TASK)
    private val contentFileClass = loadClass(CONTENT_FILE)
    private val structClass = loadClass(STRUCT_LIKE)
    private val fileOf = getMethod(taskClass, "file")
    private val lengthOf = getMethod(taskClass, "length")
    private val deletesOf = getMethod(taskClass, "deletes")
    private val recordCountOf = getMethod(contentFileClass, "recordCount")
    private val specIdOf = getMethod(contentFileClass, "specId")
    private val partitionOf = getMethod(contentFileClass, "partition")
    private val structSize = getMethod(structClass, "size")
    private val structGet =
      getMethod(structClass, "get", java.lang.Integer.TYPE, classOf[java.lang.Class[_]])
    private val isFileScoped =
      getMethod(loadClass(CONTENT_FILE_UTIL), "isFileScoped", loadClass(DELETE_FILE))

    def read(task: AnyRef): CompactionFile = {
      val file = call(fileOf, task)
      val specId = intResult(call(specIdOf, file))
      // Files of an older spec cannot be told apart by their partition values, so Iceberg
      // groups them together under an all-NULL key.
      val partition =
        if (specId == currentSpecId) partitionValues(call(partitionOf, file))
        else Seq.fill[Any](currentSpecFieldCount)(null)
      val deletes = call(deletesOf, task).asInstanceOf[java.util.List[AnyRef]]
      var fileScopedDeletedRows = 0L
      if (deletes != null) {
        deletes.asScala.foreach { delete =>
          if (call(isFileScoped, null, delete).asInstanceOf[java.lang.Boolean]) {
            fileScopedDeletedRows += longResult(call(recordCountOf, delete))
          }
        }
      }
      CompactionFile(
        task = task,
        length = longResult(call(lengthOf, task)),
        recordCount = longResult(call(recordCountOf, file)),
        specId = specId,
        partition = partition,
        deleteFileCount = if (deletes == null) 0 else deletes.size(),
        fileScopedDeletedRows = fileScopedDeletedRows)
    }

    private def partitionValues(partition: AnyRef): Seq[Any] = {
      val size = intResult(call(structSize, partition))
      (0 until size).map { i =>
        normalize(call(structGet, partition, Integer.valueOf(i), classOf[java.lang.Object]))
      }
    }

    /** Gives partition values the equality Iceberg's own partition comparator has. */
    private def normalize(value: AnyRef): Any = value match {
      case chars: CharSequence => chars.toString
      case bytes: ByteBuffer =>
        val copy = new Array[Byte](bytes.remaining())
        bytes.duplicate().get(copy)
        copy.toSeq
      case other => other
    }
  }

  // -- Read tasks ----------------------------------------------------------------------------

  /** A task a Spark task reads, with the weight Iceberg packs it by. */
  final case class ReadTask(task: AnyRef, sizeBytes: Long, filesCount: Int)

  /**
   * Splits large files into read tasks of about `splitSize`, as Iceberg does when it plans the
   * read of a staged group, and reads the weight of each. A Parquet file splits at its row group
   * boundaries. A task for a file smaller than the split size stays whole.
   */
  def splitTasks(tasks: Seq[AnyRef], splitSize: Long): Seq[ReadTask] = {
    val splittableClass = loadClass(SPLITTABLE_SCAN_TASK)
    val split = getMethod(splittableClass, "split", java.lang.Long.TYPE)
    val scanTaskClass = loadClass(SCAN_TASK)
    val sizeBytes = getMethod(scanTaskClass, "sizeBytes")
    val filesCount = getMethod(scanTaskClass, "filesCount")
    def read(task: AnyRef): ReadTask =
      ReadTask(task, longResult(call(sizeBytes, task)), intResult(call(filesCount, task)))
    tasks.flatMap { task =>
      if (splittableClass.isInstance(task)) {
        call(split, task, boxedLong(splitSize))
          .asInstanceOf[java.lang.Iterable[AnyRef]]
          .asScala
          .map(read)
          .toVector
      } else {
        Seq(read(task))
      }
    }
  }

  /**
   * Merges the tasks of a bin that read neighboring ranges of one file into a single task, as
   * `TableScanUtil.planTaskGroups` does for each task group it plans.
   */
  def mergeTasks(tasks: Seq[AnyRef]): Seq[AnyRef] = {
    val merge = getMethod(loadClass(TABLE_SCAN_UTIL), "mergeTasks", classOf[java.util.List[_]])
    call(merge, null, new java.util.ArrayList[AnyRef](tasks.asJava))
      .asInstanceOf[java.util.List[AnyRef]]
      .asScala
      .toVector
  }

  // -- Running a group -----------------------------------------------------------------------

  /**
   * Registers the table under the id a rewrite's write finds it by. A rewrite names its table by
   * an id, which Iceberg's cached table catalog resolves.
   */
  def cacheTable(table: AnyRef, id: String): Unit = {
    val cacheClass = loadClass(SPARK_TABLE_CACHE)
    val cache = call(getMethod(cacheClass, "get"), null)
    call(getMethod(cacheClass, "add", classOf[String], loadClass(TABLE)), cache, id, table)
    ()
  }

  /** Registers a group's tasks and the table under the id the group's reads and writes use. */
  def stage(table: AnyRef, groupId: String, tasks: java.util.List[AnyRef]): Unit = {
    cacheTable(table, groupId)
    val manager = call(getMethod(loadClass(SCAN_TASK_SET_MANAGER), "get"), null)
    call(
      getMethod(
        loadClass(SCAN_TASK_SET_MANAGER),
        "stageTasks",
        loadClass(TABLE),
        classOf[String],
        classOf[java.util.List[_]]),
      manager,
      table,
      groupId,
      tasks)
    ()
  }

  /**
   * Releases everything [[stage]] and the group's write registered. It runs in a `finally`, so it
   * must not replace the failure that got the group there.
   */
  def release(table: AnyRef, groupId: String): Unit = {
    try {
      val tableClass = loadClass(TABLE)
      val cacheClass = loadClass(SPARK_TABLE_CACHE)
      val cache = call(getMethod(cacheClass, "get"), null)
      val manager = call(getMethod(loadClass(SCAN_TASK_SET_MANAGER), "get"), null)
      val coordinator = call(getMethod(loadClass(FILE_REWRITE_COORDINATOR), "get"), null)
      call(getMethod(cacheClass, "remove", classOf[String]), cache, groupId)
      call(
        getMethod(loadClass(SCAN_TASK_SET_MANAGER), "removeTasks", tableClass, classOf[String]),
        manager,
        table,
        groupId)
      call(
        getMethod(
          loadClass(FILE_REWRITE_COORDINATOR),
          "clearRewrite",
          tableClass,
          classOf[String]),
        coordinator,
        table,
        groupId)
      ()
    } catch {
      case e: Exception => logWarning(s"Failed to release the staged rewrite $groupId", e)
    }
  }

  /** The data files a group's write produced. They are not part of the table yet. */
  def newFiles(table: AnyRef, groupId: String): Seq[AnyRef] = {
    val coordinatorClass = loadClass(FILE_REWRITE_COORDINATOR)
    val coordinator = call(getMethod(coordinatorClass, "get"), null)
    val files = call(
      getMethod(coordinatorClass, "fetchNewFiles", loadClass(TABLE), classOf[String]),
      coordinator,
      table,
      groupId)
    files.asInstanceOf[java.util.Set[AnyRef]].asScala.toVector
  }

  /** The deletion vectors of a group's tasks, which are dangling once the group is rewritten. */
  def deletionVectors(tasks: Seq[AnyRef]): Seq[AnyRef] = {
    val deletesOf = getMethod(loadClass(FILE_SCAN_TASK), "deletes")
    val isDV = getMethod(loadClass(CONTENT_FILE_UTIL), "isDV", loadClass(DELETE_FILE))
    tasks.flatMap { task =>
      val deletes = call(deletesOf, task).asInstanceOf[java.util.List[AnyRef]]
      if (deletes == null) Nil
      else deletes.asScala.filter(d => call(isDV, null, d).asInstanceOf[java.lang.Boolean])
    }
  }

  def dataFileOf(task: AnyRef): AnyRef =
    call(getMethod(loadClass(FILE_SCAN_TASK), "file"), task)

  def fileLocation(file: AnyRef): String =
    IcebergReflection
      .extractFileLocation(file)
      .getOrElse(throw new IllegalStateException(s"Cannot read the location of $file"))

  // -- Committing ----------------------------------------------------------------------------

  /**
   * Commits a rewrite, the way `RewriteDataFilesCommitManager.commitFileGroups` does: replace the
   * rewritten data files with the added ones, drop the deletion vectors that targeted them, and
   * validate against the snapshot the planning read so that a concurrent change to the rewritten
   * files fails the commit.
   */
  def commitRewrite(
      table: AnyRef,
      startingSnapshotId: Long,
      useStartingSequenceNumber: Boolean,
      rewrittenDataFiles: Seq[AnyRef],
      addedDataFiles: Seq[AnyRef],
      danglingDeletionVectors: Seq[AnyRef]): Unit = {
    val rewriteClass = loadClass(REWRITE_FILES)
    var rewrite = call(getMethod(loadClass(TABLE), "newRewrite"), table)
    rewrite = call(
      getMethod(rewriteClass, "validateFromSnapshot", java.lang.Long.TYPE),
      rewrite,
      boxedLong(startingSnapshotId))
    if (useStartingSequenceNumber) {
      val sequenceNumber = snapshotSequenceNumber(table, startingSnapshotId)
      rewrite = call(
        getMethod(rewriteClass, "dataSequenceNumber", java.lang.Long.TYPE),
        rewrite,
        boxedLong(sequenceNumber))
    }
    val setClass = classOf[java.util.Set[_]]
    call(
      getMethod(rewriteClass, "rewriteFiles", setClass, setClass, setClass, setClass),
      rewrite,
      uniqueDataFiles(rewrittenDataFiles),
      uniqueDeleteFiles(danglingDeletionVectors),
      uniqueDataFiles(addedDataFiles),
      new java.util.LinkedHashSet[AnyRef]())
    call(getMethod(loadClass(PENDING_UPDATE), "commit"), rewrite)
    ()
  }

  /**
   * Keeps the first of the files that Iceberg's `DataFileSet` treats as one: data files with the
   * same location.
   */
  private def uniqueDataFiles(files: Seq[AnyRef]): java.util.Set[AnyRef] =
    uniqueBy(files)(fileLocation)

  /**
   * Keeps the first of the files that Iceberg's `DeleteFileSet` treats as one. That is the same
   * location, offset and length, because the deletion vectors of many data files can share one
   * Puffin file and differ only by where in it they are.
   */
  private def uniqueDeleteFiles(files: Seq[AnyRef]): java.util.Set[AnyRef] =
    uniqueBy(files)(deleteFileKey)

  private def uniqueBy(files: Seq[AnyRef])(key: AnyRef => String): java.util.Set[AnyRef] = {
    val byKey = new java.util.LinkedHashMap[String, AnyRef]()
    files.foreach(f => byKey.putIfAbsent(key(f), f))
    new java.util.LinkedHashSet[AnyRef](byKey.values())
  }

  /** The identity `DeleteFileSet` gives a delete file, as a string. */
  def deleteFileKey(file: AnyRef): String = {
    val deleteFileClass = loadClass(DELETE_FILE)
    def optional(name: String): Any =
      IcebergReflection.findMethod(deleteFileClass, name).map(m => call(m, file)).orNull
    s"${fileLocation(file)}|${optional("contentOffset")}|${optional("contentSizeInBytes")}"
  }

  /** How many of the delete files are distinct, by the identity of `DeleteFileSet`. */
  def distinctDeleteFiles(files: Seq[AnyRef]): Int = files.map(deleteFileKey).distinct.size

  /** How many of the data files are distinct, by the identity of `DataFileSet`. */
  def distinctDataFiles(files: Seq[AnyRef]): Int = files.map(fileLocation).distinct.size

  /** Deletes files a failed or abandoned rewrite wrote. Failures are logged, never thrown. */
  def deleteFiles(table: AnyRef, files: Seq[AnyRef]): Unit = {
    if (files.nonEmpty) {
      try {
        val io = call(getMethod(loadClass(TABLE), "io"), table)
        val delete = getMethod(loadClass(FILE_IO), "deleteFile", classOf[String])
        files.foreach { file =>
          try call(delete, io, fileLocation(file))
          catch {
            case e: Exception => logWarning(s"Failed to delete rewritten file $file", e)
          }
        }
      } catch {
        case e: Exception => logWarning("Failed to clean up files from a failed rewrite", e)
      }
    }
  }

  private def isInstance(className: String, e: Throwable): Boolean =
    try loadClass(className).isInstance(e)
    catch { case _: ClassNotFoundException => false }

  /** A commit that may have succeeded, so the files it names must be kept. */
  def isCommitStateUnknown(e: Throwable): Boolean =
    isInstance("org.apache.iceberg.exceptions.CommitStateUnknownException", e)

  /** A failure after which the files of the failed commit are safe to delete. */
  def isCleanableFailure(e: Throwable): Boolean =
    isInstance("org.apache.iceberg.exceptions.CleanableFailure", e)

  /** A conflict with a concurrent operation. */
  def isCommitConflict(e: Throwable): Boolean =
    isInstance("org.apache.iceberg.exceptions.ValidationException", e) ||
      isInstance("org.apache.iceberg.exceptions.CommitFailedException", e)
}

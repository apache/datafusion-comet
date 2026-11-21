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

import java.lang.reflect.{Constructor, InvocationTargetException, Method}

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.apache.comet.iceberg.IcebergReflection.{getMethod, loadClass}

/**
 * Everything the compaction calls on Iceberg, reached by reflection because Comet does not link
 * Iceberg. The planning and the commit are the classes iceberg-java's own
 * `RewriteDataFilesSparkAction` uses, `BinPackRewriteFilePlanner` and
 * `RewriteDataFilesCommitManager`, so a call plans and commits what Iceberg's procedure would,
 * with the defaults and checks of the Iceberg release on the classpath. Each class and method
 * used here exists in Iceberg 1.10 and later, the releases of the Spark 4 profiles.
 *
 * The objects passed around are Iceberg's: a `Table`, `RewriteFileGroup`s, `FileScanTask`s and an
 * `Expression`. They are typed `AnyRef` here.
 */
private[compaction] object IcebergRewriteReflection extends Logging {

  private val TABLE = "org.apache.iceberg.Table"
  private val SNAPSHOT = "org.apache.iceberg.Snapshot"
  private val PARTITION_SPEC = "org.apache.iceberg.PartitionSpec"
  private val FILE_SCAN_TASK = "org.apache.iceberg.FileScanTask"
  private val SCAN_TASK_GROUP = "org.apache.iceberg.ScanTaskGroup"
  private val EXPRESSION = "org.apache.iceberg.expressions.Expression"
  private val EXPRESSIONS = "org.apache.iceberg.expressions.Expressions"
  private val TABLE_SCAN_UTIL = "org.apache.iceberg.util.TableScanUtil"
  private val BIN_PACK_PLANNER = "org.apache.iceberg.actions.BinPackRewriteFilePlanner"
  private val FILE_REWRITE_PLAN = "org.apache.iceberg.actions.FileRewritePlan"
  private val REWRITE_FILE_GROUP = "org.apache.iceberg.actions.RewriteFileGroup"
  private val GROUP_RESULT = "org.apache.iceberg.actions.RewriteDataFiles$FileGroupRewriteResult"
  private val COMMIT_MANAGER = "org.apache.iceberg.actions.RewriteDataFilesCommitManager"
  private val DANGLING_DELETES = "org.apache.iceberg.actions.RemoveDanglingDeleteFiles"
  private val DANGLING_DELETES_RESULT =
    "org.apache.iceberg.actions.RemoveDanglingDeleteFiles$Result"
  private val SPARK_ACTIONS = "org.apache.iceberg.spark.actions.SparkActions"
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

  /** Invokes `constructor` the way [[call]] invokes a method. */
  private def construct(constructor: Constructor[_], args: AnyRef*): AnyRef =
    try constructor.newInstance(args: _*).asInstanceOf[AnyRef]
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

  /** The branch a `SparkTable` was loaded for, such as `branch_audit` in its identifier. */
  def sparkTableBranch(sparkTable: AnyRef): Option[String] =
    Option(call(getMethod(loadClass(SPARK_TABLE), "branch"), sparkTable).asInstanceOf[String])

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
   * A file group Iceberg's planner chose, with the facts Comet reads off it once.
   *
   * @param handle
   *   Iceberg's `RewriteFileGroup`, which the commit takes
   * @param tasks
   *   the group's `FileScanTask`s, one per data file
   * @param inputSplitSize
   *   how many bytes one read task takes from the group
   * @param maxOutputFileSize
   *   the size the writer rolls files at
   * @param outputSpecId
   *   the partition spec the new files are written with
   * @param inputFileNum
   *   the number of data files the group rewrites
   */
  final case class RewriteGroup(
      handle: AnyRef,
      tasks: java.util.List[AnyRef],
      inputSplitSize: Long,
      maxOutputFileSize: Long,
      outputSpecId: Int,
      inputFileNum: Int)

  /**
   * Iceberg's plan: the groups to rewrite, in job order, and the number of groups before
   * `max-files-to-rewrite` trimmed them, which sizes the commits of partial progress.
   */
  final case class RewritePlan(groups: Seq[RewriteGroup], totalGroupCount: Int)

  /**
   * Iceberg's bin-pack planner for `snapshotId`, made the way `RewriteDataFilesSparkAction` makes
   * it. A missing filter is `Expressions.alwaysTrue()`, Iceberg's default.
   */
  def newPlanner(
      table: AnyRef,
      filter: Option[AnyRef],
      snapshotId: Long,
      caseSensitive: Boolean): AnyRef = {
    val expression =
      filter.getOrElse(call(getMethod(loadClass(EXPRESSIONS), "alwaysTrue"), null))
    val constructor = loadClass(BIN_PACK_PLANNER).getConstructor(
      loadClass(TABLE),
      loadClass(EXPRESSION),
      classOf[java.lang.Long],
      java.lang.Boolean.TYPE)
    construct(
      constructor,
      table,
      expression,
      boxedLong(snapshotId),
      java.lang.Boolean.valueOf(caseSensitive))
  }

  /** The options the planner reads, which depend on the Iceberg release. */
  def plannerOptions(planner: AnyRef): Set[String] =
    call(getMethod(loadClass(BIN_PACK_PLANNER), "validOptions"), planner)
      .asInstanceOf[java.util.Set[String]]
      .asScala
      .toSet

  /**
   * Plans the rewrite. The planner checks the values of its options first and throws an
   * `IllegalArgumentException` for one Iceberg rejects.
   */
  def plan(planner: AnyRef, options: Map[String, String]): RewritePlan = {
    val plannerClass = loadClass(BIN_PACK_PLANNER)
    call(getMethod(plannerClass, "init", classOf[java.util.Map[_, _]]), planner, options.asJava)
    val plan = call(getMethod(plannerClass, "plan"), planner)
    val planClass = loadClass(FILE_REWRITE_PLAN)
    val groups = call(getMethod(planClass, "groups"), plan)
    val handles =
      try groups.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.toVector
      finally groups.asInstanceOf[java.io.Closeable].close()
    RewritePlan(
      handles.map(rewriteGroup),
      intResult(call(getMethod(planClass, "totalGroupCount"), plan)))
  }

  private def rewriteGroup(handle: AnyRef): RewriteGroup = {
    val groupClass = loadClass(REWRITE_FILE_GROUP)
    def get(name: String): AnyRef = call(getMethod(groupClass, name), handle)
    RewriteGroup(
      handle = handle,
      tasks = get("fileScanTasks").asInstanceOf[java.util.List[AnyRef]],
      inputSplitSize = longResult(get("inputSplitSize")),
      maxOutputFileSize = longResult(get("maxOutputFileSize")),
      outputSpecId = intResult(get("outputSpecId")),
      inputFileNum = intResult(get("inputFileNum")))
  }

  /**
   * Whether every file of the group is written in the group's output spec. A group with a file of
   * another spec has to be repartitioned when it is written.
   */
  def inOutputSpec(group: RewriteGroup): Boolean = {
    val specOf = getMethod(loadClass(FILE_SCAN_TASK), "spec")
    val specId = getMethod(loadClass(PARTITION_SPEC), "specId")
    group.tasks.asScala.forall { task =>
      intResult(call(specId, call(specOf, task))) == group.outputSpecId
    }
  }

  /**
   * The bins of a group: its tasks split and packed into Spark partitions of about `splitSize` by
   * `TableScanUtil.planTaskGroups`, which is how Iceberg's staged scan plans the read of a group
   * in Iceberg's own rewrite.
   */
  def planTaskGroups(
      tasks: java.util.List[AnyRef],
      splitSize: Long,
      lookback: Int,
      openFileCost: Long): Seq[java.util.List[AnyRef]] = {
    val method = getMethod(
      loadClass(TABLE_SCAN_UTIL),
      "planTaskGroups",
      classOf[java.util.List[_]],
      java.lang.Long.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Long.TYPE)
    val taskGroups = call(
      method,
      null,
      tasks,
      boxedLong(splitSize),
      Integer.valueOf(lookback),
      boxedLong(openFileCost))
    val tasksOf = getMethod(loadClass(SCAN_TASK_GROUP), "tasks")
    taskGroups.asInstanceOf[java.util.List[AnyRef]].asScala.toVector.map { taskGroup =>
      new java.util.ArrayList[AnyRef](
        call(tasksOf, taskGroup).asInstanceOf[java.util.Collection[AnyRef]])
    }
  }

  // -- Running a job -----------------------------------------------------------------------

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

  /**
   * Releases what a job registered under `id`: the table, and the files its write staged. It runs
   * in a `finally`, so it must not replace the failure that got the job there.
   */
  def release(table: AnyRef, id: String): Unit = {
    try {
      val cacheClass = loadClass(SPARK_TABLE_CACHE)
      val cache = call(getMethod(cacheClass, "get"), null)
      call(getMethod(cacheClass, "remove", classOf[String]), cache, id)
      val coordinatorClass = loadClass(FILE_REWRITE_COORDINATOR)
      val coordinator = call(getMethod(coordinatorClass, "get"), null)
      call(
        getMethod(coordinatorClass, "clearRewrite", loadClass(TABLE), classOf[String]),
        coordinator,
        table,
        id)
      ()
    } catch {
      case e: Exception => logWarning(s"Failed to release the rewrite job $id", e)
    }
  }

  /** The data files a job's write produced. They are not part of the table yet. */
  def newFiles(table: AnyRef, id: String): java.util.Set[AnyRef] = {
    val coordinatorClass = loadClass(FILE_REWRITE_COORDINATOR)
    val coordinator = call(getMethod(coordinatorClass, "get"), null)
    call(
      getMethod(coordinatorClass, "fetchNewFiles", loadClass(TABLE), classOf[String]),
      coordinator,
      table,
      id).asInstanceOf[java.util.Set[AnyRef]]
  }

  /** Hands `group` the files written for it, which its commit adds to the table. */
  def setOutputFiles(group: RewriteGroup, files: java.util.Set[AnyRef]): Unit = {
    call(
      getMethod(loadClass(REWRITE_FILE_GROUP), "setOutputFiles", classOf[java.util.Set[_]]),
      group.handle,
      files)
    ()
  }

  // -- Committing --------------------------------------------------------------------------

  /** What Iceberg reports for a rewritten group, which the call's result row adds up. */
  final case class GroupResult(
      addedDataFiles: Int,
      rewrittenDataFiles: Int,
      rewrittenBytes: Long,
      removedDeleteFiles: Int)

  def groupResult(group: RewriteGroup): GroupResult = {
    val result = call(getMethod(loadClass(REWRITE_FILE_GROUP), "asResult"), group.handle)
    val resultClass = loadClass(GROUP_RESULT)
    def get(name: String): AnyRef = call(getMethod(resultClass, name), result)
    GroupResult(
      addedDataFiles = intResult(get("addedDataFilesCount")),
      rewrittenDataFiles = intResult(get("rewrittenDataFilesCount")),
      rewrittenBytes = longResult(get("rewrittenBytesCount")),
      removedDeleteFiles = intResult(get("removedDeleteFilesCount")))
  }

  /**
   * Iceberg's commit manager for a rewrite planned from `startingSnapshotId`. Its commits
   * validate against that snapshot, and optionally keep its data sequence number.
   */
  def commitManager(
      table: AnyRef,
      startingSnapshotId: Long,
      useStartingSequenceNumber: Boolean): AnyRef = {
    val constructor = loadClass(COMMIT_MANAGER).getConstructor(
      loadClass(TABLE),
      java.lang.Long.TYPE,
      java.lang.Boolean.TYPE,
      classOf[java.util.Map[_, _]])
    construct(
      constructor,
      table,
      boxedLong(startingSnapshotId),
      java.lang.Boolean.valueOf(useStartingSequenceNumber),
      java.util.Collections.emptyMap[String, String]())
  }

  /**
   * Commits `groups` as one `RewriteFiles` snapshot. When the commit fails in a way that cannot
   * have changed the table, Iceberg deletes the groups' new files before it rethrows.
   */
  def commitOrClean(manager: AnyRef, groups: Seq[RewriteGroup]): Unit = {
    call(
      getMethod(loadClass(COMMIT_MANAGER), "commitOrClean", classOf[java.util.Set[_]]),
      manager,
      new java.util.LinkedHashSet[AnyRef](groups.map(_.handle).asJava))
    ()
  }

  /** Deletes the files written for a group that will not be committed. Never throws. */
  def abortFileGroup(manager: AnyRef, group: RewriteGroup): Unit = {
    try {
      call(
        getMethod(loadClass(COMMIT_MANAGER), "abortFileGroup", loadClass(REWRITE_FILE_GROUP)),
        manager,
        group.handle)
      ()
    } catch {
      case e: Exception => logWarning(s"Failed to clean up the files of ${group.handle}", e)
    }
  }

  /**
   * Runs Iceberg's action that removes the delete files no live data file needs, which Iceberg's
   * rewrite runs after its commit when `remove-dangling-deletes` is set. Returns how many it
   * removed.
   */
  def removeDanglingDeletes(session: SparkSession, table: AnyRef): Int = {
    val actionsClass = loadClass(SPARK_ACTIONS)
    val actions = call(getMethod(actionsClass, "get", classOf[SparkSession]), null, session)
    val action =
      call(getMethod(actionsClass, "removeDanglingDeleteFiles", loadClass(TABLE)), actions, table)
    val result = call(getMethod(loadClass(DANGLING_DELETES), "execute"), action)
    call(getMethod(loadClass(DANGLING_DELETES_RESULT), "removedDeleteFiles"), result)
      .asInstanceOf[java.lang.Iterable[AnyRef]]
      .asScala
      .size
  }

  private def isInstance(className: String, e: Throwable): Boolean =
    try loadClass(className).isInstance(e)
    catch { case _: ClassNotFoundException => false }

  /** A conflict with a concurrent operation. */
  def isCommitConflict(e: Throwable): Boolean =
    isInstance("org.apache.iceberg.exceptions.ValidationException", e) ||
      isInstance("org.apache.iceberg.exceptions.CommitFailedException", e)
}

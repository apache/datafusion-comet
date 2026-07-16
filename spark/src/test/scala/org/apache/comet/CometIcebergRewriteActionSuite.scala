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

package org.apache.comet

import java.io.File

import scala.collection.mutable

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{CometTestBase, SparkSession}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart

import org.apache.comet.iceberg.IcebergReflection

/**
 * Verifies that Comet engages on the per-group reads issued by Iceberg's
 * RewriteDataFilesSparkAction. The action stages each rewrite group's FileScanTasks under a UUID
 * via ScanTaskSetManager and then reads them back through SparkStagedScan, which Comet's
 * CometScanRule recognises alongside SparkBatchQueryScan.
 */
class CometIcebergRewriteActionSuite extends CometTestBase with CometIcebergTestBase {

  private val catalog = "test_cat"
  private val MultiFileTableSize = 4

  test("binPack rewrite reads each file group via CometIcebergNativeScan") {
    runRewriteTest(
      RewriteCase(
        table = s"$catalog.db.binpack_test",
        configureMode = invoke(_, "binPack"),
        verifyPlans = rewritePlans => assertReadsAreComet(rewritePlans)))
  }

  test("sort rewrite runs scan, exchange, and sort natively in Comet") {
    runRewriteTest(
      RewriteCase(
        table = s"$catalog.db.sort_test",
        configureMode = invoke(_, "sort"),
        beforeRewrite = setSortOrderAsc(_, "id"),
        verifyDataAfter = assertSortedById,
        verifyPlans = { rewritePlans =>
          assertReadsAreComet(rewritePlans)
          assertOperator(rewritePlans, "CometExchange")
          assertOperator(rewritePlans, "CometSort")
        }))
  }

  // Single-column zOrder is bit-pattern-equivalent to a natural sort (no second dimension to
  // interleave with), so we expect the same ascending output as the sort test. Iceberg's
  // `INT_ORDERED_BYTES` / `INTERLEAVE_BYTES` are `ScalaUDF`s that route through Comet's codegen
  // dispatcher, so the project stays native and the shuffle picks `CometExchange` /
  // `CometNativeShuffle` rather than the columnar-row roundtrip path.
  test("single-column zOrder rewrite runs scan, native exchange, and sort natively in Comet") {
    runRewriteTest(
      RewriteCase(
        table = s"$catalog.db.zorder_test",
        configureMode = invoke(_, "zOrder", classOf[Array[String]] -> Array("id")),
        verifyDataAfter = assertSortedById,
        verifyPlans = { rewritePlans =>
          assertReadsAreComet(rewritePlans)
          assertOperator(rewritePlans, "CometExchange")
          assertOperator(rewritePlans, "CometSort")
        }))
  }

  test("RewritePositionDeleteFiles action does not convert any operators with Comet") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.position_delete_rewrite_test"
        try {
          // MOR table; produce some positional delete files for the action to consume.
          spark.sql(s"""
            CREATE TABLE $table (id INT, value DOUBLE) USING iceberg
            TBLPROPERTIES (
              'format-version' = '2',
              'write.delete.mode' = 'merge-on-read',
              'write.merge.mode' = 'merge-on-read'
            )
          """)
          spark
            .createDataFrame(Seq((1, 1.0), (2, 2.0), (3, 3.0)))
            .toDF("id", "value")
            .coalesce(1)
            .write
            .format("iceberg")
            .mode("append")
            .saveAsTable(table)
          spark.sql(s"DELETE FROM $table WHERE id = 1")
          spark.sql(s"DELETE FROM $table WHERE id = 2")

          val deleteCodes = deleteFileContentCodes(table)
          assert(
            deleteCodes.nonEmpty && deleteCodes.forall(_ == 1),
            s"Expected MOR positional delete files, got $deleteCodes")

          // Iceberg-spark-runtime-4.0 (used for the spark-4.1 and spark-4.2 profiles since no
          // 4.1+ runtime is published) calls DataSourceV2Relation.create with a signature that
          // Spark 4.1 removed (the 5th `timeTravelSpec` parameter was added). The action
          // therefore cannot run on those profiles; skip cleanly rather than fail. Tracked
          // upstream at https://github.com/apache/iceberg/issues/15238.
          val plans =
            try {
              captureSqlPlans {
                runActionChain(
                  table,
                  actionMethodName = "rewritePositionDeletes",
                  configure = identity,
                  options = Map("rewrite-all" -> "true"))
              }
            } catch {
              case e: java.lang.reflect.InvocationTargetException
                  if e.getCause.isInstanceOf[NoSuchMethodError] =>
                cancel(
                  "Iceberg's RewritePositionDeleteFiles action is incompatible with this " +
                    s"Spark/Iceberg combination: ${e.getCause.getMessage}")
            }

          // The action reads the POSITION_DELETES metadata table (different schema from the data
          // table) via SparkStagedScan. Comet must not convert any operator in those plans.
          val cometBearingPlans = plans.filter(_.containsCometOperator)
          assert(
            cometBearingPlans.isEmpty,
            "Expected no Comet operators in RewritePositionDeleteFiles plans, but found:\n" +
              dumpPlans(cometBearingPlans))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("binPack rewrite applies positional and equality deletes during compaction (MOR)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.deletes_compaction_test"
        try {
          spark.sql(s"""
            CREATE TABLE $table (id INT, value DOUBLE) USING iceberg
            TBLPROPERTIES (
              'format-version' = '2',
              'write.delete.mode' = 'merge-on-read',
              'write.merge.mode' = 'merge-on-read'
            )
          """)
          // coalesce(1) puts all 3 rows in one data file. Without it, Iceberg would split per
          // VALUES row and DELETE WHERE id=N would be a metadata-only file unlink — never
          // exercising the merge-on-read writer that produces positional delete files.
          spark
            .createDataFrame(Seq((1, 1.0), (2, 2.0), (3, 3.0)))
            .toDF("id", "value")
            .coalesce(1)
            .write
            .format("iceberg")
            .mode("append")
            .saveAsTable(table)

          // Spark DML writes a positional delete (content=1) for id=1.
          spark.sql(s"DELETE FROM $table WHERE id = 1")
          // Spark's Iceberg DSv2 connector never emits equality deletes, so we drop into the
          // Iceberg Java API to add an equality delete (content=2) for id=2.
          writeEqualityDeleteFile(loadIcebergTable(table), "id", 2)

          val rowsBefore = spark.sql(s"SELECT * FROM $table ORDER BY id").collect().toSeq
          assert(
            rowsBefore.size == 1 && rowsBefore.head.getInt(0) == 3,
            s"Expected only id=3 visible before rewrite, got $rowsBefore")
          val deleteCodesBefore = deleteFileContentCodes(table)
          assert(
            deleteCodesBefore == Seq(1, 2),
            "Expected one positional (1) and one equality (2) delete file, " +
              s"got $deleteCodesBefore")
          assert(
            sumDataFileRecords(table) == 3,
            s"Expected 3 records in data files before rewrite, got ${sumDataFileRecords(table)}")

          val plans = captureSqlPlans {
            val rewrittenCount =
              runRewriteDataFiles(table, invoke(_, "binPack"), Map("rewrite-all" -> "true"))
            assert(
              rewrittenCount >= 1,
              s"Expected >= 1 input file rewritten, got $rewrittenCount")
          }

          val rewritePlans = plans.filter(_.hasNode("AppendData"))
          assert(rewritePlans.nonEmpty, "Expected at least one rewrite plan")
          assertReadsAreComet(rewritePlans)

          val rowsAfter = spark.sql(s"SELECT * FROM $table ORDER BY id").collect().toSeq
          assert(
            rowsAfter == rowsBefore,
            s"Surviving rows changed across rewrite: before=$rowsBefore, after=$rowsAfter")
          // The strong check: post-rewrite data files physically contain exactly the surviving
          // rows. Without Comet correctly applying both positional AND equality deletes during
          // the rewrite read, the rewritten file would still contain the deleted rows and this
          // would be 3.
          assert(
            sumDataFileRecords(table) == 1,
            "Expected 1 record materialised in data files after rewrite, " +
              s"got ${sumDataFileRecords(table)}")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  // -- Test driver -----------------------------------------------------------

  private case class RewriteCase(
      table: String,
      configureMode: AnyRef => AnyRef,
      beforeRewrite: AnyRef => Unit = _ => (),
      verifyDataAfter: Seq[org.apache.spark.sql.Row] => Unit = _ => (),
      verifyPlans: Seq[CapturedPlan] => Unit)

  /**
   * End-to-end test driver: creates a multi-file Iceberg table, runs a RewriteDataFiles action in
   * the requested mode, captures the per-group SQL plans, and hands them to `verifyPlans`.
   */
  private def runRewriteTest(rc: RewriteCase): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        try {
          createMultiFileTable(rc.table, MultiFileTableSize)
          rc.beforeRewrite(loadIcebergTable(rc.table))

          val rowsBefore = spark.sql(s"SELECT * FROM ${rc.table} ORDER BY id").collect().toSeq
          val filesBefore = countDataFiles(rc.table)

          val plans = captureSqlPlans {
            val rewrittenCount = runRewriteDataFiles(rc.table, rc.configureMode)
            assert(
              rewrittenCount >= MultiFileTableSize,
              s"Expected the rewrite action to consume >= $MultiFileTableSize input files, " +
                s"got $rewrittenCount")
          }

          val rowsAfterById = spark.sql(s"SELECT * FROM ${rc.table} ORDER BY id").collect().toSeq
          val rowsAfterFileOrder = spark.sql(s"SELECT * FROM ${rc.table}").collect().toSeq
          val filesAfter = countDataFiles(rc.table)
          assertDataPreserved(rowsBefore, rowsAfterById, filesBefore, filesAfter)
          rc.verifyDataAfter(rowsAfterFileOrder)

          val rewritePlans = plans.filter(_.hasNode("AppendData"))
          assert(
            rewritePlans.nonEmpty,
            "Expected at least one captured plan with AppendData but got none.\n" +
              dumpPlans(plans))
          rc.verifyPlans(rewritePlans)
        } catch {
          case _: ClassNotFoundException =>
            cancel("Iceberg Actions API not available - requires iceberg-spark-runtime")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS ${rc.table}")
        }
      }
    }
  }

  // -- Plan capture ----------------------------------------------------------

  /**
   * One captured SQL execution: the human-readable plan string (for failure diagnostics) plus the
   * exact set of plan-node names from `SparkPlanInfo` (for typed assertions). We use
   * `SparkListener` rather than `QueryExecutionListener` because the rewrite action uses
   * `cloneSession()`, and `listenerManager` is per-session — only the SparkContext-level event
   * bus survives the clone.
   */
  private case class CapturedPlan(physicalDescription: String, nodeNames: Set[String]) {
    def hasNode(name: String): Boolean = nodeNames.contains(name)
    def containsCometOperator: Boolean = nodeNames.exists(_.startsWith("Comet"))
  }

  private def captureSqlPlans(body: => Unit): Seq[CapturedPlan] = {
    val plans = mutable.ArrayBuffer[CapturedPlan]()
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case s: SparkListenerSQLExecutionStart =>
          plans.synchronized {
            plans += CapturedPlan(s.physicalPlanDescription, collectNodeNames(s.sparkPlanInfo))
          }
        case _ =>
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    plans.synchronized { plans.toSeq }
  }

  private def collectNodeNames(info: SparkPlanInfo): Set[String] = {
    val acc = mutable.Set[String]()
    def walk(node: SparkPlanInfo): Unit = {
      acc += node.nodeName
      node.children.foreach(walk)
    }
    walk(info)
    acc.toSet
  }

  // -- Plan assertions -------------------------------------------------------

  private def assertReadsAreComet(plans: Seq[CapturedPlan]): Unit = {
    assertOperator(plans, "CometIcebergNativeScan")
    val withFallback =
      plans.count(p => p.hasNode("BatchScan") && !p.hasNode("CometIcebergNativeScan"))
    assert(
      withFallback == 0,
      s"$withFallback of ${plans.size} plans fell back to a non-Comet BatchScan.\n" +
        dumpPlans(plans))
  }

  private def assertOperator(plans: Seq[CapturedPlan], operator: String): Unit = {
    val matching = plans.count(_.hasNode(operator))
    assert(
      matching == plans.size,
      s"Expected every plan to contain $operator; only $matching of ${plans.size} did.\n" +
        dumpPlans(plans))
  }

  private def dumpPlans(plans: Seq[CapturedPlan]): String =
    plans.zipWithIndex
      .map { case (p, i) => s"--- plan $i ---\n${p.physicalDescription}" }
      .mkString("\n\n")

  /**
   * Verifies the rewrite preserved every row and reduced the data-file count. Both checks are
   * critical: a buggy native scan dropping rows or rewriting them incorrectly would still produce
   * a syntactically valid Iceberg snapshot, so plan-shape checks alone aren't sufficient.
   */
  private def assertDataPreserved(
      rowsBefore: Seq[org.apache.spark.sql.Row],
      rowsAfter: Seq[org.apache.spark.sql.Row],
      filesBefore: Long,
      filesAfter: Long): Unit = {
    assert(
      rowsAfter.size == rowsBefore.size,
      s"Row count changed across rewrite: before=${rowsBefore.size}, after=${rowsAfter.size}")
    assert(
      rowsAfter == rowsBefore,
      "Row content changed across rewrite (rows compared in id order).")
    assert(
      filesAfter < filesBefore,
      s"Expected data-file count to decrease across rewrite, got $filesBefore -> $filesAfter")
  }

  private def countDataFiles(table: String): Long =
    spark.sql(s"SELECT COUNT(*) FROM $table.files").collect()(0).getLong(0)

  /** Returns the sorted `content` codes of delete files: 1 = positional, 2 = equality. */
  private def deleteFileContentCodes(table: String): Seq[Int] =
    spark
      .sql(s"SELECT content FROM $table.delete_files ORDER BY content")
      .collect()
      .map(_.getInt(0))
      .toSeq

  /**
   * Total records across all data files (excluding delete files) in the current snapshot. Used to
   * verify whether deletes were physically materialised by the rewrite vs. only applied at read
   * time on a still-3-row data file.
   */
  private def sumDataFileRecords(table: String): Long =
    spark
      .sql(s"SELECT COALESCE(SUM(record_count), 0) FROM $table.data_files")
      .collect()(0)
      .getLong(0)

  /**
   * Asserts the given rows are sorted ascending by id when read in file order. Used by the sort
   * rewrite test to confirm the action actually produced sorted output, not just that a CometSort
   * appeared in the plan.
   */
  private def assertSortedById(rows: Seq[org.apache.spark.sql.Row]): Unit = {
    val ids = rows.map(_.getInt(0))
    assert(
      ids == ids.sorted,
      s"Expected rows to be sorted by id after sort rewrite, got first 20 ids: ${ids.take(20)}")
  }

  // -- Iceberg / Spark fixtures ---------------------------------------------

  private def withIcebergComet(warehouseDir: File)(body: => Unit): Unit =
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true")(body)

  /** Creates an Iceberg table with `numFiles` separate appends, each producing one data file. */
  private def createMultiFileTable(table: String, numFiles: Int): Unit = {
    spark.sql(s"CREATE TABLE $table (id INT, value DOUBLE) USING iceberg")
    (0 until numFiles).foreach { i =>
      spark
        .range(i * 100L, (i + 1) * 100L)
        .selectExpr("CAST(id AS INT) as id", "CAST(id * 1.5 AS DOUBLE) as value")
        .coalesce(1)
        .write
        .format("iceberg")
        .mode("append")
        .saveAsTable(table)
    }
  }

  // -- Iceberg reflection (the API isn't compile-time visible here) ----------

  private def loadIcebergTable(name: String): AnyRef =
    IcebergReflection
      .loadClass("org.apache.iceberg.spark.Spark3Util")
      .getMethod("loadIcebergTable", classOf[SparkSession], classOf[String])
      .invoke(null, spark, name)

  /** Sets a single ASC sort order on the table — required for `action.sort()` (no-args). */
  private def setSortOrderAsc(table: AnyRef, column: String): Unit = {
    val replace = invoke(table, "replaceSortOrder")
    val withAsc = invoke(replace, "asc", classOf[String] -> column)
    invoke(withAsc, "commit")
  }

  private def runRewriteDataFiles(
      table: String,
      configureMode: AnyRef => AnyRef,
      options: Map[String, String] = Map("min-input-files" -> "2")): Int = {
    val result = runActionChain(table, "rewriteDataFiles", configureMode, options)
    invoke(result, "rewrittenDataFilesCount").asInstanceOf[Int]
  }

  /**
   * Invokes `SparkActions.get(spark).<actionMethodName>(table)`, threads it through `configure`
   * (e.g. `binPack`/`sort`/`zOrder`), applies any extra options, and returns the action's
   * `.execute()` result. The result type depends on the action; callers cast / project as needed.
   */
  private def runActionChain(
      table: String,
      actionMethodName: String,
      configure: AnyRef => AnyRef,
      options: Map[String, String]): AnyRef = {
    val tableObj = loadIcebergTable(table)
    val tableIface = IcebergReflection.loadClass("org.apache.iceberg.Table")
    val actions =
      invoke(IcebergReflection.loadClass("org.apache.iceberg.spark.actions.SparkActions"), "get")
    val action = invoke(actions, actionMethodName, tableIface -> tableObj)
    val configured = configure(action)
    val withOptions = options.foldLeft(configured) { case (acc, (k, v)) =>
      invoke(acc, "option", classOf[String] -> k, classOf[String] -> v)
    }
    invoke(withOptions, "execute")
  }

  /**
   * Writes a single-column equality delete file via the Iceberg Java API and commits it to
   * `table`. Spark's Iceberg DSv2 connector never emits equality deletes from DML, so we have to
   * write them by hand to exercise the full delete surface during a rewrite read.
   *
   * Modelled on `writeEqDeleteRecord` in Iceberg's own
   * spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java
   * — same shape, but using the public `GenericAppenderFactory` API rather than the
   * package-private `GenericFileWriterFactory.Builder`, and expressed via reflection because
   * iceberg-data classes aren't compile-time visible from this suite.
   */
  private def writeEqualityDeleteFile(
      table: AnyRef,
      columnName: String,
      deleteValue: Any): Unit = {
    val schemaCls = IcebergReflection.loadClass("org.apache.iceberg.Schema")
    val tableIface = IcebergReflection.loadClass("org.apache.iceberg.Table")
    val partitionSpecCls = IcebergReflection.loadClass("org.apache.iceberg.PartitionSpec")
    val structLikeCls = IcebergReflection.loadClass("org.apache.iceberg.StructLike")
    val fileFormatCls = IcebergReflection.loadClass("org.apache.iceberg.FileFormat")
    val encryptedOutputFileCls =
      IcebergReflection.loadClass("org.apache.iceberg.encryption.EncryptedOutputFile")
    val deleteFileCls = IcebergReflection.loadClass("org.apache.iceberg.DeleteFile")
    val parquet = fileFormatCls.getField("PARQUET").get(null)

    val schema = invoke(table, "schema")
    val spec = invoke(table, "spec")
    val field = invoke(schema, "findField", classOf[String] -> columnName)
    val fieldId = invoke(field, "fieldId").asInstanceOf[Int]
    val eqDeleteRowSchema =
      invoke(schema, "select", classOf[Array[String]] -> Array(columnName))

    val genericRecord = invoke(
      IcebergReflection.loadClass("org.apache.iceberg.data.GenericRecord"),
      "create",
      schemaCls -> eqDeleteRowSchema)
    val record = invoke(
      genericRecord,
      "copy",
      classOf[java.util.Map[_, _]] ->
        java.util.Collections.singletonMap(columnName, deleteValue.asInstanceOf[AnyRef]))

    val outputFileFactoryCls =
      IcebergReflection.loadClass("org.apache.iceberg.io.OutputFileFactory")
    val builderForMethod = outputFileFactoryCls.getMethod(
      "builderFor",
      tableIface,
      java.lang.Integer.TYPE,
      java.lang.Long.TYPE)
    builderForMethod.setAccessible(true)
    val outputFileFactoryBuilder = builderForMethod.invoke(null, table, Int.box(1), Long.box(1L))
    val outputFileFactoryWithFormat =
      invoke(outputFileFactoryBuilder, "format", fileFormatCls -> parquet)
    val outputFileFactory = invoke(outputFileFactoryWithFormat, "build")
    val encryptedOutputFile = invoke(outputFileFactory, "newOutputFile")

    // GenericAppenderFactory(schema, spec, equalityFieldIds, eqDeleteRowSchema, posDeleteRowSchema)
    // is public in 1.5+; we use it instead of the package-private GenericFileWriterFactory.Builder.
    val factoryCls = IcebergReflection.loadClass("org.apache.iceberg.data.GenericAppenderFactory")
    val factoryCtor = factoryCls.getConstructor(
      schemaCls,
      partitionSpecCls,
      classOf[Array[Int]],
      schemaCls,
      schemaCls)
    factoryCtor.setAccessible(true)
    val factory = factoryCtor
      .newInstance(schema, spec, Array(fieldId), eqDeleteRowSchema, null)
      .asInstanceOf[AnyRef]

    val newEqDeleteWriter = factory.getClass.getMethod(
      "newEqDeleteWriter",
      encryptedOutputFileCls,
      fileFormatCls,
      structLikeCls)
    newEqDeleteWriter.setAccessible(true)
    val writer = newEqDeleteWriter.invoke(factory, encryptedOutputFile, parquet, null)

    // EqualityDeleteWriter.write(T) erases to write(Object) at runtime.
    val writeMethod = writer.getClass.getMethod("write", classOf[Object])
    writeMethod.setAccessible(true)
    try {
      writeMethod.invoke(writer, record)
    } finally {
      val closeMethod = writer.getClass.getMethod("close")
      closeMethod.setAccessible(true)
      closeMethod.invoke(writer)
    }
    val deleteFile = invoke(writer, "toDeleteFile")

    val rowDelta = invoke(table, "newRowDelta")
    val rowDeltaWithDelete = invoke(rowDelta, "addDeletes", deleteFileCls -> deleteFile)
    invoke(rowDeltaWithDelete, "commit")
  }

  /**
   * Reflective invocation. Pass an instance for instance methods, or a `Class[_]` (e.g. via
   * `IcebergReflection.loadClass`) for static methods. Each `(paramType, value)` pair specifies
   * one argument's declared type and value. setAccessible is forced because some Iceberg chain
   * methods (notably in pre-1.6 BaseSparkAction) live on package-private classes, which trigger
   * IllegalAccessException on JDK 11+ even for public methods unless reflective access is forced.
   */
  private def invoke(
      classOrTarget: AnyRef,
      methodName: String,
      args: (Class[_], Any)*): AnyRef = {
    val (clazz, target) = classOrTarget match {
      case c: Class[_] => (c, null)
      case t => (t.getClass, t)
    }
    val (paramTypes, values) = args.unzip
    val method = clazz.getMethod(methodName, paramTypes: _*)
    method.setAccessible(true)
    method.invoke(target, values.map(_.asInstanceOf[AnyRef]): _*)
  }
}

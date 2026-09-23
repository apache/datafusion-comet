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

package org.apache.comet.exec

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.comet.CometMergeRowsExec
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.{QueryExecution, ScalarSubquery}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

/**
 * `CometMergeRowsExec` converts Spark's `MergeRowsExec` and the `MergeRows` logical node that
 * feeds it, both defined in Spark core (`execution.datasources.v2` / `catalyst.plans.logical`),
 * not in any connector module. Spark plans a `MergeRowsExec` for MERGE INTO against any
 * `SupportsRowLevelOperations` V2 table using group-based (copy-on-write) planning, independent
 * of which connector implements the table.
 *
 * This suite pins that contract against Spark's own `InMemoryRowLevelOperationTableCatalog` test
 * catalog rather than Iceberg. `InMemoryRowLevelOperationTable` selects its write shape via the
 * `supports-deltas` table property: unset (default `false`) plans through group-based
 * `MergeRows`; `supports-deltas=true` plans a JVM `WriteDelta` whose child is *also* a
 * `MergeRowsExec` (and, with `split-updates=true`, emits `Split` for update-as-delete+insert).
 * Comet's bottom-up conversion makes that child eligible for native execution under the JVM
 * write, so both shapes are exercised here (`deltaMergeCase`) and checked against a pure-Spark
 * baseline. See `CometIcebergWriteActionSuite` for MERGE INTO coverage against real Iceberg
 * tables.
 */
abstract class CometMergeRowsNativeSuiteBase extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      // The test catalog's BatchScan is not a Comet-native scan. A broadcast join over these tiny
      // fixtures therefore leaves MergeRowsExec on the JVM instead of exercising this suite's
      // operator. Force the supported shuffle boundary while keeping AQE enabled.
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
  }

  private def assumeMerge(): Unit = assume(isSpark35Plus, "MergeRowsExec requires Spark 3.5+")

  test("MERGE all row-routing groups engage CometMergeRowsExec and match Spark") {
    assumeMerge()
    val target = s"$catalog.default.rowlevel_target"
    val source = s"$catalog.default.rowlevel_source"

    def resetTables(): Unit = {
      sql(s"DROP TABLE IF EXISTS $target")
      sql(s"DROP TABLE IF EXISTS $source")
      sql(s"CREATE TABLE $target (id INT, region STRING, amount DOUBLE) USING parquet")
      sql(s"CREATE TABLE $source (id INT, region STRING, amount DOUBLE) USING parquet")
      sql(
        s"INSERT INTO $target VALUES " +
          (0 until 20).map(i => s"($i, 'r${i % 3}', ${i * 1.5})").mkString(", "))
      sql(
        s"INSERT INTO $source VALUES " +
          (10 until 30).map(i => s"($i, 's${i % 3}', ${i * 2.0})").mkString(", "))
    }

    // Exercise all three MergeRows instruction groups in one end-to-end query: 10-19 are
    // MATCHED, 20-29 are NOT MATCHED, and 0-9 are NOT MATCHED BY SOURCE. The predicate on the
    // target-only clause also leaves 5-9 to Spark's generated catch-all Keep instruction.
    val mergeSql =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |WHEN NOT MATCHED BY SOURCE AND t.id < 5 THEN UPDATE SET t.amount = t.amount + 1000.0
         |""".stripMargin

    val captured = scala.collection.mutable.ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    spark.listenerManager.register(listener)
    try {
      resetTables()
      captured.clear()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
        sql(mergeSql)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      // Tree-string rendering uses Spark's stripped nodeName ("CometMergeRows"), not the Scala
      // class name ("CometMergeRowsExec").
      val engaged = captured.exists(_.executedPlan.toString.contains("CometMergeRows"))
      val cometResult =
        sql(s"SELECT id, region, amount FROM $target ORDER BY id").collect().map(_.toString)

      resetTables()
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sql(mergeSql)
      }
      val sparkResult =
        sql(s"SELECT id, region, amount FROM $target ORDER BY id").collect().map(_.toString)

      assert(
        engaged,
        "CometMergeRowsExec did not engage against a non-Iceberg SupportsRowLevelOperations " +
          "table")
      assert(
        cometResult.toSeq == sparkResult.toSeq,
        "native MergeRows output diverged from Spark's for a non-Iceberg source.\n" +
          s"comet: ${cometResult.mkString(", ")}\nspark: ${sparkResult.mkString(", ")}")
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("MERGE cardinality violation matches Spark's structured exception") {
    assumeMerge()
    val target = s"$catalog.default.rowlevel_target_card"
    val source = s"$catalog.default.rowlevel_source_card"

    // Two source rows both match target row id=1 -> MERGE_CARDINALITY_VIOLATION.
    sql(s"DROP TABLE IF EXISTS $target")
    sql(s"DROP TABLE IF EXISTS $source")
    sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
    sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
    sql(s"INSERT INTO $target VALUES (1, 10.0)")
    sql(s"INSERT INTO $source VALUES (1, 20.0), (1, 30.0)")

    val mergeSql =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
         |""".stripMargin

    // `withSQLConf`'s block result isn't usable directly: under the Spark 3.5 / Scala 2.12 build
    // its signature returns `Unit` regardless of the block's type (only the 4.x / Scala 2.13
    // build is generic), so `val x = withSQLConf(...) { expr }` silently infers `x: Unit` there.
    // Mutate a `var` from inside the block instead, which is portable across both.
    val failedExecutions = scala.collection.mutable.ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = ()
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        failedExecutions += qe
    }
    spark.listenerManager.register(listener)
    var cometEx: Throwable = null
    try {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
        cometEx = intercept[Exception](sql(mergeSql).collect())
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      assert(
        failedExecutions.exists(qe =>
          find(qe.executedPlan) { case _: CometMergeRowsExec => true; case _ => false }.nonEmpty),
        "cardinality-error coverage must execute through CometMergeRowsExec, not Spark fallback")
    } finally {
      spark.listenerManager.unregister(listener)
    }
    var sparkEx: Throwable = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkEx = intercept[Exception](sql(mergeSql).collect())
    }

    // Spark 3.5 returns SparkException here while Spark 4.x returns SparkRuntimeException. The
    // native path must follow the Spark version under test rather than hard-coding either class.
    assert(
      cometEx.getClass == sparkEx.getClass,
      "Comet's native MERGE cardinality violation must use Spark's baseline exception type " +
        s"${sparkEx.getClass.getName}, got ${cometEx.getClass.getName}: ${cometEx.getMessage}")

    // Spark 4.x exposes the condition directly while Spark 3.5 can wrap the structured failure in
    // a plain SparkException. Walk the cause chain first, then fall back to Spark's canonical
    // bracketed condition token when a wrapper does not implement SparkThrowable itself.
    val expectedCondition = "MERGE_CARDINALITY_VIOLATION"
    def errorCondition(ex: Throwable): String = {
      Iterator
        .iterate(ex)(_.getCause)
        .takeWhile(_ != null)
        .flatMap { current =>
          val structured = Seq("getCondition", "getErrorClass").iterator
            .flatMap { methodName =>
              try {
                Option(
                  current.getClass.getMethod(methodName).invoke(current).asInstanceOf[String])
              } catch {
                case _: NoSuchMethodException => None
              }
            }
            .find(_ != null)
          structured.orElse(
            Option(current.getMessage)
              .filter(_.contains(s"[$expectedCondition]"))
              .map(_ => expectedCondition))
        }
        .find(_ != null)
        .getOrElse(fail(
          s"${ex.getClass.getName} does not expose or carry Spark condition $expectedCondition"))
    }

    val sparkCondition = errorCondition(sparkEx)
    val cometCondition = errorCondition(cometEx)
    assert(
      sparkCondition == expectedCondition,
      s"expected Spark baseline condition $expectedCondition, got $sparkCondition")
    assert(
      cometCondition == sparkCondition,
      s"Comet error condition $cometCondition did not match Spark baseline $sparkCondition")
  }

  /** Runs a MERGE and returns the native MergeRows node when conversion is enabled. */
  private def runAndCaptureMergeExec(
      mergeSql: String,
      nativeMerge: Boolean = true): Option[CometMergeRowsExec] = {
    val captured = scala.collection.mutable.ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    spark.listenerManager.register(listener)
    try {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> nativeMerge.toString) {
        sql(mergeSql)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      captured
        .flatMap(qe =>
          find(qe.executedPlan) { case _: CometMergeRowsExec => true; case _ => false })
        .collectFirst { case m: CometMergeRowsExec => m }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("MERGE with a scalar-subquery assignment runs natively and matches Spark") {
    assumeMerge()
    val target = s"$catalog.default.subq_target"
    val source = s"$catalog.default.subq_source"

    def resetTables(): Unit = {
      sql(s"DROP TABLE IF EXISTS $target")
      sql(s"DROP TABLE IF EXISTS $source")
      sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
      sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
      sql(s"INSERT INTO $target VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
      sql(s"INSERT INTO $source VALUES (2, 200.0), (3, 300.0), (4, 400.0)")
    }

    // Spark forbids subqueries in MERGE *conditions* but allows them in assignment values. The
    // subquery lives on a `MergeRows.Instruction` (an Expression); `CometMergeRowsExec` must retain
    // it as a node expression so `CometNativeExec.prepareSubqueries` registers it, otherwise
    // execution fails with `Subquery ... not found for plan ...`.
    val mergeSql =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = (SELECT max(amount) FROM $source)
         |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables()
    val mergeExec = runAndCaptureMergeExec(mergeSql)
    assert(
      mergeExec.isDefined,
      "expected the subquery MERGE to still run through CometMergeRowsExec")
    // The node must expose the assignment's subquery so `CometNativeExec.prepareSubqueries`
    // registers it -- either the planned `execution.ScalarSubquery` or, once AQE has run, a
    // subquery-bearing expression tree. Absence here is what previously caused `Subquery not found`.
    assert(
      mergeExec.get.expressions.exists(e =>
        e.exists(_.isInstanceOf[ScalarSubquery]) || SubqueryExpression.hasSubquery(e)),
      "CometMergeRowsExec must expose the assignment subquery in `expressions`, got: " +
        mergeExec.get.expressions.mkString("; "))
    val cometResult =
      sql(s"SELECT id, amount FROM $target ORDER BY id").collect().map(_.toString).toSeq

    resetTables()
    withSQLConf(CometConf.COMET_ENABLED.key -> "false")(sql(mergeSql))
    val sparkResult =
      sql(s"SELECT id, amount FROM $target ORDER BY id").collect().map(_.toString).toSeq

    assert(
      cometResult == sparkResult,
      "native subquery MERGE diverged from Spark.\n" +
        s"comet: ${cometResult.mkString(", ")}\nspark: ${sparkResult.mkString(", ")}")
  }

  // Executes a delta-backed MERGE with Comet enabled and requires the MergeRows child below the
  // JVM WriteDelta to convert natively. The final table contents are then compared with a pure
  // Spark run of the same statement. For split updates we also assert that Spark actually planned
  // a Split instruction, so the test cannot silently stop exercising delete+reinsert semantics.
  private def deltaMergeCase(name: String, splitUpdates: Boolean, matchedClause: String): Unit = {
    test(s"delta-backed MERGE: $name") {
      assumeMerge()
      val target = s"$catalog.default.delta_t_${name.replaceAll("\\W", "_")}"
      val source = s"$catalog.default.delta_s_${name.replaceAll("\\W", "_")}"

      def reset(): Unit = {
        sql(s"DROP TABLE IF EXISTS $target")
        sql(s"DROP TABLE IF EXISTS $source")
        sql(
          s"CREATE TABLE $target (pk INT NOT NULL, amount INT) USING parquet " +
            "TBLPROPERTIES ('supports-deltas' = 'true'" +
            (if (splitUpdates) ", 'split-updates' = 'true')" else ")"))
        sql(s"CREATE TABLE $source (pk INT NOT NULL, amount INT) USING parquet")
        sql(s"INSERT INTO $target VALUES (1, 10), (2, 20), (3, 30)")
        sql(s"INSERT INTO $source VALUES (2, 200), (3, 300), (4, 400)")
      }

      val mergeSql =
        s"""MERGE INTO $target t USING $source s ON t.pk = s.pk
           |$matchedClause
           |WHEN NOT MATCHED THEN INSERT (pk, amount) VALUES (s.pk, s.amount)
           |""".stripMargin

      reset()
      val mergeExec = runAndCaptureMergeExec(mergeSql)
      assert(
        mergeExec.isDefined,
        s"delta MERGE '$name' must execute its MergeRows child natively under WriteDelta")
      if (splitUpdates) {
        assert(
          mergeExec.get.matchedInstructions.exists(_.getClass.getSimpleName == "Split"),
          s"delta MERGE '$name' must retain Spark's Split instruction for delete+reinsert")
      }
      val cometRows =
        sql(s"SELECT pk, amount FROM $target ORDER BY pk").collect().map(_.toString).toSeq

      reset()
      withSQLConf(CometConf.COMET_ENABLED.key -> "false")(sql(mergeSql))
      val sparkRows =
        sql(s"SELECT pk, amount FROM $target ORDER BY pk").collect().map(_.toString).toSeq

      assert(
        cometRows == sparkRows,
        s"native delta MERGE '$name' diverged from Spark.\n" +
          s"comet: ${cometRows.mkString(", ")}\nspark: ${sparkRows.mkString(", ")}")
    }
  }

  deltaMergeCase(
    "matched update",
    splitUpdates = false,
    "WHEN MATCHED THEN UPDATE SET t.amount = s.amount")
  deltaMergeCase(
    "split update (delete + reinsert)",
    splitUpdates = true,
    "WHEN MATCHED THEN UPDATE SET t.amount = s.amount")
  deltaMergeCase("matched delete", splitUpdates = false, "WHEN MATCHED THEN DELETE")

  /** Drops the supplied row-level test tables when they exist. */
  private def dropTables(names: String*): Unit =
    names.foreach(name => sql(s"DROP TABLE IF EXISTS $name"))

  test("disabled MergeRows flag falls back cleanly") {
    assumeMerge()
    val target = s"$catalog.default.flag_target"
    val source = s"$catalog.default.flag_source"
    dropTables(target, source)
    sql(s"CREATE TABLE $target (id INT, value INT) USING parquet")
    sql(s"CREATE TABLE $source (id INT, value INT) USING parquet")
    sql(s"INSERT INTO $target VALUES (1, 10), (2, 20)")
    sql(s"INSERT INTO $source VALUES (2, 200), (3, 300)")

    val merge =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
         |""".stripMargin

    assert(runAndCaptureMergeExec(merge, nativeMerge = false).isEmpty)
    assert(
      sql(s"SELECT id, value FROM $target ORDER BY id").collect().map(_.toString).toSeq ==
        Seq("[1,10]", "[2,200]", "[3,300]"))
  }

  test("copy-on-write matched delete runs through native MergeRows") {
    assumeMerge()
    val target = s"$catalog.default.delete_target"
    val source = s"$catalog.default.delete_source"
    dropTables(target, source)
    sql(s"CREATE TABLE $target (id INT, value INT) USING parquet")
    sql(s"CREATE TABLE $source (id INT) USING parquet")
    sql(s"INSERT INTO $target VALUES (1, 10), (2, 20), (3, 30)")
    sql(s"INSERT INTO $source VALUES (2)")

    val merge =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN DELETE
         |""".stripMargin

    assert(runAndCaptureMergeExec(merge).isDefined)
    assert(
      sql(s"SELECT id, value FROM $target ORDER BY id").collect().map(_.toString).toSeq ==
        Seq("[1,10]", "[3,30]"))
  }

  test("first matching clause prevents later ANSI predicate evaluation") {
    assumeMerge()
    val target = s"$catalog.default.order_target"
    val source = s"$catalog.default.order_source"
    dropTables(target, source)
    sql(s"CREATE TABLE $target (id INT, value INT) USING parquet")
    sql(s"CREATE TABLE $source (id INT, d INT) USING parquet")
    sql(s"INSERT INTO $target VALUES (1, 10), (2, 20)")
    sql(s"INSERT INTO $source VALUES (1, 0), (2, 2)")

    val merge =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED AND s.d = 0 THEN UPDATE SET t.value = 111
         |WHEN MATCHED AND 2 / s.d > 0 THEN UPDATE SET t.value = 222
         |""".stripMargin

    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      assert(runAndCaptureMergeExec(merge).isDefined)
    }
    assert(
      sql(s"SELECT id, value FROM $target ORDER BY id").collect().map(_.toString).toSeq ==
        Seq("[1,111]", "[2,222]"))
  }

  test("CometMergeRowsExec semantic identity reflects instruction groups and cardinality flag") {
    assumeMerge()
    val target = s"$catalog.default.eq_target"
    val source = s"$catalog.default.eq_source"
    sql(s"DROP TABLE IF EXISTS $target")
    sql(s"DROP TABLE IF EXISTS $source")
    sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
    sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
    sql(s"INSERT INTO $source VALUES (1, 100.0), (3, 300.0)")

    def merge(clauses: String): CometMergeRowsExec = {
      sql(s"DROP TABLE IF EXISTS $target")
      sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
      sql(s"INSERT INTO $target VALUES (1, 10.0), (2, 20.0)")
      val m =
        runAndCaptureMergeExec(s"MERGE INTO $target t USING $source s ON t.id = s.id\n$clauses\n")
      assert(m.isDefined)
      m.get
    }

    val update = merge("""WHEN MATCHED THEN UPDATE SET t.amount = s.amount
        |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
        |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.amount = t.amount + 1""".stripMargin)
    val delete = merge("""WHEN MATCHED THEN DELETE
        |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)""".stripMargin)

    // equal to a structural copy (hash contract), unequal to a different MERGE
    val same = update.copy()
    assert(update == same && update.hashCode() == same.hashCode())
    assert(update != delete, "different MERGE instructions must not compare equal")
    assert(
      update != update.copy(checkCardinality = !update.checkCardinality),
      "nodes differing only in checkCardinality must not compare equal")

    // The dangerous case: identical flattened expressions, different group partitioning. Moving
    // the MATCHED instruction into the NOT MATCHED BY SOURCE group leaves `expressions` (and so a
    // naive flat-list equality) unchanged, but the operator means something entirely different.
    // Move the last MATCHED instruction to the front of the (adjacent) NOT MATCHED group so the
    // flattened expression order is byte-for-byte unchanged; only the group boundary shifts.
    assume(update.matchedInstructions.nonEmpty)
    val shuffled = update.copy(
      matchedInstructions = update.matchedInstructions.dropRight(1),
      notMatchedInstructions =
        update.matchedInstructions.takeRight(1) ++ update.notMatchedInstructions)
    assert(
      update.expressions.toList == shuffled.expressions.toList,
      "test premise: the two nodes must have the same flattened expressions")
    assert(update != shuffled, "instruction-group boundaries must be part of equality")
    assert(
      update.canonicalized != shuffled.canonicalized,
      "instruction-group boundaries must survive canonicalization")
  }
}

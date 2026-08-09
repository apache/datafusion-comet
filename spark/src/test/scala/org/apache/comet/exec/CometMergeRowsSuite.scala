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

import org.apache.spark.{CometListenerBusUtils, SparkConf, SparkThrowable}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
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
 * `supports-deltas` table property: unset (default `false`) plans through `MergeRows`; `true`
 * plans through `WriteDelta` instead, a distinct row-dispatch shape this operator does not
 * target. See `CometIcebergWriteActionSuite` for MERGE INTO coverage against real Iceberg tables.
 */
class CometMergeRowsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.shuffle.partitions", "4")
  }

  private def assumeMerge(): Unit = assume(isSpark35Plus, "MergeRowsExec requires Spark 3.5+")

  test("MERGE on a non-Iceberg SupportsRowLevelOperations table engages CometMergeRowsExec") {
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

    val mergeSql =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
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

  test(
    "MERGE cardinality violation raises SparkRuntimeException, not a generic native exception") {
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
    var cometEx: Throwable = null
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
      cometEx = intercept[Exception](sql(mergeSql).collect())
    }
    var sparkEx: Throwable = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkEx = intercept[Exception](sql(mergeSql).collect())
    }

    // Both must be the same real Spark exception type/condition, not a generic
    // CometNativeException/CometQueryExecutionException wrapping an opaque message.
    val sparkRuntimeExceptionClass = "org.apache.spark.SparkRuntimeException"
    assert(
      sparkEx.getClass.getName == sparkRuntimeExceptionClass,
      s"expected Spark's own baseline to be a $sparkRuntimeExceptionClass, got ${sparkEx.getClass}")
    assert(
      cometEx.getClass.getName == sparkRuntimeExceptionClass,
      s"Comet's native MERGE cardinality violation must surface as a $sparkRuntimeExceptionClass " +
        s"like Spark's own, got ${cometEx.getClass}: ${cometEx.getMessage}")
    val cometCondition = cometEx.asInstanceOf[SparkThrowable].getErrorClass
    assert(
      cometCondition == "MERGE_CARDINALITY_VIOLATION",
      s"expected error condition MERGE_CARDINALITY_VIOLATION, got $cometCondition")
  }
}

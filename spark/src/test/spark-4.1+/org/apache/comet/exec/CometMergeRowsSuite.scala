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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf

/**
 * Spark 4.1+ compatibility coverage for MergeRowsExec.
 *
 * Spark 4.1 introduced writer-side MergeSummary discovery that specifically looks for Spark's
 * concrete MergeRowsExec. Comet must therefore retain that JVM node until it can preserve the
 * summary-aware BatchWrite.commit contract end-to-end.
 */
class CometMergeRowsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
  }

  test("Spark 4.1+ MERGE retains Spark MergeRowsExec for write-summary compatibility") {
    val target = s"$catalog.default.rowlevel_target"
    val source = s"$catalog.default.rowlevel_source"

    sql(s"DROP TABLE IF EXISTS $target")
    sql(s"DROP TABLE IF EXISTS $source")
    sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
    sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
    sql(s"INSERT INTO $target VALUES (1, 10.0), (2, 20.0)")
    sql(s"INSERT INTO $source VALUES (2, 200.0), (3, 300.0)")

    val captured = ArrayBuffer[QueryExecution]()
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
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
        sql(s"""MERGE INTO $target t USING $source s ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
             |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
             |""".stripMargin)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)

      val executedPlans = captured.map(_.executedPlan)
      val sparkMergeRows = executedPlans.exists(plan =>
        find(plan) {
          case node if node.getClass.getSimpleName == "MergeRowsExec" => true
          case _ => false
        }.nonEmpty)
      val cometMergeRows = executedPlans.exists(plan =>
        find(plan) {
          case node if node.getClass.getSimpleName == "CometMergeRowsExec" => true
          case _ => false
        }.nonEmpty)

      assert(
        sparkMergeRows,
        "Spark 4.1+ MERGE must retain the concrete Spark MergeRowsExec so the V2 writer can " +
          "derive MergeSummary")
      assert(
        !cometMergeRows,
        "Spark 4.1+ must not replace MergeRowsExec until Comet preserves MergeSummary commit " +
          "semantics")

      val rows =
        sql(s"SELECT id, amount FROM $target ORDER BY id").collect().map(_.toString).toSeq
      assert(rows == Seq("[1,10.0]", "[2,200.0]", "[3,300.0]"))
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}

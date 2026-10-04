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
import org.apache.spark.sql.comet.CometMergeRowsExec
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf

/**
 * Spark 4.1+ stock V2 writers build MergeSummary by locating the concrete Spark MergeRowsExec.
 * Native MergeRows is therefore restored when the enclosing writer remains on Spark.
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

  test("Spark 4.1+ stock V2 writer retains Spark MergeRowsExec for MergeSummary") {
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
      withSQLConf(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
        sql(s"""MERGE INTO $target t USING $source s ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
               |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
               |""".stripMargin)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)

      val executedPlans = captured.map(_.executedPlan)
      val sparkMergeRows = executedPlans.exists(plan =>
        find(plan) {
          case _: MergeRowsExec => true
          case _ => false
        }.nonEmpty)
      val cometMergeRows = executedPlans.exists(plan =>
        find(plan) {
          case _: CometMergeRowsExec => true
          case _ => false
        }.nonEmpty)

      assert(
        sparkMergeRows,
        "Spark 4.1+ stock V2 writer must retain MergeRowsExec so it can derive MergeSummary")
      assert(
        !cometMergeRows,
        "CometMergeRowsExec must not reach a stock Spark V2 writer that requires MergeRowsExec")

      val rows =
        sql(s"SELECT id, amount FROM $target ORDER BY id").collect().map(_.toString).toSeq
      assert(rows == Seq("[1,10.0]", "[2,200.0]", "[3,300.0]"))
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}

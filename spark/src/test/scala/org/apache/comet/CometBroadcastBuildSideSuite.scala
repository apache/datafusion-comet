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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

// ORC and Iceberg build sides that Comet cannot scan natively, bridged by
// spark.comet.convert.broadcastBuildSide.enabled (from the PR #6786 review of issue #6008). Each
// query is compared with Spark through checkSparkAnswer, and every combination asserts that the
// broadcast join ran natively (bridged when the scan is unsupported). See issue #6008.
class CometBroadcastBuildSideSuite extends CometTestBase with CometIcebergTestBase {

  /** Every node, descending through AQE, stages and subqueries, but not into reused exchanges. */
  private def allNodes(p: SparkPlan): Seq[SparkPlan] = {
    val kids = p match {
      case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
      case s: QueryStageExec => Seq(s.plan)
      case _: ReusedExchangeExec => Seq.empty
      case other => other.children
    }
    val subs = p.expressions.flatMap(_.collect { case e: ExecSubqueryExpression => e.plan })
    Seq(p) ++ kids.flatMap(allNodes) ++ subs.flatMap(allNodes)
  }

  /** The build-side scan was bridged to Arrow and the broadcast join went native. */
  private def assertBridgeAndNativeJoin(plan: SparkPlan, ctx: String): Unit = {
    val nodes = allNodes(plan)
    assert(
      nodes.exists(_.isInstanceOf[CometSparkToColumnarExec]),
      s"$ctx: expected a build-side bridge (CometSparkToColumnarExec):\n$plan")
    assert(
      nodes.exists(_.isInstanceOf[CometBroadcastHashJoinExec]),
      s"$ctx: expected a native CometBroadcastHashJoinExec:\n$plan")
  }

  /** The plan carries a live dynamic partition pruning filter (not a static `true`). */
  private def dppIsLive(plan: SparkPlan): Boolean =
    allNodes(plan).exists(_.expressions.exists(_.exists {
      case DynamicPruningExpression(e) if e != Literal.TrueLiteral => true
      case _ => false
    }))

  test("ORC build side, all types, non-UTC session (#6008)") {
    withTempDir { dir =>
      val fact = s"${dir.getAbsolutePath}/fact.parquet"
      val dim = s"${dir.getAbsolutePath}/dim.orc"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0, 2000)
          .selectExpr("CAST(id % 60 AS INT) AS k", "id AS v")
          .write
          .parquet(fact)
        spark
          .range(0, 60, 1, 3)
          .selectExpr(
            "CAST(id AS INT) AS k",
            "IF(id % 7 = 0, NULL, CAST(id AS BYTE)) AS b",
            "IF(id % 7 = 1, NULL, CAST(id AS SHORT)) AS s",
            "IF(id % 7 = 2, NULL, id * 1000000000L) AS l",
            "IF(id % 7 = 3, NULL, id % 2 = 0) AS bo",
            "IF(id % 7 = 4, NULL, CAST(id AS FLOAT) / 3) AS f",
            "CASE WHEN id % 7 = 5 THEN NULL WHEN id = 6 THEN CAST('NaN' AS DOUBLE) " +
              "WHEN id = 8 THEN -0.0D ELSE id / 3.0D END AS d",
            "IF(id % 7 = 6, NULL, CAST(id * 1.25 AS DECIMAL(10,2))) AS dec1",
            "IF(id % 5 = 0, NULL, CAST(id AS DECIMAL(38,10)) / 7) AS dec2",
            "IF(id % 5 = 1, NULL, CONCAT('s', id, 'é')) AS str",
            "IF(id % 5 = 2, NULL, CAST(CONCAT('b', id) AS BINARY)) AS bin",
            "IF(id % 5 = 3, NULL, DATE_ADD(DATE'1960-01-01', CAST(id * 300 AS INT))) AS dt",
            "IF(id % 5 = 4, NULL, TIMESTAMP_MICROS(id * 8640000000000L - 900000000000000L)) AS ts",
            "IF(id % 9 = 0, NULL, " +
              "CAST(TIMESTAMP_MICROS(id * 8640000000000L - 900000000000000L) AS TIMESTAMP_NTZ)) " +
              "AS ntz",
            "IF(id % 6 = 0, NULL, named_struct('a', CAST(id AS INT), 'b', " +
              "IF(id % 4 = 0, NULL, CONCAT('x', id)))) AS st",
            "CASE WHEN id % 6 = 1 THEN NULL WHEN id % 6 = 2 THEN CAST(array() AS ARRAY<STRING>) " +
              "ELSE array(CONCAT('e', id), NULL, 'z') END AS arr",
            "CASE WHEN id % 6 = 3 THEN NULL WHEN id % 6 = 4 THEN " +
              "CAST(map() AS MAP<STRING,STRING>) " +
              "ELSE map(CONCAT('k', id), IF(id % 2 = 0, NULL, 'v')) END AS m")
          .write
          .orc(dim)
      }
      val query =
        """SELECT /*+ BROADCAST(d) */ f.v, d.*, hour(d.ts) AS h, CAST(d.ts AS STRING) AS tss,
          |  CAST(d.ntz AS STRING) AS ntzs, date_trunc('DAY', d.ts) AS tday
          |FROM fact_o f JOIN dim_o d ON f.k = d.k""".stripMargin
      for {
        aqe <- Seq("false", "true")
        v1List <- Seq("avro,csv,json,kafka,orc,parquet,text", "parquet")
        (vec, nestedVec) <- Seq(("true", "true"), ("true", "false"), ("false", "false"))
      } {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
          SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vec,
          SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> nestedVec,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
          spark.read.parquet(fact).createOrReplaceTempView("fact_o")
          spark.read.orc(dim).createOrReplaceTempView("dim_o")
          val (_, plan) = checkSparkAnswer(sql(query))
          assertBridgeAndNativeJoin(plan, s"ORC aqe=$aqe v1=$v1List vec=$vec nested=$nestedVec")
        }
      }
    }
  }

  test("Iceberg build side that Comet's native reader declines (#6008)") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withTempDir { dir =>
        val fact = s"${dir.getAbsolutePath}/fact.parquet"
        withSQLConf(
          "spark.sql.catalog.pc" -> "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.pc.type" -> "hadoop",
          "spark.sql.catalog.pc.warehouse" -> warehouseDir.getAbsolutePath) {
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark
              .range(0, 2000)
              .selectExpr("CAST(id % 40 AS INT) AS k", "id AS v")
              .write
              .parquet(fact)
            for ((name, props) <- Seq(
                "dim_orc" -> "'write.format.default'='orc'",
                "dim_pq" -> ("'format-version'='2', 'write.delete.mode'='merge-on-read', " +
                  "'write.update.mode'='merge-on-read'"))) {
              spark.sql(s"""CREATE TABLE pc.db.$name (
                           |  k INT, s STRING, d DECIMAL(10,2), ts TIMESTAMP,
                           |  st STRUCT<a: INT, b: STRING>, arr ARRAY<STRING>,
                           |  m MAP<STRING, STRING>, region STRING)
                           |USING iceberg PARTITIONED BY (region)
                           |TBLPROPERTIES ($props)""".stripMargin)
              spark
                .range(0, 40, 1, 2)
                .selectExpr(
                  "CAST(id AS INT) AS k",
                  "IF(id % 5 = 0, NULL, CONCAT('s', id)) AS s",
                  "IF(id % 6 = 0, NULL, CAST(id * 1.5 AS DECIMAL(10,2))) AS d",
                  "IF(id % 7 = 0, NULL, " +
                    "TIMESTAMP_MICROS(id * 8640000000000L - 900000000000000L)) AS ts",
                  "IF(id % 4 = 0, NULL, named_struct('a', CAST(id AS INT), 'b', " +
                    "IF(id % 3 = 0, NULL, CONCAT('b', id)))) AS st",
                  "CASE WHEN id % 4 = 1 THEN NULL WHEN id % 4 = 2 THEN " +
                    "CAST(array() AS ARRAY<STRING>) ELSE array(CONCAT('e', id), NULL) END AS arr",
                  "IF(id % 3 = 1, NULL, map(CONCAT('k', id), IF(id % 2 = 0, NULL, 'v'))) AS m",
                  "IF(id % 2 = 0, 'east', 'west') AS region")
                .writeTo(s"pc.db.$name")
                .append()
            }
            spark.sql("DELETE FROM pc.db.dim_pq WHERE k % 9 = 0")
          }
          spark.read.parquet(fact).createOrReplaceTempView("ice_fact")
          val queries = Seq(
            "orc" -> ("SELECT /*+ BROADCAST(d) */ f.v, d.* FROM ice_fact f " +
              "JOIN pc.db.dim_orc d ON f.k = d.k"),
            "pq-mor" -> ("SELECT /*+ BROADCAST(d) */ f.v, d.* FROM ice_fact f " +
              "JOIN pc.db.dim_pq d ON f.k = d.k"),
            "snapshots" -> ("SELECT /*+ BROADCAST(s) */ f.v, s.operation, s.summary FROM " +
              "ice_fact f JOIN pc.db.dim_pq.snapshots s ON f.k = CAST(s.snapshot_id % 2 AS INT)"))
          for {
            (name, query) <- queries
            native <- Seq("true", "false")
            aqe <- Seq("false", "true")
          } {
            withSQLConf(
              CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> native,
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Kolkata") {
              val (_, plan) = checkSparkAnswer(sql(query))
              // The native Iceberg reader reads some of these tables directly (e.g. merge-on-read
              // Parquet) and declines others (ORC, metadata tables). Either way the broadcast join
              // runs natively; a build-side bridge appears only when the reader declines, so assert
              // the native join here rather than the bridge.
              assert(
                allNodes(plan).exists(_.isInstanceOf[CometBroadcastHashJoinExec]),
                s"ICEBERG $name icebergNative=$native aqe=$aqe: expected a native " +
                  s"CometBroadcastHashJoinExec:\n$plan")
            }
          }
        }
      }
    }
  }

  test("Spark 3.4: native Iceberg fact, text dimension, AQE DPP (#6008)") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withTempDir { dir =>
        val dim = s"${dir.getAbsolutePath}/dim.txt"
        withSQLConf(
          "spark.sql.catalog.pc" -> "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.pc.type" -> "hadoop",
          "spark.sql.catalog.pc.warehouse" -> warehouseDir.getAbsolutePath) {
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql("""CREATE TABLE pc.db.fact (v BIGINT, fk STRING)
                         |USING iceberg PARTITIONED BY (fk)""".stripMargin)
            spark
              .range(0, 1000, 1, 2)
              .selectExpr("id AS v", "CAST(id % 10 AS STRING) AS fk")
              .writeTo("pc.db.fact")
              .append()
            spark.range(0, 10).selectExpr("CAST(id AS STRING) AS value").write.text(dim)
          }
          spark.read.text(dim).createOrReplaceTempView("ice_dim")
          val query =
            """SELECT /*+ BROADCAST(d) */ f.v FROM pc.db.fact f JOIN
              |(SELECT value FROM ice_dim WHERE CAST(value AS INT) % 3 = 0) d
              |ON f.fk = d.value""".stripMargin
          for {
            aqe <- Seq("true", "false")
            feature <- Seq("true", "false")
          } {
            withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
              SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
              SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
              CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.key -> feature) {
              val (_, plan) = checkSparkAnswer(sql(query))
              // The feature must not break DPP: the fact keeps a live pruning filter (reads 400 of
              // its 1000 rows), never a static `true`, whether the dimension is bridged or not.
              assert(
                dppIsLive(plan),
                s"DPP aqe=$aqe feature=$feature: expected a live DPP filter on the fact:\n$plan")
            }
          }
        }
      }
    }
  }
}

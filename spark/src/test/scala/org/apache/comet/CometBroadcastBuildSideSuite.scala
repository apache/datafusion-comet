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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

// Tests for the broadcast build-side bridge, spark.comet.convert.broadcastBuildSide.enabled (issue
// #6008). A build-side scan that Comet cannot read natively, such as ORC, Text or an Iceberg table
// the native reader declines, is converted to Arrow so that the broadcast join runs natively. Each
// query is compared with Spark, and the tests assert where the bridge must and must not appear.
class CometBroadcastBuildSideSuite extends CometTestBase with CometIcebergTestBase {

  // Most tests use a Text build side. Text is bridged only with its own opt-in, which is off by
  // default because a Text file can hold bytes that are not valid UTF-8 (see issue #7118).
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_TEXT_ENABLED.key, "true")

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

  test("broadcast build side with unsupported text source goes native (#6008)") {
    // A tiny lookup table read from a Text file (which Comet cannot scan natively) sits on the
    // build side of a broadcast join over a large native probe. Without the auto-bridge the whole
    // join stays on Spark. With spark.comet.convert.broadcastBuildSide.enabled (default on) and
    // its Text opt-in (on for this suite) the text leaf is bridged to Arrow so the broadcast and
    // the join run natively.
    assert(CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.defaultValue.contains(true))
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0, 100)
          .selectExpr("CAST(id AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
        spark
          .range(0, 5)
          .selectExpr("CAST(id AS STRING) AS value")
          .write
          .text(allowPath)
      }

      // Cover both the AQE and non-AQE planning paths - the tag pre-pass runs per stage under AQE.
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.parquet(probePath).createOrReplaceTempView("probe_6008")
          spark.read.text(allowPath).createOrReplaceTempView("allow_6008")
          val query =
            "SELECT /*+ BROADCAST(a) */ p.v FROM probe_6008 p JOIN allow_6008 a ON p.key = a.value"

          // Default: the text build side is bridged to Arrow and the join goes native. The plan is
          // fully native (the bridge counts as a native leaf), so use checkSparkAnswerAndOperator -
          // checkSparkAnswer would fail under COMET_STRICT_TESTING on a fully-native plan.
          val (_, cometPlan) = checkSparkAnswerAndOperator(sql(query))
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
            s"aqe=$aqe: expected a CometSparkToColumnarExec on the text build side:\n" +
              cometPlan.treeString)
          assert(
            collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.nonEmpty,
            s"aqe=$aqe: expected a CometBroadcastExchangeExec:\n${cometPlan.treeString}")
          assert(
            collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
            s"aqe=$aqe: expected a CometBroadcastHashJoinExec:\n${cometPlan.treeString}")

          // Off: the unsupported text leaf blocks native execution of the broadcast and the join.
          withSQLConf(CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.key -> "false") {
            val (_, cometPlanOff) = checkSparkAnswer(sql(query))
            assert(
              collect(cometPlanOff) { case s: CometSparkToColumnarExec => s }.isEmpty,
              s"aqe=$aqe: expected no CometSparkToColumnarExec when disabled:\n" +
                cometPlanOff.treeString)
            assert(
              collect(cometPlanOff) { case b: CometBroadcastExchangeExec => b }.isEmpty,
              s"aqe=$aqe: expected no CometBroadcastExchangeExec when disabled:\n" +
                cometPlanOff.treeString)
            assert(
              collect(cometPlanOff) { case j: CometBroadcastHashJoinExec => j }.isEmpty,
              s"aqe=$aqe: expected no CometBroadcastHashJoinExec when disabled:\n" +
                cometPlanOff.treeString)
          }
        }
      }
    }
  }

  test("a Text build side keeps Spark's bytes unless its opt-in is on (#7118)") {
    // The bridge is on by default, but a Text build side also needs
    // spark.comet.convert.broadcastBuildSide.text.enabled. A Text file can hold bytes that are not
    // valid UTF-8, here a Latin-1 "é" (0xE9) and "ï" (0xEF), and converting them to Arrow replaces
    // each ill-formed sequence with U+FFFD. Without the opt-in the join stays on Spark and keeps
    // the bytes, as in 1.1.0, and the scan records why it was not converted.
    assert(CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_TEXT_ENABLED.defaultValue.contains(false))
    assert(CometConf.COMET_CONVERT_FROM_TEXT_ENABLED.defaultValue.contains(false))
    withTempDir { dir =>
      val factPath = s"${dir.getAbsolutePath}/fact.parquet"
      val dimFile = new File(dir, "dim.txt")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0, 20)
          .selectExpr("CAST(id % 2 + 1 AS STRING) AS k", "id AS v")
          .write
          .parquet(factPath)
      }
      Files.write(
        dimFile.toPath,
        "1 caf".getBytes(UTF_8) ++ Array(0xe9.toByte) ++ "\n2 na".getBytes(UTF_8) ++
          Array(0xef.toByte) ++ "ve\n".getBytes(UTF_8))
      val query =
        """SELECT /*+ BROADCAST(d) */ f.v, hex(d.value) AS h, length(d.value) AS n
          |FROM fact_7118 f JOIN dim_7118 d ON f.k = substring(d.value, 1, 1)""".stripMargin
      def hexAndLength(df: DataFrame): Set[Row] = df.select("h", "n").collect().toSet
      def bridged(df: DataFrame): Boolean =
        collect(df.queryExecution.executedPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty
      for {
        aqe <- Seq("false", "true")
        // V1 FileSourceScanExec, then V2 BatchScanExec (TextScan).
        v1List <- Seq("parquet,text", "parquet")
      } {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          spark.read.parquet(factPath).createOrReplaceTempView("fact_7118")
          spark.read.text(dimFile.getAbsolutePath).createOrReplaceTempView("dim_7118")
          val ctx = s"aqe=$aqe v1=$v1List"

          withSQLConf(CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_TEXT_ENABLED.key -> "false") {
            checkSparkAnswerAndFallbackReason(
              sql(query),
              "Comet converts a Text scan on a broadcast build side to Arrow only when")
            val df = sql(query)
            assert(hexAndLength(df) == Set(Row("3120636166E9", 6), Row("32206E61EF7665", 5)), ctx)
            assert(!bridged(df), s"$ctx: the Text build side must stay on Spark")
          }

          // With the opt-in the bridge converts the Text build side, and each ill-formed byte comes
          // back as U+FFFD (EF BF BD), which changes `hex` and `length`. The compatibility guide
          // quotes these values.
          val df = sql(query)
          assert(
            hexAndLength(df) == Set(Row("3120636166EFBFBD", 6), Row("32206E61EFBFBD7665", 7)),
            ctx)
          assert(bridged(df), s"$ctx: expected the Text build side to be bridged")

          // spark.comet.convert.text.enabled converts every Text scan, the build side included.
          withSQLConf(
            CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_TEXT_ENABLED.key -> "false",
            CometConf.COMET_CONVERT_FROM_TEXT_ENABLED.key -> "true") {
            val df = sql(query)
            assert(hexAndLength(df).size == 2, ctx)
            assert(bridged(df), s"$ctx: expected spark.comet.convert.text.enabled to convert")
          }
        }
      }
    }
  }

  test("broadcast build side bridge is gated on broadcast-exchange conversion (#6008)") {
    // The bridge is pointless when Comet broadcast exchange conversion is off: the broadcast (and
    // so the join) cannot go native. With COMET_EXEC_BROADCAST_EXCHANGE_ENABLED=false the text
    // build side must not be bridged even though broadcastBuildSide is on.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0, 100)
          .selectExpr("CAST(id AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(allowPath)
      }
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "false") {
        spark.read.parquet(probePath).createOrReplaceTempView("probe_gate_6008")
        spark.read.text(allowPath).createOrReplaceTempView("allow_gate_6008")
        val (_, cometPlan) = checkSparkAnswer(sql("""
            |SELECT /*+ BROADCAST(a) */ p.v
            |FROM probe_gate_6008 p JOIN allow_gate_6008 a ON p.key = a.value
            |""".stripMargin))
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
          "with broadcast-exchange conversion off the build side must not be bridged:\n" +
            cometPlan.treeString)
      }
    }
  }

  test("broadcast build side scan below an aggregate is bridged, shuffle stage is not (#6008)") {
    // A file scan can sit BELOW build-side operators. Broadcasting an aggregated Text table puts a
    // Text FileSourceScanExec under a partial aggregate + shuffle on the build side. The tag
    // pre-pass walks the whole broadcast subtree, so the deep text scan must be bridged to Arrow,
    // yet no query stage (e.g. the build-side shuffle under AQE) may be wrapped in the bridge.
    withTempDir { dir =>
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 20).selectExpr("CAST(id % 5 AS STRING) AS value").write.text(allowPath)
        spark
          .range(0, 100)
          .selectExpr("CAST(id % 5 AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
      }
      spark.read.text(allowPath).createOrReplaceTempView("allow_agg_6008")
      spark.read.parquet(probePath).createOrReplaceTempView("probe_agg_6008")
      val query = """
          |SELECT /*+ BROADCAST(d) */ p.v, d.n
          |FROM probe_agg_6008 p
          |JOIN (SELECT value AS k, COUNT(*) AS n FROM allow_agg_6008 GROUP BY value) d
          |  ON p.key = d.k
          |""".stripMargin
      for (aqe <- Seq(false, true)) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (10 * 1024 * 1024).toString) {
          // The plan is fully native (the bridged text scan under the aggregate is a native leaf),
          // so checkSparkAnswerAndOperator also pins that the text scan did get bridged - an
          // un-bridged Spark scan would read as non-native and fail here. It is also strict-testing
          // safe, unlike checkSparkAnswer on a fully-native plan.
          val (_, cometPlan) = checkSparkAnswerAndOperator(sql(query))
          // `collect` descends AQE query stages, so the bridge on the deep text scan is reachable
          // under AQE too (it lives in the build-side shuffle stage). Assert it in both modes - and
          // because checkSparkAnswerAndOperator only inspects the initial plan under AQE, this is
          // the only thing that checks the final AQE plan.
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
            s"aqe=$aqe: expected the build-side text scan to be bridged:\n${cometPlan.treeString}")
          // A query stage (the build-side shuffle) must never be wrapped in the bridge.
          collect(cometPlan) { case s: CometSparkToColumnarExec => s.child }.foreach { child =>
            assert(
              !child.isInstanceOf[QueryStageExec],
              s"aqe=$aqe: a QueryStageExec must not be bridged to Arrow:\n${cometPlan.treeString}")
          }
        }
      }
    }
  }

  test("per-format opt-out still wins on a broadcast build side (#6008)") {
    // A CSV build side stays a Spark FileSourceScanExec and is tagged, but the CSV arm of
    // shouldApplySparkToColumnar honors COMET_CONVERT_FROM_CSV_ENABLED and never calls the
    // broadcast-build-side bypass. So with convert-from-csv off, the CSV build side must NOT be
    // bridged and the join must stay on Spark, even with broadcastBuildSide enabled.
    withTempDir { dir =>
      val allowPath = s"${dir.getAbsolutePath}/allow.csv"
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.csv(allowPath)
        spark
          .range(0, 100)
          .selectExpr("CAST(id AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
      }
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_CSV_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.key -> "true") {
        spark.read.csv(allowPath).createOrReplaceTempView("allow_csv_6008")
        spark.read.parquet(probePath).createOrReplaceTempView("probe_csv_6008")
        val (_, cometPlan) = checkSparkAnswer(sql("""
            |SELECT /*+ BROADCAST(a) */ p.v
            |FROM probe_csv_6008 p JOIN allow_csv_6008 a ON p.key = a._c0
            |""".stripMargin))
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
          "a CSV build side with convert-from-csv disabled must not be bridged:\n" +
            cometPlan.treeString)
      }
    }
  }

  test("broadcast build side with an unsupported DSv2 scan goes native (#6008)") {
    // Exercises the BatchScanExec arm for Text: a Text scan read as DSv2 on the build side is
    // bridged so the join goes native (the ORC test covers the arm for other formats). Keep parquet
    // on the V1 list so the probe stays a native CometScan; drop text from the list so it is read
    // as a DSv2 BatchScan/TextScan.
    withTempDir { dir =>
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(allowPath)
        spark
          .range(0, 100)
          .selectExpr("CAST(id AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
      }
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        spark.read.text(allowPath).createOrReplaceTempView("allow_v2_6008")
        spark.read.parquet(probePath).createOrReplaceTempView("probe_v2_6008")
        // Fully native (bridged DSv2 text build side + native parquet probe + native join), so use
        // checkSparkAnswerAndOperator - strict-testing safe, unlike checkSparkAnswer here.
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql("""
            |SELECT /*+ BROADCAST(a) */ p.v
            |FROM probe_v2_6008 p JOIN allow_v2_6008 a ON p.key = a.value
            |""".stripMargin))
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
          s"expected the DSv2 text build side to be bridged:\n${cometPlan.treeString}")
        assert(
          collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
          s"expected the join to go native over the DSv2 build side:\n${cometPlan.treeString}")
      }
    }
  }

  test("broadcast join with an unsupported probe stays on Spark (#6008)") {
    // Both sides are unsupported Text sources. The probe is not natively scannable, so the join
    // cannot go native - and because the probe is not native, the build side must NOT be bridged
    // either (bridging it would be useless and would break Spark's broadcast/DPP reuse). This pins
    // the probe-native gate that fixed the DynamicPartitionPruningV2 regression.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.txt"
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 100).selectExpr("CAST(id AS STRING) AS value").write.text(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(allowPath)
      }
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.text(probePath).createOrReplaceTempView("probe_txt_6008")
          spark.read.text(allowPath).createOrReplaceTempView("allow_txt_6008")
          val (_, cometPlan) = checkSparkAnswer(sql("""
              |SELECT /*+ BROADCAST(a) */ p.value
              |FROM probe_txt_6008 p JOIN allow_txt_6008 a ON p.value = a.value
              |""".stripMargin))
          // The non-native probe must leave the build side un-bridged (the probe-native gate).
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
            s"aqe=$aqe: a non-native probe must leave the build side un-bridged:\n" +
              cometPlan.treeString)
          assert(
            collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.isEmpty,
            s"aqe=$aqe: the join must stay on Spark when the probe is not native:\n" +
              cometPlan.treeString)
          assert(
            collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.isEmpty,
            s"aqe=$aqe: the broadcast must stay on Spark when the join does not convert:\n" +
              cometPlan.treeString)
        }
      }
    }
  }

  test("broadcast build side on the left (BuildLeft) with a text source goes native (#6008)") {
    // All other tests broadcast the right table (BuildRight). Put the small text table first and
    // hint it so it becomes the left build side, exercising the BuildLeft arm of tagBuildIfProbeNative.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0, 100)
          .selectExpr("CAST(id AS STRING) AS key", "id AS v")
          .write
          .parquet(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(allowPath)
      }
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.text(allowPath).createOrReplaceTempView("allow_bl_6008")
        spark.read.parquet(probePath).createOrReplaceTempView("probe_bl_6008")
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql("""
            |SELECT /*+ BROADCAST(a) */ p.v
            |FROM allow_bl_6008 a JOIN probe_bl_6008 p ON a.value = p.key
            |""".stripMargin))
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
          s"expected the text build side to be bridged:\n${cometPlan.treeString}")
        assert(
          collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
          s"expected a CometBroadcastHashJoinExec:\n${cometPlan.treeString}")
      }
    }
  }

  test("broadcast nested loop join build side with a text source goes native (#6008)") {
    // Exercises the BroadcastNestedLoopJoinExec arm of tagBroadcastBuildSideLeaves: an inequality
    // join forces a BNLJ; the text build side is bridged so the BNLJ runs natively.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val allowPath = s"${dir.getAbsolutePath}/allow.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 100).selectExpr("id AS v", "CAST(id AS INT) AS k").write.parquet(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(allowPath)
      }
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.text(allowPath).createOrReplaceTempView("allow_bnlj_6008")
        spark.read.parquet(probePath).createOrReplaceTempView("probe_bnlj_6008")
        val (_, cometPlan) = checkSparkAnswer(sql("""
            |SELECT /*+ BROADCAST(a) */ p.v
            |FROM probe_bnlj_6008 p JOIN allow_bnlj_6008 a ON p.k > CAST(a.value AS INT)
            |""".stripMargin))
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
          s"expected the text build side to be bridged:\n${cometPlan.treeString}")
        assert(
          collect(cometPlan) { case j: CometBroadcastNestedLoopJoinExec => j }.nonEmpty,
          s"expected a CometBroadcastNestedLoopJoinExec:\n${cometPlan.treeString}")
      }
    }
  }

  test("AQE DPP with a text dimension on the broadcast build side runs natively (#6008)") {
    // A Text dimension on a broadcast build side joined to a PARTITIONED Parquet fact triggers DPP.
    // The build-side bridge makes the join a CometBroadcastHashJoinExec; the DPP subquery's own
    // build (the text scan) is not bridged, so CometPlanAdaptiveDynamicPruningFilters must NOT wrap
    // it in a CometBroadcastExchangeExec (that crashed with "Comet execution only takes Arrow
    // Arrays"). It falls back to a Spark DPP broadcast while the join stays native. See issue #6008.
    assume(isSpark35Plus, "Native AQE DPP requires Spark 3.5+")
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTempDir { dir =>
        val factPath = s"${dir.getAbsolutePath}/fact.parquet"
        val dimPath = s"${dir.getAbsolutePath}/dim.txt"
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark
            .range(0, 100)
            .selectExpr("CAST(id % 10 AS STRING) AS fk", "id AS v")
            .write
            .partitionBy("fk")
            .parquet(factPath)
          spark.range(0, 10).selectExpr("CAST(id AS STRING) AS value").write.text(dimPath)
        }
        spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact_txt")
        spark.read.text(dimPath).createOrReplaceTempView("dpp_dim_txt")
        // CAST(d.value AS INT) % 2 = 0 is a filter Spark cannot infer onto f.fk, so the fact is
        // pruned only via a live DPP subquery (a filter Spark can push, like d.value < '5', would
        // prune the fact statically and the test would pass even with DPP disabled).
        val df = sql("""
            |SELECT /*+ BROADCAST(d) */ f.v
            |FROM dpp_fact_txt f JOIN dpp_dim_txt d ON f.fk = d.value
            |WHERE CAST(d.value AS INT) % 2 = 0
            |""".stripMargin)
        // Must not crash, and must match Spark.
        val (_, cometPlan) = checkSparkAnswer(df)
        // The join still goes native over the bridged build side.
        assert(
          collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
          s"expected the join to run natively:\n${cometPlan.treeString}")
        // DPP is still live - the fact scan keeps a broadcast pruning subquery (Spark-side here,
        // because the unbridged text dimension build is not native).
        val dppSubqueries = collectWithSubqueries(cometPlan) {
          case s: SubqueryBroadcastExec => s
          case s: CometSubqueryBroadcastExec => s
        }
        assert(
          dppSubqueries.nonEmpty,
          s"expected a live DPP broadcast subquery on the fact scan:\n${cometPlan.treeString}")
      }
    }
  }

  test("broadcast join that stays on Spark leaves no build-side bridge (#6008)") {
    // A non-equi FULL OUTER broadcast join is a BroadcastNestedLoopJoinExec whose join type Comet
    // declines (getSupportLevel is Unsupported for FullOuter). The probe is native Parquet, so the
    // build-side gate still tags and bridges the text build - but because the join cannot go
    // native, clearWastedBuildSideBridges must undo that bridge after the transform, leaving the
    // plan exactly as it would be without the feature: a Spark BNLJ over a Spark broadcast with no
    // CometSparkToColumnarExec. Regression test for the first-round revert that never fired. #6008.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val buildPath = s"${dir.getAbsolutePath}/build.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 100).selectExpr("CAST(id AS INT) AS k", "id AS v").write.parquet(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(buildPath)
      }
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.parquet(probePath).createOrReplaceTempView("probe_fo_6008")
          spark.read.text(buildPath).createOrReplaceTempView("build_fo_6008")
          val (_, cometPlan) = checkSparkAnswer(sql("""
              |SELECT /*+ BROADCAST(a) */ p.v, a.value
              |FROM probe_fo_6008 p
              |FULL OUTER JOIN (SELECT value FROM build_fo_6008 WHERE LENGTH(value) > 0) a
              |ON p.k > CAST(a.value AS INT)
              |""".stripMargin))
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
            s"aqe=$aqe: a Spark-staying join must leave no build-side bridge:\n" +
              cometPlan.treeString)
          assert(
            collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.isEmpty,
            s"aqe=$aqe: the broadcast must stay on Spark:\n${cometPlan.treeString}")
          assert(
            collect(cometPlan) { case j: BroadcastNestedLoopJoinExec => j }.nonEmpty,
            s"aqe=$aqe: full outer join must stay a Spark BNLJ:\n${cometPlan.treeString}")
        }
      }
    }
  }

  test("Spark-staying broadcast join with an aggregate build side leaves no bridge (#6008)") {
    // Same declined FULL OUTER BNLJ, but the text build side is aggregated. The bridge is inserted
    // under the aggregate (and its CometSinkPlaceHolder) and then cleared. The first-round revert
    // rebuilt the Spark nodes from originalPlan and crashed here with a ClassCastException; the
    // tag-clear-and-retransform approach must give a clean Spark plan and the right answer. #6008.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val buildPath = s"${dir.getAbsolutePath}/build.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 100).selectExpr("CAST(id AS INT) AS k", "id AS v").write.parquet(probePath)
        spark.range(0, 20).selectExpr("CAST(id % 5 AS STRING) AS value").write.text(buildPath)
      }
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.parquet(probePath).createOrReplaceTempView("probe_foa_6008")
          spark.read.text(buildPath).createOrReplaceTempView("build_foa_6008")
          val (_, cometPlan) = checkSparkAnswer(sql("""
              |SELECT /*+ BROADCAST(a) */ p.v, a.n
              |FROM probe_foa_6008 p
              |FULL OUTER JOIN
              |  (SELECT CAST(value AS INT) AS gk, COUNT(*) AS n
              |   FROM build_foa_6008 GROUP BY value) a
              |ON p.k > a.gk
              |""".stripMargin))
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
            s"aqe=$aqe: aggregate build side must leave no bridge:\n${cometPlan.treeString}")
          assert(
            collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.isEmpty,
            s"aqe=$aqe: the broadcast must stay on Spark:\n${cometPlan.treeString}")
        }
      }
    }
  }

  test("Spark-staying broadcast join with a union build side leaves no bridge (#6008)") {
    // Same declined FULL OUTER BNLJ, but the build side is a UNION ALL of two text scans. The
    // first-round revert rebuilt each Comet node from originalPlan and failed here with "Incorrect
    // number of children"; tag-clear-and-retransform must produce a clean Spark plan. See #6008.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val buildPath1 = s"${dir.getAbsolutePath}/build1.txt"
      val buildPath2 = s"${dir.getAbsolutePath}/build2.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 100).selectExpr("CAST(id AS INT) AS k", "id AS v").write.parquet(probePath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(buildPath1)
        spark.range(5, 10).selectExpr("CAST(id AS STRING) AS value").write.text(buildPath2)
      }
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.parquet(probePath).createOrReplaceTempView("probe_fou_6008")
          spark.read.text(buildPath1).createOrReplaceTempView("build_fou1_6008")
          spark.read.text(buildPath2).createOrReplaceTempView("build_fou2_6008")
          val (_, cometPlan) = checkSparkAnswer(sql("""
              |SELECT /*+ BROADCAST(a) */ p.v, a.value
              |FROM probe_fou_6008 p
              |FULL OUTER JOIN
              |  (SELECT value FROM build_fou1_6008 UNION ALL SELECT value FROM build_fou2_6008) a
              |ON p.k > CAST(a.value AS INT)
              |""".stripMargin))
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
            s"aqe=$aqe: union build side must leave no bridge:\n${cometPlan.treeString}")
          assert(
            collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.isEmpty,
            s"aqe=$aqe: the broadcast must stay on Spark:\n${cometPlan.treeString}")
        }
      }
    }
  }

  test("a native broadcast join nested in a Spark-staying join keeps its bridge (#6008)") {
    // The outer FULL OUTER BNLJ stays on Spark, but its build side is itself a broadcast hash join
    // of a native Parquet table with a bridged text table. clearWastedBuildSideBridges must stop at
    // the inner CometBroadcastExchangeExec so that native join keeps its bridge, rather than walk
    // through it and revert the inner scan. Pins the stop-at-CometBroadcastExchangeExec case.
    withTempDir { dir =>
      val probePath = s"${dir.getAbsolutePath}/probe.parquet"
      val factPath = s"${dir.getAbsolutePath}/fact.parquet"
      val dimPath = s"${dir.getAbsolutePath}/dim.txt"
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 20).selectExpr("CAST(id AS INT) AS ok", "id AS x").write.parquet(probePath)
        spark.range(0, 100).selectExpr("CAST(id AS INT) AS k", "id AS v").write.parquet(factPath)
        spark.range(0, 5).selectExpr("CAST(id AS STRING) AS value").write.text(dimPath)
      }
      for (aqe <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          spark.read.parquet(probePath).createOrReplaceTempView("nest_outer")
          spark.read.parquet(factPath).createOrReplaceTempView("nest_fact")
          spark.read.text(dimPath).createOrReplaceTempView("nest_dim")
          val (_, cometPlan) = checkSparkAnswer(sql("""
              |SELECT /*+ BROADCAST(ib) */ o.x, ib.v
              |FROM nest_outer o
              |FULL OUTER JOIN (
              |  SELECT /*+ BROADCAST(t) */ p.k AS k, p.v AS v
              |  FROM nest_fact p JOIN nest_dim t ON p.k = CAST(t.value AS INT)
              |) ib
              |ON o.ok > ib.k
              |""".stripMargin))
          // Inner join runs natively and keeps its text bridge.
          assert(
            collect(cometPlan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
            s"aqe=$aqe: the inner join should be a CometBroadcastHashJoinExec:\n" +
              cometPlan.treeString)
          assert(
            collect(cometPlan) { case s: CometSparkToColumnarExec => s }.nonEmpty,
            s"aqe=$aqe: the inner join must keep its text bridge:\n${cometPlan.treeString}")
          // Outer full outer join stays on Spark.
          assert(
            collect(cometPlan) { case j: BroadcastNestedLoopJoinExec => j }.nonEmpty,
            s"aqe=$aqe: the outer full outer join must stay a Spark BNLJ:\n${cometPlan.treeString}")
        }
      }
    }
  }

  test("Spark 3.4 AQE DPP with a V1 Parquet fact and a text dimension (#6008)") {
    // On Spark 3.4, CometScanRule.transformV1Scan has an "AQE Dynamic Partition Pruning requires
    // Spark 3.5+" fallback that keeps a V1 scan with an AQE DPP filter on Spark. So this Parquet
    // fact is not native, hasOnlyNativeScans is false, and the build side is never tagged or
    // bridged. The feature stays out of the way on 3.4: DPP stays live and the answer matches
    // Spark. The SKIP_COMET_BROADCAST_TAG path needs a native fact; the Iceberg test in
    // CometBroadcastBuildSideSuite covers it. See #6008.
    assume(!isSpark35Plus, "This test covers the Spark 3.4 V1-fact DPP fallback")
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTempDir { dir =>
        val factPath = s"${dir.getAbsolutePath}/fact.parquet"
        val dimPath = s"${dir.getAbsolutePath}/dim.txt"
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark
            .range(0, 100)
            .selectExpr("CAST(id % 10 AS STRING) AS fk", "id AS v")
            .write
            .partitionBy("fk")
            .parquet(factPath)
          spark.range(0, 10).selectExpr("CAST(id AS STRING) AS value").write.text(dimPath)
        }
        spark.read.parquet(factPath).createOrReplaceTempView("dpp34_fact_txt")
        spark.read.text(dimPath).createOrReplaceTempView("dpp34_dim_txt")
        // CAST(d.value AS INT) % 2 = 0 is a filter Spark cannot infer onto f.fk, so the fact is
        // pruned only via a live DPP subquery.
        val df = sql("""
            |SELECT /*+ BROADCAST(d) */ f.v
            |FROM dpp34_fact_txt f JOIN dpp34_dim_txt d ON f.fk = d.value
            |WHERE CAST(d.value AS INT) % 2 = 0
            |""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)
        // The non-native V1 fact means the build side is never tagged, so nothing is bridged.
        assert(
          collect(cometPlan) { case s: CometSparkToColumnarExec => s }.isEmpty,
          s"3.4: the text dimension must not be bridged:\n${cometPlan.treeString}")
        // DPP is still live - the fact scan keeps a broadcast pruning subquery.
        val dppSubqueries = collectWithSubqueries(cometPlan) {
          case s: SubqueryBroadcastExec => s
          case s: CometSubqueryBroadcastExec => s
        }
        assert(
          dppSubqueries.nonEmpty,
          s"3.4: expected a live DPP broadcast subquery on the fact scan:\n${cometPlan.treeString}")
      }
    }
  }

  test("AQE DPP reuses the broadcast for a UNION ALL dimension (#6008)") {
    // The DPP subquery's build is a CometUnionExec - a columnar CometPlan, not a CometNativeExec.
    // isNativeBuildSide must accept it so CometPlanAdaptiveDynamicPruningFilters converts the
    // subquery's SubqueryBroadcastExec and Spark reuses the join's CometBroadcastExchangeExec as a
    // ReusedExchangeExec. The earlier CometNativeExec-only check dropped the reuse. See #6008.
    assume(isSpark35Plus, "Native AQE DPP requires Spark 3.5+")
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTempDir { dir =>
        val factPath = s"${dir.getAbsolutePath}/fact.parquet"
        val dim1Path = s"${dir.getAbsolutePath}/dim1.parquet"
        val dim2Path = s"${dir.getAbsolutePath}/dim2.parquet"
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark
            .range(0, 100)
            .selectExpr("CAST(id % 10 AS STRING) AS fk", "id AS v")
            .write
            .partitionBy("fk")
            .parquet(factPath)
          spark
            .range(0, 10)
            .selectExpr("CAST(id AS STRING) AS value", "id AS n")
            .write
            .parquet(dim1Path)
          spark
            .range(10, 20)
            .selectExpr("CAST(id AS STRING) AS value", "id AS n")
            .write
            .parquet(dim2Path)
        }
        spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact_u")
        spark.read.parquet(dim1Path).createOrReplaceTempView("dpp_dim1_u")
        spark.read.parquet(dim2Path).createOrReplaceTempView("dpp_dim2_u")
        val df = sql("""
            |SELECT /*+ BROADCAST(d) */ f.v
            |FROM dpp_fact_u f JOIN
            |  (SELECT value FROM dpp_dim1_u WHERE n % 2 = 0
            |   UNION ALL SELECT value FROM dpp_dim2_u WHERE n % 2 = 0) d
            |ON f.fk = d.value
            |""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)
        // The union dimension broadcast goes native.
        assert(
          collectWithSubqueries(cometPlan) { case b: CometBroadcastExchangeExec => b }.nonEmpty,
          "expected a native CometBroadcastExchangeExec for the union dimension:\n" +
            cometPlan.treeString)
        // DPP reuses that broadcast rather than building a second one.
        assert(
          collectWithSubqueries(cometPlan) { case r: ReusedExchangeExec => r }.nonEmpty,
          s"expected a ReusedExchangeExec under the DPP subquery:\n${cometPlan.treeString}")
      }
    }
  }
}

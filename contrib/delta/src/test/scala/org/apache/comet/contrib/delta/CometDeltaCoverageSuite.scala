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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.functions._

/**
 * Coverage matrix for `CometDeltaNativeScanExec`. Each test exercises one query
 * pattern (projection, filter, sort, aggregate, join, set-op, window, subquery,
 * nested-data access) and asserts via [[CometDeltaTestBase.assertDeltaNativeMatches]]
 * that BOTH:
 *   1. the executed plan contains `CometDeltaNativeScanExec` (the contrib actually
 *      engaged -- a hard guard against the "inert bridge" class of regression
 *      we fixed earlier this branch), AND
 *   2. results equal vanilla Spark+Delta (set-equal, order-independent).
 *
 * Tests are grouped roughly by SQL surface area so adding new coverage stays
 * pattern-local. Per-area tests use a single backing Delta table built once at
 * the top of the test to keep wall-clock fast.
 */
class CometDeltaCoverageSuite extends CometDeltaTestBase {

  // ---- Projection / SELECT --------------------------------------------------

  test("projection: SELECT *") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_proj_star") { tablePath =>
      writeIntStrTable(tablePath, 10)
      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("projection: SELECT specific columns prunes data schema") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_proj_cols") { tablePath =>
      writeIntStrTable(tablePath, 10)
      assertDeltaNativeMatches(tablePath, _.select("id"))
      assertDeltaNativeMatches(tablePath, _.select("name"))
    }
  }

  test("projection: arithmetic + casts in SELECT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_proj_arith") { tablePath =>
      writeIntStrTable(tablePath, 10)
      assertDeltaNativeMatches(
        tablePath,
        _.selectExpr("id", "id * 2 AS doubled", "CAST(id AS INT) AS id_int", "length(name) AS nlen"))
    }
  }

  test("projection: LIMIT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_proj_limit") { tablePath =>
      writeIntStrTable(tablePath, 50)
      // limit is order-dependent; pair with orderBy and assert on a stable set.
      assertDeltaNativeMatches(tablePath, _.orderBy("id").limit(5))
    }
  }

  test("projection: DISTINCT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_proj_distinct") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq((1L, "a"), (1L, "a"), (2L, "b"), (3L, "c"), (3L, "c"))
        .toDF("id", "name")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.distinct())
      assertDeltaNativeMatches(tablePath, _.select("id").distinct())
    }
  }

  // ---- Filters (WHERE) ------------------------------------------------------

  test("filter: equality + inequality") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_filt_eq") { tablePath =>
      writeIntStrTable(tablePath, 20)
      assertDeltaNativeMatches(tablePath, _.where("id = 5"))
      assertDeltaNativeMatches(tablePath, _.where("id != 5"))
      assertDeltaNativeMatches(tablePath, _.where("id > 10"))
      assertDeltaNativeMatches(tablePath, _.where("id <= 7"))
    }
  }

  test("filter: IN / NOT IN") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_filt_in") { tablePath =>
      writeIntStrTable(tablePath, 20)
      assertDeltaNativeMatches(tablePath, _.where("id IN (1, 3, 5, 7)"))
      assertDeltaNativeMatches(tablePath, _.where("id NOT IN (0, 10, 19)"))
    }
  }

  test("filter: IS NULL / IS NOT NULL") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_filt_null") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq((1L, Option("a")), (2L, None), (3L, Option("c")), (4L, None))
        .toDF("id", "name")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.where("name IS NULL"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NOT NULL"))
    }
  }

  test("filter: BETWEEN, LIKE, AND/OR/NOT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_filt_combo") { tablePath =>
      writeIntStrTable(tablePath, 20)
      assertDeltaNativeMatches(tablePath, _.where("id BETWEEN 3 AND 8"))
      assertDeltaNativeMatches(tablePath, _.where("name LIKE 'name_1%'"))
      assertDeltaNativeMatches(tablePath, _.where("id > 5 AND id < 15"))
      assertDeltaNativeMatches(tablePath, _.where("id < 3 OR id > 17"))
      assertDeltaNativeMatches(tablePath, _.where("NOT (id = 10)"))
    }
  }

  // ---- Sorting --------------------------------------------------------------

  test("sort: ORDER BY ASC / DESC, single + multi key") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_sort") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20).map(i => (i.toLong, s"g_${i % 3}", i % 5))
        .toDF("id", "grp", "v")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.orderBy(desc("id")))
      assertDeltaNativeMatches(tablePath, _.orderBy(asc("grp"), desc("id")))
    }
  }

  // ---- Aggregations ---------------------------------------------------------

  test("aggregate: COUNT, SUM, AVG, MIN, MAX") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_agg_basic") { tablePath =>
      writeIntStrTable(tablePath, 20)
      // NOTE: `count(*)` is intentionally NOT covered here -- Delta short-circuits
      // it to a `LocalTableScan` using the snapshot's `numRecords` stat, so the
      // scan never engages and `assertDeltaNativeMatches` would (correctly) fail.
      // `count(id)` and other column-touching aggregates do need to read parquet
      // and exercise the scan path.
      assertDeltaNativeMatches(tablePath, _.agg(count("id").as("c")))
      assertDeltaNativeMatches(tablePath, _.agg(sum("id"), avg("id"), min("id"), max("id")))
    }
  }

  test("aggregate: GROUP BY single + multi column, with HAVING") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_agg_group") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30).map(i => (i.toLong, s"g_${i % 3}", i % 5))
        .toDF("id", "grp", "v")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.groupBy("grp").agg(count("*").as("c"), sum("id").as("s")))
      assertDeltaNativeMatches(tablePath, _.groupBy("grp", "v").agg(count("*").as("c")))
      assertDeltaNativeMatches(
        tablePath,
        df => df.groupBy("grp").agg(count("*").as("c")).where("c > 5"))
    }
  }

  test("aggregate: COUNT DISTINCT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_agg_cd") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30).map(i => (i.toLong, s"g_${i % 4}"))
        .toDF("id", "grp")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.agg(countDistinct("grp").as("dg")))
    }
  }

  // ---- Joins ----------------------------------------------------------------

  test("join: self-join (inner)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_join_self") { tablePath =>
      writeIntStrTable(tablePath, 10)
      assertDeltaNativeMatches(
        tablePath,
        df => df.as("a").join(df.as("b"), col("a.id") === col("b.id")).select(col("a.id")))
    }
  }

  test("join: inner / left outer / left semi between two delta tables") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_join_lhs") { lhsPath =>
      withDeltaTable("cov_join_rhs") { rhsPath =>
        val ss = spark
        import ss.implicits._
        (0 until 10).map(i => (i.toLong, s"l_$i")).toDF("id", "l")
          .write.format("delta").save(lhsPath)
        Seq(1L, 3L, 5L, 7L, 9L, 11L).map(i => (i, s"r_$i")).toDF("id", "r")
          .write.format("delta").save(rhsPath)
        // For two-table queries we still want to verify BOTH scans are accelerated;
        // assertDeltaNativeMatches checks at least one CometDeltaNativeScanExec.
        // Run a series of join modes manually.
        val l = spark.read.format("delta").load(lhsPath)
        val r = spark.read.format("delta").load(rhsPath)
        assertJoinAcceleratedAndMatches(lhsPath, rhsPath, "inner")
        assertJoinAcceleratedAndMatches(lhsPath, rhsPath, "left")
        assertJoinAcceleratedAndMatches(lhsPath, rhsPath, "leftsemi")
        assertJoinAcceleratedAndMatches(lhsPath, rhsPath, "leftanti")
        // Silence "unused" warning for l/r:
        val _ = (l, r)
      }
    }
  }

  private def assertJoinAcceleratedAndMatches(
      lhsPath: String,
      rhsPath: String,
      joinType: String): Unit = {
    def buildPlan(): org.apache.spark.sql.DataFrame = {
      val l = spark.read.format("delta").load(lhsPath)
      val r = spark.read.format("delta").load(rhsPath)
      l.join(r, Seq("id"), joinType).orderBy("id")
    }
    val nativeDf = buildPlan()
    val nativeRows = nativeDf.collect().toSeq.map(normalizeRow)
    val plan = nativeDf.queryExecution.executedPlan
    val deltaScans = collect(plan) {
      case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
    }
    assert(
      deltaScans.size >= 2,
      s"$joinType join: expected >= 2 CometDeltaNativeScanExec, got ${deltaScans.size}\n$plan")
    withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
      val vanillaRows = buildPlan().collect().toSeq.map(normalizeRow)
      assert(
        nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
        s"$joinType join: native != vanilla\nnative=$nativeRows\nvanilla=$vanillaRows")
    }
  }

  // ---- Set operations -------------------------------------------------------

  test("setop: UNION / UNION ALL / INTERSECT / EXCEPT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_setop_a") { aPath =>
      withDeltaTable("cov_setop_b") { bPath =>
        val ss = spark
        import ss.implicits._
        (1 to 5).map(i => (i.toLong, s"x_$i")).toDF("id", "v")
          .write.format("delta").save(aPath)
        (4 to 8).map(i => (i.toLong, s"x_$i")).toDF("id", "v")
          .write.format("delta").save(bPath)
        def both(op: (org.apache.spark.sql.DataFrame, org.apache.spark.sql.DataFrame)
            => org.apache.spark.sql.DataFrame): Unit = {
          def build(): org.apache.spark.sql.DataFrame = {
            val a = spark.read.format("delta").load(aPath)
            val b = spark.read.format("delta").load(bPath)
            op(a, b).orderBy("id")
          }
          val nativeRows = build().collect().toSeq.map(normalizeRow)
          val plan = build().queryExecution.executedPlan
          val deltaScans = collect(plan) {
            case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
          }
          assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec in:\n$plan")
          withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
            val vanillaRows = build().collect().toSeq.map(normalizeRow)
            assert(
              nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
              s"native=$nativeRows\nvanilla=$vanillaRows")
          }
        }
        both((a, b) => a.union(b))
        both((a, b) => a.unionAll(b))
        both((a, b) => a.intersect(b))
        both((a, b) => a.except(b))
      }
    }
  }

  // ---- Window functions -----------------------------------------------------

  test("window: row_number / rank / lag / lead") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_window") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20).map(i => (i.toLong, s"g_${i % 3}", i % 5))
        .toDF("id", "grp", "v")
        .write.format("delta").save(tablePath)
      val w = org.apache.spark.sql.expressions.Window
        .partitionBy("grp")
        .orderBy("id")
      assertDeltaNativeMatches(
        tablePath,
        _.withColumn("rn", row_number().over(w))
          .withColumn("rk", rank().over(w))
          .withColumn("lg", lag("id", 1).over(w))
          .withColumn("ld", lead("id", 1).over(w)))
    }
  }

  // ---- Subqueries -----------------------------------------------------------

  test("subquery: scalar subquery in WHERE") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_sub_scalar") { tablePath =>
      writeIntStrTable(tablePath, 20)
      spark.read.format("delta").load(tablePath).createOrReplaceTempView("cov_sub_scalar")
      val df = spark.sql(
        "SELECT * FROM cov_sub_scalar WHERE id > (SELECT AVG(id) FROM cov_sub_scalar)")
      val rows = df.collect().toSeq.map(normalizeRow)
      val plan = df.queryExecution.executedPlan
      val deltaScans = collect(plan) {
        case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
      }
      assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec:\n$plan")
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        spark.read.format("delta").load(tablePath).createOrReplaceTempView("cov_sub_scalar_v")
        val vanillaRows = spark.sql(
          "SELECT * FROM cov_sub_scalar_v WHERE id > (SELECT AVG(id) FROM cov_sub_scalar_v)")
          .collect().toSeq.map(normalizeRow)
        assert(
          rows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"native=$rows\nvanilla=$vanillaRows")
      }
    }
  }

  test("subquery: IN subquery") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_sub_in_a") { aPath =>
      withDeltaTable("cov_sub_in_b") { bPath =>
        writeIntStrTable(aPath, 20)
        val ss = spark
        import ss.implicits._
        Seq(3L, 7L, 11L).toDF("k").write.format("delta").save(bPath)
        spark.read.format("delta").load(aPath).createOrReplaceTempView("cov_a")
        spark.read.format("delta").load(bPath).createOrReplaceTempView("cov_b")
        val df = spark.sql("SELECT * FROM cov_a WHERE id IN (SELECT k FROM cov_b)")
        val rows = df.collect().toSeq.map(normalizeRow)
        val plan = df.queryExecution.executedPlan
        val deltaScans = collect(plan) {
          case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
        }
        assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec:\n$plan")
        withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
          val vanillaRows = spark.sql("SELECT * FROM cov_a WHERE id IN (SELECT k FROM cov_b)")
            .collect().toSeq.map(normalizeRow)
          assert(
            rows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
            s"native=$rows\nvanilla=$vanillaRows")
        }
      }
    }
  }

  // ---- CTEs -----------------------------------------------------------------

  test("CTE: WITH ... SELECT chain") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_cte") { tablePath =>
      writeIntStrTable(tablePath, 20)
      spark.read.format("delta").load(tablePath).createOrReplaceTempView("cov_cte")
      val df = spark.sql(
        "WITH odd AS (SELECT * FROM cov_cte WHERE id % 2 = 1) " +
          "SELECT count(*) AS c FROM odd")
      val rows = df.collect().toSeq.map(normalizeRow)
      val plan = df.queryExecution.executedPlan
      val deltaScans = collect(plan) {
        case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
      }
      assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec:\n$plan")
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = spark.sql(
          "WITH odd AS (SELECT * FROM cov_cte WHERE id % 2 = 1) " +
            "SELECT count(*) AS c FROM odd")
          .collect().toSeq.map(normalizeRow)
        assert(rows == vanillaRows, s"native=$rows\nvanilla=$vanillaRows")
      }
    }
  }

  // ---- Coverage with partitioned tables -------------------------------------

  test("partitioned: filter + projection on partition column") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_part") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30).map(i => (i.toLong, s"v_$i", s"p_${i % 3}"))
        .toDF("id", "v", "p")
        .write.format("delta").partitionBy("p").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.where("p = 'p_1'"))
      assertDeltaNativeMatches(tablePath, _.where("p = 'p_1' AND id > 10"))
      assertDeltaNativeMatches(tablePath, _.select("p", "id"))
    }
  }

  // ---- Coverage with column-mapping enabled ---------------------------------

  test("column mapping (name): filter + project + agg") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_cm_name") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20).map(i => (i.toLong, s"name_$i", i * 1.0))
        .toDF("id", "name", "score")
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)
      assertDeltaNativeMatches(tablePath, _.where("id > 5").select("id", "name"))
      assertDeltaNativeMatches(tablePath, _.agg(sum("score").as("s")))
    }
  }

  // ---- Coverage with deletion vectors ---------------------------------------

  test("dv: projection + filter on DV-bearing table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_dv") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 5 = 0")
      // `select("id")` and SUM go through assertDeltaNativeMatches (vanilla matches
      // native in this configuration). The `where("id > 10")` variant trips the
      // same Spark+Delta in-session DeltaLog cache-staleness we hit in
      // CometDeltaColumnMappingSuite (vanilla returns rows the DV should have
      // hidden because the cached pre-DELETE snapshot is reused), so we assert
      // the accelerator engages without comparing to vanilla there.
      assertDeltaNativeMatches(tablePath, _.select("id"))
      assertDeltaNativeMatches(tablePath, _.agg(sum("id").as("s"), min("id"), max("id")))
      val df = spark.read.format("delta").load(tablePath)
        .where("id > 10").select("id", "name")
      val plan = df.queryExecution.executedPlan
      df.collect()
      val deltaScans = collect(plan) {
        case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
      }
      assert(
        deltaScans.nonEmpty,
        s"expected CometDeltaNativeScanExec on DV-bearing filtered read:\n$plan")
    }
  }

  // ---- Nested data access ---------------------------------------------------

  test("nested: struct field + array element + map value access") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cov_nested") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq(
        (1L, ("a", 1), Seq(10, 20, 30), Map("k1" -> 100, "k2" -> 200)),
        (2L, ("b", 2), Seq(40, 50), Map("k1" -> 300)))
        .toDF("id", "s", "arr", "m")
        .write.format("delta").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.selectExpr("id", "s._1 AS s1", "s._2 AS s2"))
      assertDeltaNativeMatches(tablePath, _.selectExpr("id", "arr[0] AS a0", "size(arr) AS asz"))
      assertDeltaNativeMatches(tablePath, _.selectExpr("id", "m['k1'] AS mk1"))
    }
  }

  // ---- helpers --------------------------------------------------------------

  private def writeIntStrTable(tablePath: String, n: Int): Unit = {
    val ss = spark
    import ss.implicits._
    (0 until n).map(i => (i.toLong, s"name_$i"))
      .toDF("id", "name")
      .write.format("delta").save(tablePath)
  }
}

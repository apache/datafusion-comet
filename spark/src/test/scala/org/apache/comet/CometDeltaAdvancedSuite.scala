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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec

class CometDeltaAdvancedSuite extends CometDeltaTestBase {

  test("aggregation and join over delta inputs") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("agg_join") { tablePath =>
      val ss = spark
      import ss.implicits._

      val leftPath = new java.io.File(tablePath, "left").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "right").getAbsolutePath

      (0 until 12)
        .map(i => (i.toLong, s"name_$i", (i % 3).toLong))
        .toDF("id", "name", "grp")
        .write
        .format("delta")
        .save(leftPath)

      (0 until 3)
        .map(i => (i.toLong, s"group_$i"))
        .toDF("grp_id", "grp_label")
        .write
        .format("delta")
        .save(rightPath)

      val grouped =
        spark.sql(s"SELECT grp, COUNT(*) AS n, SUM(id) AS s FROM delta.`$leftPath` GROUP BY grp")
      val groupedPlan = grouped.queryExecution.executedPlan
      assert(
        collect(groupedPlan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected CometDeltaNativeScanExec in plan, got:\n$groupedPlan")

      val nativeGrouped = grouped.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"SELECT grp, COUNT(*) AS n, SUM(id) AS s FROM delta.`$leftPath` GROUP BY grp")
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeGrouped.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
          s"native=$nativeGrouped\nvanilla=$vanilla")
      }

      val joined = spark.sql(s"""
           |SELECT l.id, l.name, r.grp_label
           |FROM delta.`$leftPath` l
           |JOIN delta.`$rightPath` r ON l.grp = r.grp_id
           |""".stripMargin)
      val joinPlan = joined.queryExecution.executedPlan
      assert(
        collect(joinPlan) { case s: CometDeltaNativeScanExec => s }.size == 2,
        s"expected 2 CometDeltaNativeScanExec in join plan:\n$joinPlan")

      val nativeJoined = joined.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
                  |SELECT l.id, l.name, r.grp_label
                  |FROM delta.`$leftPath` l
                  |JOIN delta.`$rightPath` r ON l.grp = r.grp_id
                  |""".stripMargin)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeJoined.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
          s"native=$nativeJoined\nvanilla=$vanilla")
      }
    }
  }

  test("union of two delta tables") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("union") { tablePath =>
      val ss = spark
      import ss.implicits._
      val leftPath = new java.io.File(tablePath, "l").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "r").getAbsolutePath
      (0 until 5)
        .map(i => (i.toLong, s"l_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(leftPath)
      (5 until 10)
        .map(i => (i.toLong, s"r_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(rightPath)

      val df =
        spark.sql(s"SELECT * FROM delta.`$leftPath` UNION ALL SELECT * FROM delta.`$rightPath`")
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.size == 2,
        s"expected 2 Delta scans:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"SELECT * FROM delta.`$leftPath` UNION ALL SELECT * FROM delta.`$rightPath`")
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("self-join on delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("self_join") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, (i / 2).toLong, s"name_$i"))
        .toDF("id", "grp", "name")
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.sql(s"""
        SELECT a.id, a.name, b.name AS partner
        FROM delta.`$tablePath` a
        JOIN delta.`$tablePath` b ON a.grp = b.grp AND a.id < b.id
      """)
      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT a.id, a.name, b.name AS partner
            FROM delta.`$tablePath` a
            JOIN delta.`$tablePath` b ON a.grp = b.grp AND a.id < b.id
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("LEFT OUTER JOIN of two delta tables") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("left_join") { tablePath =>
      val ss = spark
      import ss.implicits._
      val leftPath = new java.io.File(tablePath, "l").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "r").getAbsolutePath
      (0 until 5)
        .map(i => (i.toLong, s"l_$i"))
        .toDF("id", "lname")
        .write
        .format("delta")
        .save(leftPath)
      (0 until 3)
        .map(i => (i.toLong, s"r_$i"))
        .toDF("id", "rname")
        .write
        .format("delta")
        .save(rightPath)

      val df = spark.sql(s"""
        SELECT l.id, l.lname, r.rname
        FROM delta.`$leftPath` l
        LEFT JOIN delta.`$rightPath` r ON l.id = r.id
      """)
      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT l.id, l.lname, r.rname
            FROM delta.`$leftPath` l
            LEFT JOIN delta.`$rightPath` r ON l.id = r.id
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("window function over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("window") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, (i % 4).toLong, i * 1.5))
        .toDF("id", "grp", "score")
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.sql(s"""
        SELECT id, grp, score,
               ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn
        FROM delta.`$tablePath`
      """)
      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT id, grp, score,
                   ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn
            FROM delta.`$tablePath`
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("distinct and group by having") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("distinct_having") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, (i % 5).toLong, s"name_${i % 7}"))
        .toDF("id", "grp", "name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.select("grp").distinct())
      assertDeltaNativeMatches(
        tablePath,
        df =>
          df.groupBy("grp")
            .count()
            .where("count > 5"))
    }
  }

  test("dynamic partition pruning through join") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("dpp") { tablePath =>
      val ss = spark
      import ss.implicits._

      val factPath = new java.io.File(tablePath, "fact").getAbsolutePath
      val dimPath = new java.io.File(tablePath, "dim").getAbsolutePath

      (0 until 100)
        .map(i => (i.toLong, s"item_$i", Seq("us", "eu", "ap")(i % 3)))
        .toDF("id", "item", "region")
        .write
        .partitionBy("region")
        .format("delta")
        .save(factPath)

      Seq(("us", "United States"))
        .toDF("region", "region_name")
        .write
        .format("delta")
        .save(dimPath)

      val query = s"""
        SELECT f.id, f.item, d.region_name
        FROM delta.`$factPath` f
        JOIN delta.`$dimPath` d ON f.region = d.region
      """
      val df = spark.sql(query)
      val rows = df.collect()
      assert(
        rows.forall(_.getString(2) == "United States"),
        s"expected all rows to have region_name='United States'")
      assert(rows.length == 34, s"expected 34 rows (100/3 rounded), got ${rows.length}")

      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark.sql(query).collect()
        assert(
          rows.map(_.toSeq).toSet == vanilla.map(_.toSeq).toSet,
          s"DPP result differs from vanilla")
      }
    }
  }

  test("DPP prunes files at scan level") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("dpp_pruning") { tablePath =>
      val ss = spark
      import ss.implicits._

      val factPath = new java.io.File(tablePath, "fact").getAbsolutePath
      val dimPath = new java.io.File(tablePath, "dim").getAbsolutePath

      (0 until 30)
        .map(i => (i.toLong, s"item_$i", Seq("us", "eu", "ap")(i % 3)))
        .toDF("id", "item", "region")
        .repartition(1)
        .write
        .partitionBy("region")
        .format("delta")
        .save(factPath)

      Seq(("us", "United States"))
        .toDF("region", "region_name")
        .write
        .format("delta")
        .save(dimPath)

      val query = s"""
        SELECT f.id, f.item, d.region_name
        FROM delta.`$factPath` f
        JOIN delta.`$dimPath` d ON f.region = d.region
      """
      val df = spark.sql(query)
      val rows = df.collect()
      assert(rows.length == 10, s"expected 10 rows, got ${rows.length}")
      assert(rows.forall(_.getString(2) == "United States"))

      val scans = collect(df.queryExecution.executedPlan) { case s: CometDeltaNativeScanExec =>
        s
      }
      assert(scans.nonEmpty, "expected CometDeltaNativeScanExec in DPP plan")
    }
  }

  test("planning metrics: total_files and dv_files") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("metrics") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(4)
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.read.format("delta").load(tablePath)
      df.collect()

      val scans = collect(df.queryExecution.executedPlan) { case s: CometDeltaNativeScanExec =>
        s
      }
      assert(scans.nonEmpty, "expected CometDeltaNativeScanExec")

      val scan = scans.head
      assert(
        scan.metrics.contains("total_files"),
        s"missing total_files metric; available: ${scan.metrics.keys.mkString(", ")}")
      assert(scan.metrics("total_files").value > 0, "total_files should be > 0")
      assert(scan.metrics.contains("dv_files"), "missing dv_files metric")
    }
  }

  test("fallback on unsupported filesystem scheme") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("scheme_fallback") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 5)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.read.format("delta").load(tablePath)
      val scans = collect(df.queryExecution.executedPlan) { case s: CometDeltaNativeScanExec =>
        s
      }
      assert(scans.nonEmpty, "expected CometDeltaNativeScanExec for local file scheme")
    }
  }
}

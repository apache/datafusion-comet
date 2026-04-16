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

/**
 * Core read tests for the native Delta Lake scan path. Covers: basic reads, projections, filters,
 * partitioning, schema evolution, time travel, complex types, and primitive type coverage.
 *
 * Column mapping and deletion vector tests are in CometDeltaColumnMappingSuite. Joins,
 * aggregations, DPP, metrics, and other advanced queries are in CometDeltaAdvancedSuite.
 */
class CometDeltaNativeSuite extends CometDeltaTestBase {

  test("read a tiny unpartitioned delta table via the native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("smoke") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("multi-file delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multifile") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(3)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("projection pushdown reads only selected columns") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("projection") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
        .toDF("id", "name", "score", "active")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.select("id", "score"))
    }
  }

  test("partitioned delta table surfaces partition column values") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("partitioned") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12)
        .map(i => (i.toLong, s"name_$i", if (i < 6) "a" else "b"))
        .toDF("id", "name", "category")
        .write
        .partitionBy("category")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("filter pushdown returns correct rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id >= 5 AND id < 15"))
    }
  }

  test("complex types: array, map, and struct") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("complex") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 5).map { i =>
        (i.toLong, Seq(i, i + 1, i + 2), Map(s"k$i" -> s"v$i"), (s"inner_$i", i.toDouble))
      }
      rows
        .toDF("id", "tags", "props", "nested")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("predicate variety: eq, lt, gt, is null, in, and/or") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("predicates") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 20).map { i =>
        val name: String = if (i % 5 == 0) null else s"name_$i"
        (i.toLong, name, i * 1.5)
      }
      rows
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id = 7"))
      assertDeltaNativeMatches(tablePath, _.where("id < 5"))
      assertDeltaNativeMatches(tablePath, _.where("id > 15"))
      assertDeltaNativeMatches(tablePath, _.where("id <= 3 OR id >= 17"))
      assertDeltaNativeMatches(tablePath, _.where("id IN (2, 4, 6, 8)"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NULL"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NOT NULL AND score > 10.0"))
      assertDeltaNativeMatches(tablePath, _.where("NOT (id = 5)"))
    }
  }

  test("empty delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("empty") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq
        .empty[(Long, String)]
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("schema evolution: new column added in later commit") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("schema_evolution") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 5)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      (5 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("time travel by version reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("time_travel") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 5)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      (100 until 103)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)

      val native = spark.read.format("delta").option("versionAsOf", "0").load(tablePath)
      val plan = native.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected CometDeltaNativeScanExec in plan, got:\n$plan")

      val nativeRows = native.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanillaRows = spark.read
          .format("delta")
          .option("versionAsOf", "0")
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"native time-travel result did not match vanilla")
      }
      assert(nativeRows.size == 5, s"expected 5 rows from versionAsOf=0, got ${nativeRows.size}")
    }
  }

  test("time travel by timestamp reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("timestamp_travel") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 5)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      val logFile0 = new java.io.File(tablePath, "_delta_log/00000000000000000000.json")
      val t0 = logFile0.lastModified()
      val targetTs = t0 + 500L

      (100 until 102)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)

      // Set commit 1's mtime to be clearly after the target timestamp so Delta
      // resolves timestampAsOf to commit 0 deterministically (no Thread.sleep).
      val logFile1 = new java.io.File(tablePath, "_delta_log/00000000000000000001.json")
      logFile1.setLastModified(t0 + 2000L)

      val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val tsString = fmt.format(new java.util.Date(targetTs))
      assertDeltaNativeMatches(
        tablePath,
        _ => spark.read.format("delta").option("timestampAsOf", tsString).load(tablePath))
    }
  }

  test("multi-column partitioning") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multipart") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = for {
        region <- Seq("us", "eu", "ap")
        tier <- Seq("free", "pro")
        i <- 0 until 4
      } yield (i.toLong, s"name_$region${tier}_$i", region, tier)

      rows
        .toDF("id", "name", "region", "tier")
        .write
        .partitionBy("region", "tier")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("region = 'us'"))
      assertDeltaNativeMatches(tablePath, _.where("region = 'eu' AND tier = 'pro'"))
    }
  }

  test("typed partition columns: int, long, date") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("typed_partitions") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 8).map { i =>
        (
          i.toLong,
          s"name_$i",
          i % 3,
          (1000L + i),
          java.sql.Date.valueOf(f"2024-01-${(i % 5) + 1}%02d"))
      }
      rows
        .toDF("id", "name", "p_int", "p_long", "p_date")
        .write
        .partitionBy("p_int", "p_long", "p_date")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("p_int = 1"))
      assertDeltaNativeMatches(tablePath, _.where("p_date >= DATE'2024-01-03'"))
    }
  }

  test("multiple appends produce many files, native scan reads them all") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multi_append") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 5).foreach { commit =>
        (0 until 4)
          .map(i => ((commit * 10 + i).toLong, s"c${commit}_r$i"))
          .toDF("id", "name")
          .repartition(1)
          .write
          .format("delta")
          .mode(if (commit == 0) "overwrite" else "append")
          .save(tablePath)
      }

      assertDeltaNativeMatches(tablePath, identity)
      assert(spark.read.format("delta").load(tablePath).count() == 20, "expected 20 rows")
    }
  }

  test("column name case insensitivity") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("case_insensitive") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("Id", "Name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, df => df.select("id", "name"))
      assertDeltaNativeMatches(tablePath, df => df.where("id > 2"))
    }
  }

  test("reordered and duplicated projections") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("reordered_proj") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("a", "b", "c")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, df => df.select("c", "a", "b"))
      assertDeltaNativeMatches(tablePath, df => df.selectExpr("a", "a AS a2", "b"))
    }
  }

  test("deeply nested complex types") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("nested_complex") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 4).map { i =>
        (
          i.toLong,
          Seq((s"a_$i", i * 1.5), (s"b_$i", i * 2.5)),
          Map(s"k_$i" -> Seq(i, i + 1, i + 2)),
          ((i, s"inner_$i"), (i * 10).toLong))
      }
      rows
        .toDF("id", "entries", "props", "nested")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("order by and limit over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("order_limit") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i", (19 - i) * 1.5))
        .toDF("id", "name", "score")
        .repartition(4)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, df => df.orderBy("score").limit(5))
      assertDeltaNativeMatches(tablePath, df => df.orderBy(df("id").desc).limit(3))
    }
  }

  test("filter that yields an empty result") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("empty_filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id > 999"))
    }
  }

  test("COUNT(*) over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("count_star") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 25)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(3)
        .write
        .format("delta")
        .save(tablePath)

      val count = spark.sql(s"SELECT COUNT(*) AS n FROM delta.`$tablePath`").collect()
      assert(count.length == 1 && count(0).getLong(0) == 25L, s"COUNT(*) returned $count")

      val filteredCount =
        spark.sql(s"SELECT COUNT(*) AS n FROM delta.`$tablePath` WHERE name LIKE 'name_1%'")
      val plan = filteredCount.queryExecution.executedPlan
      val hasDeltaScan = collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty
      assert(hasDeltaScan, s"expected CometDeltaNativeScanExec in filtered-count plan")
      val expected = (0 until 25).count(i => s"name_$i".startsWith("name_1"))
      val actual = filteredCount.collect()(0).getLong(0)
      assert(actual == expected.toLong, s"expected $expected, got $actual")
    }
  }

  test("struct field access and array element in SELECT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("struct_access") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 6).map { i =>
        (i.toLong, (s"first_$i", s"last_$i"), Seq(i * 10, i * 20, i * 30))
      }
      rows
        .toDF("id", "name", "values")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(
        tablePath,
        _.selectExpr("id", "name._1 AS first", "name._2 AS last"))
      assertDeltaNativeMatches(
        tablePath,
        _.selectExpr("id", "values[0] AS v0", "values[2] AS v2"))
    }
  }

  test("null values throughout the data") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("nulls") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 12).map { i =>
        (
          if (i % 3 == 0) null else Long.box(i.toLong),
          if (i % 4 == 0) null else s"name_$i",
          if (i % 5 == 0) null else Double.box(i * 1.5))
      }
      rows
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("id IS NULL"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NOT NULL AND score IS NULL"))
    }
  }

  test("partitioned table with filter on non-partition column") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_data_filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 24)
        .map(i => (i.toLong, s"name_$i", if (i < 12) "hot" else "cold"))
        .toDF("id", "name", "tier")
        .write
        .partitionBy("tier")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id % 2 = 0"))
      assertDeltaNativeMatches(tablePath, _.where("tier = 'hot' AND id >= 4"))
    }
  }

  test("write, overwrite, append, read full history") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("history") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 5)
        .map(i => (i.toLong, s"v0_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)
      (0 until 3)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)
      (10 until 13)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("append")
        .save(tablePath)

      (0 to 2).foreach { v =>
        val native = spark.read
          .format("delta")
          .option("versionAsOf", v.toString)
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
          val vanilla = spark.read
            .format("delta")
            .option("versionAsOf", v.toString)
            .load(tablePath)
            .collect()
            .toSeq
            .map(normalizeRow)
          assert(
            native.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
            s"v$v mismatch")
        }
      }
    }
  }

  test("partition pruning with case-insensitive column names") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("case_insensitive_partition") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"name_$i", if (i < 10) "Hot" else "Cold"))
        .toDF("id", "name", "Tier")
        .write
        .partitionBy("Tier")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("tier = 'Hot'"))
      assertDeltaNativeMatches(tablePath, _.where("TIER = 'Cold'"))
      assertDeltaNativeMatches(tablePath, _.where("tier = 'Hot' AND id > 3"))
    }
  }

  test("wider primitive type coverage") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("primitives") { tablePath =>
      val ss = spark
      import ss.implicits._

      val rows = (0 until 8).map { i =>
        (
          i % 2 == 0,
          i.toByte,
          i.toShort,
          i,
          i.toLong,
          i.toFloat * 0.5f,
          i.toDouble * 0.25,
          s"str_$i",
          Array[Byte](i.toByte, (i + 1).toByte),
          java.sql.Date.valueOf(f"2024-01-${(i % 28) + 1}%02d"),
          new java.sql.Timestamp(1700000000000L + i * 1000L),
          BigDecimal(i) + BigDecimal("0.125"))
      }
      rows
        .toDF("b", "i8", "i16", "i32", "i64", "f32", "f64", "s", "bin", "d", "ts", "dec")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("MERGE INSERT overflow throws under ANSI storeAssignmentPolicy") {
    // Matches a failing-regression config: ansi.enabled=FALSE but
    // storeAssignmentPolicy=ANSI, Comet in OFFHEAP mode (the regression diff
    // uses offheap; CometTestBase runs onheap).
    withSQLConf(
      org.apache.spark.sql.internal.SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
      org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED.key -> "false",
      CometConf.COMET_ONHEAP_ENABLED.key -> "false") {
      withDeltaTable("merge-cast-overflow") { tablePath =>
        spark.sql(s"CREATE TABLE delta.`$tablePath` (key INT, value TINYINT) USING DELTA")
        spark.sql(s"CREATE OR REPLACE TEMP VIEW comet_src AS SELECT 0 AS key, 128 AS value")

        val e = intercept[Throwable] {
          spark.sql(s"""
            MERGE INTO delta.`$tablePath` t
            USING comet_src s
            ON t.key = s.key
            WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)
          """)
        }
        assert(
          Option(e.getMessage).exists(m =>
            m.contains("CAST_OVERFLOW") || m.contains("DELTA_CAST_OVERFLOW_IN_TABLE_WRITE") ||
              m.contains("out of range") || m.contains("128")),
          s"Expected an overflow error, got: $e\nmessage: ${e.getMessage}")
      }
    }
  }

  test("Streaming MERGE INSERT overflow throws under ANSI storeAssignmentPolicy") {
    import org.apache.spark.sql.DataFrame
    withSQLConf(
      org.apache.spark.sql.internal.SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ANSI",
      org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED.key -> "true") {
      withDeltaTable("stream-merge-overflow-src") { srcPath =>
        withDeltaTable("stream-merge-overflow-tgt") { tgtPath =>
          spark.sql(s"CREATE TABLE delta.`$srcPath` (key INT, value INT) USING DELTA")
          spark.sql(s"CREATE TABLE delta.`$tgtPath` (key INT, value TINYINT) USING DELTA")
          spark.sql(s"INSERT INTO delta.`$srcPath`(key, value) VALUES(0, 128)")

          def upsertToDelta(batch: DataFrame, batchId: Long): Unit = {
            batch.createOrReplaceTempView("mb")
            batch.sparkSession.sql(s"""
              MERGE INTO delta.`$tgtPath` t
              USING mb s
              ON s.key = t.key
              WHEN NOT MATCHED THEN INSERT *
            """)
          }

          val sourceStream = spark.readStream.format("delta").load(srcPath)
          val sw = sourceStream.writeStream
            .format("delta")
            .foreachBatch(upsertToDelta _)
            .outputMode("update")
            .start()

          val e = intercept[Throwable] {
            sw.processAllAvailable()
          }
          sw.stop()
          assert(
            Option(e.getMessage).exists(m =>
              m.contains("CAST_OVERFLOW") || m.contains("DELTA_CAST_OVERFLOW_IN_TABLE_WRITE") ||
                m.contains("out of range") || m.contains("128")),
            s"Expected overflow error, got:\n$e\nmessage=${e.getMessage}")
        }
      }
    }
  }
}

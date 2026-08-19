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

// Audit 5: filter pushdown coverage.
//
// For each Spark filter shape we want the native scan to honour (either
// applied native-side or correctly post-filtered to identical results),
// write a fixture and assert native results match vanilla. The audit is
// correctness-focused, not performance: a filter that's correctly
// post-applied is acceptable; a filter that returns wrong rows is not.
//
// Filter shapes covered:
//   * EqualTo / EqualNullSafe (=, <=>)
//   * GreaterThan/LessThan/GreaterThanOrEqual/LessThanOrEqual
//   * IsNull / IsNotNull (data and partition columns)
//   * In / NotIn
//   * StringStartsWith / EndsWith / Contains
//   * AND / OR / NOT combinations
//   * Filter on nested struct field
//   * Filter on cast-coerced literal
//
// Past regressions: stats-based data skipping returned null aggregates
// when partition filters weren't passed through to refreshedSnapshotFiles
// (commit 95de524d).
class CometDeltaFilterPushdownAuditSuite extends CometDeltaTestBase {

  private def withTestTable(name: String)(body: String => Unit): Unit = {
    withDeltaTable(name) { tablePath =>
      val ss = spark
      import ss.implicits._
      // A mix of ids, strings, nulls, and a nested struct -- enough to
      // exercise most filter shapes.
      val data = (0 until 20).map { i =>
        val s = if (i % 7 == 0) null else s"name_$i"
        (i.toLong, s, (i % 3).toLong)
      }
      data.toDF("id", "name", "bucket").repartition(2)
        .write.format("delta").save(tablePath)
      body(tablePath)
    }
  }

  test("EqualTo, EqualNullSafe") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_eq") { tablePath =>
      assertDeltaNativeMatches(tablePath, _.filter(col("id") === 5).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("name") === "name_3").orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("name") <=> null).orderBy("id"))
    }
  }

  test("GreaterThan / LessThan / GreaterThanOrEqual / LessThanOrEqual") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_cmp") { tablePath =>
      assertDeltaNativeMatches(tablePath, _.filter(col("id") > 10).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("id") >= 10).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("id") < 5).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("id") <= 5).orderBy("id"))
    }
  }

  test("IsNull / IsNotNull on data column") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_null") { tablePath =>
      assertDeltaNativeMatches(tablePath, _.filter(col("name").isNull).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("name").isNotNull).orderBy("id"))
    }
  }

  test("In / NotIn (small and large lists)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_in") { tablePath =>
      assertDeltaNativeMatches(
        tablePath, _.filter(col("id").isin(1L, 5L, 13L)).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(!col("id").isin(1L, 5L, 13L)).orderBy("id"))
      // Larger list (forces InSet on Spark side)
      val many = (0L until 30L by 2L).toSeq
      assertDeltaNativeMatches(
        tablePath, _.filter(col("id").isin(many: _*)).orderBy("id"))
    }
  }

  test("StringStartsWith / EndsWith / Contains") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_str") { tablePath =>
      assertDeltaNativeMatches(
        tablePath, _.filter(col("name").startsWith("name_1")).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(col("name").endsWith("9")).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(col("name").contains("_1")).orderBy("id"))
    }
  }

  test("AND / OR / NOT combinations") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_bool") { tablePath =>
      assertDeltaNativeMatches(
        tablePath,
        _.filter((col("id") > 5 && col("id") < 15) || col("bucket") === 0)
          .orderBy("id"))
      assertDeltaNativeMatches(
        tablePath,
        _.filter(!(col("id") < 10) && col("name").isNotNull).orderBy("id"))
    }
  }

  test("filter on partition column (data-skipping path)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("flt_part") { tablePath =>
      val ss = spark
      import ss.implicits._
      val data = (0 until 30).map(i => (i.toLong, s"n_$i", (i % 5).toLong))
      data.toDF("id", "name", "p").write.format("delta")
        .partitionBy("p").save(tablePath)
      assertDeltaNativeMatches(tablePath, _.filter(col("p") === 2L).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(col("p").isin(1L, 3L)).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("p") > 2L).orderBy("id"))
    }
  }

  test("filter on nested struct field") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("flt_nest") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, s STRUCT<a:INT,b:STRING>)
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, NAMED_STRUCT('a', 10, 'b', 'x')),
           |(2, NAMED_STRUCT('a', 20, 'b', 'y')),
           |(3, NAMED_STRUCT('a', 30, 'b', NULL)),
           |(4, NULL)""".stripMargin)
      assertDeltaNativeMatches(
        tablePath, _.filter(col("s.a") > 15).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(col("s.b").isNull).orderBy("id"))
    }
  }

  test("filter on cast-coerced literal (BIGINT vs INT)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withTestTable("flt_cast") { tablePath =>
      // `id` is BIGINT; literal 5 is INT and Spark inserts a cast.
      assertDeltaNativeMatches(tablePath, _.filter(col("id") === 5).orderBy("id"))
      assertDeltaNativeMatches(tablePath, _.filter(col("id") < lit(10)).orderBy("id"))
    }
  }

  // Regression: DV + range filter. On a DV-bearing single-file table
  // (rows 0..29, DELETE id%4=0), `id > 10 AND id < 25` once dropped rows
  // 11..18 because data-filter pushdown to parquet skipped non-matching
  // rows, decoupling DeltaSyntheticColumnsStream's running row-offset
  // counter from the true parquet row_index -- so the DV bitmap got
  // applied to the wrong stream positions. Fixed by suppressing
  // data-filter pushdown when `emit_is_row_deleted` is set (core_glue.rs).
  // This MUST now match vanilla exactly.
  test("DV + range filter returns correct rows (regression)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("flt_dv") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30).map(i => (i.toLong, s"n_$i")).toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 4 = 0")
      assertDeltaNativeMatches(
        tablePath, _.filter(col("id") > 10 && col("id") < 25).orderBy("id"))
      // Also exercise filters whose pushdown range straddles the DV'd
      // indexes from both sides.
      assertDeltaNativeMatches(
        tablePath, _.filter(col("id") < 10).orderBy("id"))
      assertDeltaNativeMatches(
        tablePath, _.filter(col("id") >= 5 && col("id") <= 28).orderBy("id"))
    }
  }
}

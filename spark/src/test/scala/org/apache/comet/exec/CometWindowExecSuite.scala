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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometWindowExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lead, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometWindowExecSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  test("lead/lag should return the default value if the offset row does not exist") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      checkSparkAnswer(sql("""
                             |SELECT
                             |  lag(123, 100, 321) OVER (ORDER BY id) as lag,
                             |  lead(123, 100, 321) OVER (ORDER BY id) as lead
                             |FROM (SELECT 1 as id) tmp
      """.stripMargin))

      checkSparkAnswer(sql("""
                             |SELECT
                             |  lag(123, 100, a) OVER (ORDER BY id) as lag,
                             |  lead(123, 100, a) OVER (ORDER BY id) as lead
                             |FROM (SELECT 1 as id, 2 as a) tmp
      """.stripMargin))
    }
  }

  test("window query with rangeBetween") {

    // values are int
    val df = Seq(1, 2, 4, 3, 2, 1).toDF("value")
    val window = Window.orderBy($"value".desc)

    // ranges are long
    val df2 = df.select(
      $"value",
      sum($"value").over(window.rangeBetween(Window.unboundedPreceding, 1L)),
      sum($"value").over(window.rangeBetween(1L, Window.unboundedFollowing)))

    // Comet does not support RANGE BETWEEN
    // https://github.com/apache/datafusion-comet/issues/1246
    val (_, cometPlan) = checkSparkAnswer(df2)
    val cometWindowExecs = collect(cometPlan) { case w: CometWindowExec =>
      w
    }
    assert(cometWindowExecs.isEmpty)
  }

  // based on Spark's SQLWindowFunctionSuite test of the same name
  test("window function: partition and order expressions") {
    for (shuffleMode <- Seq("auto", "native", "jvm")) {
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
        val df =
          Seq((1, "a", 5), (2, "a", 6), (3, "b", 7), (4, "b", 8), (5, "c", 9), (6, "c", 10)).toDF(
            "month",
            "area",
            "product")
        df.createOrReplaceTempView("windowData")
        val df2 = sql("""
                        |select month, area, product, sum(product + 1) over (partition by 1 order by 2)
                        |from windowData
          """.stripMargin)
        checkSparkAnswer(df2)
        val cometShuffles = collect(df2.queryExecution.executedPlan) {
          case _: CometShuffleExchangeExec => true
        }
        if (shuffleMode == "jvm" || shuffleMode == "auto") {
          assert(cometShuffles.length == 1)
        } else {
          // we fall back to Spark for shuffle because we do not support
          // native shuffle with a LocalTableScan input, and we do not fall
          // back to Comet columnar shuffle due to
          // https://github.com/apache/datafusion-comet/issues/1248
          assert(cometShuffles.isEmpty)
        }
      }
    }
  }

  test(
    "fall back to Spark when the partition spec and order spec are not the same for window function") {
    withTempView("test") {
      sql("""
            |CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
            | (1, true), (1, false),
            |(2, true), (3, false), (4, true) AS test(k, v)
            |""".stripMargin)

      val df = sql("""
          SELECT k, v, every(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg
                     |""".stripMargin)
      checkSparkAnswer(df)
    }
  }

  test("Native window operator should be CometUnaryExec") {
    withTempView("testData") {
      sql("""
            |CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
            |(null, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
            |(1, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
            |(1, 2L, 2.5D, date("2017-08-02"), timestamp_seconds(1502000000), "a"),
            |(2, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "a"),
            |(1, null, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "b"),
            |(2, 3L, 3.3D, date("2017-08-03"), timestamp_seconds(1503000000), "b"),
            |(3, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "b"),
            |(null, null, null, null, null, null),
            |(3, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), null)
            |AS testData(val, val_long, val_double, val_date, val_timestamp, cate)
            |""".stripMargin)
      val df1 = sql("""
                      |SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val ROWS CURRENT ROW)
                      |FROM testData ORDER BY cate, val
                      |""".stripMargin)
      checkSparkAnswer(df1)
    }
  }

  test("Window range frame with long boundary should not fail") {
    val df =
      Seq((1L, "1"), (1L, "1"), (2147483650L, "1"), (3L, "2"), (2L, "1"), (2147483650L, "2"))
        .toDF("key", "value")

    checkSparkAnswer(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(0, 2147483648L))))
    checkSparkAnswer(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(-2147483649L, 0))))
  }

  test("Unsupported window expression should fall back to Spark") {
    checkAnswer(
      spark.sql("select sum(a) over () from values 1.0, 2.0, 3.0 T(a)"),
      Row(6.0) :: Row(6.0) :: Row(6.0) :: Nil)
    checkAnswer(
      spark.sql("select avg(a) over () from values 1.0, 2.0, 3.0 T(a)"),
      Row(2.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("Repeated shuffle exchange don't fail") {
    Seq("true", "false").foreach { aqeEnabled =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled,
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
        val df =
          Seq(("a", 1, 1), ("a", 2, 2), ("b", 1, 3), ("b", 1, 4)).toDF("key1", "key2", "value")
        val windowSpec = Window.partitionBy("key1", "key2").orderBy("value")

        val windowed = df
          // repartition by subset of window partitionBy keys which satisfies ClusteredDistribution
          .repartition($"key1")
          .select(lead($"key1", 1).over(windowSpec), lead($"value", 1).over(windowSpec))

        checkSparkAnswer(windowed)
      }
    }
  }

  test("aggregate window function for all types") {
    val numValues = 2048

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempPath { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            Seq(128, numValues + 100).foreach { batchSize =>
              withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
                (1 to 11).foreach { col =>
                  val aggregateFunctions =
                    List(s"COUNT(_$col)", s"MAX(_$col)", s"MIN(_$col)", s"SUM(_$col)")
                  aggregateFunctions.foreach { function =>
                    val df1 = sql(s"SELECT $function OVER() FROM tbl")
                    checkSparkAnswerWithTolerance(df1, 1e-6)

                    val df2 = sql(s"SELECT $function OVER(order by _2) FROM tbl")
                    checkSparkAnswerWithTolerance(df2, 1e-6)

                    val df3 = sql(s"SELECT $function OVER(order by _2 desc) FROM tbl")
                    checkSparkAnswerWithTolerance(df3, 1e-6)

                    val df4 = sql(s"SELECT $function OVER(partition by _2 order by _2) FROM tbl")
                    checkSparkAnswerWithTolerance(df4, 1e-6)
                  }
                }

                // SUM doesn't work for Date type. org.apache.spark.sql.AnalysisException will be thrown.
                val aggregateFunctionsWithoutSum = List("COUNT(_12)", "MAX(_12)", "MIN(_12)")
                aggregateFunctionsWithoutSum.foreach { function =>
                  val df1 = sql(s"SELECT $function OVER() FROM tbl")
                  checkSparkAnswerWithTolerance(df1, 1e-6)

                  val df2 = sql(s"SELECT $function OVER(order by _2) FROM tbl")
                  checkSparkAnswerWithTolerance(df2, 1e-6)

                  val df3 = sql(s"SELECT $function OVER(order by _2 desc) FROM tbl")
                  checkSparkAnswerWithTolerance(df3, 1e-6)

                  val df4 = sql(s"SELECT $function OVER(partition by _2 order by _2) FROM tbl")
                  checkSparkAnswerWithTolerance(df4, 1e-6)
                }
              }
            }
          }
        }
      }
    }
  }

  ignore("Windows support") {
    Seq("true", "false").foreach(aqeEnabled =>
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
        withParquetTable((0 until 10).map(i => (i, 10 - i)), "t1") { // TODO: test nulls
          val aggregateFunctions =
            List(
              "COUNT(_1)",
              "COUNT(*)",
              "MAX(_1)",
              "MIN(_1)",
              "SUM(_1)"
            ) // TODO: Test all the aggregates

          aggregateFunctions.foreach { function =>
            val queries = Seq(
              s"SELECT $function OVER() FROM t1",
              s"SELECT $function OVER(order by _2) FROM t1",
              s"SELECT $function OVER(order by _2 desc) FROM t1",
              s"SELECT $function OVER(partition by _2 order by _2) FROM t1",
              s"SELECT $function OVER(rows between 1 preceding and 1 following) FROM t1",
              s"SELECT $function OVER(order by _2 rows between 1 preceding and current row) FROM t1",
              s"SELECT $function OVER(order by _2 rows between current row and 1 following) FROM t1")

            queries.foreach { query =>
              checkSparkAnswerAndFallbackReason(query, "Window expressions are not supported")
            }
          }
        }
      })
  }

  test("window: simple COUNT(*) without frame") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("SELECT a, b, c, COUNT(*) OVER () as cnt FROM window_test")
      checkSparkAnswerAndFallbackReason(df, "Window expressions are not supported")
    }
  }

  test("window: simple SUM with PARTITION BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("SELECT a, b, c, SUM(c) OVER (PARTITION BY a) as sum_c FROM window_test")
      checkSparkAnswerAndFallbackReason(df, "Window expressions are not supported")
    }
  }

  // TODO: AVG with PARTITION BY and ORDER BY not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: AVG with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df =
        sql("SELECT a, b, c, AVG(c) OVER (PARTITION BY a ORDER BY b) as avg_c FROM window_test")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: MIN and MAX with ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          MIN(c) OVER (ORDER BY b) as min_c,
          MAX(c) OVER (ORDER BY b) as max_c
        FROM window_test
      """)
      checkSparkAnswerAndFallbackReason(df, "Window expressions are not supported")
    }
  }

  // TODO: COUNT with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW produces incorrect results
  // Returns wrong cnt values - ordering issue causes swapped values for rows with same partition
  ignore("window: COUNT with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          COUNT(*) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cnt
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: SUM with ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING produces incorrect results
  ignore("window: SUM with ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: AVG with ROWS BETWEEN produces incorrect results
  // Returns wrong avg_c values - calculation appears to be off
  ignore("window: AVG with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          AVG(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: SUM with ROWS BETWEEN produces incorrect results
  ignore("window: SUM with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: COUNT with ROWS BETWEEN not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: COUNT with ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          COUNT(*) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as cnt
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: MAX with ROWS BETWEEN UNBOUNDED not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: MAX with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          MAX(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: ROW_NUMBER not supported
  // Falls back to Spark Window operator
  ignore("window: ROW_NUMBER with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) as row_num
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: RANK not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: RANK with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          RANK() OVER (PARTITION BY a ORDER BY b) as rnk
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: DENSE_RANK not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: DENSE_RANK with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          DENSE_RANK() OVER (PARTITION BY a ORDER BY b) as dense_rnk
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: PERCENT_RANK not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: PERCENT_RANK with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          PERCENT_RANK() OVER (PARTITION BY a ORDER BY b) as pct_rnk
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: NTILE not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: NTILE with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          NTILE(4) OVER (PARTITION BY a ORDER BY b) as ntile_4
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: LAG produces incorrect results
  ignore("window: LAG with default offset") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LAG(c) OVER (PARTITION BY a ORDER BY b) as lag_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: LAG with offset 2 produces incorrect results
  ignore("window: LAG with offset 2 and default value") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LAG(c, 2, -1) OVER (PARTITION BY a ORDER BY b) as lag_c_2
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: LEAD produces incorrect results
  ignore("window: LEAD with default offset") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LEAD(c) OVER (PARTITION BY a ORDER BY b) as lead_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: LEAD with offset 2 produces incorrect results
  ignore("window: LEAD with offset 2 and default value") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LEAD(c, 2, -1) OVER (PARTITION BY a ORDER BY b) as lead_c_2
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: FIRST_VALUE causes encoder error
  // org.apache.spark.SparkUnsupportedOperationException: [ENCODER_NOT_FOUND] Not found an encoder of the type Any
  ignore("window: FIRST_VALUE with default ignore nulls") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, if (i % 7 == 0) null else i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          FIRST_VALUE(c) OVER (PARTITION BY a ORDER BY b) as first_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: LAST_VALUE causes encoder error
  // org.apache.spark.SparkUnsupportedOperationException: [ENCODER_NOT_FOUND] Not found an encoder of the type Any
  ignore("window: LAST_VALUE with ROWS frame") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, if (i % 7 == 0) null else i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LAST_VALUE(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: NTH_VALUE returns incorrect results - produces 0 instead of null for first row,
  ignore("window: NTH_VALUE with position 2") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          NTH_VALUE(c, 2) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as nth_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: CUME_DIST not supported - falls back to Spark Window operator
  // Error: "Partitioning and sorting specifications must be the same"
  ignore("window: CUME_DIST with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          CUME_DIST() OVER (PARTITION BY a ORDER BY b) as cume_dist
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Multiple window functions with mixed frame types (RowFrame and RangeFrame)
  ignore("window: multiple window functions in single query") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) as row_num,
          RANK() OVER (PARTITION BY a ORDER BY b) as rnk,
          SUM(c) OVER (PARTITION BY a ORDER BY b) as sum_c,
          AVG(c) OVER (PARTITION BY a ORDER BY b) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Different window specifications not fully supported
  // Falls back to Spark Project and Window operators
  ignore("window: different window specifications in single query") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b) as sum_by_a,
          SUM(c) OVER (PARTITION BY b ORDER BY a) as sum_by_b,
          COUNT(*) OVER () as total_count
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: ORDER BY DESC with aggregation not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: ORDER BY DESC with aggregation") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b DESC) as sum_c_desc
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Multiple PARTITION BY columns not supported
  // Falls back to Spark Window operator
  ignore("window: multiple PARTITION BY columns") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i % 2, i))
        .toDF("a", "b", "c", "d")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, d, c,
          SUM(c) OVER (PARTITION BY a, b ORDER BY d) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Multiple ORDER BY columns not supported
  // Falls back to Spark Window operator
  ignore("window: multiple ORDER BY columns") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i % 2, i))
        .toDF("a", "b", "c", "d")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, d, c,
          ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, d, c) as row_num
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: RANGE BETWEEN with numeric ORDER BY not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: RANGE BETWEEN with numeric ORDER BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i, i * 2))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i, i * 2))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Complex expressions in window functions not fully supported
  // Falls back to Spark Project operator
  ignore("window: complex expression in window function") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c * 2 + 1) OVER (PARTITION BY a ORDER BY b) as sum_expr
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Window function with WHERE clause not supported
  // Falls back to Spark Window operator - "Partitioning and sorting specifications must be the same"
  ignore("window: window function with WHERE clause") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b) as sum_c
        FROM window_test
        WHERE a > 0
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: Window function with GROUP BY not fully supported
  // Falls back to Spark Project and Window operators
  ignore("window: window function with GROUP BY") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, SUM(c) as total_c,
          RANK() OVER (ORDER BY SUM(c) DESC) as rnk
        FROM window_test
        GROUP BY a, b
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: ROWS BETWEEN with negative offset produces incorrect results
  ignore("window: ROWS BETWEEN with negative offset") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          AVG(c) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // TODO: All ranking functions together produce incorrect row_num values
  ignore("window: all ranking functions together") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) as row_num,
          RANK() OVER (PARTITION BY a ORDER BY b) as rnk,
          DENSE_RANK() OVER (PARTITION BY a ORDER BY b) as dense_rnk,
          PERCENT_RANK() OVER (PARTITION BY a ORDER BY b) as pct_rnk,
          CUME_DIST() OVER (PARTITION BY a ORDER BY b) as cume_dist,
          NTILE(3) OVER (PARTITION BY a ORDER BY b) as ntile_3
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }
}

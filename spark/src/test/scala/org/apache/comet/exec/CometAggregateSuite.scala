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

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.optimizer.EliminateSorts
import org.apache.spark.sql.comet.CometHashAggregateExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{avg, count_distinct, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

/**
 * Test suite dedicated to Comet native aggregate operator
 */
class CometAggregateSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("avg decimal") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(random, spark, filename, 10000, DataGenOptions())
      }
      val tableName = "avg_decimal"
      withTable(tableName) {
        val table = spark.read.parquet(filename).coalesce(1)
        table.createOrReplaceTempView(tableName)
        // we fall back to Spark for avg on decimal due to the following issue
        // https://github.com/apache/datafusion-comet/issues/1371
        // once this is fixed, we should change this test to
        // checkSparkAnswerAndNumOfAggregates
        checkSparkAnswer(s"SELECT c1, avg(c7) FROM $tableName GROUP BY c1 ORDER BY c1")
      }
    }
  }

  test("stddev_pop should return NaN for some cases") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXPR_STDDEV_ENABLED.key -> "true") {
      Seq(true, false).foreach { nullOnDivideByZero =>
        withSQLConf("spark.sql.legacy.statisticalAggregate" -> nullOnDivideByZero.toString) {

          val data: Seq[(Float, Int)] = Seq((Float.PositiveInfinity, 1))
          withParquetTable(data, "tbl", false) {
            val df = sql("SELECT stddev_pop(_1), stddev_pop(_2) FROM tbl")
            checkSparkAnswerAndOperator(df)
          }
        }
      }
    }
  }

  test("count with aggregation filter") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val df1 = sql("SELECT count(DISTINCT 2), count(DISTINCT 2,3)")
      checkSparkAnswer(df1)

      val df2 = sql("SELECT count(DISTINCT 2), count(DISTINCT 3,2)")
      checkSparkAnswer(df2)
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

  test("multiple column distinct count") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val df1 = Seq(
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("a", "b", "d"),
        ("x", "y", "z"),
        ("x", "q", null.asInstanceOf[String]))
        .toDF("key1", "key2", "key3")

      checkSparkAnswer(df1.agg(count_distinct($"key1", $"key2")))
      checkSparkAnswer(df1.agg(count_distinct($"key1", $"key2", $"key3")))
      checkSparkAnswer(df1.groupBy($"key1").agg(count_distinct($"key2", $"key3")))
    }
  }

  test("Only trigger Comet Final aggregation on Comet partial aggregation") {
    withTempView("lowerCaseData") {
      lowerCaseData.createOrReplaceTempView("lowerCaseData")
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
        val df = sql("SELECT LAST(n) FROM lowerCaseData")
        checkSparkAnswer(df)
      }
    }
  }

  test(
    "Average expression in Comet Final should handle " +
      "all null inputs from partial Spark aggregation") {
    withTempView("allNulls") {
      allNulls.createOrReplaceTempView("allNulls")
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
        val df = sql("select sum(a), avg(a) from allNulls")
        checkSparkAnswer(df)
      }
    }
  }

  test("Aggregation without aggregate expressions should use correct result expressions") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 10000, 10, false)
        withParquetTable(path.toUri.toString, "tbl") {
          val df = sql("SELECT _g5 FROM tbl GROUP BY _g1, _g2, _g3, _g4, _g5")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("Final aggregation should not bind to the input of partial aggregation") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test")
          makeParquetFile(path, 10000, 10, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            val df = sql("SELECT * FROM tbl").groupBy("_g1").agg(sum($"_3" + $"_g3"))
            checkSparkAnswerAndOperator(df)
          }
        }
      }
    }
  }

  test("Ensure traversed operators during finding first partial aggregation are all native") {
    withTable("lineitem", "part") {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

        sql(
          "CREATE TABLE lineitem(l_extendedprice DOUBLE, l_quantity DOUBLE, l_partkey STRING) USING PARQUET")
        sql("INSERT INTO TABLE lineitem VALUES (1.0, 1.0, '1')")

        sql(
          "CREATE TABLE part(p_partkey STRING, p_brand STRING, p_container STRING) USING PARQUET")
        sql("INSERT INTO TABLE part VALUES ('1', 'Brand#23', 'MED BOX')")

        val df = sql("""select
            sum(l_extendedprice) / 7.0 as avg_yearly
            from
            lineitem,
            part
              where
              p_partkey = l_partkey
              and p_brand = 'Brand#23'
          and p_container = 'MED BOX'
          and l_quantity < (
            select
          0.2 * avg(l_quantity)
          from
          lineitem
          where
          l_partkey = p_partkey
          )""")
        checkAnswer(df, Row(null))
      }
    }
  }

  test("SUM decimal supports emit.first") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> EliminateSorts.ruleName,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test")
          makeParquetFile(path, 10000, 10, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            checkSparkAnswerAndOperator(
              sql("SELECT * FROM tbl").sort("_g1").groupBy("_g1").agg(sum("_8")))
          }
        }
      }
    }
  }

  test("AVG decimal supports emit.first") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> EliminateSorts.ruleName,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test")
          makeParquetFile(path, 10000, 10, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            checkSparkAnswerAndOperator(
              sql("SELECT * FROM tbl").sort("_g1").groupBy("_g1").agg(avg("_8")))
          }
        }
      }
    }
  }

  test("Fix NPE in partial decimal sum") {
    val table = "tbl"
    withTable(table) {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        withTable(table) {
          sql(s"CREATE TABLE $table(col DECIMAL(5, 2)) USING PARQUET")
          sql(s"INSERT INTO TABLE $table VALUES (CAST(12345.01 AS DECIMAL(5, 2)))")
          val df = sql(s"SELECT SUM(col + 100000.01) FROM $table")
          checkAnswer(df, Row(null))
        }
      }
    }
  }

  test("fix: Decimal Average should not enable native final aggregation") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test")
          makeParquetFile(path, 1000, 10, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            checkSparkAnswer("SELECT _g1, AVG(_7) FROM tbl GROUP BY _g1")
            checkSparkAnswer("SELECT _g1, AVG(_8) FROM tbl GROUP BY _g1")
            checkSparkAnswer("SELECT _g1, AVG(_9) FROM tbl GROUP BY _g1")
          }
        }
      }
    }
  }

  test("trivial case") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable((0 until 5).map(i => (i, i)), "tbl", dictionaryEnabled) {
        val df1 = sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2")
        checkAnswer(df1, Row(0, 0) :: Row(1, 1) :: Row(2, 2) :: Row(3, 3) :: Row(4, 4) :: Nil)

        val df2 = sql("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2")
        checkAnswer(df2, Row(0, 1) :: Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)

        val df3 = sql("SELECT COUNT(_1), COUNT(_2) FROM tbl")
        checkAnswer(df3, Row(5, 5) :: Nil)

        checkSparkAnswerAndOperator("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
      }
    }
  }

  test("avg") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 10).map(i => ((i + 1) * (i + 1), (i + 1) / 2)),
        "tbl",
        dictionaryEnabled) {

        checkSparkAnswerAndOperator("SELECT _2, AVG(_1) FROM tbl GROUP BY _2")
        checkSparkAnswerAndOperator("SELECT AVG(_2) FROM tbl")
      }
    }
  }

  test("count, avg with null") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int, col2 int) using parquet")
          sql(s"insert into $table values(1, 1), (2, 1), (3, 2), (null, 2), (null, 1)")
          checkSparkAnswerAndOperator(s"SELECT COUNT(col1) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT col2, COUNT(col1) FROM $table GROUP BY col2")
          checkSparkAnswerAndOperator(s"SELECT avg(col1) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT col2, avg(col1) FROM $table GROUP BY col2")
        }
      }
    }
  }

  test("SUM/AVG non-decimal overflow") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(Seq((0, 100.toLong), (0, Long.MaxValue)), "tbl", dictionaryEnabled) {
        checkSparkAnswerAndOperator("SELECT SUM(_2) FROM tbl GROUP BY _1")
        checkSparkAnswerAndOperator("SELECT AVG(_2) FROM tbl GROUP BY _1")
      }
    }
  }

  test("simple SUM, COUNT, MIN, MAX, AVG with non-distinct group keys") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable((0 until 5).map(i => (i, i % 2)), "tbl", dictionaryEnabled) {
        val df1 = sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2")
        checkAnswer(df1, Row(0, 6) :: Row(1, 4) :: Nil)
        val df2 = sql("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2")
        checkAnswer(df2, Row(0, 3) :: Row(1, 2) :: Nil)
        checkSparkAnswerAndOperator("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
        checkSparkAnswerAndOperator("SELECT _2, AVG(_1) FROM tbl GROUP BY _2")
      }
    }
  }

  test("group-by on variable length types") {
    Seq(true, false).foreach { nativeShuffleEnabled =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withSQLConf(
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> nativeShuffleEnabled.toString,
          CometConf.COMET_SHUFFLE_MODE.key -> "native") {
          withParquetTable(
            (0 until 100).map(i => (i, (i % 10).toString)),
            "tbl",
            dictionaryEnabled) {
            val n = if (nativeShuffleEnabled) 2 else 1
            checkSparkAnswerAndNumOfAggregates("SELECT _2, SUM(_1) FROM tbl GROUP BY _2", n)
            checkSparkAnswerAndNumOfAggregates("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2", n)
            checkSparkAnswerAndNumOfAggregates("SELECT _2, MIN(_1) FROM tbl GROUP BY _2", n)
            checkSparkAnswerAndNumOfAggregates("SELECT _2, MAX(_1) FROM tbl GROUP BY _2", n)
            checkSparkAnswerAndNumOfAggregates("SELECT _2, AVG(_1) FROM tbl GROUP BY _2", n)
          }
        }
      }
    }
  }

  test("simple SUM, COUNT, MIN, MAX, AVG with non-distinct + null group keys") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 10).map { i =>
          (i, if (i % 3 == 0) null.asInstanceOf[Int] else i % 3)
        },
        "tbl",
        dictionaryEnabled) {
        val df1 = sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2")
        checkAnswer(df1, Row(null.asInstanceOf[Int], 18) :: Row(1, 12) :: Row(2, 15) :: Nil)

        val df2 = sql("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2")
        checkAnswer(df2, Row(null.asInstanceOf[Int], 4) :: Row(1, 3) :: Row(2, 3) :: Nil)

        val df3 = sql("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
        checkAnswer(df3, Row(null.asInstanceOf[Int], 0, 9) :: Row(1, 1, 7) :: Row(2, 2, 8) :: Nil)
        checkSparkAnswerAndOperator(sql("SELECT _2, AVG(_1) FROM tbl GROUP BY _2"))
      }
    }
  }

  test("simple SUM, COUNT, MIN, MAX, AVG with null aggregates") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 10).map { i =>
          (
            if (i % 2 == 0) null.asInstanceOf[Int] else i,
            if (i % 3 == 0) null.asInstanceOf[Int] else i % 3)
        },
        "tbl",
        dictionaryEnabled) {
        val df1 = sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2")
        checkAnswer(df1, Row(null.asInstanceOf[Int], 12) :: Row(1, 8) :: Row(2, 5) :: Nil)

        val df2 = sql("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2")
        checkAnswer(df2, Row(null.asInstanceOf[Int], 4) :: Row(1, 3) :: Row(2, 3) :: Nil)

        val df3 = sql("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
        checkAnswer(df3, Row(null.asInstanceOf[Int], 0, 9) :: Row(1, 0, 7) :: Row(2, 0, 5) :: Nil)

        checkSparkAnswerAndOperator(sql("SELECT _2, AVG(_1) FROM tbl GROUP BY _2"))
      }
    }
  }

  test("simple SUM, MIN, MAX, AVG with all nulls") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 10).map { i =>
          (null.asInstanceOf[Int], if (i % 3 == 0) null.asInstanceOf[Int] else i % 3)
        },
        "tbl",
        dictionaryEnabled) {
        val df = sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2")
        checkAnswer(
          df,
          Seq(
            Row(null.asInstanceOf[Int], null.asInstanceOf[Int]),
            Row(1, null.asInstanceOf[Int]),
            Row(2, null.asInstanceOf[Int])))

        val df2 = sql("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
        checkAnswer(
          df2,
          Seq(
            Row(null.asInstanceOf[Int], null.asInstanceOf[Int], null.asInstanceOf[Int]),
            Row(1, null.asInstanceOf[Int], null.asInstanceOf[Int]),
            Row(2, null.asInstanceOf[Int], null.asInstanceOf[Int])))
        checkSparkAnswerAndOperator(sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2"))
      }
    }
  }

  test("SUM, COUNT, MIN, MAX, AVG on float & double") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT _g5, SUM(_5), COUNT(_5), MIN(_5), MAX(_5), AVG(_5) FROM tbl GROUP BY _g5")
          checkSparkAnswerAndOperator(
            "SELECT _g6, SUM(_6), COUNT(_6), MIN(_6), MAX(_6), AVG(_6) FROM tbl GROUP BY _g6")
        }
      }
    }
  }

  test("SUM, MIN, MAX, AVG for NaN, -0.0 and 0.0") {
    // NaN should be grouped together, and -0.0 and 0.0 should also be grouped together
    Seq(true, false).foreach { dictionaryEnabled =>
      val data: Seq[(Float, Int)] = Seq(
        (Float.NaN, 1),
        (-0.0.asInstanceOf[Float], 2),
        (0.0.asInstanceOf[Float], 3),
        (Float.NaN, 4))
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
        withParquetTable(data, "tbl", dictionaryEnabled) {
          checkSparkAnswer("SELECT SUM(_2), MIN(_2), MAX(_2), _1 FROM tbl GROUP BY _1")
          checkSparkAnswer("SELECT MIN(_1), MAX(_1), MIN(_2), MAX(_2) FROM tbl")
          checkSparkAnswer("SELECT AVG(_2), _1 FROM tbl GROUP BY _1")
          checkSparkAnswer("SELECT AVG(_1), AVG(_2) FROM tbl")
        }
      }
    }
  }

  test("SUM/MIN/MAX/AVG on decimal") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          checkSparkAnswer("SELECT _g1, SUM(_7), MIN(_7), MAX(_7), AVG(_7) FROM tbl GROUP BY _g1")
          checkSparkAnswer("SELECT _g1, SUM(_8), MIN(_8), MAX(_8), AVG(_8) FROM tbl GROUP BY _g1")
          checkSparkAnswer("SELECT _g1, SUM(_9), MIN(_9), MAX(_9), AVG(_9) FROM tbl GROUP BY _g1")
        }
      }
    }
  }

  test("multiple SUM/MIN/MAX/AVG on decimal and non-decimal") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          checkSparkAnswer(
            "SELECT _g1, COUNT(_6), COUNT(_7), SUM(_6), SUM(_7), MIN(_6), MIN(_7), MAX(_6), MAX(_7), AVG(_6), AVG(_7) FROM tbl GROUP BY _g1")
          checkSparkAnswer(
            "SELECT _g1, COUNT(_7), COUNT(_8), SUM(_7), SUM(_8), MIN(_7), MIN(_8), MAX(_7), MAX(_8), AVG(_7), AVG(_8) FROM tbl GROUP BY _g1")
          checkSparkAnswer(
            "SELECT _g1, COUNT(_8), COUNT(_9), SUM(_8), SUM(_9), MIN(_8), MIN(_9), MAX(_8), MAX(_9), AVG(_8), AVG(_9) FROM tbl GROUP BY _g1")
          checkSparkAnswer(
            "SELECT _g1, COUNT(_9), COUNT(_1), SUM(_9), SUM(_1), MIN(_9), MIN(_1), MAX(_9), MAX(_1), AVG(_9), AVG(_1) FROM tbl GROUP BY _g1")
        }
      }
    }
  }

  test("SUM/AVG on decimal with different precisions") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          Seq("SUM", "AVG").foreach { FN =>
            checkSparkAnswerAndOperator(
              s"SELECT _g1, $FN(_8 + CAST(1 AS DECIMAL(20, 10))) FROM tbl GROUP BY _g1")
            checkSparkAnswerAndOperator(
              s"SELECT _g1, $FN(_8 - CAST(-1 AS DECIMAL(10, 3))) FROM tbl GROUP BY _g1")
            checkSparkAnswerAndOperator(
              s"SELECT _g1, $FN(_9 * CAST(3.14 AS DECIMAL(4, 3))) FROM tbl GROUP BY _g1")
            checkSparkAnswerAndOperator(
              s"SELECT _g1, $FN(_9 / CAST(1.2345 AS DECIMAL(35, 10))) FROM tbl GROUP BY _g1")
          }
        }
      }
    }
  }

  test("SUM decimal with DF") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(true, false).foreach { nativeShuffleEnabled =>
        withSQLConf(
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> nativeShuffleEnabled.toString,
          CometConf.COMET_SHUFFLE_MODE.key -> "native") {
          withTempDir { dir =>
            val path = new Path(dir.toURI.toString, "test")
            makeParquetFile(path, 1000, 20, dictionaryEnabled)
            withParquetTable(path.toUri.toString, "tbl") {
              val expectedNumOfCometAggregates = if (nativeShuffleEnabled) 2 else 1

              checkSparkAnswerAndNumOfAggregates(
                "SELECT _g2, SUM(_7) FROM tbl GROUP BY _g2",
                expectedNumOfCometAggregates)
              checkSparkAnswerAndNumOfAggregates(
                "SELECT _g3, SUM(_8) FROM tbl GROUP BY _g3",
                expectedNumOfCometAggregates)
              checkSparkAnswerAndNumOfAggregates(
                "SELECT _g4, SUM(_9) FROM tbl GROUP BY _g4",
                expectedNumOfCometAggregates)
              checkSparkAnswerAndNumOfAggregates(
                "SELECT SUM(_7) FROM tbl",
                expectedNumOfCometAggregates)
              checkSparkAnswerAndNumOfAggregates(
                "SELECT SUM(_8) FROM tbl",
                expectedNumOfCometAggregates)
              checkSparkAnswerAndNumOfAggregates(
                "SELECT SUM(_9) FROM tbl",
                expectedNumOfCometAggregates)
            }
          }
        }
      }
    }
  }

  test("COUNT/MIN/MAX on date, timestamp") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT _g1, COUNT(_10), MIN(_10), MAX(_10) FROM tbl GROUP BY _g1")
          checkSparkAnswerAndOperator(
            "SELECT _g1, COUNT(_11), MIN(_11), MAX(_11) FROM tbl GROUP BY _g1")
          checkSparkAnswerAndOperator(
            "SELECT _g1, COUNT(_12), MIN(_12), MAX(_12) FROM tbl GROUP BY _g1")
        }
      }
    }
  }

  // TODO re-enable once https://github.com/apache/datafusion-comet/issues/1646 is implemented
  ignore("single group-by column + aggregate column, multiple batches, no null") {
    val numValues = 10000

    Seq(1, 100, 10000).foreach { numGroups =>
      Seq(128, 1024, numValues + 1).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withParquetTable(
              (0 until numValues).map(i => (i, Random.nextInt() % numGroups)),
              "tbl",
              dictionaryEnabled) {
              withView("v") {
                sql("CREATE TEMP VIEW v AS SELECT _1, _2 FROM tbl ORDER BY _1")
                checkSparkAnswerAndOperator(
                  "SELECT _2, SUM(_1), SUM(DISTINCT _1), MIN(_1), MAX(_1), COUNT(_1)," +
                    " COUNT(DISTINCT _1), AVG(_1), FIRST(_1), LAST(_1) FROM v GROUP BY _2")
              }
            }
          }
        }
      }
    }
  }

  // TODO re-enable once https://github.com/apache/datafusion-comet/issues/1646 is implemented
  ignore("multiple group-by columns + single aggregate column (first/last), with nulls") {
    val numValues = 10000

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(128, numValues + 100).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                withView("v") {
                  sql("CREATE TEMP VIEW v AS SELECT _g1, _g2, _3 FROM tbl ORDER BY _3")
                  checkSparkAnswerAndOperator(
                    "SELECT _g1, _g2, FIRST(_3) FROM v GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                  checkSparkAnswerAndOperator(
                    "SELECT _g1, _g2, LAST(_3) FROM v GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                  checkSparkAnswerAndOperator(
                    "SELECT _g1, _g2, FIRST(_3) IGNORE NULLS FROM v GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                  checkSparkAnswerAndOperator(
                    "SELECT _g1, _g2, LAST(_3) IGNORE NULLS FROM v GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                }
              }
            }
          }
        }
      }
    }
  }

  test("multiple group-by columns + single aggregate column, with nulls") {
    val numValues = 10000

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(128, numValues + 100).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                checkSparkAnswer(
                  "SELECT _g1, _g2, SUM(_3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                checkSparkAnswer(
                  "SELECT _g1, _g2, COUNT(_3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                checkSparkAnswer(
                  "SELECT _g1, _g2, SUM(DISTINCT _3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                checkSparkAnswer(
                  "SELECT _g1, _g2, COUNT(DISTINCT _3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                checkSparkAnswer(
                  "SELECT _g1, _g2, MIN(_3), MAX(_3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
                checkSparkAnswer(
                  "SELECT _g1, _g2, AVG(_3) FROM tbl GROUP BY _g1, _g2 ORDER BY _g1, _g2")
              }
            }
          }
        }
      }
    }
  }

  test("string should be supported") {
    withTable("t") {
      sql("CREATE TABLE t(v VARCHAR(3), i INT) USING PARQUET")
      sql("INSERT INTO t VALUES ('c', 1)")
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
        checkSparkAnswerAndNumOfAggregates("SELECT v, sum(i) FROM t GROUP BY v ORDER BY v", 1)
      }
    }
  }

  // TODO re-enable once https://github.com/apache/datafusion-comet/issues/1646 is implemented
  ignore("multiple group-by columns + multiple aggregate column (first/last), with nulls") {
    val numValues = 10000

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(128, numValues + 100).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                withView("v") {
                  sql("CREATE TEMP VIEW v AS SELECT _g3, _g4, _3, _4 FROM tbl ORDER BY _3, _4")
                  checkSparkAnswerAndOperator(
                    "SELECT _g3, _g4, FIRST(_3), FIRST(_4) FROM v GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                  checkSparkAnswerAndOperator(
                    "SELECT _g3, _g4, LAST(_3), LAST(_4) FROM v GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                }
              }
            }
          }
        }
      }
    }

  }

  test("multiple group-by columns + multiple aggregate column, with nulls") {
    val numValues = 10000

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(128, numValues + 100).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                checkSparkAnswer(
                  "SELECT _g3, _g4, SUM(_3), SUM(_4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, SUM(DISTINCT _3), SUM(DISTINCT _4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, COUNT(_3), COUNT(_4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, COUNT(DISTINCT _3), COUNT(DISTINCT _4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, MIN(_3), MAX(_3), MIN(_4), MAX(_4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, AVG(_3), AVG(_4) FROM tbl GROUP BY _g3, _g4 ORDER BY _g3, _g4")
              }
            }
          }
        }
      }
    }
  }

  // TODO re-enable once https://github.com/apache/datafusion-comet/issues/1646 is implemented
  ignore("all types first/last, with nulls") {
    val numValues = 2048

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempPath { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            Seq(128, numValues + 100).foreach { batchSize =>
              withSQLConf(
                CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
                CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {

                // Test all combinations of different aggregation & group-by types
                (1 to 14).foreach { gCol =>
                  withView("v") {
                    sql(s"CREATE TEMP VIEW v AS SELECT _g$gCol, _1, _2, _3, _4 " +
                      "FROM tbl ORDER BY _1, _2, _3, _4")
                    checkSparkAnswerAndOperator(
                      s"SELECT _g$gCol, FIRST(_1), FIRST(_2), FIRST(_3), " +
                        s"FIRST(_4), LAST(_1), LAST(_2), LAST(_3), LAST(_4) FROM v GROUP BY _g$gCol ORDER BY _g$gCol")
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("first/last with ignore null") {
    val data = Range(0, 8192).flatMap(n => Seq((n, 1), (n, 2))).toDF("a", "b")
    withTempDir { dir =>
      val filename = s"${dir.getAbsolutePath}/first_last_ignore_null.parquet"
      data.write.parquet(filename)
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "100") {
        spark.read.parquet(filename).createOrReplaceTempView("t1")
        for (expr <- Seq("first", "last")) {
          // deterministic query that should return one non-null value per group
          val df = spark.sql(
            s"SELECT a, $expr(IF(b==1,null,b)) IGNORE NULLS FROM t1 GROUP BY a ORDER BY a")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("all types, with nulls") {
    val numValues = 2048

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempPath { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            Seq(128, numValues + 100).foreach { batchSize =>
              withSQLConf(
                CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
                CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {

                // Test all combinations of different aggregation & group-by types
                (1 to 14).foreach { gCol =>
                  checkSparkAnswer(s"SELECT _g$gCol, SUM(_1), SUM(_2), COUNT(_3), COUNT(_4), " +
                    s"MIN(_1), MAX(_4), AVG(_2), AVG(_4) FROM tbl GROUP BY _g$gCol ORDER BY _g$gCol")
                  checkSparkAnswer(
                    s"SELECT _g$gCol, SUM(DISTINCT _3) FROM tbl GROUP BY _g$gCol ORDER BY _g$gCol")
                  checkSparkAnswer(
                    s"SELECT _g$gCol, COUNT(DISTINCT _1) FROM tbl GROUP BY _g$gCol ORDER BY _g$gCol")
                }
              }
            }
          }
        }
      }
    }
  }

  test("test final count") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(false, true).foreach { dictionaryEnabled =>
        withParquetTable((0 until 5).map(i => (i, i % 2)), "tbl", dictionaryEnabled) {
          checkSparkAnswerAndNumOfAggregates("SELECT _2, COUNT(_1) FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates("select count(_1) from tbl", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, COUNT(_1), SUM(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT COUNT(_1), COUNT(_2) FROM tbl", 2)
        }
      }
    }
  }

  test("test final min/max") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withParquetTable((0 until 5).map(i => (i, i % 2)), "tbl", dictionaryEnabled) {
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1), MAX(_1), COUNT(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT MIN(_1), MAX(_1), COUNT(_1) FROM tbl", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1), MAX(_1), COUNT(_1), SUM(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT MIN(_1), MIN(_2), MAX(_1), MAX(_2), COUNT(_1), COUNT(_2) FROM tbl",
            2)
        }
      }
    }
  }

  test("test final min/max/count with result expressions") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withParquetTable((0 until 5).map(i => (i, i % 2)), "tbl", dictionaryEnabled) {
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1) + 2, COUNT(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2, COUNT(_1) + 2 FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2 + 2, COUNT(_1) FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1) + MAX(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2, MIN(_1) + _2 FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2 + 2, MIN(_1), MAX(_1), COUNT(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1), MAX(_1) + 2, COUNT(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2, SUM(_1) + 2 FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2 + 2, SUM(_1) FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates("SELECT _2, SUM(_1 + 1) FROM tbl GROUP BY _2", 2)

          // result expression is unsupported by Comet, so only partial aggregation should be used
          val df = sql(
            "SELECT _2, MIN(_1) + java_method('java.lang.Math', 'random') " +
              "FROM tbl GROUP BY _2")
          assert(getNumCometHashAggregate(df) == 1)
        }
      }
    }
  }

  test("test final sum") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(false, true).foreach { dictionaryEnabled =>
        withParquetTable((0L until 5L).map(i => (i, i % 2)), "tbl", dictionaryEnabled) {
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, SUM(_1), MIN(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates("SELECT SUM(_1) FROM tbl", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1), MAX(_1), COUNT(_1), SUM(_1), AVG(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT MIN(_1), MIN(_2), MAX(_1), MAX(_2), COUNT(_1), COUNT(_2), SUM(_1), SUM(_2) FROM tbl",
            2)
        }
      }
    }
  }

  test("avg/sum overflow on decimal(38, _)") {
    withSQLConf(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
      val table = "overflow_decimal_38"
      withTable(table) {
        sql(s"create table $table(a decimal(38, 2), b INT) using parquet")
        sql(s"insert into $table values(42.00, 1), (999999999999999999999999999999999999.99, 1)")
        checkSparkAnswerAndNumOfAggregates(s"select sum(a) from $table", 2)
        sql(s"insert into $table values(42.00, 2), (99999999999999999999999999999999.99, 2)")
        sql(s"insert into $table values(999999999999999999999999999999999999.99, 3)")
        sql(s"insert into $table values(99999999999999999999999999999999.99, 4)")
        checkSparkAnswerAndNumOfAggregates(
          s"select avg(a), sum(a) from $table group by b order by b",
          2)
      }
    }
  }

  test("test final avg") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withParquetTable(
          (0 until 5).map(i => (i.toDouble, i.toDouble % 2)),
          "tbl",
          dictionaryEnabled) {
          checkSparkAnswerAndNumOfAggregates("SELECT _2 , AVG(_1) FROM tbl GROUP BY _2", 2)
          checkSparkAnswerAndNumOfAggregates("SELECT AVG(_1) FROM tbl", 2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT _2, MIN(_1), MAX(_1), COUNT(_1), SUM(_1), AVG(_1) FROM tbl GROUP BY _2",
            2)
          checkSparkAnswerAndNumOfAggregates(
            "SELECT MIN(_1), MIN(_2), MAX(_1), MAX(_2), COUNT(_1), COUNT(_2), SUM(_1), SUM(_2), AVG(_1), AVG(_2) FROM tbl",
            2)
        }
      }
    }
  }

  test("final decimal avg") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withSQLConf("parquet.enable.dictionary" -> dictionaryEnabled.toString) {
          val table = s"final_decimal_avg_$dictionaryEnabled"
          withTable(table) {
            sql(s"create table $table(a decimal(38, 37), b INT) using parquet")
            sql(s"insert into $table values(-0.0000000000000000000000000000000000002, 1)")
            sql(s"insert into $table values(-0.0000000000000000000000000000000000002, 1)")
            sql(s"insert into $table values(-0.0000000000000000000000000000000000004, 2)")
            sql(s"insert into $table values(-0.0000000000000000000000000000000000004, 2)")
            sql(s"insert into $table values(-0.00000000000000000000000000000000000002, 3)")
            sql(s"insert into $table values(-0.00000000000000000000000000000000000002, 3)")
            sql(s"insert into $table values(-0.00000000000000000000000000000000000004, 4)")
            sql(s"insert into $table values(-0.00000000000000000000000000000000000004, 4)")
            sql(s"insert into $table values(0.13344406545919155429936259114971302408, 5)")
            sql(s"insert into $table values(0.13344406545919155429936259114971302408, 5)")

            checkSparkAnswerAndNumOfAggregates(s"SELECT b , AVG(a) FROM $table GROUP BY b", 2)
            checkSparkAnswerAndNumOfAggregates(s"SELECT AVG(a) FROM $table", 2)
            checkSparkAnswerAndNumOfAggregates(
              s"SELECT b, MIN(a), MAX(a), COUNT(a), SUM(a), AVG(a) FROM $table GROUP BY b",
              2)
            checkSparkAnswerAndNumOfAggregates(
              s"SELECT MIN(a), MAX(a), COUNT(a), SUM(a), AVG(a) FROM $table",
              2)
          }
        }
      }
    }
  }

  test("test partial avg") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 5).map(i => (i.toDouble, i.toDouble % 2)),
        "tbl",
        dictionaryEnabled) {
        withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
          checkSparkAnswerAndNumOfAggregates("SELECT _2 , AVG(_1) FROM tbl GROUP BY _2", 1)
        }
      }
    }
  }

  test("avg null handling") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      val table = "avg_null_handling"
      withTable(table) {
        sql(s"create table $table(a double, b double) using parquet")
        sql(s"insert into $table values(1, 1.0)")
        sql(s"insert into $table values(null, null)")
        sql(s"insert into $table values(1, 2.0)")
        sql(s"insert into $table values(null, null)")
        sql(s"insert into $table values(2, null)")
        sql(s"insert into $table values(2, null)")

        val query = sql(s"select a, AVG(b) from $table GROUP BY a")
        checkSparkAnswerAndOperator(query)
      }
    }
  }

  test("Decimal Avg with DF") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(true, false).foreach { nativeShuffleEnabled =>
        withSQLConf(
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> nativeShuffleEnabled.toString,
          CometConf.COMET_SHUFFLE_MODE.key -> "native",
          CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
          withTempDir { dir =>
            val path = new Path(dir.toURI.toString, "test")
            makeParquetFile(path, 1000, 20, dictionaryEnabled)
            withParquetTable(path.toUri.toString, "tbl") {
              val expectedNumOfCometAggregates = if (nativeShuffleEnabled) 2 else 1

              checkSparkAnswerAndNumOfAggregates(
                "SELECT _g2, AVG(_7) FROM tbl GROUP BY _g2",
                expectedNumOfCometAggregates)

              checkSparkAnswerWithTol("SELECT _g3, AVG(_8) FROM tbl GROUP BY _g3")
              assert(getNumCometHashAggregate(
                sql("SELECT _g3, AVG(_8) FROM tbl GROUP BY _g3")) == expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT _g4, AVG(_9) FROM tbl GROUP BY _g4",
                expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT AVG(_7) FROM tbl",
                expectedNumOfCometAggregates)

              checkSparkAnswerWithTol("SELECT AVG(_8) FROM tbl")
              assert(getNumCometHashAggregate(
                sql("SELECT AVG(_8) FROM tbl")) == expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT AVG(_9) FROM tbl",
                expectedNumOfCometAggregates)
            }
          }
        }
      }
    }
  }

  // TODO enable once https://github.com/apache/datafusion-comet/issues/1267 is implemented
  ignore("distinct") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq("native", "jvm").foreach { cometShuffleMode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> cometShuffleMode) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
              val cometColumnShuffleEnabled = cometShuffleMode == "jvm"
              val table = "test"
              withTable(table) {
                sql(s"create table $table(col1 int, col2 int, col3 int) using parquet")
                sql(
                  s"insert into $table values(1, 1, 1), (1, 1, 1), (1, 3, 1), (1, 4, 2), (5, 3, 2)")

                var expectedNumOfCometAggregates = 2

                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT DISTINCT(col2) FROM $table",
                  expectedNumOfCometAggregates)

                expectedNumOfCometAggregates = 4

                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT COUNT(distinct col2) FROM $table",
                  expectedNumOfCometAggregates)

                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT COUNT(distinct col2), col1 FROM $table group by col1",
                  expectedNumOfCometAggregates)

                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT SUM(distinct col2) FROM $table",
                  expectedNumOfCometAggregates)

                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT SUM(distinct col2), col1 FROM $table group by col1",
                  expectedNumOfCometAggregates)

                checkSparkAnswerAndNumOfAggregates(
                  "SELECT COUNT(distinct col2), SUM(distinct col2), col1, COUNT(distinct col2)," +
                    s" SUM(distinct col2) FROM $table group by col1",
                  expectedNumOfCometAggregates)

                expectedNumOfCometAggregates = if (cometColumnShuffleEnabled) 2 else 1
                checkSparkAnswerAndNumOfAggregates(
                  "SELECT COUNT(col2), MIN(col2), COUNT(DISTINCT col2), SUM(col2)," +
                    s" SUM(DISTINCT col2), COUNT(DISTINCT col2), col1 FROM $table group by col1",
                  expectedNumOfCometAggregates)
              }
            }
          }
        }
      }
    }
  }

  // TODO re-enable once https://github.com/apache/datafusion-comet/issues/1646 is implemented
  ignore("first/last") {
    withSQLConf(
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      Seq(true, false).foreach { dictionary =>
        withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
          val table = "test"
          withTable(table) {
            sql(s"create table $table(col1 int, col2 int, col3 int) using parquet")
            sql(
              s"insert into $table values(4, 1, 1), (4, 1, 1), (3, 3, 1)," +
                " (2, 4, 2), (1, 3, 2), (null, 1, 1)")
            withView("t") {
              sql("CREATE VIEW t AS SELECT col1, col3 FROM test ORDER BY col1")

              var expectedNumOfCometAggregates = 2
              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1), LAST(col1) FROM t",
                expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1), LAST(col1), MIN(col1), COUNT(col1) FROM t",
                expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1), LAST(col1), col3 FROM t GROUP BY col3",
                expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1), LAST(col1), MIN(col1), COUNT(col1), col3 FROM t GROUP BY col3",
                expectedNumOfCometAggregates)

              expectedNumOfCometAggregates = 0
              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1, true), LAST(col1) FROM t",
                expectedNumOfCometAggregates)

              checkSparkAnswerAndNumOfAggregates(
                "SELECT FIRST(col1), LAST(col1, true), col3 FROM t GROUP BY col3",
                expectedNumOfCometAggregates)
            }
          }
        }
      }
    }
  }

  test("test bool_and/bool_or") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq("native", "jvm").foreach { cometShuffleMode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> cometShuffleMode) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
              val table = "test"
              withTable(table) {
                sql(s"create table $table(a boolean, b int) using parquet")
                sql(s"insert into $table values(true, 1)")
                sql(s"insert into $table values(false, 2)")
                sql(s"insert into $table values(true, 3)")
                sql(s"insert into $table values(true, 3)")
                // Spark maps BOOL_AND to MIN and BOOL_OR to MAX
                checkSparkAnswerAndNumOfAggregates(
                  s"SELECT MIN(a), MAX(a), BOOL_AND(a), BOOL_OR(a) FROM $table",
                  2)
              }
            }
          }
        }
      }
    }
  }

  test("bitwise aggregate") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      Seq(true, false).foreach { dictionary =>
        withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
          val table = "test"
          withTable(table) {
            sql(s"create table $table(col1 long, col2 int, col3 short, col4 byte) using parquet")
            sql(
              s"insert into $table values(4, 1, 1, 3), (4, 1, 1, 3), (3, 3, 1, 4)," +
                " (2, 4, 2, 5), (1, 3, 2, 6), (null, 1, 1, 7)")
            val expectedNumOfCometAggregates = 2
            checkSparkAnswerAndNumOfAggregates(
              "SELECT BIT_AND(col1), BIT_OR(col1), BIT_XOR(col1)," +
                " BIT_AND(col2), BIT_OR(col2), BIT_XOR(col2)," +
                " BIT_AND(col3), BIT_OR(col3), BIT_XOR(col3)," +
                " BIT_AND(col4), BIT_OR(col4), BIT_XOR(col4) FROM test",
              expectedNumOfCometAggregates)

            // Make sure the combination of BITWISE aggregates and other aggregates work OK
            checkSparkAnswerAndNumOfAggregates(
              "SELECT BIT_AND(col1), BIT_OR(col1), BIT_XOR(col1)," +
                " BIT_AND(col2), BIT_OR(col2), BIT_XOR(col2)," +
                " BIT_AND(col3), BIT_OR(col3), BIT_XOR(col3)," +
                " BIT_AND(col4), BIT_OR(col4), BIT_XOR(col4), MIN(col1), COUNT(col1) FROM test",
              expectedNumOfCometAggregates)

            checkSparkAnswerAndNumOfAggregates(
              "SELECT BIT_AND(col1), BIT_OR(col1), BIT_XOR(col1)," +
                " BIT_AND(col2), BIT_OR(col2), BIT_XOR(col2)," +
                " BIT_AND(col3), BIT_OR(col3), BIT_XOR(col3)," +
                " BIT_AND(col4), BIT_OR(col4), BIT_XOR(col4), col3 FROM test GROUP BY col3",
              expectedNumOfCometAggregates)

            // Make sure the combination of BITWISE aggregates and other aggregates work OK
            // with group by
            checkSparkAnswerAndNumOfAggregates(
              "SELECT BIT_AND(col1), BIT_OR(col1), BIT_XOR(col1)," +
                " BIT_AND(col2), BIT_OR(col2), BIT_XOR(col2)," +
                " BIT_AND(col3), BIT_OR(col3), BIT_XOR(col3)," +
                " BIT_AND(col4), BIT_OR(col4), BIT_XOR(col4)," +
                " MIN(col1), COUNT(col1), col3 FROM test GROUP BY col3",
              expectedNumOfCometAggregates)
          }
        }
      }
    }
  }

  def setupAndTestAggregates(
      table: String,
      data: Seq[(Any, Any, Any)],
      dataTypes: (String, String, String),
      aggregates: String): Unit = {
    val (type1, type2, type3) = dataTypes
    withTable(table) {
      sql(s"create table $table(col1 $type1, col2 $type2, col3 $type3) using parquet")
      val values = data
        .map { case (c1, c2, c3) =>
          s"($c1, $c2, $c3)"
        }
        .mkString(", ")
      sql(s"insert into $table values $values")

      val expectedNumOfCometAggregates = 2

      checkSparkAnswerWithTolAndNumOfAggregates(
        s"SELECT $aggregates FROM $table",
        expectedNumOfCometAggregates)

      checkSparkAnswerWithTolAndNumOfAggregates(
        s"SELECT $aggregates FROM $table GROUP BY col3",
        expectedNumOfCometAggregates)
    }
  }

  test("covariance & correlation") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq("jvm", "native").foreach { cometShuffleMode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> cometShuffleMode) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
              Seq(true, false).foreach { nullOnDivideByZero =>
                withSQLConf(
                  "spark.sql.legacy.statisticalAggregate" -> nullOnDivideByZero.toString) {
                  val table = "test"
                  val aggregates =
                    "covar_samp(col1, col2), covar_pop(col1, col2), corr(col1, col2)"
                  setupAndTestAggregates(
                    table,
                    Seq((1, 4, 1), (2, 5, 1), (3, 6, 2)),
                    ("double", "double", "double"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((1, 4, 3), (2, -5, 3), (3, 6, 1)),
                    ("double", "double", "double"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((1.1, 4.1, 2.3), (2, 5, 1.5), (3, 6, 2.3)),
                    ("double", "double", "double"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq(
                      (1, 4, 1),
                      (2, 5, 2),
                      (3, 6, 3),
                      (1.1, 4.4, 1),
                      (2.2, 5.5, 2),
                      (3.3, 6.6, 3)),
                    ("double", "double", "double"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((1, 4, 1), (2, 5, 2), (3, 6, 3)),
                    ("int", "int", "int"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((1, 4, 2), (null, null, 2), (3, 6, 1), (3, 3, 1)),
                    ("int", "int", "int"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((1, 4, 1), (null, 5, 1), (2, 5, 2), (9, null, 2), (3, 6, 2)),
                    ("int", "int", "int"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((null, null, 1), (1, 2, 1), (null, null, 2)),
                    ("int", "int", "int"),
                    aggregates)
                  setupAndTestAggregates(
                    table,
                    Seq((null, null, 1), (null, null, 1), (null, null, 2)),
                    ("int", "int", "int"),
                    aggregates)
                }
              }
            }
          }
        }
      }
    }
  }

  test("var_pop and var_samp") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq("native", "jvm").foreach { cometShuffleMode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> cometShuffleMode) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
              Seq(true, false).foreach { nullOnDivideByZero =>
                withSQLConf(
                  "spark.sql.legacy.statisticalAggregate" -> nullOnDivideByZero.toString) {
                  val table = "test"
                  withTable(table) {
                    sql(s"create table $table(col1 int, col2 int, col3 int, col4 float, col5 double, col6 int) using parquet")
                    sql(s"insert into $table values(1, null, null, 1.1, 2.2, 1)," +
                      " (2, null, null, 3.4, 5.6, 1), (3, null, 4, 7.9, 2.4, 2)")
                    val expectedNumOfCometAggregates = 2
                    checkSparkAnswerWithTolAndNumOfAggregates(
                      "SELECT var_samp(col1), var_samp(col2), var_samp(col3), var_samp(col4), var_samp(col5) FROM test",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerWithTolAndNumOfAggregates(
                      "SELECT var_pop(col1), var_pop(col2), var_pop(col3), var_pop(col4), var_samp(col5) FROM test",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerAndNumOfAggregates(
                      "SELECT var_samp(col1), var_samp(col2), var_samp(col3), var_samp(col4), var_samp(col5)" +
                        " FROM test GROUP BY col6",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerAndNumOfAggregates(
                      "SELECT var_pop(col1), var_pop(col2), var_pop(col3), var_pop(col4), var_samp(col5)" +
                        " FROM test GROUP BY col6",
                      expectedNumOfCometAggregates)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("stddev_pop and stddev_samp") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXPR_STDDEV_ENABLED.key -> "true") {
      Seq("native", "jvm").foreach { cometShuffleMode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> cometShuffleMode) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
              Seq(true, false).foreach { nullOnDivideByZero =>
                withSQLConf(
                  "spark.sql.legacy.statisticalAggregate" -> nullOnDivideByZero.toString) {
                  val table = "test"
                  withTable(table) {
                    sql(s"create table $table(col1 int, col2 int, col3 int, col4 float, " +
                      "col5 double, col6 int) using parquet")
                    sql(s"insert into $table values(1, null, null, 1.1, 2.2, 1), " +
                      "(2, null, null, 3.4, 5.6, 1), (3, null, 4, 7.9, 2.4, 2)")
                    val expectedNumOfCometAggregates = 2
                    checkSparkAnswerWithTolAndNumOfAggregates(
                      "SELECT stddev_samp(col1), stddev_samp(col2), stddev_samp(col3), " +
                        "stddev_samp(col4), stddev_samp(col5) FROM test",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerWithTolAndNumOfAggregates(
                      "SELECT stddev_pop(col1), stddev_pop(col2), stddev_pop(col3), " +
                        "stddev_pop(col4), stddev_pop(col5) FROM test",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerAndNumOfAggregates(
                      "SELECT stddev_samp(col1), stddev_samp(col2), stddev_samp(col3), " +
                        "stddev_samp(col4), stddev_samp(col5) FROM test GROUP BY col6",
                      expectedNumOfCometAggregates)
                    checkSparkAnswerWithTolAndNumOfAggregates(
                      "SELECT stddev_pop(col1), stddev_pop(col2), stddev_pop(col3), " +
                        "stddev_pop(col4), stddev_pop(col5) FROM test GROUP BY col6",
                      expectedNumOfCometAggregates)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  protected def checkSparkAnswerAndNumOfAggregates(query: String, numAggregates: Int): Unit = {
    val df = sql(query)
    checkSparkAnswer(df)
    val actualNumAggregates = getNumCometHashAggregate(df)
    assert(
      actualNumAggregates == numAggregates,
      s"Expected $numAggregates Comet aggregate operators, but found $actualNumAggregates")
  }

  protected def checkSparkAnswerWithTolAndNumOfAggregates(
      query: String,
      numAggregates: Int,
      absTol: Double = 1e-6): Unit = {
    val df = sql(query)
    checkSparkAnswerWithTol(df, absTol)
    val actualNumAggregates = getNumCometHashAggregate(df)
    assert(
      actualNumAggregates == numAggregates,
      s"Expected $numAggregates Comet aggregate operators, but found $actualNumAggregates")
  }

  def getNumCometHashAggregate(df: DataFrame): Int = {
    val sparkPlan = stripAQEPlan(df.queryExecution.executedPlan)
    sparkPlan.collect { case s: CometHashAggregateExec => s }.size
  }

  test("groupby with map column") {
    assume(isSpark40Plus, "Groupby on map type is supported in Spark 4.0 and beyond")

    def runTests(tableName: String): Unit = {
      // Group on second map column with just aggregate.
      checkSparkAnswerAndOperator(s"select count(*) AS cnt from $tableName group by _2")

      // Group on second map column with just grouping column.
      checkSparkAnswerAndOperator(s"select _2 from $tableName group by _2")

      // Group on second map column with different aggregates.
      checkSparkAnswerAndOperator(s"select _2, count(*) AS cnt from $tableName group by _2")
      checkSparkAnswerAndOperator(s"select _2, sum(_1) AS total from $tableName group by _2")
      checkSparkAnswerAndOperator(
        s"select _2, count(*) AS cnt, sum(_1) AS sum_val from $tableName group by _2")

      // Group on second map column with aggregate and filtering.
      checkSparkAnswerAndOperator(
        s"select _2, count(*) AS cnt from $tableName group by _2 having count(*) > 1")
      checkSparkAnswerAndOperator(
        s"select _2, count(*) AS cnt from $tableName WHERE _2 IS not null group by _2")

      // Group on second map column with aggregate and order by.
      checkSparkAnswerAndOperator(
        s"select _2, count(*) AS cnt from $tableName group by _2 order by cnt DESC")

      // Group on third map column with aggregate.
      checkSparkAnswerAndOperator(s"select _3, count(*) AS cnt from $tableName group by _3")
      checkSparkAnswerAndOperator(s"select _3, sum(_1) AS total from $tableName group by _3")

      // Group on third map column with different aggregates.
      checkSparkAnswerAndOperator(
        s"select _3, count(*) AS cnt, sum(_1) AS sum_val from $tableName group by _3")

      // Group on third map column with aggregate and filtering.
      checkSparkAnswerAndOperator(
        s"select _3, count(*) AS cnt from $tableName WHERE _3 IS not null group by _3")

      // Group on third map column with aggregate and order by.
      checkSparkAnswerAndOperator(
        s"select _3, count(*) AS cnt from $tableName group by _3 order by cnt DESC")

      // Group on both map columns with aggregate.
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt from $tableName group by _2, _3")

      // Group on both map columns with aggregate. The columns are selected in different order.
      checkSparkAnswerAndOperator(
        s"select _3, count(*), _2 AS cnt from $tableName group by _2, _3")

      // Group on both map columns with different aggregates.
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt, sum(_1) AS sum_val from $tableName group by _2, _3")

      // Group on both map column with aggregate and filtering.
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt from $tableName WHERE _2 IS not null group by _2, _3")
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt from $tableName WHERE _3 IS not null group by _2, _3")
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt from $tableName WHERE _2 IS not null AND _3 IS not null group by _2, _3")
      checkSparkAnswerAndOperator(
        s"select _2, _3, count(*) AS cnt from $tableName WHERE _2 IS not null group by _2, _3 order by cnt DESC")
    }

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
      CometConf.COMET_ENABLE_GROUPING_ON_MAP_TYPE.key -> "true") {

      withParquetTable(
        Seq(
          (1, Map("a" -> 1, "b" -> 2), Map(1 -> "a", 2 -> "b")),
          (2, Map("b" -> 2, "a" -> 1), Map(2 -> "b", 1 -> "a")),
          (3, Map("a" -> 5, "b" -> 6), Map(1 -> "c", 2 -> "d")),
          (4, Map("a" -> 1, "b" -> 2), Map(1 -> "a", 2 -> "b")),
          (5, Map("c" -> 3), Map(3 -> "e")),
          (6, Map("a" -> 1, "b" -> 2, "c" -> 3), Map(1 -> "a", 2 -> "b", 3 -> "e")),
          (7, Map.empty[String, Int], Map.empty[Int, String]),
          (8, Map("b" -> 3, "a" -> 5), Map(2 -> "b", 1 -> "a")),
          (9, null, null),
          (10, Map("d" -> 4, "e" -> 5, "f" -> 6), Map(4 -> "f", 5 -> "g", 6 -> "h")),
          (11, Map("datafusion" -> 4, "comet" -> 5), Map(1 -> "datafusion", 2 -> "comet")),
          (12, Map("comet" -> 5, "datafusion" -> 4), Map(2 -> "comet", 1 -> "datafusion")),
          (13, Map("a" -> 1, "b" -> 2), Map(-1 -> "a", 2 -> "b")),
          (14, Map("b" -> 2, "a" -> 1), Map(1 -> "a", 2 -> "b")),
          (15, Map("b" -> 2, "a" -> 1), Map(2 -> "b", -1 -> "a"))),
        "map_tbl") {
        runTests("map_tbl")
      }
    }
  }

}

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
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.optimizer.EliminateSorts
import org.apache.spark.sql.comet.CometHashAggregateExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus

/**
 * Test suite dedicated to Comet native aggregate operator
 */
class CometAggregateSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("Ensure traversed operators during finding first partial aggregation are all native") {
    withTable("lineitem", "part") {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {

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
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test")
          makeParquetFile(path, 10000, 10, dictionaryEnabled)
          withParquetTable(path.toUri.toString, "tbl") {
            checkSparkAnswer(sql("SELECT * FROM tbl").sort("_g1").groupBy("_g1").agg(sum("_8")))
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
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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

        checkSparkAnswer("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
      }
    }
  }

  test("avg") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 10).map(i => ((i + 1) * (i + 1), (i + 1) / 2)),
        "tbl",
        dictionaryEnabled) {

        checkSparkAnswer("SELECT _2, AVG(_1) FROM tbl GROUP BY _2")
        checkSparkAnswer("SELECT AVG(_2) FROM tbl")
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
          checkSparkAnswer(s"SELECT COUNT(col1) FROM $table")
          checkSparkAnswer(s"SELECT col2, COUNT(col1) FROM $table GROUP BY col2")
          checkSparkAnswer(s"SELECT avg(col1) FROM $table")
          checkSparkAnswer(s"SELECT col2, avg(col1) FROM $table GROUP BY col2")
        }
      }
    }
  }

  test("SUM/AVG non-decimal overflow") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(Seq((0, 100.toLong), (0, Long.MaxValue)), "tbl", dictionaryEnabled) {
        checkSparkAnswer("SELECT SUM(_2) FROM tbl GROUP BY _1")
        checkSparkAnswer("SELECT AVG(_2) FROM tbl GROUP BY _1")
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
        checkSparkAnswer("SELECT _2, MIN(_1), MAX(_1) FROM tbl GROUP BY _2")
        checkSparkAnswer("SELECT _2, AVG(_1) FROM tbl GROUP BY _2")
      }
    }
  }

  test("group-by on variable length types") {
    Seq(true, false).foreach { nativeShuffleEnabled =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withSQLConf(
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> nativeShuffleEnabled.toString,
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
        checkSparkAnswer(sql("SELECT _2, AVG(_1) FROM tbl GROUP BY _2"))
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

        checkSparkAnswer(sql("SELECT _2, AVG(_1) FROM tbl GROUP BY _2"))
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
        checkSparkAnswer(sql("SELECT _2, SUM(_1) FROM tbl GROUP BY _2"))
      }
    }
  }

  test("SUM, COUNT, MIN, MAX, AVG on float & double") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        makeParquetFile(path, 1000, 10, dictionaryEnabled)
        withParquetTable(path.toUri.toString, "tbl") {
          checkSparkAnswer(
            "SELECT _g5, SUM(_5), COUNT(_5), MIN(_5), MAX(_5), AVG(_5) FROM tbl GROUP BY _g5")
          checkSparkAnswer(
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
      withParquetTable(data, "tbl", dictionaryEnabled) {
        checkSparkAnswer("SELECT SUM(_2), MIN(_2), MAX(_2), _1 FROM tbl GROUP BY _1")
        checkSparkAnswer("SELECT MIN(_1), MAX(_1), MIN(_2), MAX(_2) FROM tbl")
        checkSparkAnswer("SELECT AVG(_2), _1 FROM tbl GROUP BY _1")
        checkSparkAnswer("SELECT AVG(_1), AVG(_2) FROM tbl")
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
            checkSparkAnswer(
              s"SELECT _g1, $FN(_8 + CAST(1 AS DECIMAL(20, 10))) FROM tbl GROUP BY _g1")
            checkSparkAnswer(
              s"SELECT _g1, $FN(_8 - CAST(-1 AS DECIMAL(10, 3))) FROM tbl GROUP BY _g1")
            checkSparkAnswer(
              s"SELECT _g1, $FN(_9 * CAST(3.14 AS DECIMAL(4, 3))) FROM tbl GROUP BY _g1")
            checkSparkAnswer(
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
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
          checkSparkAnswer("SELECT _g1, COUNT(_10), MIN(_10), MAX(_10) FROM tbl GROUP BY _g1")
          checkSparkAnswer("SELECT _g1, COUNT(_11), MIN(_11), MAX(_11) FROM tbl GROUP BY _g1")
          checkSparkAnswer("SELECT _g1, COUNT(_12), MIN(_12), MAX(_12) FROM tbl GROUP BY _g1")
        }
      }
    }
  }

  test("single group-by column + aggregate column, multiple batches, no null") {
    val numValues = 10000

    Seq(1, 100, 10000).foreach { numGroups =>
      Seq(128, 1024, numValues + 1).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withParquetTable(
              (0 until numValues).map(i => (i, Random.nextInt() % numGroups)),
              "tbl",
              dictionaryEnabled) {
              withView("v") {
                sql("CREATE TEMP VIEW v AS SELECT _1, _2 FROM tbl ORDER BY _1")
                checkSparkAnswer(
                  "SELECT _2, SUM(_1), SUM(DISTINCT _1), MIN(_1), MAX(_1), COUNT(_1)," +
                    " COUNT(DISTINCT _1), AVG(_1), FIRST(_1), LAST(_1) FROM v GROUP BY _2")
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
          withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                withView("v") {
                  sql("CREATE TEMP VIEW v AS SELECT _g1, _g2, _3 FROM tbl ORDER BY _3")
                  checkSparkAnswer("SELECT _g1, _g2, FIRST(_3) FROM v GROUP BY _g1, _g2")
                  checkSparkAnswer("SELECT _g1, _g2, LAST(_3) FROM v GROUP BY _g1, _g2")
                }
                checkSparkAnswer("SELECT _g1, _g2, SUM(_3) FROM tbl GROUP BY _g1, _g2")
                checkSparkAnswer("SELECT _g1, _g2, COUNT(_3) FROM tbl GROUP BY _g1, _g2")
                checkSparkAnswer("SELECT _g1, _g2, SUM(DISTINCT _3) FROM tbl GROUP BY _g1, _g2")
                checkSparkAnswer("SELECT _g1, _g2, COUNT(DISTINCT _3) FROM tbl GROUP BY _g1, _g2")
                checkSparkAnswer("SELECT _g1, _g2, MIN(_3), MAX(_3) FROM tbl GROUP BY _g1, _g2")
                checkSparkAnswer("SELECT _g1, _g2, AVG(_3) FROM tbl GROUP BY _g1, _g2")
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
      checkSparkAnswerAndNumOfAggregates("SELECT v, sum(i) FROM t GROUP BY v ORDER BY v", 1)
    }
  }

  test("multiple group-by columns + multiple aggregate column, with nulls") {
    val numValues = 10000

    Seq(1, 100, numValues).foreach { numGroups =>
      Seq(128, numValues + 100).foreach { batchSize =>
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
            withTempPath { dir =>
              val path = new Path(dir.toURI.toString, "test.parquet")
              makeParquetFile(path, numValues, numGroups, dictionaryEnabled)
              withParquetTable(path.toUri.toString, "tbl") {
                withView("v") {
                  sql("CREATE TEMP VIEW v AS SELECT _g3, _g4, _3, _4 FROM tbl ORDER BY _3, _4")
                  checkSparkAnswer(
                    "SELECT _g3, _g4, FIRST(_3), FIRST(_4) FROM v GROUP BY _g3, _g4")
                  checkSparkAnswer("SELECT _g3, _g4, LAST(_3), LAST(_4) FROM v GROUP BY _g3, _g4")
                }
                checkSparkAnswer("SELECT _g3, _g4, SUM(_3), SUM(_4) FROM tbl GROUP BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, SUM(DISTINCT _3), SUM(DISTINCT _4) FROM tbl GROUP BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, COUNT(_3), COUNT(_4) FROM tbl GROUP BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, COUNT(DISTINCT _3), COUNT(DISTINCT _4) FROM tbl GROUP BY _g3, _g4")
                checkSparkAnswer(
                  "SELECT _g3, _g4, MIN(_3), MAX(_3), MIN(_4), MAX(_4) FROM tbl GROUP BY _g3, _g4")
                checkSparkAnswer("SELECT _g3, _g4, AVG(_3), AVG(_4) FROM tbl GROUP BY _g3, _g4")
              }
            }
          }
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
              withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {

                // Test all combinations of different aggregation & group-by types
                (1 to 4).foreach { col =>
                  (1 to 14).foreach { gCol =>
                    withView("v") {
                      sql(s"CREATE TEMP VIEW v AS SELECT _g$gCol, _$col FROM tbl ORDER BY _$col")
                      checkSparkAnswer(s"SELECT _g$gCol, FIRST(_$col) FROM v GROUP BY _g$gCol")
                      checkSparkAnswer(s"SELECT _g$gCol, LAST(_$col) FROM v GROUP BY _g$gCol")
                    }
                    checkSparkAnswer(s"SELECT _g$gCol, SUM(_$col) FROM tbl GROUP BY _g$gCol")
                    checkSparkAnswer(
                      s"SELECT _g$gCol, SUM(DISTINCT _$col) FROM tbl GROUP BY _g$gCol")
                    checkSparkAnswer(s"SELECT _g$gCol, COUNT(_$col) FROM tbl GROUP BY _g$gCol")
                    checkSparkAnswer(
                      s"SELECT _g$gCol, COUNT(DISTINCT _$col) FROM tbl GROUP BY _g$gCol")
                    checkSparkAnswer(
                      s"SELECT _g$gCol, MIN(_$col), MAX(_$col) FROM tbl GROUP BY _g$gCol")
                    checkSparkAnswer(s"SELECT _g$gCol, AVG(_$col) FROM tbl GROUP BY _g$gCol")
                  }
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
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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

  test("test final avg") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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
    // TODO: enable decimal average for Spark 3.2 & 3.3
    assume(isSpark34Plus)

    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withSQLConf("parquet.enable.dictionary" -> dictionaryEnabled.toString) {
          val table = "t1"
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

            checkSparkAnswerAndNumOfAggregates("SELECT b , AVG(a) FROM t1 GROUP BY b", 2)
            checkSparkAnswerAndNumOfAggregates("SELECT AVG(a) FROM t1", 2)
            checkSparkAnswerAndNumOfAggregates(
              "SELECT b, MIN(a), MAX(a), COUNT(a), SUM(a), AVG(a) FROM t1 GROUP BY b",
              2)
            checkSparkAnswerAndNumOfAggregates(
              "SELECT MIN(a), MAX(a), COUNT(a), SUM(a), AVG(a) FROM t1",
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
        checkSparkAnswerAndNumOfAggregates("SELECT _2 , AVG(_1) FROM tbl GROUP BY _2", 1)
      }
    }
  }

  test("avg null handling") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
      val table = "t1"
      withTable(table) {
        sql(s"create table $table(a double, b double) using parquet")
        sql(s"insert into $table values(1, 1.0)")
        sql(s"insert into $table values(null, null)")
        sql(s"insert into $table values(1, 2.0)")
        sql(s"insert into $table values(null, null)")
        sql(s"insert into $table values(2, null)")
        sql(s"insert into $table values(2, null)")

        val query = sql(s"select a, AVG(b) from $table GROUP BY a")
        checkSparkAnswer(query)
      }
    }
  }

  test("Decimal Avg with DF") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(true, false).foreach { nativeShuffleEnabled =>
        withSQLConf(
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> nativeShuffleEnabled.toString,
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
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

  test("distinct") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      Seq(true, false).foreach { cometColumnShuffleEnabled =>
        withSQLConf(
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> cometColumnShuffleEnabled.toString) {
          Seq(true, false).foreach { dictionary =>
            withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
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

                expectedNumOfCometAggregates = 1
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

  test("first/last") {
    withSQLConf(
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
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

  protected def checkSparkAnswerAndNumOfAggregates(query: String, numAggregates: Int): Unit = {
    val df = sql(query)
    checkSparkAnswer(df)
    val actualNumAggregates = getNumCometHashAggregate(df)
    assert(
      actualNumAggregates == numAggregates,
      s"Expected $numAggregates Comet aggregate operators, but found $actualNumAggregates")
  }

  def getNumCometHashAggregate(df: DataFrame): Int = {
    val sparkPlan = stripAQEPlan(df.queryExecution.executedPlan)
    sparkPlan.collect { case s: CometHashAggregateExec => s }.size
  }

  def makeParquetFile(
      path: Path,
      total: Int,
      numGroups: Int,
      dictionaryEnabled: Boolean): Unit = {
    val schemaStr =
      """
        |message root {
        |  optional INT32                    _1(INT_8);
        |  optional INT32                    _2(INT_16);
        |  optional INT32                    _3;
        |  optional INT64                    _4;
        |  optional FLOAT                    _5;
        |  optional DOUBLE                   _6;
        |  optional INT32                    _7(DECIMAL(5, 2));
        |  optional INT64                    _8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _9(DECIMAL(38, 37));
        |  optional INT64                    _10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _12(DATE);
        |  optional INT32                    _g1(INT_8);
        |  optional INT32                    _g2(INT_16);
        |  optional INT32                    _g3;
        |  optional INT64                    _g4;
        |  optional FLOAT                    _g5;
        |  optional DOUBLE                   _g6;
        |  optional INT32                    _g7(DECIMAL(5, 2));
        |  optional INT64                    _g8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _g9(DECIMAL(38, 37));
        |  optional INT64                    _g10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _g11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _g12(DATE);
        |  optional BINARY                   _g13(UTF8);
        |  optional BINARY                   _g14;
        |}
      """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(schema, path, dictionaryEnabled = true)

    val rand = scala.util.Random
    val expected = (0 until total).map { i =>
      // use a single value for the first page, to make sure dictionary encoding kicks in
      if (rand.nextBoolean()) None
      else {
        if (dictionaryEnabled) Some(i % 10) else Some(i)
      }
    }

    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i.toByte)
          record.add(1, i.toShort)
          record.add(2, i)
          record.add(3, i.toLong)
          record.add(4, rand.nextFloat())
          record.add(5, rand.nextDouble())
          record.add(6, i)
          record.add(7, i.toLong)
          record.add(8, (i % 10).toString * 16)
          record.add(9, i.toLong)
          record.add(10, i.toLong)
          record.add(11, i)
          record.add(12, i.toByte % numGroups)
          record.add(13, i.toShort % numGroups)
          record.add(14, i % numGroups)
          record.add(15, i.toLong % numGroups)
          record.add(16, rand.nextFloat())
          record.add(17, rand.nextDouble())
          record.add(18, i)
          record.add(19, i.toLong)
          record.add(20, (i % 10).toString * 16)
          record.add(21, i.toLong)
          record.add(22, i.toLong)
          record.add(23, i)
          record.add(24, (i % 10).toString * 24)
          record.add(25, (i % 10).toString * 36)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
  }
}

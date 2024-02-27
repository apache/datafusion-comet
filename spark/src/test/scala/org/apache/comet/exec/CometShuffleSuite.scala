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
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.execution.shuffle.{CometShuffleDependency, CometShuffleExchangeExec, CometShuffleManager}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus

abstract class CometShuffleSuiteBase extends CometTestBase with AdaptiveSparkPlanHelper {

  protected val adaptiveExecutionEnabled: Boolean

  protected val fastMergeEnabled: Boolean = true

  protected val numElementsForceSpillThreshold: Int = 10

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptiveExecutionEnabled.toString)
      .set("spark.shuffle.unsafe.fastMergeEnabled", fastMergeEnabled.toString)
  }

  protected val asyncShuffleEnable: Boolean

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> asyncShuffleEnable.toString,
        CometConf.COMET_EXEC_SHUFFLE_SPILL_THRESHOLD.key -> numElementsForceSpillThreshold.toString) {
        testFun
      }
    }
  }

  import testImplicits._

  test("Native shuffle with dictionary of binary") {
    Seq("true", "false").foreach { dictionaryEnabled =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
        withParquetTable(
          (0 until 1000).map(i => (i % 5, (i % 5).toString.getBytes())),
          "tbl",
          dictionaryEnabled.toBoolean) {
          val shuffled = sql("SELECT * FROM tbl").repartition(2, $"_2")

          checkCometExchange(shuffled, 1, true)
          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("columnar shuffle on nested struct including nulls") {
    Seq(10, 201).foreach { numPartitions =>
      Seq("1.0", "10.0").foreach { ratio =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
          withParquetTable(
            (0 until 50).map(i =>
              (i, Seq((i + 1, i.toString), null, (i + 3, (i + 3).toString)), i + 1)),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 1)
              .repartition(numPartitions, $"_1", $"_2", $"_3")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }
        }
      }
    }
  }

  test("columnar shuffle on struct including nulls") {
    Seq(10, 201).foreach { numPartitions =>
      Seq("1.0", "10.0").foreach { ratio =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
          val data: Seq[(Int, (Int, String))] =
            Seq((1, (0, "1")), (2, (3, "3")), (3, null))
          withParquetTable(data, "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 1)
              .repartition(numPartitions, $"_1", $"_2")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }
        }
      }
    }
  }

  test("columnar shuffle on array/struct map key/value") {
    Seq("false", "true").foreach { execEnabled =>
      Seq(10, 201).foreach { numPartitions =>
        Seq("1.0", "10.0").foreach { ratio =>
          withSQLConf(
            CometConf.COMET_EXEC_ENABLED.key -> execEnabled,
            CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
            withParquetTable((0 until 50).map(i => (Map(Seq(i, i + 1) -> i), i + 1)), "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_2")

              checkSparkAnswer(df)
              // Array map key array element fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }

            withParquetTable((0 until 50).map(i => (Map(i -> Seq(i, i + 1)), i + 1)), "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_2")

              checkSparkAnswer(df)
              // Array map value array element fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }

            withParquetTable((0 until 50).map(i => (Map((i, i.toString) -> i), i + 1)), "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_2")

              checkSparkAnswer(df)
              // Struct map key array element fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }

            withParquetTable((0 until 50).map(i => (Map(i -> (i, i.toString)), i + 1)), "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_2")

              checkSparkAnswer(df)
              // Struct map value array element fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }
          }
        }
      }
    }
  }

  test("columnar shuffle on map array element") {
    Seq("false", "true").foreach { execEnabled =>
      Seq(10, 201).foreach { numPartitions =>
        Seq("1.0", "10.0").foreach { ratio =>
          withSQLConf(
            CometConf.COMET_EXEC_ENABLED.key -> execEnabled,
            CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
            withParquetTable(
              (0 until 50).map(i => ((Seq(Map(1 -> i)), Map(2 -> i), Map(3 -> i)), i + 1)),
              "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_2")

              checkSparkAnswer(df)
              // Map array element fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }
          }
        }
      }
    }
  }

  test("RoundRobinPartitioning is supported by columnar shuffle") {
    withSQLConf(
      // AQE has `ShuffleStage` which is a leaf node which blocks
      // collecting `CometShuffleExchangeExec` node.
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl")
        val shuffled = df
          .select($"_1" + 1 as ("a"))
          .filter($"a" > 4)
          .repartition(10)
          .limit(2)

        checkAnswer(shuffled, Row(5) :: Nil)
        val cometShuffleExecs = checkCometExchange(shuffled, 1, false)

        cometShuffleExecs(0).outputPartitioning.getClass.getName
          .contains("RoundRobinPartitioning")
      }
    }
  }

  test("columnar shuffle on map") {
    def genTuples[K](num: Int, keys: Seq[K]): Seq[(
        Int,
        Map[K, Boolean],
        Map[K, Byte],
        Map[K, Short],
        Map[K, Int],
        Map[K, Long],
        Map[K, Float],
        Map[K, Double],
        Map[K, java.sql.Date],
        Map[K, java.sql.Timestamp],
        Map[K, java.math.BigDecimal],
        Map[K, Array[Byte]],
        Map[K, String])] = {
      (0 until num).map(i =>
        (
          i + 1,
          Map(keys(0) -> (i > 10), keys(1) -> (i > 20)),
          Map(keys(0) -> i.toByte, keys(1) -> (i + 1).toByte),
          Map(keys(0) -> i.toShort, keys(1) -> (i + 1).toShort),
          Map(keys(0) -> i, keys(1) -> (i + 1)),
          Map(keys(0) -> i.toLong, keys(1) -> (i + 1).toLong),
          Map(keys(0) -> i.toFloat, keys(1) -> (i + 1).toFloat),
          Map(keys(0) -> i.toDouble, keys(1) -> (i + 1).toDouble),
          Map(
            keys(0) -> new java.sql.Date(i.toLong),
            keys(1) -> new java.sql.Date((i + 1).toLong)),
          Map(
            keys(0) -> new java.sql.Timestamp(i.toLong),
            keys(1) -> new java.sql.Timestamp((i + 1).toLong)),
          Map(
            keys(0) -> new java.math.BigDecimal(i.toLong),
            keys(1) -> new java.math.BigDecimal((i + 1).toLong)),
          Map(keys(0) -> i.toString.getBytes(), keys(1) -> (i + 1).toString.getBytes()),
          Map(keys(0) -> i.toString, keys(1) -> (i + 1).toString)))
    }

    Seq(10, 201).foreach { numPartitions =>
      Seq("1.0", "10.0").foreach { ratio =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
          // Boolean key
          withParquetTable(genTuples(50, Seq(true, false)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Byte key
          withParquetTable(genTuples(50, Seq(0.toByte, 1.toByte)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Short key
          withParquetTable(genTuples(50, Seq(0.toShort, 1.toShort)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Int key
          withParquetTable(genTuples(50, Seq(0, 1)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Long key
          withParquetTable(genTuples(50, Seq(0.toLong, 1.toLong)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Float key
          withParquetTable(genTuples(50, Seq(0.toFloat, 1.toFloat)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Double key
          withParquetTable(genTuples(50, Seq(0.toDouble, 1.toDouble)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Date key
          withParquetTable(
            genTuples(50, Seq(new java.sql.Date(0.toLong), new java.sql.Date(1.toLong))),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Timestamp key
          withParquetTable(
            genTuples(
              50,
              Seq(new java.sql.Timestamp(0.toLong), new java.sql.Timestamp(1.toLong))),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Decimal key
          withParquetTable(
            genTuples(
              50,
              Seq(new java.math.BigDecimal(0.toLong), new java.math.BigDecimal(1.toLong))),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // String key
          withParquetTable(genTuples(50, Seq(0.toString, 1.toString)), "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }

          // Binary key
          withParquetTable(
            genTuples(50, Seq(0.toString.getBytes(), 1.toString.getBytes())),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_1" > 10)
              .repartition(
                numPartitions,
                $"_2",
                $"_3",
                $"_4",
                $"_5",
                $"_6",
                $"_7",
                $"_8",
                $"_9",
                $"_10",
                $"_11",
                $"_12",
                $"_13")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }
        }
      }
    }
  }

  test("columnar shuffle on array") {
    Seq(10, 201).foreach { numPartitions =>
      Seq("1.0", "10.0").foreach { ratio =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
          withParquetTable(
            (0 until 50).map(i =>
              (
                Seq(i + 1, i + 2, i + 3),
                Seq(i.toLong, (i + 2).toLong, (i + 5).toLong),
                Seq(i.toString, (i + 3).toString, (i + 2).toString),
                Seq(
                  (
                    i + 1,
                    Seq(i + 3, i + 1, i + 2), // nested array in struct
                    Seq(i.toLong, (i + 2).toLong, (i + 5).toLong),
                    Seq(i.toString, (i + 3).toString, (i + 2).toString),
                    (i + 2).toString)),
                i + 1)),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_5" > 10)
              .repartition(numPartitions, $"_1", $"_2", $"_3", $"_4", $"_5")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }
        }
      }
    }
  }

  test("columnar shuffle on nested array") {
    Seq("false", "true").foreach { execEnabled =>
      Seq(10, 201).foreach { numPartitions =>
        Seq("1.0", "10.0").foreach { ratio =>
          withSQLConf(
            CometConf.COMET_EXEC_ENABLED.key -> execEnabled,
            CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
            withParquetTable(
              (0 until 50).map(i => (Seq(Seq(i + 1), Seq(i + 2), Seq(i + 3)), i + 1)),
              "tbl") {
              val df = sql("SELECT * FROM tbl")
                .filter($"_2" > 10)
                .repartition(numPartitions, $"_1", $"_2")
                .sortWithinPartitions($"_1")

              checkSparkAnswer(df)
              // Nested array fallback to Spark shuffle for now
              checkCometExchange(df, 0, false)
            }
          }
        }
      }
    }
  }

  test("columnar shuffle on nested struct") {
    Seq(10, 201).foreach { numPartitions =>
      Seq("1.0", "10.0").foreach { ratio =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio) {
          withParquetTable(
            (0 until 50).map(i =>
              ((i, 2.toString, (i + 1).toLong, (3.toString, i + 1, (i + 2).toLong)), i + 1)),
            "tbl") {
            val df = sql("SELECT * FROM tbl")
              .filter($"_2" > 10)
              .repartition(numPartitions, $"_1", $"_2")
              .sortWithinPartitions($"_1")

            checkSparkAnswer(df)
            checkCometExchange(df, 1, false)
          }
        }
      }
    }
  }

  test("fix: Dictionary arrays imported from native should not be overridden") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_BATCH_SIZE.key -> "10",
        CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_EXPR_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 50).map(i => (1.toString, 2.toString, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl")
            .filter($"_1" === 1.toString)
            .repartition(numPartitions, $"_1", $"_2")
            .sortWithinPartitions($"_1")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("fix: closing sliced dictionary Comet vector should not close dictionary array") {
    (0 to 10).foreach { _ =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_BATCH_SIZE.key -> "10",
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1.1",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_SPILL_THRESHOLD.key -> "1000000000",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        val table1 = (0 until 1000)
          .map(i => (111111.toString, 2222222.toString, 3333333.toString, i.toLong))
          .toDF("a", "b", "c", "d")
        val table2 = (0 until 1000)
          .map(i => (3333333.toString, 2222222.toString, 111111.toString, i.toLong))
          .toDF("e", "f", "g", "h")
        withParquetTable(table1, "tbl_a") {
          withParquetTable(table2, "tbl_b") {
            val df = sql(
              "select a, b, count(distinct h) from tbl_a, tbl_b " +
                "where c = e and b = '2222222' and a not like '2' group by a, b")
            checkSparkAnswer(df)
          }
        }
      }
    }
  }

  test("fix: Dictionary field should have distinct dict_id") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "2.0",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable(
          (0 until 10000).map(i => (1.toString, 2.toString, (i + 1).toLong)),
          "tbl") {
          assert(
            sql("SELECT * FROM tbl").repartition(numPartitions, $"_1", $"_2").count() == sql(
              "SELECT * FROM tbl")
              .count())
          val shuffled = sql("SELECT * FROM tbl").repartition(numPartitions, $"_1")
          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("dictionary shuffle") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "2.0",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 10000).map(i => (1.toString, (i + 1).toLong)), "tbl") {
          assert(
            sql("SELECT * FROM tbl").repartition(numPartitions, $"_1").count() == sql(
              "SELECT * FROM tbl")
              .count())
          val shuffled = sql("SELECT * FROM tbl").select($"_1").repartition(numPartitions, $"_1")
          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("dictionary shuffle: fallback to string") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1000000000.0",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 10000).map(i => (1.toString, (i + 1).toLong)), "tbl") {
          assert(
            sql("SELECT * FROM tbl").repartition(numPartitions, $"_1").count() == sql(
              "SELECT * FROM tbl")
              .count())
          val shuffled = sql("SELECT * FROM tbl").select($"_1").repartition(numPartitions, $"_1")
          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("fix: inMemSorter should be reset after spilling") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 10000).map(i => (1, (i + 1).toLong)), "tbl") {
        assert(
          sql("SELECT * FROM tbl").repartition(201, $"_1").count() == sql("SELECT * FROM tbl")
            .count())
      }
    }
  }

  test("fix: native Unsafe row accessors return incorrect results") {
    Seq(10, 201).foreach { numPartitions =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, false, 10000, 10010)

        Seq(
          $"_1",
          $"_2",
          $"_3",
          $"_4",
          $"_5",
          $"_6",
          $"_7",
          $"_8",
          $"_13",
          $"_14",
          $"_15",
          $"_16",
          $"_17",
          $"_18",
          $"_19",
          $"_20").foreach { col =>
          withSQLConf(
            CometConf.COMET_EXEC_ENABLED.key -> "false",
            CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
            readParquetFile(path.toString) { df =>
              val shuffled = df.select(col).repartition(numPartitions, col)
              checkSparkAnswer(shuffled)
            }
          }
        }
      }
    }
  }

  test("fix: StreamReader should always set useDecimal128 as true") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withTempPath { dir =>
          val data = makeDecimalRDD(1000, DecimalType(12, 2), false)
          data.write.parquet(dir.getCanonicalPath)
          readParquetFile(dir.getCanonicalPath) { df =>
            {
              val shuffled = df.repartition(numPartitions, $"dec")
              checkSparkAnswer(shuffled)
            }
          }
        }
      }
    }
  }

  test("fix: Native Unsafe decimal accessors return incorrect results") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withTempPath { dir =>
          val data = makeDecimalRDD(1000, DecimalType(22, 2), false)
          data.write.parquet(dir.getCanonicalPath)
          readParquetFile(dir.getCanonicalPath) { df =>
            {
              val shuffled = df.repartition(numPartitions, $"dec")
              checkSparkAnswer(shuffled)
            }
          }
        }
      }
    }
  }

  test("Comet shuffle reader should respect spark.comet.batchSize") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 10000).map(i => (1, (i + 1).toLong)), "tbl") {
          assert(
            sql("SELECT * FROM tbl").repartition(numPartitions, $"_1").count() == sql(
              "SELECT * FROM tbl").count())
        }
      }
    }
  }

  test("Arrow shuffle should work with BatchScan") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "", // Use DataSourceV2
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", // Disable AQE
      CometConf.COMET_SCAN_ENABLED.key -> "false", // Disable CometScan to use Spark BatchScan
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl")
        val shuffled = df
          .select($"_1" + 1 as ("a"))
          .filter($"a" > 4)
          .repartitionByRange(10, $"_2")
          .limit(2)
          .repartition(10, $"_1")

        checkAnswer(shuffled, Row(5) :: Nil)
      }
    }
  }

  test("Columnar shuffle for large shuffle partition number") {
    Seq(10, 200, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl")

          val shuffled = df.repartitionByRange(numPartitions, $"_2")

          val cometShuffleExecs = checkCometExchange(shuffled, 1, false)
          // `CometSerializedShuffleHandle` is used for large shuffle partition number,
          // i.e., sort-based shuffle writer
          cometShuffleExecs(0).shuffleDependency.shuffleHandle.getClass.getName
            .contains("CometSerializedShuffleHandle")

          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("grouped aggregate: Comet shuffle") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
        val df = sql("SELECT count(_2), sum(_2) FROM tbl GROUP BY _1")
        checkCometExchange(df, 1, true)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("hash shuffle: Comet shuffle") {
    // Disable CometExec to explicit test Comet Arrow shuffle path
    Seq(true, false).foreach { execEnabled =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> execEnabled.toString,
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> (!execEnabled).toString) {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
          val shuffled1 = df.repartition(10, $"_1")

          // If Comet execution is disabled, `Sort` operator is Spark operator
          // and jvm arrow shuffle is applied.
          checkCometExchange(shuffled1, 1, execEnabled)
          checkSparkAnswer(shuffled1)

          val shuffled2 = df.repartition(10, $"_1", $"_2")

          checkCometExchange(shuffled2, 1, execEnabled)
          checkSparkAnswer(shuffled2)

          val shuffled3 = df.repartition(10, $"_2", $"_1")

          checkCometExchange(shuffled3, 1, execEnabled)
          checkSparkAnswer(shuffled3)
        }
      }
    }
  }

  test("Comet shuffle: different data type") {
    // Disable CometExec to explicit test Comet native shuffle path
    Seq(true, false).foreach { execEnabled =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          val all_types = if (isSpark34Plus) {
            Seq(
              $"_1",
              $"_2",
              $"_3",
              $"_4",
              $"_5",
              $"_6",
              $"_7",
              $"_8",
              $"_9",
              $"_10",
              $"_11",
              $"_13",
              $"_14",
              $"_15",
              $"_16",
              $"_17",
              $"_18",
              $"_19",
              $"_20")
          } else {
            Seq(
              $"_1",
              $"_2",
              $"_3",
              $"_4",
              $"_5",
              $"_6",
              $"_7",
              $"_8",
              $"_9",
              $"_10",
              $"_11",
              $"_13",
              $"_15",
              $"_16",
              $"_18",
              $"_19",
              $"_20")
          }
          all_types.foreach { col =>
            withSQLConf(
              CometConf.COMET_EXEC_ENABLED.key -> execEnabled.toString,
              CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
              "parquet.enable.dictionary" -> dictionaryEnabled.toString) {
              readParquetFile(path.toString) { df =>
                val shuffled = df
                  .select($"_1")
                  .repartition(10, col)
                checkCometExchange(shuffled, 1, true)
                if (execEnabled) {
                  checkSparkAnswerAndOperator(shuffled)
                } else {
                  checkSparkAnswer(shuffled)
                }
              }
            }
          }
        }
      }
    }
  }

  test("hash shuffle: Comet columnar shuffle") {
    Seq(10, 200, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl")

          val shuffled1 =
            df.repartitionByRange(numPartitions, $"_2").limit(2).repartition(numPartitions, $"_1")

          // 3 exchanges are expected: 1) shuffle to repartition by range, 2) shuffle to global limit, 3) hash shuffle
          checkCometExchange(shuffled1, 3, false)
          checkSparkAnswer(shuffled1)

          val shuffled2 = df
            .repartitionByRange(numPartitions, $"_2")
            .limit(2)
            .repartition(numPartitions, $"_1", $"_2")

          checkCometExchange(shuffled2, 3, false)
          checkSparkAnswer(shuffled2)

          val shuffled3 = df
            .repartitionByRange(numPartitions, $"_2")
            .limit(2)
            .repartition(numPartitions, $"_2", $"_1")

          checkCometExchange(shuffled3, 3, false)
          checkSparkAnswer(shuffled3)
        }
      }
    }
  }

  test("Comet columnar shuffle shuffle: different data type") {
    Seq(10, 200, 201).foreach { numPartitions =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)

          Seq(
            $"_1",
            $"_2",
            $"_3",
            $"_4",
            $"_5",
            $"_6",
            $"_7",
            $"_8",
            $"_9",
            $"_10",
            $"_11",
            $"_13",
            $"_14",
            $"_15",
            $"_16",
            $"_17",
            $"_18",
            $"_19",
            $"_20").foreach { col =>
            withSQLConf(
              CometConf.COMET_EXEC_ENABLED.key -> "false",
              CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
              CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
              readParquetFile(path.toString) { df =>
                val shuffled = df
                  .select($"_1")
                  .repartition(numPartitions, col)
                val cometShuffleExecs = checkCometExchange(shuffled, 1, false)
                if (numPartitions > 200) {
                  // For sort-based shuffle writer
                  cometShuffleExecs(0).shuffleDependency.shuffleHandle.getClass.getName
                    .contains("CometSerializedShuffleHandle")
                }
                checkSparkAnswer(shuffled)
              }
            }
          }
        }
      }
    }
  }

  test("Comet native operator after Comet shuffle") {
    Seq(true, false).foreach { columnarShuffle =>
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> columnarShuffle.toString) {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl")

          val shuffled1 = df
            .repartition(10, $"_2")
            .select($"_1", $"_1" + 1, $"_2" + 2)
            .repartition(10, $"_1")
            .filter($"_1" > 1)

          // 2 Comet shuffle exchanges are expected
          checkCometExchange(shuffled1, 2, !columnarShuffle)
          checkSparkAnswer(shuffled1)

          val shuffled2 = df
            .repartitionByRange(10, $"_2")
            .select($"_1", $"_1" + 1, $"_2" + 2)
            .repartition(10, $"_1")
            .filter($"_1" > 1)

          // 2 Comet shuffle exchanges are expected, if columnar shuffle is enabled
          if (columnarShuffle) {
            checkCometExchange(shuffled2, 2, !columnarShuffle)
          } else {
            // Because the first exchange from the bottom is range exchange which native shuffle
            // doesn't support. So Comet exec operators stop before the first exchange and thus
            // there is no Comet exchange.
            checkCometExchange(shuffled2, 0, true)
          }
          checkSparkAnswer(shuffled2)
        }
      }
    }
  }

  test("Comet shuffle: single partition") {
    Seq(true, false).foreach { execEnabled =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> execEnabled.toString,
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> (!execEnabled).toString) {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)

          val shuffled = df.repartition(1)

          checkCometExchange(shuffled, 1, execEnabled)
          checkSparkAnswer(shuffled)
        }
      }
    }
  }

  test("fix: comet native shuffle with binary data") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
      withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT cast(cast(_1 as STRING) as BINARY) as binary, _2 FROM tbl")

        val shuffled = df.repartition(1, $"binary")

        checkCometExchange(shuffled, 1, true)
        checkSparkAnswer(shuffled)
      }
    }
  }

  test("Comet shuffle metrics") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
      withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
        val shuffled = df.repartition(10, $"_1")

        checkCometExchange(shuffled, 1, true)
        checkSparkAnswer(shuffled)

        // Materialize the shuffled data
        shuffled.collect()
        val metrics = find(shuffled.queryExecution.executedPlan) {
          case _: CometShuffleExchangeExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("shuffleRecordsWritten"))
        assert(metrics("shuffleRecordsWritten").value == 5L)
      }
    }
  }

  test("sort-based shuffle metrics") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
        val shuffled = df.repartition(201, $"_1")

        checkCometExchange(shuffled, 1, false)
        checkSparkAnswer(shuffled)

        // Materialize the shuffled data
        shuffled.collect()
        val metrics = find(shuffled.queryExecution.executedPlan) {
          case _: CometShuffleExchangeExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("shuffleRecordsWritten"))
        assert(metrics("shuffleRecordsWritten").value == 5L)

        assert(metrics.contains("shuffleBytesWritten"))
        assert(metrics("shuffleBytesWritten").value > 0)

        assert(metrics.contains("shuffleWriteTime"))
        assert(metrics("shuffleWriteTime").value > 0)
      }
    }
  }
}

class CometAsyncShuffleSuite extends CometShuffleSuiteBase {
  override protected val asyncShuffleEnable: Boolean = true

  protected val adaptiveExecutionEnabled: Boolean = true
}

class CometAsyncNonFastMergeShuffleSuite extends CometShuffleSuiteBase {
  override protected val fastMergeEnabled: Boolean = false

  protected val adaptiveExecutionEnabled: Boolean = true

  protected val asyncShuffleEnable: Boolean = true
}

class CometNonFastMergeShuffleSuite extends CometShuffleSuiteBase {
  override protected val fastMergeEnabled: Boolean = false

  protected val adaptiveExecutionEnabled: Boolean = true

  protected val asyncShuffleEnable: Boolean = false
}

class CometShuffleSuite extends CometShuffleSuiteBase {
  override protected val asyncShuffleEnable: Boolean = false

  protected val adaptiveExecutionEnabled: Boolean = true

  import testImplicits._

  // TODO: this test takes ~5mins to run, we should reduce the test time.
  // Because this test takes too long, we only have it in `CometShuffleSuite`.
  test("fix: Too many task completion listener of ArrowReaderIterator causes OOM") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "1",
      CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable((0 until 1000000).map(i => (1, (i + 1).toLong)), "tbl") {
        assert(
          sql("SELECT * FROM tbl").repartition(201, $"_1").count() == sql("SELECT * FROM tbl")
            .count())
      }
    }
  }
}

class DisableAQECometShuffleSuite extends CometShuffleSuiteBase {
  override protected val asyncShuffleEnable: Boolean = false

  protected val adaptiveExecutionEnabled: Boolean = false
}

class DisableAQECometAsyncShuffleSuite extends CometShuffleSuiteBase {
  override protected val asyncShuffleEnable: Boolean = true

  protected val adaptiveExecutionEnabled: Boolean = false
}

class CometShuffleEncryptionSuite extends CometTestBase {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.io.encryption.enabled", "true")
  }

  test("comet columnar shuffle with encryption") {
    Seq(10, 201).foreach { numPartitions =>
      Seq(true, false).foreach { dictionaryEnabled =>
        Seq(true, false).foreach { asyncEnabled =>
          withTempDir { dir =>
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 1000)

            (1 until 10).map(i => $"_$i").foreach { col =>
              withSQLConf(
                CometConf.COMET_EXEC_ENABLED.key -> "false",
                CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
                CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true",
                CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> asyncEnabled.toString) {
                readParquetFile(path.toString) { df =>
                  val shuffled = df
                    .select($"_1")
                    .repartition(numPartitions, col)
                  checkSparkAnswer(shuffled)
                }
              }
            }
          }
        }
      }
    }
  }
}

class CometShuffleManagerSuite extends CometTestBase {

  test("should not bypass merge sort if executor cores are too high") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ASYNC_MAX_THREAD_NUM.key -> "100") {
      val conf = new SparkConf()
      conf.set("spark.executor.cores", "1")

      val rdd = spark.emptyDataFrame.rdd.map(x => (0, x))

      val dependency = new CometShuffleDependency[Int, Row, Row](
        _rdd = rdd,
        serializer = null,
        shuffleWriterProcessor = null,
        partitioner = new Partitioner {
          override def numPartitions: Int = 50
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        })

      assert(CometShuffleManager.shouldBypassMergeSort(conf, dependency))

      conf.set("spark.executor.cores", "10")
      assert(!CometShuffleManager.shouldBypassMergeSort(conf, dependency))
    }
  }
}

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
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus

class CometNativeShuffleSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  import testImplicits._

  // TODO: this test takes a long time to run, we should reduce the test time.
  test("fix: Too many task completion listener of ArrowReaderIterator causes OOM") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "1") {
      withParquetTable((0 until 100000).map(i => (1, (i + 1).toLong)), "tbl") {
        assert(
          sql("SELECT * FROM tbl").repartition(201, $"_1").count() == sql("SELECT * FROM tbl")
            .count())
      }
    }
  }

  test("native shuffle: different data type") {
    Seq(true, false).foreach { execEnabled =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 1000)
          var allTypes: Seq[Int] = (1 to 20)
          if (!isSpark34Plus) {
            // TODO: Remove this once after https://github.com/apache/arrow/issues/40038 is fixed
            allTypes = allTypes.filterNot(Set(14).contains)
          }
          allTypes.map(i => s"_$i").foreach { c =>
            withSQLConf(
              CometConf.COMET_EXEC_ENABLED.key -> execEnabled.toString,
              "parquet.enable.dictionary" -> dictionaryEnabled.toString) {
              readParquetFile(path.toString) { df =>
                val shuffled = df
                  .select($"_1")
                  .repartition(10, col(c))
                checkShuffleAnswer(shuffled, 1, checkNativeOperators = execEnabled)
              }
            }
          }
        }
      }
    }
  }

  test("hash-based native shuffle") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
      val shuffled1 = df.repartition(10, $"_1")
      checkShuffleAnswer(shuffled1, 1)

      val shuffled2 = df.repartition(10, $"_1", $"_2")
      checkShuffleAnswer(shuffled2, 1)

      val shuffled3 = df.repartition(10, $"_2", $"_1")
      checkShuffleAnswer(shuffled3, 1)
    }
  }

  test("native shuffle: single partition") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)

      val shuffled = df.repartition(1)
      checkShuffleAnswer(shuffled, 1)
    }
  }

  test("native shuffle with dictionary of binary") {
    Seq("true", "false").foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 1000).map(i => (i % 5, (i % 5).toString.getBytes())),
        "tbl",
        dictionaryEnabled.toBoolean) {
        val shuffled = sql("SELECT * FROM tbl").repartition(2, $"_2")
        checkShuffleAnswer(shuffled, 1)
      }
    }
  }

  test("native operator after native shuffle") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl")

      val shuffled1 = df
        .repartition(10, $"_2")
        .select($"_1", $"_1" + 1, $"_2" + 2)
        .repartition(10, $"_1")
        .filter($"_1" > 1)

      // 2 Comet shuffle exchanges are expected
      checkShuffleAnswer(shuffled1, 2)

      val shuffled2 = df
        .repartitionByRange(10, $"_2")
        .select($"_1", $"_1" + 1, $"_2" + 2)
        .repartition(10, $"_1")
        .filter($"_1" > 1)

      // Because the first exchange from the bottom is range exchange which native shuffle
      // doesn't support. So Comet exec operators stop before the first exchange and thus
      // there is no Comet exchange.
      checkShuffleAnswer(shuffled2, 0)
    }
  }

  test("grouped aggregate: native shuffle") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df = sql("SELECT count(_2), sum(_2) FROM tbl GROUP BY _1")
      checkShuffleAnswer(df, 1, checkNativeOperators = true)
    }
  }

  test("native shuffle metrics") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
      val shuffled = df.repartition(10, $"_1")

      checkShuffleAnswer(shuffled, 1)

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

  test("fix: Dictionary arrays imported from native should not be overridden") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "10") {
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

  test("fix: Comet native shuffle with binary data") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT cast(cast(_1 as STRING) as BINARY) as binary, _2 FROM tbl")

      val shuffled = df.repartition(1, $"binary")
      checkShuffleAnswer(shuffled, 1)
    }
  }

  /**
   * Checks that `df` produces the same answer as Spark does, and has the `expectedNum` Comet
   * exchange operators. When `checkNativeOperators` is true, this also checks that all operators
   * used by `df` are Comet native operators.
   */
  private def checkShuffleAnswer(
      df: DataFrame,
      expectedNum: Int,
      checkNativeOperators: Boolean = false): Unit = {
    checkCometExchange(df, expectedNum, true)
    if (checkNativeOperators) {
      checkSparkAnswerAndOperator(df)
    } else {
      checkSparkAnswer(df)
    }
  }
}

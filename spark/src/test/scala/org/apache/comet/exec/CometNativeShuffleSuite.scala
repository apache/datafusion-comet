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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus

class CometNativeShuffleSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf
      .set(CometConf.COMET_EXEC_ENABLED.key, "true")
      .set(CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key, "false")
      .set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
  }

  import testImplicits._

  // TODO: this test takes ~5mins to run, we should reduce the test time.
  test("fix: Too many task completion listener of ArrowReaderIterator causes OOM") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "1") {
      withParquetTable((0 until 1000000).map(i => (1, (i + 1).toLong)), "tbl") {
        assert(
          sql("SELECT * FROM tbl").repartition(201, $"_1").count() == sql("SELECT * FROM tbl")
            .count())
      }
    }
  }

  test("native shuffle: different data type") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 1000)
        var allTypes: Seq[Int] = (1 to 20)
        if (isSpark34Plus) {
          allTypes = allTypes.filterNot(Set(14, 17).contains)
        }
        allTypes.map(i => s"_$i").foreach { c =>
          withSQLConf(
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            "parquet.enable.dictionary" -> dictionaryEnabled.toString) {
            readParquetFile(path.toString) { df =>
              val shuffled = df
                .select($"_1")
                .repartition(10, col(c))
              checkCometExchange(shuffled, 1, true)
              checkSparkAnswerAndOperator(shuffled)
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

      checkCometExchange(shuffled1, 1, true)
      checkSparkAnswer(shuffled1)

      val shuffled2 = df.repartition(10, $"_1", $"_2")

      checkCometExchange(shuffled2, 1, true)
      checkSparkAnswer(shuffled2)

      val shuffled3 = df.repartition(10, $"_2", $"_1")

      checkCometExchange(shuffled3, 1, true)
      checkSparkAnswer(shuffled3)
    }
  }

  test("columnar shuffle: single partition") {
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

  test("native shuffle metrics") {
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

  test("fix: Dictionary arrays imported from native should not be overridden") {
    Seq(10, 201).foreach { numPartitions =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "10",
        CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_EXPR_ENABLED.key -> "true") {
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

  test("fix: comet native shuffle with binary data") {
    withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT cast(cast(_1 as STRING) as BINARY) as binary, _2 FROM tbl")

      val shuffled = df.repartition(1, $"binary")

      checkCometExchange(shuffled, 1, true)
      checkSparkAnswer(shuffled)
    }
  }
}

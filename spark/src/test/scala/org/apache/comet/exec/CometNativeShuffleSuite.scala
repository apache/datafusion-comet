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

import scala.concurrent.duration.DurationInt

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf

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
    // https://github.com/apache/datafusion-comet/issues/1538
    assume(CometConf.COMET_NATIVE_SCAN_IMPL.get() != CometConf.SCAN_NATIVE_DATAFUSION)
    Seq(true, false).foreach { execEnabled =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 1000)
          var allTypes: Seq[Int] = (1 to 20)
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

  test("native shuffle on nested array") {
    Seq("false", "true").foreach { _ =>
      Seq(10, 201).foreach { numPartitions =>
        Seq("1.0", "10.0").foreach { ratio =>
          withSQLConf(
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> ratio,
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
            withParquetTable(
              (0 until 50).map(i => (i, Seq(Seq(i + 1), Seq(i + 2), Seq(i + 3)), i + 1)),
              "tbl") {
              var df = sql("SELECT * FROM tbl")
                .filter($"_3" > 10)
                .repartition(numPartitions, $"_2")

              // Partitioning on nested array falls back to Spark
              checkShuffleAnswer(df, 0)

              df = sql("SELECT * FROM tbl")
                .filter($"_3" > 10)
                .repartition(numPartitions, $"_1")

              // Partitioning on primitive type, with nested array in other cols works with native.
              checkShuffleAnswer(df, 1)
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
    Seq("true", "false").zip(Seq("true", "false")).foreach { partitioning =>
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_WITH_HASH_PARTITIONING_ENABLED.key -> partitioning._1,
        CometConf.COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED.key -> partitioning._2) {
        withParquetTable((0 until 5).map(i => (i, (i + 1).toLong)), "tbl") {
          val df = sql("SELECT * FROM tbl")

          val shuffled = df
            .repartition(10, $"_2")
            .select($"_1", $"_1" + 1, $"_2" + 2)
            .repartition(10, $"_1")
            .filter($"_1" > 1)

          // We expect a hash and range partitioned exchanges. If both are true, we'll get two
          // native exchanges. Otherwise both will fall back.
          if (partitioning._1 == "true" && partitioning._2 == "true") {
            checkShuffleAnswer(shuffled, 2)
          } else {
            checkShuffleAnswer(shuffled, 0)
          }
        }
      }
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

      assert(metrics.contains("shuffleBytesWritten"))
      assert(metrics("shuffleBytesWritten").value > 0)

      assert(metrics.contains("dataSize"))
      assert(metrics("dataSize").value > 0L)

      assert(metrics.contains("shuffleWriteTime"))
      assert(metrics("shuffleWriteTime").value > 0L)
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

  test("fix: Comet native shuffle deletes shuffle files after query") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      var df = sql("SELECT count(_2), sum(_2) FROM tbl GROUP BY _1")
      df.collect()
      val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager
      assert(diskBlockManager.getAllFiles().nonEmpty)
      df = null
      eventually(timeout(30.seconds), interval(1.seconds)) {
        System.gc()
        assert(diskBlockManager.getAllFiles().isEmpty)
      }
    }
  }

  // This duplicates behavior seen in a much more complicated Spark SQL test
  // group-analytics.sql
  test("range partitioning with GROUPING functions should not cause ClassCastException") {
    // Reproduces the issue where Cast expressions in ORDER BY cause ClassCastException
    // when trying to cast to AttributeReference in RangePartitioning deduplication logic
    withParquetTable(
      Seq(("Math", 2020), ("Math", 2021), ("Physics", 2020), ("Physics", 2021)),
      "courseSales") {
      val df = sql("""
        SELECT _1, _2, GROUPING(_1), GROUPING(_2)
        FROM courseSales
        GROUP BY CUBE(_1, _2)
        ORDER BY GROUPING(_1), GROUPING(_2), _1, _2
      """)

      // This should not throw ClassCastException during RangePartitioning
      // The ORDER BY with GROUPING functions creates Cast expressions that cause the issue
      df.repartitionByRange(2, col("_1")).collect()
    }
  }

  // This duplicates behavior seen in a much more complicated Spark SQL test
  // "SPARK-44647: test join key is the second cluster key"
  test("range partitioning with duplicate column references") {
    // Test with wider schema and non-adjacent duplicate columns
    withParquetTable(
      (0 until 100).map(i => (i % 10, (i % 5).toByte, i % 3, i % 7, (i % 4).toShort, i.toString)),
      "tbl") {

      val df = sql("SELECT * FROM tbl")

      // Test case 1: Adjacent duplicates (original case)
      val rangePartitioned1 = df.repartitionByRange(3, df.col("_1"), df.col("_1"), df.col("_2"))
      checkShuffleAnswer(rangePartitioned1, 1)

      // Test case 2: Non-adjacent duplicates in wider schema
      // Duplicate _1 at positions 0 and 3, with different columns in between
      val rangePartitioned2 =
        df.repartitionByRange(4, df.col("_1"), df.col("_3"), df.col("_5"), df.col("_1"))
      checkShuffleAnswer(rangePartitioned2, 1)

      // Test case 3: Multiple duplicate pairs
      // _1 duplicated at positions 0,2 and _4 duplicated at positions 1,3
      val rangePartitioned3 =
        df.repartitionByRange(4, df.col("_1"), df.col("_4"), df.col("_1"), df.col("_4"))
      checkShuffleAnswer(rangePartitioned3, 1)

      // Test case 4: Triple duplicates with gaps
      val rangePartitioned4 = df.repartitionByRange(
        5,
        df.col("_1"),
        df.col("_2"),
        df.col("_1"),
        df.col("_3"),
        df.col("_1"))
      checkShuffleAnswer(rangePartitioned4, 1)
    }
  }

  // This adapts the PySpark example in https://github.com/apache/datafusion-comet/issues/1906 to
  // test for incorrect partition values after native RangePartitioning
  test("fix: range partitioning #1906") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED.key -> "true") {
      withParquetTable((0 until 100000).map(i => (i, i + 1)), "tbl") {
        val df = sql("SELECT * from tbl")

        // Repartition with two sort columns
        val df_range_partitioned = df.repartitionByRange(10, df.col("_1"), df.col("_2"))

        val partition_bounds = df_range_partitioned.rdd
          .mapPartitionsWithIndex((idx: Int, iterator: Iterator[Row]) => {
            // Find the min and max value in each partition
            var min: Option[Int] = None
            var max: Option[Int] = None
            iterator.foreach((row: Row) => {
              val row_val = row.get(0).asInstanceOf[Int]
              if (min.isEmpty || row_val < min.get) {
                min = Some(row_val)
              }
              if (max.isEmpty || row_val > max.get) {
                max = Some(row_val)
              }
            })
            Iterator.single((idx, min, max))
          })
          .collect()

        // Check min and max values in each partition
        for (i <- partition_bounds.indices.init) {
          val currentPartition = partition_bounds(i)
          val nextPartition = partition_bounds(i + 1)

          if (currentPartition._2.isDefined && currentPartition._3.isDefined) {
            val currentMin = currentPartition._2.get
            val currentMax = currentPartition._3.get
            assert(currentMin <= currentMax)
          }

          if (currentPartition._3.isDefined && nextPartition._2.isDefined) {
            val currentMax = currentPartition._3.get
            val nextMin = nextPartition._2.get
            assert(currentMax <= nextMin)
          }
        }

      }
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

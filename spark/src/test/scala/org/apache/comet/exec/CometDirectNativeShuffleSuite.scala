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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf

/**
 * Test suite for the direct native shuffle execution optimization.
 *
 * This optimization allows the native shuffle writer to directly execute the child native plan
 * instead of reading intermediate batches via JNI. This avoids the JNI round-trip for
 * single-source native plans (e.g., Scan -> Filter -> Project -> Shuffle).
 */
class CometDirectNativeShuffleSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion",
        CometConf.COMET_SHUFFLE_DIRECT_NATIVE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  import testImplicits._

  test("direct native execution: simple scan with hash partitioning") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(10, $"_1")

      // Verify the optimization is applied
      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1, "Expected exactly one shuffle")
      assert(
        shuffles.head.isDirectNativeExecution,
        "Direct native execution should be enabled for single-source native scan")

      // Verify correctness
      checkSparkAnswer(df)
    }
  }

  test("direct native execution: scan with filter and project") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong, i.toString)), "tbl") {
      val df = sql("SELECT _1, _2 * 2 as doubled FROM tbl WHERE _1 > 10")
        .repartition(10, $"_1")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(
        shuffles.head.isDirectNativeExecution,
        "Direct native execution should work with filter and project")

      checkSparkAnswer(df)
    }
  }

  test("direct native execution: single partition") {
    withParquetTable((0 until 50).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(1)

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(
        shuffles.head.isDirectNativeExecution,
        "Direct native execution should work with single partition")

      checkSparkAnswer(df)
    }
  }

  test("direct native execution: multiple hash columns") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong, i.toString)), "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(10, $"_1", $"_2")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(
        shuffles.head.isDirectNativeExecution,
        "Direct native execution should work with multiple hash columns")

      checkSparkAnswer(df)
    }
  }

  test("direct native execution: aggregation before shuffle") {
    withParquetTable((0 until 100).map(i => (i % 10, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT _1, SUM(_2) as total FROM tbl GROUP BY _1")
        .repartition(5, col("_1"))

      // This involves partial aggregation -> shuffle -> final aggregation
      // The direct native execution applies to the shuffle that reads from the partial agg
      checkSparkAnswer(df)
    }
  }

  test("direct native execution disabled: config is false") {
    withSQLConf(CometConf.COMET_SHUFFLE_DIRECT_NATIVE_ENABLED.key -> "false") {
      withParquetTable((0 until 50).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl").repartition(10, $"_1")

        val shuffles = findShuffleExchanges(df)
        assert(shuffles.length == 1)
        assert(
          !shuffles.head.isDirectNativeExecution,
          "Direct native execution should be disabled when config is false")

        checkSparkAnswer(df)
      }
    }
  }

  test("direct native execution disabled: range partitioning") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").repartitionByRange(10, $"_1")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(
        !shuffles.head.isDirectNativeExecution,
        "Direct native execution should not be used for range partitioning")

      checkSparkAnswer(df)
    }
  }

  test("direct native execution disabled: JVM columnar shuffle mode") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      withParquetTable((0 until 50).map(i => (i, (i + 1).toLong)), "tbl") {
        val df = sql("SELECT * FROM tbl").repartition(10, $"_1")

        // JVM shuffle mode uses CometColumnarShuffle, not CometNativeShuffle
        val shuffles = findShuffleExchanges(df)
        shuffles.foreach { shuffle =>
          assert(
            !shuffle.isDirectNativeExecution,
            "Direct native execution should not be used with JVM shuffle mode")
        }

        checkSparkAnswer(df)
      }
    }
  }

  test("direct native execution: multiple shuffles in same query") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl")
        .repartition(10, $"_1")
        .select($"_1", $"_2" + 1 as "_2_plus")
        .repartition(5, $"_2_plus")

      // First shuffle reads from scan, second reads from previous shuffle output
      // Only the first shuffle should use direct native execution
      val shuffles = findShuffleExchanges(df)
      // AQE might combine some shuffles, so just verify results are correct
      checkSparkAnswer(df)
    }
  }

  test("direct native execution: various data types") {
    withParquetTable(
      (0 until 50).map(i =>
        (i, i.toLong, i.toFloat, i.toDouble, i.toString, i % 2 == 0, BigDecimal(i))),
      "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(10, $"_1")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(shuffles.head.isDirectNativeExecution)

      checkSparkAnswer(df)
    }
  }

  test("direct native execution: complex filter and multiple projections") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong, i % 5)), "tbl") {
      val df = sql("""
          |SELECT _1 * 2 as doubled,
          |       _2 + _3 as sum_col,
          |       _1 + _2 as combined
          |FROM tbl
          |WHERE _1 > 20 AND _3 < 3
          |""".stripMargin)
        .repartition(10, col("doubled"))

      val shuffles = findShuffleExchanges(df)
      // Note: Native shuffle might fall back depending on expression support
      // Just verify correctness - the optimization is best-effort
      checkSparkAnswer(df)
    }
  }

  test("direct native execution: results match non-optimized path") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong, i.toString)), "tbl") {
      // Run with optimization enabled
      val dfOptimized = sql("SELECT _1, _2 FROM tbl WHERE _1 > 50").repartition(10, $"_1")
      val optimizedResult = dfOptimized.collect().sortBy(_.getInt(0))

      // Run with optimization disabled and collect results
      var nonOptimizedResult: Array[org.apache.spark.sql.Row] = Array.empty
      withSQLConf(CometConf.COMET_SHUFFLE_DIRECT_NATIVE_ENABLED.key -> "false") {
        val dfNonOptimized = sql("SELECT _1, _2 FROM tbl WHERE _1 > 50").repartition(10, $"_1")
        nonOptimizedResult = dfNonOptimized.collect().sortBy(_.getInt(0))
      }

      // Results should match
      assert(optimizedResult.length == nonOptimizedResult.length, "Row counts should match")
      optimizedResult.zip(nonOptimizedResult).foreach { case (opt, nonOpt) =>
        assert(opt == nonOpt, s"Rows should match: $opt vs $nonOpt")
      }
    }
  }

  test("direct native execution: large number of partitions") {
    withParquetTable((0 until 1000).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(201, $"_1")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(shuffles.head.isDirectNativeExecution)

      checkSparkAnswer(df)
    }
  }

  test("direct native execution: empty table") {
    withParquetTable(Seq.empty[(Int, Long)], "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(10, $"_1")

      // Should handle empty tables gracefully
      val result = df.collect()
      assert(result.isEmpty)
    }
  }

  test("direct native execution: all rows filtered out") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl WHERE _1 > 1000").repartition(10, $"_1")

      val shuffles = findShuffleExchanges(df)
      assert(shuffles.length == 1)
      assert(shuffles.head.isDirectNativeExecution)

      val result = df.collect()
      assert(result.isEmpty, "Result should be empty when all rows are filtered")
    }
  }

  /**
   * Helper method to find CometShuffleExchangeExec nodes in a DataFrame's execution plan.
   */
  private def findShuffleExchanges(df: DataFrame): Seq[CometShuffleExchangeExec] = {
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    plan.collect { case s: CometShuffleExchangeExec => s }
  }
}

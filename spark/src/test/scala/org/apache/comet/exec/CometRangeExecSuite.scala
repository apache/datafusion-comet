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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.{CometRangeExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometRangeExecSuite extends CometTestBase {

  // CometTestBase enables the Spark-to-Arrow conversions, including the one for Range. Use their
  // production defaults so these tests see CometRangeExec on its own.
  override protected def sparkConf: SparkConf =
    super.sparkConf.setAll(sparkToArrowConversionConfs(enabled = false))

  /** Defines a test that runs with `CometRangeExec` enabled. */
  private def rangeTest(name: String)(f: => Unit): Unit =
    test(name) {
      withSQLConf(CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true")(f)
    }

  private def cometRanges(plan: SparkPlan): Seq[CometRangeExec] =
    collect(plan) { case r: CometRangeExec => r }

  private def checkCometRange(df: => DataFrame): SparkPlan = {
    val (_, cometPlan) = checkSparkAnswerAndOperator(df, Seq(classOf[CometRangeExec]))
    cometPlan
  }

  // (start, end, step, splits). The first rows are the cases in Spark's DataFrameRangeSuite.
  private val ranges: Seq[(Long, Long, Long, Int)] = Seq(
    (0L, 10L, 1L, 15),
    (3L, 15L, 3L, 2),
    (1L, -2L, -2L, 6),
    (-3L, -8L, -2L, 1),
    (-8L, -4L, 2L, 1),
    (Long.MinValue, Long.MaxValue, Long.MaxValue, 100),
    (Long.MaxValue, Long.MinValue, Long.MinValue, 100),
    (-1000000L, 1000000L, 111111L, 4),
    (0L, 100L, 2L, 3),
    (100L, -100L, -2L, 3),
    (-1500L, 1500L, 3L, 5),
    (10L, 0L, -1L, 1),
    // More slices than values, and a step larger than the range.
    (0L, 3L, 1L, 8),
    (0L, 10L, 100L, 4),
    // Ranges that end at the edges of the Long range.
    (Long.MaxValue - 10, Long.MaxValue, 3L, 2),
    (Long.MinValue + 10, Long.MinValue, -3L, 2),
    // A product of the index and the step that does not fit in a long, with values that do.
    (Long.MinValue, Long.MaxValue, 1L << 52, 1))

  // Ranges with no rows, which Spark plans as RangeExec over an empty RDD.
  private val emptyRanges: Seq[(Long, Long, Long, Int)] = Seq(
    (1L, -2L, 1L, 4),
    (-10L, -9L, -20L, 1),
    (Long.MaxValue - 3, Long.MinValue + 2, 1L, 2),
    (Long.MaxValue - 3, Long.MaxValue - 3, 1L, 2))

  rangeTest("range is replaced and feeds the native operators above it") {
    val plan = checkCometRange(
      spark.range(0, 1000, 3, 4).selectExpr("id", "id * 2 AS doubled").filter("id % 2 = 0"))
    val range = cometRanges(plan).head
    assert(range.outputPartitioning == range.originalPlan.outputPartitioning)
    assert(range.outputOrdering == range.originalPlan.outputOrdering)
  }

  rangeTest("range matches Spark for edge-case bounds, steps and splits") {
    ranges.foreach { case (start, end, step, splits) =>
      withClue(s"range($start, $end, $step, $splits): ") {
        checkCometRange(spark.range(start, end, step, splits).selectExpr("id", "id % 7 AS m"))
      }
    }
  }

  rangeTest("a range no native operator consumes stays on Spark") {
    // Generating it natively would only add a conversion back to rows.
    val df = spark.range(0, 100, 1, 2).toDF()
    val (_, plan) = checkSparkAnswer(df)
    assert(cometRanges(plan).isEmpty, plan)
    assert(collect(plan) { case r: RangeExec => r }.nonEmpty, plan)
  }

  rangeTest("empty ranges") {
    emptyRanges.foreach { case (start, end, step, splits) =>
      withClue(s"range($start, $end, $step, $splits): ") {
        val df = spark.range(start, end, step, splits).selectExpr("id + 1 AS next")
        checkCometRange(df)
        assert(df.collect().isEmpty)
      }
    }
  }

  rangeTest("empty range under a global aggregate") {
    // The native plan has no partitions, and the aggregate still returns its one row.
    val df = spark.range(1, -2, 1, 4).agg(count("id"), sum("id"))
    checkSparkAnswer(df)
    assert(df.collect().toSeq == Seq(Row(0L, null)))
  }

  rangeTest("a range whose arithmetic may overflow stays on Spark") {
    val reason = "Spark can return different rows for a range whose arithmetic overflows"
    // Spark's generated code returns no rows here, because the end of its batch wraps, and its
    // interpreted RangeExec returns four.
    val overflowing = spark.range(Long.MinValue, Long.MaxValue, 1L << 62, 1).selectExpr("id + 1")
    Seq("true", "false").foreach { codegen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen) {
        checkSparkAnswerAndFallbackReason(overflowing, reason)
      }
    }
    // An element count that does not fit in a long. Only with codegen, since Spark's interpreted
    // RangeExec would produce every value.
    checkSparkAnswerAndFallbackReason(
      spark.range(Long.MinValue, Long.MaxValue, 1, 4).selectExpr("id + 1"),
      reason)
    // A range CometRangeExec declines can still use the Spark-to-Arrow conversion.
    withSQLConf(CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
      val (_, plan) = checkSparkAnswer(overflowing)
      assert(collect(plan) { case c: CometSparkToColumnarExec => c }.nonEmpty, plan)
    }
  }

  rangeTest("a range with fewer than one slice fails as in Spark") {
    Seq(0, -1).foreach { splits =>
      withClue(s"$splits slices: ") {
        val df = spark.range(0, 10, 1, splits).selectExpr("id + 1")
        val (sparkError, cometError) = checkSparkAnswerMaybeThrows(df)
        assert(sparkError.isDefined && cometError.isDefined, (sparkError, cometError))
        assert(cometError.get.getMessage == sparkError.get.getMessage)
        assert(cometRanges(df.queryExecution.executedPlan).isEmpty)
      }
    }
  }

  rangeTest("range with randomized parameters") {
    // Mirrors Spark's DataFrameRangeSuite test of the same name.
    val maxNumSteps = 10L * 1000
    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    def randomBound(): Long = {
      val n = random.nextLong() % (Long.MaxValue / (100 * maxNumSteps))
      if (random.nextBoolean()) n else -n
    }

    for (_ <- 1 to 10) {
      val start = randomBound()
      val end = randomBound()
      val numSteps = (math.abs(random.nextLong()) % maxNumSteps) + 1
      val stepAbs = (math.abs(end - start) / numSteps) + 1
      val step = if (start < end) stepAbs else -stepAbs
      val partitions = random.nextInt(20) + 1

      withClue(s"seed = $seed start = $start end = $end step = $step partitions = $partitions") {
        checkCometRange(spark.range(start, end, step, partitions).agg(count("id"), sum("id")))
      }
    }
  }

  rangeTest("values come out in order across batch boundaries") {
    // Batches of 7 rows split Spark's 1000-value batches at many different offsets.
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "7") {
      Seq((0L, 10000L, 3L, 1), (10000L, 0L, -3L, 1), (-5000L, 5000L, 1L, 7)).foreach {
        case (start, end, step, splits) =>
          withClue(s"range($start, $end, $step, $splits): ") {
            val df = spark.range(start, end, step, splits).selectExpr("id", "id * 3 AS tripled")
            checkCometRange(df)
            assert(df.collect().map(_.getLong(0)).toSeq == (start until end by step))
          }
      }
    }
  }

  rangeTest("SQL range() is replaced") {
    checkCometRange(sql("SELECT id, id * 3 AS tripled FROM range(3)"))
    checkCometRange(sql("SELECT sum(id), count(*) FROM range(5, 0, -1, 2)"))
  }

  rangeTest("output rows metric") {
    val df = spark.range(0, 1000, 1, 3).selectExpr("id + 1")
    df.collect()
    val range = cometRanges(df.queryExecution.executedPlan).head
    // The native operator reports DataFusion's baseline metrics.
    assert(range.metrics("output_rows").value == 1000)
  }

  test("range stays on Spark by default") {
    val df = spark.range(0, 100, 1, 2).selectExpr("id + 1")
    df.collect()
    val plan = df.queryExecution.executedPlan
    assert(cometRanges(plan).isEmpty, plan)
    assert(collect(plan) { case r: RangeExec => r }.nonEmpty, plan)
  }

  rangeTest("takes precedence over the Spark-to-Arrow conversion") {
    withSQLConf(CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
      val plan = checkCometRange(spark.range(0, 100, 1, 2).selectExpr("id + 1"))
      assert(collect(plan) { case c: CometSparkToColumnarExec => c }.isEmpty, plan)
    }
  }

  rangeTest("exchanges are reused over equal ranges but not different ones") {
    def counts(end: Long): DataFrame =
      spark.range(0, end, 1, 4).groupBy((col("id") % 10).as("k")).count()
    checkCometRange(counts(100).union(counts(200)))
    val plan = checkCometRange(counts(100).union(counts(100)))
    assertExchangeReuseOver(plan, "equal ranges") { case r: CometRangeExec => r }
  }
}

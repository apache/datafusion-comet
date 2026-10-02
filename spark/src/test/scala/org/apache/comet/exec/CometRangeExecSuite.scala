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
import org.apache.spark.sql.comet.{CometNativeRangeExec, CometRangeExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}
import org.apache.spark.sql.functions.{col, count, sum}

import org.apache.comet.CometConf

class CometRangeExecSuite extends CometTestBase {

  // CometTestBase enables the Spark-to-Arrow conversion, which also applies to Range. Use its
  // production default so these tests see CometRangeExec on its own.
  override protected def sparkConf: SparkConf =
    super.sparkConf.remove(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key)

  /**
   * Defines the test once for each generator: native (`CometNativeRangeExec`) and JVM
   * (`CometRangeExec`). The body receives the class it should find in the plan.
   */
  private def rangeTest(name: String)(f: Class[_ <: SparkPlan] => Unit): Unit = {
    Seq(true, false).foreach { native =>
      test(s"$name (${if (native) "native" else "jvm"})") {
        withSQLConf(
          CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
          CometConf.COMET_EXEC_RANGE_NATIVE_ENABLED.key -> native.toString) {
          f(if (native) classOf[CometNativeRangeExec] else classOf[CometRangeExec])
        }
      }
    }
  }

  private def cometRanges(plan: SparkPlan): Seq[SparkPlan] =
    collect(plan) {
      case r: CometRangeExec => r
      case r: CometNativeRangeExec => r
    }

  private def originalRange(range: SparkPlan): RangeExec = range match {
    case r: CometRangeExec => r.originalPlan
    case r: CometNativeRangeExec => r.originalPlan
  }

  private def checkCometRange(rangeClass: Class[_ <: SparkPlan], df: => DataFrame): SparkPlan = {
    val (_, cometPlan) = checkSparkAnswerAndOperator(df, Seq(rangeClass))
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
    // Spark's generated code returns no rows here: the batch end wraps around, while the
    // interpreted RangeExec returns four. Comet follows the generated code, which Spark runs by
    // default.
    (Long.MinValue, Long.MaxValue, 1L << 62, 1))

  // Ranges with no rows, which Spark plans as RangeExec over an empty RDD.
  private val emptyRanges: Seq[(Long, Long, Long, Int)] = Seq(
    (1L, -2L, 1L, 4),
    (-10L, -9L, -20L, 1),
    (Long.MaxValue - 3, Long.MinValue + 2, 1L, 2),
    (Long.MaxValue - 3, Long.MaxValue - 3, 1L, 2))

  rangeTest("range is replaced and feeds the native operators above it") { rangeClass =>
    val plan = checkCometRange(
      rangeClass,
      spark.range(0, 1000, 3, 4).selectExpr("id", "id * 2 AS doubled").filter("id % 2 = 0"))
    val range = cometRanges(plan).head
    assert(range.outputPartitioning == originalRange(range).outputPartitioning)
    assert(range.outputOrdering == originalRange(range).outputOrdering)
  }

  rangeTest("range matches Spark for edge-case bounds, steps and splits") { rangeClass =>
    ranges.foreach { case (start, end, step, splits) =>
      withClue(s"range($start, $end, $step, $splits): ") {
        // Read the range on its own, and under a native projection.
        checkCometRange(rangeClass, spark.range(start, end, step, splits).toDF())
        checkCometRange(
          rangeClass,
          spark.range(start, end, step, splits).selectExpr("id", "id % 7 AS m"))
      }
    }
  }

  rangeTest("empty ranges") { rangeClass =>
    emptyRanges.foreach { case (start, end, step, splits) =>
      withClue(s"range($start, $end, $step, $splits): ") {
        val df = spark.range(start, end, step, splits).selectExpr("id + 1 AS next")
        checkCometRange(rangeClass, df)
        assert(df.collect().isEmpty)
      }
    }
  }

  rangeTest("range with randomized parameters") { rangeClass =>
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
      val expected = start until end by step

      withClue(s"seed = $seed start = $start end = $end step = $step partitions = $partitions") {
        val df = spark.range(start, end, step, partitions).agg(count("id"), sum("id"))
        checkCometRange(rangeClass, df)
        val row = df.collect().head
        assert(row.getLong(0) == expected.size)
        if (expected.nonEmpty) {
          assert(row.getLong(1) == expected.sum)
        }
      }
    }
  }

  rangeTest("values come out in order across batch boundaries") { rangeClass =>
    // Batches of 7 rows split Spark's 1000-value batches at many different offsets.
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "7") {
      Seq((0L, 10000L, 3L, 1), (10000L, 0L, -3L, 1), (-5000L, 5000L, 1L, 7)).foreach {
        case (start, end, step, splits) =>
          withClue(s"range($start, $end, $step, $splits): ") {
            val df = spark.range(start, end, step, splits).toDF()
            assert(cometRanges(df.queryExecution.executedPlan).nonEmpty)
            assert(df.collect().toSeq == (start until end by step).map(Row(_)))
            checkCometRange(
              rangeClass,
              spark.range(start, end, step, splits).selectExpr("id * 3"))
          }
      }
    }
  }

  rangeTest("SQL range() is replaced") { rangeClass =>
    checkCometRange(rangeClass, sql("SELECT id, id * 3 AS tripled FROM range(3)"))
    checkCometRange(rangeClass, sql("SELECT sum(id), count(*) FROM range(5, 0, -1, 2)"))
  }

  rangeTest("output rows metric") { rangeClass =>
    val df = spark.range(0, 1000, 1, 3).selectExpr("id + 1")
    df.collect()
    val range = cometRanges(df.queryExecution.executedPlan).head
    // The native operator reports DataFusion's baseline metrics.
    val metric =
      if (rangeClass == classOf[CometNativeRangeExec]) "output_rows" else "numOutputRows"
    assert(range.metrics(metric).value == 1000)
  }

  test("range stays on Spark by default") {
    val df = spark.range(0, 100, 1, 2).selectExpr("id + 1")
    df.collect()
    val plan = df.queryExecution.executedPlan
    assert(cometRanges(plan).isEmpty, plan)
    assert(collect(plan) { case r: RangeExec => r }.nonEmpty, plan)
  }

  rangeTest("takes precedence over the Spark-to-Arrow conversion") { rangeClass =>
    withSQLConf(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true") {
      val plan = checkCometRange(rangeClass, spark.range(0, 100, 1, 2).selectExpr("id + 1"))
      assert(collect(plan) { case c: CometSparkToColumnarExec => c }.isEmpty, plan)
    }
  }

  rangeTest("exchanges over different ranges are not reused") { rangeClass =>
    def counts(end: Long): DataFrame =
      spark.range(0, end, 1, 4).groupBy((col("id") % 10).as("k")).count()
    checkCometRange(rangeClass, counts(100).union(counts(200)))
  }

  rangeTest("exchanges over equal ranges are reused") { rangeClass =>
    def counts(): DataFrame =
      spark.range(0, 100, 1, 4).groupBy((col("id") % 10).as("k")).count()
    val plan = checkCometRange(rangeClass, counts().union(counts()))
    assertExchangeReuseOver(plan, "equal ranges") {
      case r: CometRangeExec => r
      case r: CometNativeRangeExec => r
    }
  }
}

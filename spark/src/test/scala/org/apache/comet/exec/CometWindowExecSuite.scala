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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Divide, Expression, MakeDecimal, WindowExpression}
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometNativeExec, CometSortExec, CometWindowExec, CometWindowGroupLimitExec}
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.window.{WindowExec => SparkWindowExec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, expr, lead, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.{CometConf, CometExplainInfo, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}

class CometWindowExecSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "true",
        "spark.comet.operator.WindowExec.allowIncompatible" -> "true",
        "spark.comet.explain.fallback.enabled" -> "true",
        "spark.comet.explain.fallback.log.enabled" -> "true",
        "spark.comet.exec.localTableScan.enabled" -> "true") {
        testFun
      }
    }
  }

  private def assertCometWindowExecExists(plan: SparkPlan): Unit = {
    val cometWindowExecs = collect(plan) { case w: CometWindowExec =>
      w
    }
    assert(cometWindowExecs.nonEmpty)
  }

  private def sparkWindowExpressions(plan: SparkPlan): Seq[Expression] = {
    collect(plan) { case w: SparkWindowExec =>
      w.windowExpression
    }.flatten
  }

  private def assertSparkPlanHasDecimalAvgRewrite(plan: SparkPlan): Unit = {
    val windowExprs = sparkWindowExpressions(plan)
    assert(
      windowExprs.exists {
        case Alias(Cast(Divide(_: WindowExpression, _, _), _: DecimalType, _, _), _) => true
        case _ => false
      },
      s"Expected Spark decimal AVG rewrite in window expressions, but found: $windowExprs")
  }

  private def assertSparkPlanHasDecimalSumRewrite(plan: SparkPlan): Unit = {
    val windowExprs = sparkWindowExpressions(plan)
    assert(
      windowExprs.exists {
        case Alias(MakeDecimal(_: WindowExpression, _, _, _), _) => true
        case _ => false
      },
      s"Expected Spark decimal SUM rewrite in window expressions, but found: $windowExprs")
  }

  private def randomDecimalString(rng: Random, precision: Int, scale: Int): String = {
    val maxIntegerDigits = math.max(1, precision - scale)
    val integerDigits = 1 + rng.nextInt(math.min(maxIntegerDigits, 6))
    val integerPart = randomDigits(rng, integerDigits, allowLeadingZero = false)
    val fractionalPart = randomDigits(rng, scale, allowLeadingZero = true)
    val sign = if (rng.nextBoolean()) "-" else ""
    if (scale == 0) {
      s"$sign$integerPart"
    } else {
      s"$sign$integerPart.$fractionalPart"
    }
  }

  private def randomDigits(rng: Random, length: Int, allowLeadingZero: Boolean): String = {
    val digits = new StringBuilder(length)
    (0 until length).foreach { pos =>
      val digit =
        if (pos == 0 && !allowLeadingZero) {
          1 + rng.nextInt(9)
        } else {
          rng.nextInt(10)
        }
      digits.append(('0' + digit).toChar)
    }
    digits.toString()
  }

  for {
    orderColumn <- Seq("f", "d")
    // Scalar floating-point order keys are admitted under strict floating point (#5506), so the
    // native plan and the rank distribution must be identical either way.
    strictFloatingPoint <- Seq("false", "true")
  } {
    test(
      s"window group limit: $orderColumn NaN and signed zero peers at the cutoff " +
        s"(strictFloatingPoint=$strictFloatingPoint)") {
      assume(isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")

      val positiveFloatNaN = java.lang.Float.intBitsToFloat(0x7fc00001)
      val negativeFloatNaN = java.lang.Float.intBitsToFloat(0xffc00002)
      val positiveDoubleNaN = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
      val negativeDoubleNaN = java.lang.Double.longBitsToDouble(0xfff8000000000002L)
      val data = Seq(
        (0, 1, Some(positiveFloatNaN), Some(positiveDoubleNaN), 1),
        (0, 2, Some(negativeFloatNaN), Some(negativeDoubleNaN), 0),
        (0, 3, Some(positiveFloatNaN), Some(positiveDoubleNaN), 0),
        (0, 4, Some(negativeFloatNaN), Some(negativeDoubleNaN), 1),
        (0, 5, Some(1.0f), Some(1.0d), 0),
        (0, 6, None, None, 0),
        (1, 7, Some(0.0f), Some(0.0d), 1),
        (1, 8, Some(-0.0f), Some(-0.0d), 0),
        (1, 9, Some(0.0f), Some(0.0d), 0),
        (1, 10, Some(-0.0f), Some(-0.0d), 1),
        (1, 11, Some(-1.0f), Some(-1.0d), 0),
        (1, 12, None, None, 0))
      val expectedBits = data.map { case (_, id, f, d, _) =>
        (
          id,
          (f.map(java.lang.Float.floatToRawIntBits), d.map(java.lang.Double.doubleToRawLongBits)))
      }.toMap

      def assertRawBits(rows: Seq[Row]): Unit = {
        rows.foreach { row =>
          val floatBits =
            if (row.isNullAt(2)) None
            else Some(java.lang.Float.floatToRawIntBits(row.getFloat(2)))
          val doubleBits =
            if (row.isNullAt(3)) None
            else Some(java.lang.Double.doubleToRawLongBits(row.getDouble(3)))
          assert((floatBits, doubleBits) == expectedBits(row.getInt(1)))
        }
      }

      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> strictFloatingPoint,
        CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "true") {
        withTempView("floating_window_peers") {
          // LocalTableScan preserves NaN payloads, unlike a Parquet round trip. Check the input
          // bits so that canonicalization before the native sort cannot hide the regression.
          data
            .toDF("p", "id", "f", "d", "secondary")
            .createOrReplaceTempView("floating_window_peers")
          assertRawBits(sql("SELECT * FROM floating_window_peers").collect().toSeq)

          for {
            rankFunction <- Seq("RANK", "DENSE_RANK")
            (ordering, cutoff) <- Seq("DESC NULLS LAST" -> 1, "ASC NULLS FIRST" -> 3)
            secondaryKey <- Seq(false, true)
          } {
            // With a secondary key, sorting raw NaN/zero representations separates rows that
            // must be peers. Normalizing only the limit operator's equality keys is insufficient.
            val orderBy = s"$orderColumn $ordering" + (if (secondaryKey) ", secondary" else "")
            val query = sql(s"""
                 |SELECT p, id, f, d, rnk FROM (
                 |  SELECT *, $rankFunction() OVER (PARTITION BY p ORDER BY $orderBy) AS rnk
                 |  FROM floating_window_peers
                 |) WHERE rnk <= $cutoff
                 |""".stripMargin)
            checkSparkAnswerAndOperator(
              query,
              Seq(classOf[CometSortExec], classOf[CometWindowGroupLimitExec]))

            val actual = query.collect().toSeq
            val peerIds = if (secondaryKey) Seq(2, 3, 8, 9) else Seq(1, 2, 3, 4, 7, 8, 9, 10)
            val preceding = if (cutoff == 3) Seq(6 -> 1, 12 -> 1, 5 -> 2, 11 -> 2) else Seq.empty
            val expected = peerIds.map(_ -> cutoff) ++ preceding
            assert(actual.map(row => row.getInt(1) -> row.getInt(4)).sorted == expected.sorted)
            // Canonical comparison keys must not alter the selected rows' floating values.
            assertRawBits(actual)
          }
        }
      }
    }
  }

  test("window group limit: floating-point values nested in the order key") {
    assume(isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")
    // The native RANK and DENSE_RANK find ties by byte equality, on order keys that the native
    // planner has normalized, nested floats included. So [-0.0] and [0.0] are peers, and the
    // cutoff keeps both rows, as Spark does (#5507). These keys, read back from Parquet, can hold
    // a null element, but their default null order places it where Spark does (#6476), so they
    // stay native in strict floating-point mode too. ROW_NUMBER never compares peers, and Spark
    // normalizes nested floating-point partition keys itself.
    withTempDir { dir =>
      val path = new Path(dir.toString, "nested_float_order").toString
      Seq((1, -0.0f, -0.0d), (2, 0.0f, 0.0d), (3, 1.0f, 1.0d))
        .toDF("id", "f", "d")
        .write
        .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("nested_float_order")

      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> "false",
        CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "true") {
        for {
          rankFunction <- Seq("RANK", "DENSE_RANK")
          // A struct needs a second key, because a lone struct sort key falls back on its own.
          orderBy <- Seq("array(f)", "array(d)", "named_struct('x', d), id > 0")
        } {
          def rankLimit(): DataFrame = sql(s"""
               |SELECT id FROM (
               |  SELECT id, $rankFunction() OVER (ORDER BY $orderBy) AS rnk
               |  FROM nested_float_order
               |) WHERE rnk <= 1
               |""".stripMargin)

          checkSparkAnswerAndOperator(rankLimit(), Seq(classOf[CometWindowGroupLimitExec]))
          assert(rankLimit().collect().map(_.getInt(0)).sorted.toSeq == Seq(1, 2))

          withSQLConf(CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> "true") {
            checkSparkAnswerAndOperator(rankLimit(), Seq(classOf[CometWindowGroupLimitExec]))
            assert(rankLimit().collect().map(_.getInt(0)).sorted.toSeq == Seq(1, 2))
          }
        }

        checkSparkAnswerAndOperator(
          sql("""
              |SELECT id FROM (
              |  SELECT id, ROW_NUMBER() OVER (PARTITION BY id > 0 ORDER BY array(d), id) AS rn
              |  FROM nested_float_order
              |) WHERE rn <= 2
              |""".stripMargin),
          Seq(classOf[CometWindowGroupLimitExec]))

        checkSparkAnswer(sql("""
            |SELECT id FROM (
            |  SELECT id, RANK() OVER (PARTITION BY array(d) ORDER BY id) AS rnk
            |  FROM nested_float_order
            |) WHERE rnk <= 1
            |""".stripMargin))
      }
    }
  }

  test("window group limit: nested null peers with non-default root null ordering") {
    assume(isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      "spark.sql.optimizer.windowGroupLimitThreshold" -> "1000",
      CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "true") {
      withTempView("nested_null_group_limit") {
        Seq((1, Some(1)), (2, Some(1)), (3, Some(2)), (4, None), (5, None))
          .toDF("id", "i")
          .createOrReplaceTempView("nested_null_group_limit")

        for ((ordering, cutoff, expected) <- Seq(
            ("ASC NULLS LAST", 3, Seq((1, 3), (2, 3), (4, 1), (5, 1))),
            ("DESC NULLS FIRST", 2, Seq((1, 2), (2, 2), (3, 1))))) {
          val query = sql(s"""
               |SELECT id, rnk FROM (
               |  SELECT id, RANK() OVER (ORDER BY array(i) $ordering) AS rnk
               |  FROM nested_null_group_limit
               |) WHERE rnk <= $cutoff
               |""".stripMargin)
          checkSparkAnswerAndOperator(
            query,
            Seq(classOf[CometSortExec], classOf[CometWindowGroupLimitExec]))
          assert(
            query.collect().map(row => row.getInt(0) -> row.getInt(1)).sorted.toSeq == expected)
        }
      }
    }
  }

  for (orderColumn <- Seq("f", "d")) {
    test(s"window: scalar floating-point order key under strict floating point ($orderColumn)") {
      // Deliberately not gated on Spark 3.5, unlike the WindowGroupLimit tests above, so the
      // admission narrowed in #5506 has window coverage on every supported Spark version.
      // The default RANGE frame spans all peers, so -0.0 and +0.0 landing in one peer group is
      // directly visible in the running sum, and the result does not depend on tie order.
      withTempDir { dir =>
        val path = new Path(dir.toString, "window_strict_fp").toString
        Seq(
          (1, -0.0f, -0.0d),
          (2, 0.0f, 0.0d),
          (3, 1.0f, 1.0d),
          (4, -1.0f, -1.0d),
          (5, Float.NaN, Double.NaN))
          .toDF("id", "f", "d")
          .write
          .parquet(path)
        spark.read.parquet(path).createOrReplaceTempView("window_strict_fp")

        withSQLConf(
          CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> "true",
          CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "false") {
          checkSparkAnswerAndOperator(
            sql(s"""SELECT id, SUM(id) OVER (ORDER BY $orderColumn) AS running
                   |FROM window_strict_fp ORDER BY id""".stripMargin),
            Seq(classOf[CometWindowExec]))
        }
      }
    }
  }

  for {
    partitionColumn <- Seq("f", "d")
    (function, groupLimit) <- Seq(
      "RANK()" -> false,
      "PERCENT_RANK()" -> false,
      "NTILE(2)" -> false,
      "RANK()" -> true)
  } {
    test(
      s"window: floating partition keys ($partitionColumn, $function, group limit=$groupLimit)") {
      assume(!groupLimit || isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")

      val partitionKeys = Seq(
        (Some(1.0f), Some(1.0d)),
        (Some(2.0f), Some(2.0d)),
        (None, None),
        (Some(0.0f), Some(0.0d)),
        (Some(-0.0f), Some(-0.0d)),
        (Some(Float.NaN), Some(Double.NaN)),
        (
          Some(java.lang.Float.intBitsToFloat(0xffc00002)),
          Some(java.lang.Double.longBitsToDouble(0xfff8000000000002L))))
      val data = partitionKeys.zipWithIndex.flatMap { case ((f, d), index) =>
        (0 until 4).map(i => (index * 4 + i, f, d, i % 2))
      }

      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> "false",
        CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> groupLimit.toString) {
        withTempView("floating_window_partitions") {
          data
            .toDF("id", "f", "d", "secondary")
            .createOrReplaceTempView("floating_window_partitions")

          for (partitionBy <- Seq(partitionColumn, s"secondary, $partitionColumn")) {
            // Spark normalizes floating partition keys before constructing the child sort.
            // Native sort and window expressions must still agree on these keys, including
            // for bounded RANK and non-bounded PERCENT_RANK/NTILE without WindowGroupLimit.
            val windowQuery = s"""
                 |SELECT id, $function OVER (PARTITION BY $partitionBy ORDER BY id) AS rnk
                 |FROM floating_window_partitions
                 |""".stripMargin
            val query = if (groupLimit) {
              s"SELECT * FROM ($windowQuery) WHERE rnk <= 1"
            } else {
              windowQuery
            }
            val operators = Seq(classOf[CometSortExec], classOf[CometWindowExec]) ++
              (if (groupLimit) Seq(classOf[CometWindowGroupLimitExec]) else Seq.empty)
            val (_, cometPlan) = checkSparkAnswerAndOperator(sql(query), operators)
            if (!groupLimit) {
              assert(collect(cometPlan) { case w: CometWindowGroupLimitExec => w }.isEmpty)
            }
          }
        }
      }
    }
  }

  test("lead/lag should return the default value if the offset row does not exist") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      checkSparkAnswerAndOperator(sql("""
                             |SELECT
                             |  lag(123, 100, 321) OVER (ORDER BY id) as lag,
                             |  lead(123, 100, 321) OVER (ORDER BY id) as lead
                             |FROM (SELECT 1 as id) tmp
      """.stripMargin))

      checkSparkAnswerAndOperator(sql("""
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

    // ranges are long. Spark encodes PRECEDING/FOLLOWING via the sign of the bound;
    // `rangeBetween(unboundedPreceding, 1L)` produces upper=1 FOLLOWING, which is
    // representable in our proto and runs natively.
    val df2 =
      df.select($"value", sum($"value").over(window.rangeBetween(Window.unboundedPreceding, 1L)))

    val (_, cometPlan) = checkSparkAnswerAndOperator(df2)
    val cometWindowExecs = collect(cometPlan) { case w: CometWindowExec =>
      w
    }
    assert(cometWindowExecs.nonEmpty)
  }

  test("window query with rangeBetween FOLLOWING lower bound falls back to Spark") {
    // `rangeBetween(1L, unboundedFollowing)` puts a positive offset (FOLLOWING semantic)
    // in the lower bound position, which the proto only encodes as Preceding. We fall
    // back to Spark rather than misinterpret the bound.
    val df = Seq(1, 2, 4, 3, 2, 1).toDF("value")
    val window = Window.orderBy($"value".desc)
    val df2 =
      df.select($"value", sum($"value").over(window.rangeBetween(1L, Window.unboundedFollowing)))

    checkSparkAnswerAndFallbackReason(df2, "Unsupported RANGE frame lower offset")
    val cometWindowExecs = collect(df2.queryExecution.executedPlan) { case w: CometWindowExec =>
      w
    }
    assert(cometWindowExecs.isEmpty)
  }

  test("window query with rangeBetween PRECEDING upper bound falls back to Spark") {
    // `rangeBetween(unboundedPreceding, -1L)` puts a negative offset (PRECEDING semantic)
    // in the upper bound position, which the proto only encodes as Following. We fall
    // back to Spark rather than serialize the magnitude as a FOLLOWING bound.
    val df = Seq(1, 2, 4, 3, 2, 1).toDF("value")
    val window = Window.orderBy($"value".desc)
    val df2 =
      df.select($"value", sum($"value").over(window.rangeBetween(Window.unboundedPreceding, -1L)))

    checkSparkAnswerAndFallbackReason(df2, "Unsupported RANGE frame upper offset")
    val cometWindowExecs = collect(df2.queryExecution.executedPlan) { case w: CometWindowExec =>
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
        checkSparkAnswerAndOperator(df2)
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
      checkSparkAnswerAndOperator(df)
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
      checkSparkAnswerAndOperator(df1)
    }
  }

  test("Native window operator reports its output rows and time") {
    withTempDir { dir =>
      (0 until 1000)
        .map(i => (i % 10, i))
        .toDF("k", "v")
        .write
        .mode("overwrite")
        .parquet(dir.toString)
      withTempView("window_metrics") {
        spark.read.parquet(dir.toString).createOrReplaceTempView("window_metrics")
        // `max` needs no cast projection. The planner test in `planner.rs` covers the window
        // that does, such as `row_number`.
        val (_, plan) = checkSparkAnswerAndOperator(
          sql("SELECT max(v) OVER (PARTITION BY k ORDER BY v) FROM window_metrics"))
        val windows = collect(plan) { case w: CometWindowExec => w }
        assert(windows.length == 1)
        assert(windows.head.metrics("output_rows").value == 1000)
        assert(windows.head.metrics("elapsed_compute").value > 0)
      }
    }
  }

  test("Window range frame with long boundary should not fail") {
    val df =
      Seq((1L, "1"), (1L, "1"), (2147483650L, "1"), (3L, "2"), (2L, "1"), (2147483650L, "2"))
        .toDF("key", "value")

    checkSparkAnswerAndOperator(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(0, 2147483648L))))
    checkSparkAnswerAndOperator(
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
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        val df =
          Seq(("a", 1, 1), ("a", 2, 2), ("b", 1, 3), ("b", 1, 4)).toDF("key1", "key2", "value")
        val windowSpec = Window.partitionBy("key1", "key2").orderBy("value")

        val windowed = df
          // repartition by subset of window partitionBy keys which satisfies ClusteredDistribution
          .repartition($"key1")
          .select(lead($"key1", 1).over(windowSpec), lead($"value", 1).over(windowSpec))

        checkSparkAnswerAndOperator(windowed)
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
                  // _10 and _11 are TIMESTAMP columns; Spark allows SUM(timestamp)
                  // via an implicit cast to DOUBLE, which is semantically meaningless
                  // for a real query and introduces a Cast(TimestampType, DoubleType)
                  // that Comet does not support. Exclude SUM for those columns the
                  // same way _12 (DATE) is excluded below.
                  // _9 is high precision decimal and pure Spark fails on this query
                  val aggregateFunctions =
                    if (col == 9 || col == 10 || col == 11) {
                      List(s"COUNT(_$col)", s"MAX(_$col)", s"MIN(_$col)")
                    } else {
                      List(s"COUNT(_$col)", s"MAX(_$col)", s"MIN(_$col)", s"SUM(_$col)")
                    }
                  aggregateFunctions.foreach { function =>
                    val df1 = sql(s"SELECT $function OVER() FROM tbl")
                    checkSparkAnswerAndOperatorWithTol(df1)

                    val df2 = sql(s"SELECT $function OVER(order by _2) FROM tbl")
                    checkSparkAnswerAndOperatorWithTol(df2)

                    val df3 = sql(s"SELECT $function OVER(order by _2 desc) FROM tbl")
                    checkSparkAnswerAndOperatorWithTol(df3)

                    val df4 = sql(s"SELECT $function OVER(partition by _2 order by _2) FROM tbl")
                    checkSparkAnswerAndOperatorWithTol(df4)
                  }
                }

                // SUM doesn't work for Date type. org.apache.spark.sql.AnalysisException will be thrown.
                val aggregateFunctionsWithoutSum = List("COUNT(_12)", "MAX(_12)", "MIN(_12)")
                aggregateFunctionsWithoutSum.foreach { function =>
                  val df1 = sql(s"SELECT $function OVER() FROM tbl")
                  checkSparkAnswerAndOperatorWithTol(df1)

                  val df2 = sql(s"SELECT $function OVER(order by _2) FROM tbl")
                  checkSparkAnswerAndOperatorWithTol(df2)

                  val df3 = sql(s"SELECT $function OVER(order by _2 desc) FROM tbl")
                  checkSparkAnswerAndOperatorWithTol(df3)

                  val df4 = sql(s"SELECT $function OVER(partition by _2 order by _2) FROM tbl")
                  checkSparkAnswerAndOperatorWithTol(df4)
                }
              }
            }
          }
        }
      }
    }
  }

  test("Windows support") {
    for {
      aqeEnabled <- Seq("true", "false")
      ansiEnabled <- Seq("true", "false")
    } {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> ansiEnabled,
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
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
              s"SELECT $function OVER(partition by _2 order by _2) FROM t1")
            queries.foreach { query =>
              checkSparkAnswerAndOperator(query)
            }

            val slidingQueries = Seq(
              s"SELECT $function OVER(rows between 1 preceding and 1 following) FROM t1",
              s"SELECT $function OVER(order by _2 rows between 1 preceding and current row) FROM t1",
              s"SELECT $function OVER(order by _2 rows between current row and 1 following) FROM t1")

            slidingQueries.foreach { query =>
              checkSparkAnswerAndOperator(query)
            }
          }
        }
      }
    }
  }

  for (ansiEnabled <- Seq("true", "false")) {
    test(s"sliding integral ROWS sums (ANSI=$ansiEnabled)") {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> ansiEnabled,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
        val values = Seq(
          (1, 1, Some(Long.MaxValue)),
          (1, 2, Some(1L)),
          (1, 3, Some(-1L)),
          (1, 4, None),
          (1, 5, None),
          (2, 1, Some(Long.MinValue)),
          (2, 2, Some(-1L)),
          (2, 3, Some(1L)),
          (2, 4, None),
          (2, 5, None),
          (3, 1, None),
          (3, 2, None),
          (3, 3, None))
        withParquetTable(values, "sliding_rows_sum") {
          val df = spark.table("sliding_rows_sum").toDF("g", "id", "v")
          // DataFrame bounds are literals, so PRECEDING needs no constant folding.
          val frame = Window.partitionBy("g").orderBy("id").rowsBetween(-1, Window.currentRow)
          checkSparkAnswerAndOperator(df.select($"g", $"id", expr("try_sum(v)").over(frame)))
          val widerFrame =
            Window.partitionBy("g").orderBy("id").rowsBetween(-2, Window.currentRow)
          withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "2") {
            checkSparkAnswerAndOperator(
              df.select($"g", $"id", expr("try_sum(v)").over(widerFrame)))
          }

          // Exercise successful ANSI execution separately from the overflow cases.
          val sumInput = if (SQLConf.get.ansiEnabled) df.where($"id" > 1) else df
          checkSparkAnswerAndOperator(sumInput.select($"g", $"id", sum("v").over(frame)))
          for (dataType <- Seq("tinyint", "smallint", "int")) {
            checkSparkAnswerAndOperator(df.select(sum($"id".cast(dataType)).over(frame)))
          }

          if (SQLConf.get.ansiEnabled) {
            for (group <- Seq(1, 2)) {
              val (sparkError, cometError) =
                checkSparkAnswerMaybeThrows(df.where($"g" === group).select(sum("v").over(frame)))
              assert(sparkError.exists(_.getMessage.contains("ARITHMETIC_OVERFLOW")))
              assert(cometError.exists(_.getMessage.contains("ARITHMETIC_OVERFLOW")))
            }
          }

          // Floating-point sums remain native in both modes.
          checkSparkAnswerAndOperator(
            df.where($"id" > 1)
              .select(
                sum($"v".cast("double")).over(frame),
                expr("try_sum(CAST(v AS DOUBLE))").over(frame)))
        }
      }
    }
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
      checkSparkAnswerAndOperator(df)
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
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: AVG with PARTITION BY and ORDER BY") {
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

  Seq("Sum", "Average").foreach { name =>
    test(s"issue #5499: rewritten decimal window $name retains fallback reasons") {
      withTempView("decimal_window_fallback") {
        Seq((1, "10.10"), (1, "20.25"), (2, "30.33"))
          .toDF("g", "raw_v")
          .selectExpr("g", "CAST(raw_v AS DECIMAL(8,2)) AS v")
          .createOrReplaceTempView("decimal_window_fallback")
        val key = CometConf.getExprEnabledConfigKey(name)
        withSQLConf(
          key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false",
          CometConf.COMET_STRICT_FALLBACK_REASONS.key -> "true") {
          val function = if (name == "Sum") "SUM" else "AVG"
          val df = sql(s"""SELECT $function(v) OVER (
            PARTITION BY g ORDER BY v
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            FROM decimal_window_fallback""")
          val plan = df.queryExecution.executedPlan
          if (name == "Sum") assertSparkPlanHasDecimalSumRewrite(plan)
          else if (isSpark40Plus) assertSparkPlanHasDecimalAvgRewrite(plan)
          val window = collect(plan) { case w: SparkWindowExec => w }.head
          // Spark inserts the row transition after Comet conversion. The strict fallback check
          // saw this native child before that transition was added.
          def unwrapTransitions(child: SparkPlan): SparkPlan = child match {
            case stage: WholeStageCodegenExec => unwrapTransitions(stage.child)
            case adapter: InputAdapter => unwrapTransitions(adapter.child)
            case transition: CometColumnarToRowExec => unwrapTransitions(transition.child)
            case other => other
          }
          assert(unwrapTransitions(window.child).isInstanceOf[CometNativeExec], plan.toString)
          val expected = Set(s"Expression support is disabled. Set $key=true to enable it.")
          assert(window.getTagValue(CometExplainInfo.FALLBACK_REASONS).contains(expected))
          // The original owner must carry the reason from the reconstructed window expression.
          if (name == "Sum" || isSpark40Plus) {
            assert(
              window.windowExpression.head
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .contains(expected))
          }
          assert(new ExtendedExplainInfo().generateExtendedInfo(plan).contains(expected.head))
        }
      }
    }
  }

  test("window: decimal AVG with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      Seq((1, "10.10"), (1, "20.25"), (1, "30.33"), (1, "41.00"), (2, "11.11"), (2, "22.22"))
        .toDF("g", "raw_v")
        .selectExpr("g", "CAST(raw_v AS DECIMAL(10,2)) AS v")
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_avg")
      val df = sql("""
        SELECT g, v, run_avg
        FROM (
          SELECT g, v,
            AVG(v) OVER (
              PARTITION BY g
              ORDER BY v
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS run_avg
          FROM dec_avg
        )
        ORDER BY g, v
      """)
      val (sparkPlan, cometPlan) = checkSparkAnswerAndOperator(df)
      if (isSpark40Plus) {
        assertSparkPlanHasDecimalAvgRewrite(sparkPlan)
      }
      assertCometWindowExecExists(cometPlan)
    }
  }

  test("window: high-precision decimal AVG and TRY_AVG fall back to Spark") {
    withTempDir { dir =>
      Seq((1, 1, "0.6"), (1, 2, "0.6"))
        .toDF("g", "ord", "raw_v")
        .selectExpr(
          "g",
          "ord",
          "CAST(raw_v AS DECIMAL(27,27)) AS v27",
          "CAST(raw_v AS DECIMAL(28,28)) AS v28",
          "CAST(raw_v AS DECIMAL(38,38)) AS v38")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)
      withParquetTable(spark.read.parquet(dir.toString), "window_avg") {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          // Pin the 27/28 boundary and both error modes when the running sum exceeds 38 digits.
          for ((aggregate, precision) <- Seq(
              ("AVG", 27),
              ("AVG", 28),
              ("AVG", 38),
              ("TRY_AVG", 38))) {
            val df = sql(s"""
              SELECT g, ord, $aggregate(v$precision) OVER (
                PARTITION BY g ORDER BY ord
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg
              FROM window_avg ORDER BY g, ord
            """)
            if (precision == 27) {
              val (_, plan) = checkSparkAnswerAndOperator(df)
              assertCometWindowExecExists(plan)
            } else {
              val (_, plan) = checkSparkAnswerAndFallbackReason(
                df,
                "AVG on DECIMAL with precision 28 or more is not supported")
              assert(collect(plan) { case window: SparkWindowExec => window }.nonEmpty)
              assert(collect(plan) { case window: CometWindowExec => window }.isEmpty)
            }
          }
        }
      }
    }
  }

  test("window: decimal AVG fuzz with PARTITION BY and ORDER BY") {
    Seq((9, 1), (9, 4), (10, 2), (11, 3), (11, 6)).foreach { case (precision, scale) =>
      withTempDir { dir =>
        val rng = new Random(precision * 31L + scale)
        (0 until 120)
          .map { i =>
            (i % 7, i, randomDecimalString(rng, precision, scale))
          }
          .toDF("g", "ord", "raw_v")
          .selectExpr("g", "ord", s"CAST(raw_v AS DECIMAL($precision,$scale)) AS v")
          .repartition(1)
          .write
          .mode("overwrite")
          .parquet(dir.toString)

        spark.read.parquet(dir.toString).createOrReplaceTempView("dec_avg_fuzz")
        val df = sql("""
          SELECT g, ord, v, run_avg
          FROM (
            SELECT g, ord, v,
              AVG(v) OVER (
                PARTITION BY g
                ORDER BY ord
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              ) AS run_avg
            FROM dec_avg_fuzz
          )
          ORDER BY g, ord
        """)
        val (sparkPlan, cometPlan) = checkSparkAnswerAndOperator(df)
        if (isSpark40Plus) {
          assertSparkPlanHasDecimalAvgRewrite(sparkPlan)
        }
        assertCometWindowExecExists(cometPlan)
      }
    }
  }

  test("window: decimal SUM with PARTITION BY and ORDER BY") {
    withTempDir { dir =>
      Seq(
        (1, 1, "10.10"),
        (1, 2, "20.25"),
        (1, 3, "-5.35"),
        (2, 1, "11.11"),
        (2, 2, "22.22"),
        (2, 3, "33.33"))
        .toDF("g", "ord", "raw_v")
        .selectExpr("g", "ord", "CAST(raw_v AS DECIMAL(8,2)) AS v")
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_sum")
      val df = sql("""
        SELECT g, ord, v, run_sum
        FROM (
          SELECT g, ord, v,
            SUM(v) OVER (
              PARTITION BY g
              ORDER BY ord
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS run_sum
          FROM dec_sum
        )
        ORDER BY g, ord
      """)
      val (sparkPlan, cometPlan) = checkSparkAnswerAndOperator(df)
      assertSparkPlanHasDecimalSumRewrite(sparkPlan)
      assertCometWindowExecExists(cometPlan)
    }
  }

  test("window: decimal SUM recovers from an intermediate overflow in an expanding frame") {
    // Running sums 0.6, 1.2, 0.6 at DECIMAL(38,38): only the middle frame leaves the
    // precision, so the third row must recover instead of staying latched at null.
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        withTempDir { dir =>
          Seq((1, "0.6"), (2, "0.6"), (3, "-0.6"))
            .toDF("ord", "raw_v")
            .selectExpr("ord", "CAST(raw_v AS DECIMAL(38,38)) AS v")
            .repartition(1)
            .write
            .mode("overwrite")
            .parquet(dir.toString)

          spark.read.parquet(dir.toString).createOrReplaceTempView("dec_sum_recover")
          def runningSums(function: String): DataFrame = sql(s"""
            SELECT ord, run_sum
            FROM (
              SELECT ord,
                $function(v) OVER (
                  ORDER BY ord
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS run_sum
              FROM dec_sum_recover
            )
            ORDER BY ord
          """)
          def assertRecovered(function: String): Unit = {
            val (_, cometPlan) = checkSparkAnswerAndOperator(runningSums(function))
            assertCometWindowExecExists(cometPlan)
            val answer = runningSums(function).collect().toSeq
            val recovered = new java.math.BigDecimal("0.6").setScale(38)
            assert(
              answer == Seq(Row(1, recovered), Row(2, null), Row(3, recovered)),
              s"$function running sums were $answer, expected 0.6, null, 0.6")
          }
          if (ansiEnabled) {
            // The frame whose sum is 1.2 holds a value that does not fit, so Spark fails in
            // toPrecision with the out-of-range error rather than the latched sum overflow.
            val errorClass =
              if (isSpark40Plus) "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
              else "NUMERIC_VALUE_OUT_OF_RANGE"
            checkSparkError(runningSums("SUM"), errorClass)
          } else {
            assertRecovered("SUM")
          }
          // try_sum returns null for the frame that does not fit in every mode.
          assertRecovered("try_sum")
        }
      }
    }
  }

  test("window: decimal AVG at maximum precision falls back for every frame") {
    // Spark's window buffer keeps the running sum unbounded, so DECIMAL(38,38) inputs 0.6, 0.6,
    // -0.6 average to 0.6, 0.6, 0.2 in a running frame and to 0.2 over the whole partition. The
    // native AVG latches once the sum leaves the precision, returning NULL or raising under
    // ANSI instead of Spark's average for the last running frame and the whole partition.
    // https://github.com/apache/datafusion-comet/issues/5418
    val low = new java.math.BigDecimal("0.2").setScale(38)
    val high = new java.math.BigDecimal("0.6").setScale(38)
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        withTempDir { dir =>
          Seq((1, "0.6"), (2, "0.6"), (3, "-0.6"))
            .toDF("ord", "raw_v")
            .selectExpr("ord", "CAST(raw_v AS DECIMAL(38,38)) AS v")
            .repartition(1)
            .write
            .mode("overwrite")
            .parquet(dir.toString)

          spark.read.parquet(dir.toString).createOrReplaceTempView("dec_avg_max_precision")
          for {
            function <- Seq("AVG", "try_avg")
            (frame, expected) <- Seq(
              "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -> Seq(high, high, low),
              "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" -> Seq(low, low, low))
          } {
            val df = sql(s"""
              SELECT ord, $function(v) OVER (ORDER BY ord $frame) AS run_avg
              FROM dec_avg_max_precision
              ORDER BY ord
            """)
            val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
              df,
              "AVG on DECIMAL with precision 28 or more is not supported")
            assert(collect(cometPlan) { case w: CometWindowExec => w }.isEmpty, cometPlan)
            checkAnswer(df, expected.zipWithIndex.map { case (avg, i) => Row(i + 1, avg) })
          }
        }
      }
    }
  }

  test("window: decimal AVG below precision 28 runs natively by default") {
    // Widening the rescaled numerator removes the precision-25 fallback. The separate
    // precision-28 guard still protects Spark's unbounded intermediate sum.
    // https://github.com/apache/datafusion-comet/issues/7007
    withTempDir { dir =>
      Seq((1, "1.5"), (2, "2.5"))
        .toDF("ord", "raw_v")
        .selectExpr("ord", "CAST(raw_v AS DECIMAL(25,2)) AS v")
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_avg_rescale")
      val query = "SELECT ord, AVG(v) OVER (ORDER BY ord) AS run_avg FROM dec_avg_rescale"
      val (_, cometPlan) = checkSparkAnswerAndOperator(query)
      assertCometWindowExecExists(cometPlan)
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
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: COUNT with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      // Excluding c column from the result dataset
      // as there is no output order guarantee for both Spark or DataFusion
      // because C column is not part of partitioning and sorting
      val df = sql("""
        select a, b, cnt from(
          SELECT a, b, c,
            COUNT(*) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cnt
          FROM window_test
        )
      """)
      checkSparkAnswerAndOperator(df)

      val df1 = sql("""
          SELECT a, b, c,
            COUNT(*) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cnt
          FROM window_test
      """)
      checkSparkAnswerAndOperator(df1)
    }
  }

  test("window: SUM with ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    withTempDir { dir =>
      (0 until 30)
        .map(i => (i % 3, i % 5, i))
        .toDF("a", "b", "c")
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          SUM(c) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: AVG with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING") {
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
          AVG(c) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: SUM with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW") {
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
          SUM(c) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: COUNT with ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING") {
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
          COUNT(*) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as cnt
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: MAX with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
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

  test("window: ROW_NUMBER with PARTITION BY and ORDER BY") {
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

  test("window: RANK with PARTITION BY and ORDER BY") {
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

  test("window: DENSE_RANK with PARTITION BY and ORDER BY") {
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

  test("window: PERCENT_RANK with PARTITION BY and ORDER BY") {
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

  test("window: NTILE with PARTITION BY and ORDER BY") {
    // Comet #4255 / DataFusion #22049 reported wrong bucket sizes from
    // NTILE when the partition size is not evenly divisible by the bucket
    // count (e.g. NTILE(4) over 10 rows yielded sizes 3,2,3,2 instead of
    // 3,3,2,2). The bug was fixed upstream by DataFusion #22051 and is
    // present from DataFusion 54.0.0 onwards. This test exercises the
    // previously-buggy uneven-division path natively to catch regressions
    // if Comet is rebased onto a DataFusion that reverts the fix.
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
        SELECT a, b,
          NTILE(4) OVER (PARTITION BY a ORDER BY b) as ntile_4
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LAG with default offset") {
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
          LAG(c) OVER (PARTITION BY a ORDER BY b, c) as lag_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LAG with offset 2 and default value") {
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
          LAG(c, 2, -1) OVER (PARTITION BY a ORDER BY b, c) as lag_c_2
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LAG with IGNORE NULLS") {
    withTempDir { dir =>
      Seq((1, 1, Some(10)), (1, 2, None), (1, 3, Some(30)), (2, 1, None), (2, 2, Some(20)))
        .toDF("a", "b", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b, c,
          LAG(c) IGNORE NULLS OVER (PARTITION BY a ORDER BY b) as lag_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LEAD with default offset") {
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
          LEAD(c) OVER (PARTITION BY a ORDER BY b, c) as lead_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LEAD with offset 2 and default value") {
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
          LEAD(c, 2, -1) OVER (PARTITION BY a ORDER BY b, c) as lead_c_2
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: LEAD with IGNORE NULLS") {
    withTempDir { dir =>
      Seq((1, 1, Some(10)), (1, 2, None), (1, 3, Some(30)), (2, 1, None), (2, 2, Some(20)))
        .toDF("a", "b", "c")
        .repartition(3)
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, b,
          LEAD(c) IGNORE NULLS OVER (PARTITION BY a ORDER BY b) as lead_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: FIRST_VALUE with default ignore nulls") {
    withTempDir { dir =>
      (0 until 30)
        .map(i =>
          (i % 3, i % 5, if (i % 7 == 0) null.asInstanceOf[Integer] else Integer.valueOf(i)))
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

  test("window: LAST_VALUE with ROWS frame") {
    withTempDir { dir =>
      (0 until 30)
        .map(i =>
          (i % 3, i % 5, if (i % 7 == 0) null.asInstanceOf[Integer] else Integer.valueOf(i)))
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

  test("window: NTH_VALUE with position 2") {
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
        SELECT a, b,
          NTH_VALUE(c, 2) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as nth_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: CUME_DIST with PARTITION BY and ORDER BY") {
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

  test("window: multiple window functions in single query") {
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
        SELECT a, b,
          ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) as row_num,
          RANK() OVER (PARTITION BY a ORDER BY b) as rnk,
          SUM(c) OVER (PARTITION BY a ORDER BY b) as sum_c,
          AVG(c) OVER (PARTITION BY a ORDER BY b) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: different window specifications in single query") {
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

  test("window: ORDER BY DESC with aggregation") {
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

  test("window: multiple PARTITION BY columns") {
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

  test("window: multiple ORDER BY columns") {
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

  test("window: RANGE BETWEEN with numeric ORDER BY") {
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

  test("window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
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

  test("window: complex expression in window function") {
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

  test("window: window function with WHERE clause") {
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

  test("window: window function with GROUP BY") {
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

  test("window: ROWS BETWEEN with negative offset") {
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
        SELECT a, b,
          AVG(c) OVER (PARTITION BY a ORDER BY b, c ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as avg_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: all ranking functions together") {
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
        SELECT a, b,
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

  test("window: FIRST_VALUE/LAST_VALUE/NTH_VALUE IGNORE NULLS with leading nulls") {
    withTempDir { dir =>
      Seq(
        ("eng", 1, Option(100), 2020),
        ("eng", 2, Option(100), 2021),
        ("eng", 3, Option(150), 2022),
        ("eng", 4, Option.empty[Int], 2023),
        ("eng", 5, Option(200), 2024),
        ("sales", 6, Option(90), 2020),
        ("sales", 7, Option(90), 2021),
        ("sales", 8, Option.empty[Int], 2022),
        ("sales", 9, Option(110), 2023),
        ("sales", 10, Option(120), 2024),
        ("ops", 11, Option.empty[Int], 2020),
        ("ops", 12, Option.empty[Int], 2021),
        ("ops", 13, Option(50), 2022))
        .toDF("dept", "id", "salary", "hire_yr")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("emp")
      val df = sql("""
        SELECT dept, id, salary,
          first_value(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv_ignore,
          nth_value(salary, 1) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv_ignore,
          last_value(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv_ignore

        FROM emp
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for issue #4834: RANGE frame with explicit PRECEDING/FOLLOWING offset
  // on DATE ORDER BY. The planner wraps the offset as IntervalDayTime so these
  // run natively; `checkSparkAnswerAndOperator` pins that.
  // ---------------------------------------------------------------------------

  private def writeDateWindowFixture(dir: java.io.File): Unit = {
    (0 until 30)
      .map { i =>
        val day = (i % 28) + 1
        (i % 3, java.sql.Date.valueOf(f"2024-01-$day%02d"), i * 2)
      }
      .toDF("a", "d", "c")
      .repartition(3)
      .write
      .mode("overwrite")
      .parquet(dir.toString)
  }

  test("window: RANGE BETWEEN N PRECEDING AND N FOLLOWING with DATE ORDER BY") {
    withTempDir { dir =>
      writeDateWindowFixture(dir)
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN N PRECEDING AND CURRENT ROW with DATE ORDER BY") {
    withTempDir { dir =>
      writeDateWindowFixture(dir)
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN CURRENT ROW AND N FOLLOWING with DATE ORDER BY") {
    withTempDir { dir =>
      writeDateWindowFixture(dir)
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN UNBOUNDED PRECEDING AND N FOLLOWING with DATE ORDER BY") {
    withTempDir { dir =>
      writeDateWindowFixture(dir)
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DATE ORDER BY and duplicate dates") {
    // RANGE (not ROWS) must aggregate all peer rows that share the same date.
    // With duplicates we can tell the two apart: ROWS would only include the
    // literal N rows, RANGE includes every row whose date is within +/- N days.
    withTempDir { dir =>
      Seq(
        (1, java.sql.Date.valueOf("2024-01-01"), 10),
        (1, java.sql.Date.valueOf("2024-01-01"), 20),
        (1, java.sql.Date.valueOf("2024-01-02"), 30),
        (1, java.sql.Date.valueOf("2024-01-02"), 40),
        (1, java.sql.Date.valueOf("2024-01-05"), 50),
        (1, java.sql.Date.valueOf("2024-01-05"), 60))
        .toDF("a", "d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DATE ORDER BY and NULL dates") {
    // Null dates land at the head under ASC NULLS FIRST (Spark's default).
    // Verifies the native frame comparator handles Date32 nulls the same way
    // Spark does when computing the boundary.
    withTempDir { dir =>
      Seq(
        (1, Option.empty[java.sql.Date], 10),
        (1, Option(java.sql.Date.valueOf("2024-01-01")), 20),
        (1, Option(java.sql.Date.valueOf("2024-01-02")), 30),
        (1, Option.empty[java.sql.Date], 40),
        (1, Option(java.sql.Date.valueOf("2024-01-05")), 50))
        .toDF("a", "d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW with DATE ORDER BY") {
    // Regression baseline: no explicit offset, so this ran natively even
    // before DATE offsets did.
    withTempDir { dir =>
      writeDateWindowFixture(dir)
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE offset on DATE ORDER BY that wraps an Int falls back") {
    // Spark adds the offset to the date as a plain Int, so 2147483647 FOLLOWING
    // wraps below the current row and every frame is empty, while DataFusion
    // would treat the overflowing bound as the end of the partition.
    // https://github.com/apache/datafusion-comet/issues/5022
    withTempDir { dir =>
      Seq("2024-01-01", "2024-01-02", "2024-01-05")
        .map(java.sql.Date.valueOf)
        .toDF("d")
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(dir.toString)
      spark.read.parquet(dir.toString).createOrReplaceTempView("date_wrap")
      for (order <- Seq("ASC", "DESC")) {
        val df = sql(s"""
          SELECT d, COUNT(*) OVER (ORDER BY d $order
                                   RANGE BETWEEN CURRENT ROW AND 2147483647 FOLLOWING) AS cnt
          FROM date_wrap
        """)
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          df,
          "RANGE frame on DATE ORDER BY with an offset above 2144550751 days is not supported")
        assert(collect(cometPlan) { case w: CometWindowExec => w }.isEmpty, cometPlan)
        // Descending, the bound is `d - offset`, which does not wrap for these dates.
        if (order == "ASC") {
          checkAnswer(df.select("cnt"), Seq(Row(0L), Row(0L), Row(0L)))
        }
      }
    }
  }

  test("window: largest RANGE offset on DATE ORDER BY that cannot wrap runs natively") {
    // 2144550751 + the day number of 9999-12-31 is Int.MaxValue, so no date in
    // Spark's supported range wraps. Past chrono's date range the native bound
    // becomes the partition edge, which agrees with Spark's unwrapped bound
    // lying beyond every row.
    withTempDir { dir =>
      // 0001-01-01 predates the Gregorian cutover, and Spark 3.x defaults the write rebase mode to
      // EXCEPTION, so write it as-is.
      withSQLConf("spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED") {
        Seq("0001-01-01", "1970-01-01", "2024-01-05", "9999-12-31")
          .map(java.sql.Date.valueOf)
          .toDF("d")
          .repartition(1)
          .write
          .mode("overwrite")
          .parquet(dir.toString)
      }
      spark.read.parquet(dir.toString).createOrReplaceTempView("date_edge")
      for {
        order <- Seq("ASC", "DESC")
        frame <- Seq(
          "RANGE BETWEEN 2144550751 PRECEDING AND CURRENT ROW",
          "RANGE BETWEEN CURRENT ROW AND 2144550751 FOLLOWING",
          "RANGE BETWEEN 2144550751 PRECEDING AND 2144550751 FOLLOWING")
      } {
        val df = sql(s"""
          SELECT d, COUNT(*) OVER (ORDER BY d $order $frame) AS cnt
          FROM date_edge
        """)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for issue #4834: RANGE frame with explicit PRECEDING/FOLLOWING offset
  // on DECIMAL ORDER BY. Comet previously fell back to Spark because DataFusion
  // rejected boundary arithmetic that widened precision (e.g. Decimal(10,0) +
  // offset -> Decimal(11,0) vs current Decimal(10,0), "Uncomparable values").
  // The upstream fix (apache/datafusion#22174, closes #22113) makes
  // ScalarValue::partial_cmp ignore precision for Decimal128 when scales match,
  // and was first released in DataFusion 54.0.0. These tests pin the behavior
  // with `checkSparkAnswerAndOperator` so results must match Spark AND run
  // natively.
  // ---------------------------------------------------------------------------

  private def writeDecimalWindowFixture(dir: java.io.File, decimalType: String): Unit = {
    (0 until 30)
      .map { i =>
        // Fractional part rotates 0.00/0.25/0.50/0.75 so RANGE peers vary; c
        // stays as the aggregate payload.
        val whole = i % 10
        val frac = (i % 4) * 25
        (i % 3, f"$whole%d.$frac%02d", i * 2)
      }
      .toDF("a", "d_str", "c")
      .selectExpr("a", s"CAST(d_str AS $decimalType) AS d", "c")
      .repartition(3)
      .write
      .mode("overwrite")
      .parquet(dir.toString)
  }

  test("window: RANGE BETWEEN N PRECEDING AND N FOLLOWING with DECIMAL ORDER BY") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN N PRECEDING AND CURRENT ROW with DECIMAL ORDER BY") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN CURRENT ROW AND N FOLLOWING with DECIMAL ORDER BY") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN UNBOUNDED PRECEDING AND N FOLLOWING with DECIMAL ORDER BY") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL ORDER BY and duplicate values") {
    // RANGE (not ROWS) must aggregate all peer rows that share the same value.
    // Duplicates let us tell ROWS from RANGE and expose peer-boundary bugs.
    withTempDir { dir =>
      Seq(
        (1, "1.00", 10),
        (1, "1.00", 20),
        (1, "2.00", 30),
        (1, "2.00", 40),
        (1, "5.00", 50),
        (1, "5.00", 60))
        .toDF("a", "d_str", "c")
        .selectExpr("a", "CAST(d_str AS DECIMAL(10, 2)) AS d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL ORDER BY and NULL values") {
    // Null decimals land at the head under ASC NULLS FIRST (Spark's default).
    // Ensures the native comparator handles Decimal nulls the same way.
    withTempDir { dir =>
      Seq(
        (1, Option.empty[String], 10),
        (1, Option("1.00"), 20),
        (1, Option("2.00"), 30),
        (1, Option.empty[String], 40),
        (1, Option("5.00"), 50))
        .toDF("a", "d_str", "c")
        .selectExpr("a", "CAST(d_str AS DECIMAL(10, 2)) AS d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL ORDER BY DESC") {
    // Descending order swaps the direction of `current +/- offset` in
    // calculate_index_of_row; a bug specific to DESC would only show here.
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d DESC
                       RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL(10, 0) ORDER BY (zero scale)") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 0)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL(18, 6) ORDER BY (high precision & scale)") {
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(18, 6)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with negative DECIMAL ORDER BY values") {
    // Negative current + positive offset crosses zero, which is a common
    // arithmetic edge for decimal signed comparisons.
    withTempDir { dir =>
      Seq(
        (1, "-5.50", 10),
        (1, "-3.00", 20),
        (1, "-1.25", 30),
        (1, "0.00", 40),
        (1, "1.25", 50),
        (1, "3.00", 60),
        (1, "5.50", 70))
        .toDF("a", "d_str", "c")
        .selectExpr("a", "CAST(d_str AS DECIMAL(10, 2)) AS d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with fractional offset on DECIMAL ORDER BY") {
    // Fractional offsets (0.5 PRECEDING) exercise scale-aware boundary
    // arithmetic. Spark accepts decimal literals as offsets when the ORDER BY
    // column is decimal.
    withTempDir { dir =>
      writeDecimalWindowFixture(dir, "DECIMAL(10, 2)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("window: RANGE BETWEEN with DECIMAL(38, 0) ORDER BY (max precision)") {
    // Max-precision Decimal storage. Values and offset are kept well away from
    // the Decimal128 limit, so `current +/- offset` never overflows and the
    // frame runs natively; the overflow case falls back and is covered by the
    // tests further down.
    withTempDir { dir =>
      Seq((1, "10", 10), (1, "13", 20), (1, "15", 30), (1, "18", 40), (1, "22", 50))
        .toDF("a", "d_str", "c")
        .selectExpr("a", "CAST(d_str AS DECIMAL(38, 0)) AS d", "c")
        .write
        .mode("overwrite")
        .parquet(dir.toString)

      spark.read.parquet(dir.toString).createOrReplaceTempView("window_test_dec")
      val df = sql("""
        SELECT a, d, c,
          SUM(c) OVER (PARTITION BY a ORDER BY d
                       RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING) as sum_c
        FROM window_test_dec
      """)
      checkSparkAnswerAndOperator(df)
    }
  }

  private def writeDecimalOverflowFixture(
      dir: java.io.File,
      values: Seq[String],
      decimalType: String = "DECIMAL(38, 0)"): Unit = {
    values
      .map(Option(_))
      .:+(Option.empty[String])
      .toDF("d_str")
      .selectExpr(s"CAST(d_str AS $decimalType) AS d")
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(dir.toString)
  }

  test("window: RANGE offset on DECIMAL(38, 0) ORDER BY that overflows Decimal128 falls back") {
    // Spark's DecimalAddNoOverflowCheck keeps the exact bound, so the bound 2N of the row N stays
    // below the trailing NULL. DataFusion's i128 bound overflows and becomes the partition edge,
    // which takes the NULL in, so this falls back.
    // https://github.com/apache/datafusion-comet/issues/5022
    val n = "90000000000000000000000000000000000000"
    withTempDir { dir =>
      writeDecimalOverflowFixture(dir, Seq("0", n))
      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_overflow")
      for (ansi <- Seq("true", "false")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val df = sql(s"""
            SELECT d, COUNT(*) OVER (ORDER BY d ASC NULLS LAST
                                     RANGE BETWEEN $n PRECEDING AND $n FOLLOWING) AS cnt
            FROM dec_overflow
          """)
          val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
            df,
            "RANGE frame on decimal(38,0) ORDER BY with an offset whose bound can overflow " +
              "Decimal128 is not supported")
          assert(collect(cometPlan) { case w: CometWindowExec => w }.isEmpty, cometPlan)
          checkAnswer(df, Seq(Row(BigDecimal(0), 2L), Row(BigDecimal(n), 2L), Row(null, 1L)))
        }
      }
    }
  }

  test(
    "window: largest RANGE offset on DECIMAL(38, 0) ORDER BY that cannot overflow runs natively") {
    // The limit is 2^127 - 1 minus the largest DECIMAL(38, 0), that is
    // 70141183460469231731687303715884105728, so `current +/- offset` stays in an i128 for every
    // value of the type, including the type's extremes next to NULLs at either edge. Spark negates
    // a PRECEDING offset with 34 significant digits, which would round the limit itself past it,
    // so use the largest 34-digit offset under it.
    val maxValue = "9" * 38
    val offset = "70141183460469231731687303715884100000"
    withTempDir { dir =>
      writeDecimalOverflowFixture(dir, Seq(s"-$maxValue", "0", maxValue))
      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_edge")
      for {
        ansi <- Seq("true", "false")
        order <- Seq("ASC NULLS LAST", "ASC NULLS FIRST", "DESC NULLS LAST", "DESC NULLS FIRST")
      } {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val df = sql(s"""
            SELECT d, COUNT(*) OVER (ORDER BY d $order
                                     RANGE BETWEEN $offset PRECEDING AND $offset FOLLOWING) AS cnt
            FROM dec_edge
          """)
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  // With a non-zero scale the limit is the same 2^127 - 1 minus the largest unscaled value,
  // 70141183460469231731687303715884105728 unscaled, that is
  // 7014118346046923173168730371.5884105728 at scale 10. A FOLLOWING offset is not negated, so
  // it reaches Spark's frame exactly.
  private val maxDecimal38s10 = "9" * 28 + "." + "9" * 10
  private val decimal38s10Limit = "7014118346046923173168730371.5884105728"

  test(
    "window: RANGE offset on DECIMAL(38, 10) ORDER BY just over the Decimal128 limit falls back") {
    // The bound of the largest value, one unscaled unit past 2^127 - 1, overflows natively and
    // becomes the partition edge, which takes the trailing NULL in under NULLS LAST.
    // https://github.com/apache/datafusion-comet/issues/5022
    val offset = "7014118346046923173168730371.5884105729"
    withTempDir { dir =>
      writeDecimalOverflowFixture(
        dir,
        Seq(s"-$maxDecimal38s10", "0", maxDecimal38s10),
        "DECIMAL(38, 10)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_s10_over")
      for {
        ansi <- Seq("true", "false")
        order <- Seq("ASC NULLS LAST", "DESC NULLS FIRST")
      } {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val df = sql(s"""
            SELECT d, COUNT(*) OVER (ORDER BY d $order
                                     RANGE BETWEEN CURRENT ROW AND $offset FOLLOWING) AS cnt
            FROM dec_s10_over
          """)
          val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
            df,
            "RANGE frame on decimal(38,10) ORDER BY with an offset whose bound can overflow " +
              "Decimal128 is not supported")
          assert(collect(cometPlan) { case w: CometWindowExec => w }.isEmpty, cometPlan)
        }
      }
    }
  }

  test("window: RANGE offset on DECIMAL(38, 10) ORDER BY at the Decimal128 limit runs natively") {
    // At the limit itself the bound of the largest value is exactly 2^127 - 1. A PRECEDING
    // offset is negated with 34 significant digits, so the two-sided frame uses the largest
    // 34-digit offset under the limit.
    withTempDir { dir =>
      writeDecimalOverflowFixture(
        dir,
        Seq(s"-$maxDecimal38s10", "0", maxDecimal38s10),
        "DECIMAL(38, 10)")
      spark.read.parquet(dir.toString).createOrReplaceTempView("dec_s10_edge")
      val under = "7014118346046923173168730371.5884100000"
      for {
        ansi <- Seq("true", "false")
        order <- Seq("ASC NULLS LAST", "ASC NULLS FIRST", "DESC NULLS LAST", "DESC NULLS FIRST")
        frame <- Seq(
          s"RANGE BETWEEN CURRENT ROW AND $decimal38s10Limit FOLLOWING",
          s"RANGE BETWEEN $under PRECEDING AND $under FOLLOWING")
      } {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val df = sql(s"""
            SELECT d, COUNT(*) OVER (ORDER BY d $order $frame) AS cnt
            FROM dec_s10_edge
          """)
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }
}

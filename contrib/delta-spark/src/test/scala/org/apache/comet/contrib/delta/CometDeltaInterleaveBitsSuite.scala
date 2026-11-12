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

package org.apache.comet.contrib.delta

import scala.util.Random

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.delta.expressions.InterleaveBits
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.serde.Unsupported

/**
 * Delta's `InterleaveBits` through the contrib serde and the native kernel. Delta picks a fast or
 * a default implementation when the expression is built, so every query is built and checked
 * under both settings.
 */
class CometDeltaInterleaveBitsSuite extends CometDeltaTestBase {

  private type Pair = (Integer, Integer)

  private val x = UnresolvedAttribute("x")
  private val y = UnresolvedAttribute("y")

  private def interleave(children: Expression*): Column =
    getColumnFromExpression(InterleaveBits(children))

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02X").mkString

  /** Runs `f` with Delta's fast interleave path on, then off. */
  private def withBothDeltaPaths(f: Boolean => Unit): Unit =
    Seq(true, false).foreach { fast =>
      withSQLConf(DeltaSQLConf.FAST_INTERLEAVE_BITS_ENABLED.key -> fast.toString)(f(fast))
    }

  /** Asserts every `InterleaveBits` in `df` was built with Delta's fast path set to `fast`. */
  private def assertDeltaPath(df: DataFrame, fast: Boolean): Unit = {
    // Delta reads the flag when the expression is built, so the field shows which path ran.
    val field = classOf[InterleaveBits].getDeclaredField("fastInterleaveBitsEnabled")
    field.setAccessible(true)
    val nodes = df.queryExecution.optimizedPlan.flatMap(_.expressions.flatMap(_.collect {
      case e: InterleaveBits => e
    }))
    assert(nodes.nonEmpty, s"no InterleaveBits in\n${df.queryExecution.optimizedPlan}")
    nodes.foreach(e => assert(field.getBoolean(e) == fast))
  }

  /** A Parquet table with int columns `x` and `y` holding `pairs`. */
  private def withPairs(pairs: Seq[Pair])(f: DataFrame => Unit): Unit =
    withTempPath { dir =>
      val schema =
        StructType(Seq(StructField("x", IntegerType), StructField("y", IntegerType)))
      val rows = pairs.map { case (a, b) => Row(a, b) }
      spark
        .createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
        .write
        .parquet(dir.getCanonicalPath)
      f(spark.read.parquet(dir.getCanonicalPath))
    }

  private val extrema: Seq[Integer] = Seq(Int.MinValue, Int.MaxValue, 0, -1, null)

  /** Every pair of extrema and nulls, plus seeded random pairs with some nulls. */
  private val generatedPairs: Seq[Pair] = {
    val random = new Random(7573L)
    def value(i: Int, nullEvery: Int): Integer =
      if (i % nullEvery == 0) null else Integer.valueOf(random.nextInt())
    val corners = for (a <- extrema; b <- extrema) yield (a, b)
    corners ++ (0 until 1000).map(i => (value(i, 17), value(i, 19)))
  }

  test("oracle bytes") {
    val pairs: Seq[Pair] = Seq((0x000ff0ff, 0xfff00f00), (-1, null), (null, -1))
    val expected = Set(
      ("555555AAAA55AAAA", "000FF0FF", "FFF00F00"),
      ("AAAAAAAAAAAAAAAA", "FFFFFFFF", "00000000"),
      ("5555555555555555", "00000000", "FFFFFFFF"))
    withPairs(pairs) { df =>
      withBothDeltaPaths { fast =>
        val query =
          df.select(interleave(x, y).as("xy"), interleave(x).as("x1"), interleave(y).as("y1"))
        assertDeltaPath(query, fast)
        assert(query.schema("xy").dataType == BinaryType && !query.schema("xy").nullable)
        checkSparkAnswerAndImpl(query, native = Seq("interleavebits"))
        val actual = query.collect().map { r =>
          (
            hex(r.getAs[Array[Byte]](0)),
            hex(r.getAs[Array[Byte]](1)),
            hex(r.getAs[Array[Byte]](2)))
        }
        assert(actual.length == pairs.length)
        assert(actual.toSet == expected)
      }
    }
  }

  /** A small batch size, so each task's rows span several Arrow batches. */
  private val smallBatches = CometConf.COMET_BATCH_SIZE.key -> "100"

  test("nulls, extrema, generated pairs") {
    withPairs(generatedPairs) { df =>
      withBothDeltaPaths { fast =>
        withSQLConf(smallBatches) {
          val query = df.select(
            col("x"),
            col("y"),
            interleave(x, y).as("xy"),
            interleave(y, x).as("yx"),
            interleave(x).as("x1"))
          assertDeltaPath(query, fast)
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("column and literal mix") {
    withPairs(generatedPairs) { df =>
      withBothDeltaPaths { fast =>
        withSQLConf(smallBatches) {
          val query = df.select(
            col("x"),
            col("y"),
            interleave(x, Literal(-1)).as("x_lit"),
            interleave(Literal(-1), y).as("lit_y"))
          assertDeltaPath(query, fast)
          assert(!query.schema("x_lit").nullable && !query.schema("lit_y").nullable)
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("empty table") {
    withPairs(Seq.empty) { df =>
      withBothDeltaPaths { fast =>
        val query = df.select(interleave(x, y).as("xy"), interleave(x).as("x1"))
        assertDeltaPath(query, fast)
        checkSparkAnswerAndImpl(query, native = Seq("interleavebits"))
        assert(query.collect().isEmpty)
      }
    }
  }

  test("all literal inputs") {
    val excludeConstantFolding =
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    // Not foldable, so excluding ConstantFolding must not change the plan or the answer.
    Seq(Seq.empty, Seq(excludeConstantFolding)).foreach { extraConf =>
      withPairs(generatedPairs.take(10)) { df =>
        withBothDeltaPaths { fast =>
          withSQLConf(extraConf: _*) {
            val query = df.select(
              col("x"),
              interleave(Literal(1), Literal(2)).as("one_two"),
              interleave(Literal(null, IntegerType), Literal(-1)).as("null_minus_one"),
              interleave(Literal(0x000ff0ff)).as("one_input"))
            assertDeltaPath(query, fast)
            assert(!query.schema("null_minus_one").nullable)
            checkSparkAnswerAndImpl(query, native = Seq("interleavebits"))
            val actual = query.collect()
            assert(actual.length == 10)
            actual.foreach { r =>
              assert(hex(r.getAs[Array[Byte]](1)) == "0000000000000006")
              assert(hex(r.getAs[Array[Byte]](2)) == "5555555555555555")
              assert(hex(r.getAs[Array[Byte]](3)) == "000FF0FF")
            }
          }
        }
      }
    }
  }

  test("falls back for 3 inputs") {
    withPairs(generatedPairs.take(50)) { df =>
      withBothDeltaPaths { _ =>
        checkSparkAnswerAndFallbackReason(
          df.select(interleave(x, y, x).as("xyx")),
          "InterleaveBits supports 1 or 2 inputs, got 3")
      }
    }
  }

  test("support level rejects a non-int child") {
    assert(
      CometDeltaInterleaveBits
        .getSupportLevel(InterleaveBits(Seq(Literal(1L))))
        .isInstanceOf[Unsupported])
    assert(
      CometDeltaInterleaveBits
        .getSupportLevel(InterleaveBits(Seq.empty))
        .isInstanceOf[Unsupported])
  }

  test("disabled by config") {
    val key = CometConf.getExprEnabledConfigKey("InterleaveBits")
    withPairs(generatedPairs.take(50)) { df =>
      withSQLConf(key -> "false") {
        checkSparkAnswerAndFallbackReason(
          df.select(interleave(x, y).as("xy")),
          s"Expression support is disabled. Set $key=true")
      }
    }
  }

  test("OPTIMIZE ZORDER BY still matches Spark (Spark JVM path)") {
    // The Z-order inputs here are Delta's range-partition expressions, which Comet does not
    // convert, so this projection falls back to Spark. It checks that OPTIMIZE still works.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark
        .range(0, 200, 1, 4)
        .selectExpr("cast(id % 13 as int) as a", "cast(id % 7 as int) as b", "id")
        .write
        .format("delta")
        .save(path)
      val before = spark.read.format("delta").load(path).collect().toSeq
      sql(s"OPTIMIZE delta.`$path` ZORDER BY (a, b)")
      val lastOperation =
        sql(s"DESCRIBE HISTORY delta.`$path`").orderBy(col("version").desc).head()
      assert(lastOperation.getAs[String]("operation") == "OPTIMIZE")
      val zOrderBy = lastOperation
        .getAs[scala.collection.Map[String, String]]("operationParameters")
        .get("zOrderBy")
      assert(zOrderBy.contains("""["a","b"]"""), s"zOrderBy: $zOrderBy")
      checkAnswer(spark.read.format("delta").load(path), before)
    }
  }
}

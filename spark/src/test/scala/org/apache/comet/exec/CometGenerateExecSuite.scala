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

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.CometExplodeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometGenerateExecSuite extends CometTestBase {

  import testImplicits._

  for (generator <- Seq("explode", "posexplode");
    input <- Seq("s.arr", "slice(s.arr, 1, 10)");
    adaptive <- Seq(false, true)) {
    test(s"generator identity preserves exchange reuse: $generator($input), AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
        val rows = Seq(
          (1, Array[Integer](10, 20)),
          (2, Array.empty[Integer]),
          (3, null.asInstanceOf[Array[Integer]]),
          (4, Array[Integer](null)))
        val data = rows.toDF("k", "arr").selectExpr("k", "named_struct('arr', arr) AS s")
        withTempPath { path =>
          data.write.parquet(path.getAbsolutePath)
          withParquetTable(path.getAbsolutePath, "t") {
            val outputs = if (generator == "posexplode") "(pos, v)" else "v"
            def branch(outer: Boolean): DataFrame = {
              val function = if (outer) s"${generator}_outer" else generator
              sql(s"SELECT k, $function($input) AS $outputs FROM t").repartition(2, col("k"))
            }
            def nativeGenerator(df: DataFrame): CometExplodeExec =
              collectFirst(df.queryExecution.executedPlan) { case generate: CometExplodeExec =>
                generate
              }.getOrElse(fail("Expected native generator"))

            val ordinary = branch(false)
            val outer = branch(true)
            // Computed inputs avoid InferFiltersFromGenerate masking unequal semantics.
            checkSparkAnswerAndOperator(ordinary.unionAll(outer), classOf[ReusedExchangeExec])
            val populated = if (generator == "posexplode") {
              Seq(Row(1, 0, 10), Row(1, 1, 20), Row(4, 0, null))
            } else {
              Seq(Row(1, 10), Row(1, 20), Row(4, null))
            }
            val padded = if (generator == "posexplode") {
              Seq(Row(2, null, null), Row(3, null, null))
            } else {
              Seq(Row(2, null), Row(3, null))
            }
            checkAnswer(ordinary.unionAll(outer), populated ++ populated ++ padded)
            assert(!nativeGenerator(ordinary).sameResult(nativeGenerator(outer)))

            val same = branch(false)
            assert(nativeGenerator(ordinary).sameResult(nativeGenerator(same)))
            assert(
              nativeGenerator(ordinary).semanticHash() == nativeGenerator(same).semanticHash())
            val (_, reusedPlan) =
              checkSparkAnswerAndOperator(ordinary.unionAll(same), classOf[ReusedExchangeExec])
            if (adaptive) {
              assert(reusedPlan.isInstanceOf[AdaptiveSparkPlanExec])
            }
            assertExchangeReuseOver(reusedPlan, "Expected equivalent post-generator reuse") {
              case generate: CometExplodeExec => generate
            }
          }
        }
      }
    }
  }

  test("posexplode with a computed array from Parquet") {
    withSQLConf(CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val input = Seq((1, "axb"), (2, ""), (3, null), (4, "xxc"))
      withParquetDataFrame(input) { parquet =>
        withParquetTable(parquet.toDF("id", "s"), "t") {
          for (generator <- Seq("posexplode", "posexplode_outer")) {
            val df = sql(s"SELECT id, $generator(split(s, 'x')) AS (pos, value) FROM t")
            checkSparkAnswerAndOperator(df)
          }
        }
      }
    }
  }

  test("explode with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with empty array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2)), (2, Array.empty[Int]), (3, Array(3)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with null array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some(Array(1, 2))), (2, None), (3, Some(Array(3))))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with empty array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2)), (2, Array.empty[Int]), (3, Array(3)))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with null array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some(Array(1, 2))), (2, None), (3, Some(Array(3))))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with multiple columns") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, "A", Array(1, 2, 3)), (2, "B", Array(4, 5)), (3, "C", Array(6)))
        .toDF("id", "name", "arr")
        .selectExpr("id", "name", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with array of strings") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array("a", "b", "c")), (2, Array("d", "e")), (3, Array("f")))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with filter") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5, 6)), (3, Array(7, 8, 9)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
        .filter(col("value") > 5)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode fallback when disabled") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "false") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndFallbackReason(
        df,
        "Native support for operator GenerateExec is disabled")
    }
  }

  for (generator <- Seq("explode", "explode_outer", "posexplode", "posexplode_outer")) {
    test(s"$generator with JVM-fed map input") {
      withSQLConf(
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
        val input = Seq(
          (1, Map("b" -> Integer.valueOf(20), "a" -> Integer.valueOf(10))),
          (2, Map.empty[String, Integer]),
          (3, null.asInstanceOf[Map[String, Integer]]),
          (4, Map("null" -> null.asInstanceOf[Integer])))
          .toDF("id", "m")
        checkMapGenerator(input, "m", generator)
      }
    }

    test(s"$generator with map input across batch boundaries") {
      withSQLConf(
        CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
        CometConf.COMET_BATCH_SIZE.key -> "4") {
        val rows = (0 until 16).map { i =>
          val m = i % 4 match {
            case 0 => null.asInstanceOf[Map[Int, java.lang.Integer]]
            case 1 => Map.empty[Int, java.lang.Integer]
            case _ =>
              (0 until 13).map { j =>
                j -> (if (j % 3 == 0) null else java.lang.Integer.valueOf(i * 100 + j))
              }.toMap
          }
          val booleans = Option(m)
            .map(_.map { case (key, value) =>
              // A period of five cannot hide a wrong bitmap offset at four-row batch boundaries.
              key -> (if (value == null) null else java.lang.Boolean.valueOf((i + key) % 5 < 2))
            })
            .orNull
          (i, m, booleans)
        }
        withParquetDataFrame(rows) { input =>
          // One map exceeds the output batch size. Carry the map through too, so
          // outer padding cannot accidentally replace the original empty map.
          val maps = input.toDF("id", "ints", "booleans")
          Seq("ints", "booleans").foreach { name =>
            withClue(s"$name: ") { checkMapGenerator(maps, name, generator) }
          }
        }
      }
    }
  }

  for (generator <- Seq("explode_outer", "posexplode_outer")) {
    test(s"$generator with batches containing only null and empty maps") {
      withSQLConf(
        CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
        CometConf.COMET_BATCH_SIZE.key -> "4") {
        val rows = (0 until 16).map { i =>
          val m = if (i % 2 == 0) null else Map.empty[Int, java.lang.Boolean]
          (i, m)
        }
        withParquetDataFrame(rows) { input =>
          checkMapGenerator(input.toDF("id", "m"), "m", generator)
        }
      }
    }
  }

  private def checkMapGenerator(input: DataFrame, name: String, generator: String): Unit = {
    val query = input.selectExpr("id", name, s"$generator($name)")
    checkSparkAnswerAndOperator(query, Seq(classOf[CometExplodeExec]))
    checkSparkSchema(query)
  }

  test("explode with nullable projected column") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some("A"), Array(1, 2)), (2, None, Array(3, 4)), (3, Some("C"), Array(5)))
        .toDF("id", "name", "arr")
        .selectExpr("id", "name", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with nullable projected column") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df =
        Seq((1, Some("A"), Array(1, 2)), (2, None, Array.empty[Int]), (3, Some("C"), Array(5)))
          .toDF("id", "name", "arr")
          .selectExpr("id", "name", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with mixed null, empty, and non-empty arrays") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(
        (1, Some(Array(1, 2))),
        (2, None),
        (3, Some(Array.empty[Int])),
        (4, Some(Array(3))),
        (5, None),
        (6, Some(Array(4, 5, 6))))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with mixed null, empty, and non-empty arrays") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(
        (1, Some(Array(1, 2))),
        (2, None),
        (3, Some(Array.empty[Int])),
        (4, Some(Array(3))),
        (5, None),
        (6, Some(Array(4, 5, 6))))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with multiple nullable columns") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(
        (Some(1), Some("A"), Some(100), Array(1, 2)),
        (None, Some("B"), None, Array(3)),
        (Some(3), None, Some(300), Array(4, 5)),
        (None, None, None, Array(6)))
        .toDF("id", "name", "value", "arr")
        .selectExpr("id", "name", "value", "explode(arr) as element")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(10, 20, 30)), (2, Array(40, 50)), (3, Array(60)))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with empty array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2)), (2, Array.empty[Int]), (3, Array(3)))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with null array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some(Array(1, 2))), (2, None), (3, Some(Array(3))))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode_outer with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(10, 20, 30)), (2, Array(40, 50)), (3, Array(60)))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode_outer(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with array of strings") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array("a", "b", "c")), (2, Array("d", "e")), (3, Array("f")))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with nullable elements") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(
        (1, Array[Option[Int]](Some(1), None, Some(3))),
        (2, Array[Option[Int]](None, Some(5))),
        (3, Array[Option[Int]](Some(6))))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with multiple projected columns") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df =
        Seq((1, "A", Array(10, 20, 30)), (2, "B", Array(40, 50)), (3, "C", Array(60)))
          .toDF("id", "name", "arr")
          .selectExpr("id", "name", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode with map input falls back when disabled") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "false") {
      val df = Seq((1, Map("a" -> 1, "b" -> 2)), (2, Map("c" -> 3)))
        .toDF("id", "map")
        .selectExpr("id", "posexplode(map) as (pos, key, value)")
      checkSparkAnswerAndFallbackReason(
        df,
        "Native support for operator GenerateExec is disabled")
    }
  }

  test("posexplode with array of structs") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(
        (1, Array((10, "a"), (20, "b"))),
        (2, Array((30, "c"))),
        (3, Array.empty[(Int, String)]))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
        .selectExpr("id", "pos", "value._1 as v1", "value._2 as v2")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode in lateral view") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      withTempView("t") {
        Seq((1, Array(10, 20, 30)), (2, Array(40, 50)), (3, Array(60)))
          .toDF("id", "arr")
          .createOrReplaceTempView("t")
        val df =
          sql("SELECT t.id, p.pos, p.col FROM t LATERAL VIEW posexplode(t.arr) p AS pos, col")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("posexplode of literal array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq(1, 2, 3)
        .toDF("id")
        .selectExpr("id", "posexplode(array(100, 200, 300)) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode across batch boundary with small batch size") {
    // Force ScanExec to emit multiple small batches so that UnnestExec sees the parallel
    // positions/values lists across batch boundaries. Element values are non-trivial so wrong
    // alignment between pos and value would be visible in the answer.
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "4") {
      val rows = (1 to 12).map { i =>
        (i, (0 until (i % 5 + 1)).map(j => i * 100 + j).toArray)
      }
      val df = rows
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer across batch boundary with mixed empty/null rows") {
    // Mix null, empty, and non-empty rows and force multiple small batches so that the
    // per-row output lengths are recomputed on each batch with a different offset pattern,
    // and so that some chunk boundaries fall on a substituted row.
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "4") {
      val rows: Seq[(Int, Option[Array[Int]])] = (1 to 40).map { i =>
        val arr = i % 5 match {
          case 0 => None
          case 1 => Some(Array.empty[Int])
          case _ => Some((0 until (i % 5)).map(j => i * 100 + j).toArray)
        }
        (i, arr)
      }
      val df = rows
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode_outer across batch boundary with mixed empty/null rows") {
    // Same shape as the explode_outer counterpart but exercises the parallel positions
    // branch, where the `pos` and `value` arrays are unnested together and must be padded
    // to the same per-row length.
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "4") {
      val rows: Seq[(Int, Option[Array[Int]])] = (1 to 40).map { i =>
        val arr = i % 5 match {
          case 0 => None
          case 1 => Some(Array.empty[Int])
          case _ => Some((0 until (i % 5)).map(j => i * 100 + j).toArray)
        }
        (i, arr)
      }
      val df = rows
        .toDF("id", "arr")
        .selectExpr("id", "posexplode_outer(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  // Regression tests for https://github.com/apache/datafusion-comet/issues/5224.
  //
  // A native limit with a non-zero offset produces a batch whose `ListArray` has a non-zero
  // offset base (`LimitStream::poll_and_skip` does `batch.slice(self.skip, ...)`). Before the
  // fix, `ListPositionsExpr` rebuilt a fresh values array numbered from zero but reused the
  // input's original offset buffer, so `ListArray::new` panicked with
  // "Max offset of N exceeds length of values M".
  //
  // `spark.sql.leafNodeDefaultParallelism = 1` is required rather than cosmetic: with the
  // default parallelism each partition produces a one-row batch, `LimitStream` discards whole
  // batches instead of slicing, and the bug is masked. AQE off just keeps the plan readable.

  test("posexplode over limit with offset") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.leafNodeDefaultParallelism" -> "1",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      withTempView("t") {
        Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)), (4, Array(7, 8)), (5, Array(9)))
          .toDF("id", "arr")
          .createOrReplaceTempView("t")
        val df = sql("SELECT id, posexplode(arr) FROM (SELECT id, arr FROM t LIMIT 4 OFFSET 1)")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("posexplode_outer over limit with offset") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.leafNodeDefaultParallelism" -> "1",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      withTempView("t") {
        Seq(
          (1, Some(Array(1, 2, 3))),
          (2, None),
          (3, Some(Array.empty[Int])),
          (4, Some(Array(7, 8))),
          (5, None))
          .toDF("id", "arr")
          .createOrReplaceTempView("t")
        val df =
          sql("SELECT id, posexplode_outer(arr) FROM (SELECT id, arr FROM t LIMIT 4 OFFSET 1)")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("explode over limit with offset") {
    // Plain `explode` does not build `ListPositionsExpr`, so it did not trigger #5224.
    // Guarded here so that a future regression in the values path is caught alongside the
    // posexplode fix.
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.leafNodeDefaultParallelism" -> "1",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      withTempView("t") {
        Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)), (4, Array(7, 8)), (5, Array(9)))
          .toDF("id", "arr")
          .createOrReplaceTempView("t")
        val df = sql("SELECT id, explode(arr) FROM (SELECT id, arr FROM t LIMIT 4 OFFSET 1)")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("explode_outer over limit with offset") {
    // Exercises the outer path on a sliced input with a non-zero offset base, which is what
    // the fix in `ListPositionsExpr` keeps the parallel `pos` branch safe against. This test
    // covers the `explode_outer` shape without the `pos` branch.
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.leafNodeDefaultParallelism" -> "1",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      withTempView("t") {
        Seq(
          (1, Some(Array(1, 2, 3))),
          (2, None),
          (3, Some(Array.empty[Int])),
          (4, Some(Array(7, 8))),
          (5, None))
          .toDF("id", "arr")
          .createOrReplaceTempView("t")
        val df =
          sql("SELECT id, explode_outer(arr) FROM (SELECT id, arr FROM t LIMIT 4 OFFSET 1)")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  // A single input row whose flattened output exceeds COMET_BATCH_SIZE forces
  // UnnestExec to emit multiple output batches from one input row. This is a
  // distinct axis from cross-batch input slicing (covered above): pos values
  // must remain contiguous across the split, and `explode` / `explode_outer`
  // must preserve element order. Existing batch-boundary tests use arrays of
  // at most 5 elements against COMET_BATCH_SIZE=4, so no test forced this
  // one-input-many-output-batches shape until now.

  test("posexplode single row exceeds batch size") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "8") {
      val df = Seq(
        (1, (0 until 30).toArray),
        (2, Array(100, 101)),
        (3, Array.empty[Int]),
        (4, (0 until 20).map(i => 200 + i).toArray))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("posexplode_outer single row exceeds batch size") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "8") {
      val df = Seq(
        (1, Some((0 until 30).toArray)),
        (2, Some(Array(100, 101))),
        (3, None),
        (4, Some(Array.empty[Int])),
        (5, Some((0 until 20).map(i => 200 + i).toArray)))
        .toDF("id", "arr")
        .selectExpr("id", "posexplode_outer(arr) as (pos, value)")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode single row exceeds batch size") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "8") {
      val df = Seq(
        (1, (0 until 30).toArray),
        (2, Array(100, 101)),
        (3, Array.empty[Int]),
        (4, (0 until 20).map(i => 200 + i).toArray))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  // The native explode slices each output batch out of the exploded child instead of gathering
  // it, so an exploded boolean, or a boolean field of an exploded struct, leaves native at a
  // non-zero bit offset. Arrow Java ignores that offset on import, so native has to zero it at
  // every level before export, including in a struct built over the booleans and in the input to
  // a Scala UDF, or they come back wrong after the first output batch.
  // https://github.com/apache/datafusion-comet/issues/6464
  private def withBooleanArrays(numRows: Long, arrayLength: Long)(f: => Unit): Unit = {
    withTempPath { dir =>
      // One file, so a single input batch explodes into several output batches.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(0L, numRows, 1L, 1)
          .selectExpr(
            "id",
            s"transform(sequence(1, $arrayLength), i -> named_struct(" +
              "'b', hash(id, i) % 2 = 0, " +
              "'bn', IF(hash(id, i, 7) % 5 = 0, NULL, hash(id, i, 3) % 2 = 0), " +
              "'n', id * 1000 + i)) AS structs",
            // No NULLs, so the null check Spark wraps around a primitive UDF argument selects
            // every row, and the UDF reads the exploded batch rather than a filtered copy.
            s"transform(sequence(1, $arrayLength), i -> hash(id, i, 11) % 2 = 0) AS bools")
          .write
          .parquet(dir.getCanonicalPath)
      }
      withParquetTable(dir.getCanonicalPath, "t")(f)
    }
  }

  for (generator <- Seq("explode", "explode_outer", "posexplode", "posexplode_outer")) {
    test(s"$generator of structs keeps boolean fields past the first output batch") {
      // With the default batch size, 13 elements a row give output batches of 8190 rows, so the
      // later batches start both on and off a byte boundary.
      withBooleanArrays(numRows = 3000, arrayLength = 13) {
        checkSparkAnswerAndOperator(sql(s"SELECT id, $generator(structs) FROM t"))
      }
    }
  }

  test("explode of structs keeps boolean fields when one row exceeds the batch size") {
    // A row longer than the batch size is unnested in one build, which is then sliced into
    // batch-size pieces on the way out, here at elements 100 and 200.
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "100") {
      withBooleanArrays(numRows = 1, arrayLength = 250) {
        checkSparkAnswerAndOperator(sql("SELECT id, explode(structs) FROM t"))
      }
    }
  }

  test("named_struct over an exploded boolean keeps its values") {
    withBooleanArrays(numRows = 3000, arrayLength = 13) {
      checkSparkAnswerAndOperator(
        sql("SELECT id, named_struct('v', v) FROM (SELECT id, explode(bools) AS v FROM t)"))
    }
  }

  test("boolean ScalaUDF over an exploded boolean keeps its values") {
    spark.udf.register("flip", (x: Boolean) => !x)
    withBooleanArrays(numRows = 3000, arrayLength = 13) {
      checkSparkAnswerAndOperator(
        sql("SELECT id, flip(v) FROM (SELECT id, explode(bools) AS v FROM t)"))
    }
  }

}

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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometExplodeExec
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf

class CometGenerateExecSuite extends CometTestBase {

  import testImplicits._

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

  test("explode with map input falls back") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Map("a" -> 1, "b" -> 2)), (2, Map("c" -> 3)))
        .toDF("id", "map")
        .selectExpr("id", "explode(map) as (key, value)")
      checkSparkAnswerAndFallbackReason(
        df,
        "Comet only supports explode/explode_outer for arrays, not maps")
    }
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

  test("posexplode with map input falls back") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Map("a" -> 1, "b" -> 2)), (2, Map("c" -> 3)))
        .toDF("id", "map")
        .selectExpr("id", "posexplode(map) as (pos, key, value)")
      checkSparkAnswerAndFallbackReason(
        df,
        "Comet only supports explode/explode_outer for arrays, not maps")
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
    // Mix null, empty, and non-empty rows and force multiple small batches so that
    // `ListEmptyToNullExpr` runs on each batch and its fast/slow path split is exercised
    // more than once with different offset patterns.
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
    // branch. With the pre-projection introduced for outer, `ListEmptyToNullExpr` runs once
    // per batch and both branches share the same materialized array.
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
    // Exercises `ListEmptyToNullExpr` on a sliced input with a non-zero offset base. The
    // helper preserves the base offset and passes it through unchanged, so the fix in
    // `ListPositionsExpr` is what actually keeps the parallel `pos` branch safe. This test
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

  // N rows each expanding by 2 can exceed spark.comet.batchSize even when no single
  // array is larger than the batch. Distinct from "explode single row exceeds batch
  // size" above. leafNodeDefaultParallelism=1 keeps the input in one partition so
  // UnnestExec can emit one oversized batch for BatchSplitExec to slice.
  test("native explode splits oversized output batches") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "8",
      "spark.sql.leafNodeDefaultParallelism" -> "1") {
      val df = (0 until 16)
        .map(i => if (i % 2 == 0) "a" else "b")
        .toDF("s")
        .selectExpr("explode(array(s, s)) as e")
      val (_, cometPlan) =
        checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometExplodeExec]))

      val explode = find(cometPlan) {
        case _: CometExplodeExec => true
        case _ => false
      }.get

      val metrics = explode.metrics
      assert(metrics.contains("input_rows"))
      assert(metrics.contains("output_rows"))
      assert(metrics.contains("batches_split"))

      assert(metrics("input_rows").value == 16L)
      assert(metrics("output_rows").value == 32L)
      assert(metrics("batches_split").value > 0L)
    }
  }

}

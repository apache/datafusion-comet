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

package org.apache.comet

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Higher-order function regression coverage for the codegen dispatcher.
 *
 * Spark's HOFs (`ArrayTransform`, `ArrayFilter`, `ArrayAggregate`, `ArrayExists`, `ZipWith`,
 * `MapFilter`, etc.) all extend `CodegenFallback`. The dispatcher's `canHandle` admits them.
 * `CodegenFallback.doGenCode` emits a single `((Expression) references[N]).eval(row)` call site
 * per HOF. The kernel dispatches to `Expression.eval(InternalRow)`, which iterates the array,
 * mutates `NamedLambdaVariable.value`'s `AtomicReference` per element, and recursively evaluates
 * the lambda body. Lambda-body leaf reads resolve through the kernel's typed Arrow getters since
 * the kernel is an `InternalRow`.
 *
 * Cost model: per-row interpreted-eval inside the HOF subtree. Surrounding native operators stay
 * native. Surrounding non-HOF expressions stay codegen.
 *
 * Each Spark task gets its own `boundExpr` Java object. The dispatcher's compile cache lives on
 * the per-task instance, not the companion, so concurrent partitions cannot race on a shared
 * `NamedLambdaVariable.value`. The two-collects test below regresses this.
 */
class CometCodegenHOFSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private def withArrayIntTable(rows: String)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (a ARRAY<INT>) USING parquet")
      sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("ArrayTransform inside identity ScalaUDF over Array<Int>") {
    // Regresses the simplest HOF shape: `idArr(transform(a, x -> x + 1))`. Tree contains one
    // CodegenFallback HOF. The kernel splices its interpreted-eval call site into the per-row
    // body and the result ArrayData feeds the ListVector output writer. Null and empty rows
    // exercise the HOF's null-on-null-arg path and the empty-iteration path.
    spark.udf.register("idArr", (arr: Seq[Int]) => arr)
    withArrayIntTable("(array(1, 2, 3)), (array(-5, 5)), (array()), (null)") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idArr(transform(a, x -> x + 1)) FROM t"))
      }
    }
  }

  test("array_max over ArrayTransform inside identity ScalaUDF") {
    // Regresses composed CodegenFallback subtrees: array_max consumes the ArrayData transform
    // produces. Both run interpreted. The kernel splices both eval call sites into the same
    // per-row body. Empty/null rows exercise array_max's null-on-empty path.
    spark.udf.register("idIntBoxed", (i: java.lang.Integer) => i)
    withArrayIntTable("(array(1, 2, 3)), (array(-5, 5)), (null), (array(0))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT idIntBoxed(array_max(transform(a, x -> x * 2))) FROM t"))
      }
    }
  }

  test("array_max over ArrayFilter inside identity ScalaUDF") {
    // Regresses ArrayFilter (distinct HOF class from ArrayTransform). Filter producing an
    // empty array from non-empty input exercises array_max(emptyArray) downstream.
    spark.udf.register("idIntBoxed", (i: java.lang.Integer) => i)
    withArrayIntTable("(array(1, -1, 2)), (array(-5, -2)), (array()), (null)") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT idIntBoxed(array_max(filter(a, x -> x > 0))) FROM t"))
      }
    }
  }

  test("ArrayExists respects three-valued logic through codegen dispatcher") {
    withArrayIntTable("(array(1, null, 3)), (array(1, 2, 3)), (array()), (null)") {
      Seq(false, true).foreach { followThreeValuedLogic =>
        withSQLConf(
          "spark.sql.legacy.followThreeValuedLogicInArrayExists" ->
            followThreeValuedLogic.toString) {
          assertCodegenRan {
            checkSparkAnswerAndOperator(sql("SELECT exists(a, x -> x > 10) FROM t"))
          }
        }
      }
    }
  }

  test("HOF query produces correct results across two collects (per-task isolation regression)") {
    // Regresses the per-task `boundExpr` isolation. When the dispatcher's compile cache lived on
    // the companion object, multiple tasks shared one `boundExpr` and concurrent partitions
    // raced on `NamedLambdaVariable.value`'s `AtomicReference`, producing off-by-one element
    // values. The fix moved the cache to the per-task instance so each task deserializes its own
    // boundExpr. Two collects of the same query must each match Spark's interpreter.
    spark.udf.register("idArr", (arr: Seq[Int]) => arr)
    withArrayIntTable("(array(1, 2)), (array(3, 4)), (array(5))") {
      val q = "SELECT idArr(transform(a, x -> x + 1)) FROM t"
      checkSparkAnswerAndOperator(sql(q))
      checkSparkAnswerAndOperator(sql(q))
    }
  }

  private def withStringArrayTable(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (a ARRAY<STRING>, c INT) USING parquet")
      sql("INSERT INTO t VALUES (array('a', NULL, '', 'b'), 1), (array(), 2), (NULL, 3)")
      f
    }
  }

  private def dispatcherActivity(f: => Unit): Long = {
    CometScalaUDFCodegen.resetStats()
    f
    val stats = CometScalaUDFCodegen.stats()
    stats.compileCount + stats.cacheHitCount
  }

  test("ArrayFilter with a natively supported lambda body does not use the dispatcher") {
    withStringArrayTable {
      Seq(
        "SELECT filter(a, x -> nvl(x, '') != '') FROM t",
        "SELECT filter(a, x -> x IS NOT NULL AND length(x) > c) FROM t").foreach { query =>
        val native = dispatcherActivity {
          checkSparkAnswerAndOperator(sql(query))
        }
        assert(native == 0, s"expected no dispatcher activity for $query")

        withSQLConf(CometConf.COMET_NATIVE_LAMBDA_ENABLED.key -> "false") {
          val dispatched = dispatcherActivity {
            checkSparkAnswerAndOperator(sql(query))
          }
          assert(dispatched >= 1, s"expected the dispatcher to run $query")
        }
      }
    }
  }

  test("ArrayFilter keeps a lambda the native side cannot evaluate on the dispatcher") {
    withStringArrayTable {
      Seq(
        // The body calls out to the JVM, so it cannot be split from the dispatcher.
        "SELECT filter(a, x -> x RLIKE '^a') FROM t",
        // A nested lambda.
        "SELECT filter(a, x -> exists(a, y -> y = x)) FROM t",
        // A lambda of two parameters.
        "SELECT filter(a, (x, i) -> i > 0) FROM t").foreach { query =>
        val activity = dispatcherActivity {
          checkSparkAnswerAndOperator(sql(query))
        }
        assert(activity >= 1, s"expected the dispatcher to run $query")
      }
    }
  }

  test(
    "ArrayFilter keeps a lambda that short-circuits an operation that can raise on the dispatcher") {
    withArrayIntTable("(array(0, 1, 5)), (array()), (null)") {
      val query = "SELECT filter(a, x -> x != 0 AND 100 / x > 1) FROM t"
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        // Spark skips `100 / x` for the element 0. The native side would raise on it.
        val ansi = dispatcherActivity {
          checkSparkAnswerAndOperator(sql(query))
        }
        assert(ansi >= 1, "expected the dispatcher to run the lambda under ANSI mode")
      }
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        val nonAnsi = dispatcherActivity {
          checkSparkAnswerAndOperator(sql(query))
        }
        assert(nonAnsi == 0, "expected the lambda to run natively when ANSI mode is off")
      }
    }
  }
}

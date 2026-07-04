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
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Sentences}
import org.apache.spark.sql.comet.{CometFilterExec, CometProjectExec}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{ArrayType, StringType}

import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Tests for the partial project fallback (JVM expression detour). When a projection or filter
 * contains an expression with no native translation, [[CometConf.COMET_EXEC_JVM_DETOUR_ENABLED]]
 * routes just that subtree through the Arrow-direct codegen dispatcher instead of falling the
 * whole operator back to Spark.
 *
 * Two ways of producing an "unsupported" expression are used:
 *   - `sentences(str)`, which Comet has no native serde for (so it exercises the `case _ =>`
 *     fallthrough the hook backstops), is deterministic, and is a `CodegenFallback` returning
 *     `array<array<string>>` (interpreted eval + a nested-complex result across the FFI
 *     boundary).
 *   - a registered built-in whose per-expression serde is disabled
 *     (`spark.comet.expression.<Class>.enabled=false`), which routes a genuinely-supported
 *     expression through the same fallthrough. This lets the parity tests below ship a chosen
 *     input type (decimal, timestamp, array, map, a subquery, a nondeterministic child) across
 *     the FFI boundary through the hook while staying cheap and stable as native coverage
 *     evolves.
 */
class CometPartialProjectFallbackSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private def withStringCol(values: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val rows = values
        .map(v => if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')")
        .mkString(", ")
      if (values.nonEmpty) sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  private def withDetour[T](enabled: Boolean)(f: => T): T =
    withSQLConf(CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> enabled.toString)(f)

  /** Whether the (already-executed) DataFrame's plan kept a native `CometProjectExec`. */
  private def hasCometProject(df: DataFrame): Boolean =
    collect(stripAQEPlan(df.queryExecution.executedPlan)) { case p: CometProjectExec =>
      p
    }.nonEmpty

  test("unsupported projection expression stays native with the detour enabled") {
    withStringCol("Hello World. Foo bar.", "One sentence here.", null) {
      withDetour(enabled = true) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT sentences(s) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("same expression falls the operator back to Spark with the detour disabled (default)") {
    withStringCol("Hello World. Foo bar.", null) {
      withDetour(enabled = false) {
        CometScalaUDFCodegen.resetStats()
        // Results still match Spark (the projection just runs on the JVM Spark path), but the
        // project operator is NOT accelerated and the dispatcher never fires.
        val df = sql("SELECT sentences(s) FROM t")
        checkSparkAnswer(df)
        assert(
          !hasCometProject(df),
          "expected the projection to fall back to Spark with the detour disabled")
        val after = CometScalaUDFCodegen.stats()
        assert(
          after.compileCount == 0 && after.cacheHitCount == 0,
          s"expected no dispatcher activity with the detour disabled, got $after")
      }
    }
  }

  test("detour fires at the outermost unsupported node, keeping supported ancestors native") {
    // `size(sentences(s))`: `size` has a native translation, only `sentences` does not. The hook
    // must detour `sentences` alone and let the native `size` reference its output. Proof: a
    // compiled kernel exists whose signature is `sentences`'s — one `VarCharVector` input and an
    // `array<array<string>>` output. If the detour granularity were the whole top-level
    // expression, the only kernel would have an IntegerType output (`size`'s) and this signature
    // would be absent. Presence is a robust check against the append-only JVM-wide signature set.
    withStringCol("Hello World. Foo bar.", "Solo.", null) {
      withDetour(enabled = true) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT size(sentences(s)) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
        // Structural match on the signature (ignoring `containsNull` flags, which vary by
        // context): one VarCharVector input, output `array<array<string>>` — the sentences subtree.
        val sigs = CometScalaUDFCodegen.snapshotCompiledSignatures()
        val present = sigs.exists {
          case (inputs, ArrayType(ArrayType(StringType, _), _)) =>
            inputs.map(_.getSimpleName) == IndexedSeq("VarCharVector")
          case _ => false
        }
        assert(
          present,
          "expected a kernel (VarCharVector -> array<array<string>>) for the sentences subtree " +
            s"only; cache had ${sigs.map { case (c, d) => (c.map(_.getSimpleName), d) }}")
      }
    }
  }

  test("unsupported expression in a filter predicate keeps the filter native") {
    // The detour gate is set around `CometFilterExec.convert` as well. `size(sentences(s)) > 0`
    // has an unsupported `sentences` leaf under an otherwise-native predicate.
    withStringCol("Hello World. Foo bar.", "", "Solo.", null) {
      withDetour(enabled = true) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT s FROM t WHERE size(sentences(s)) > 0"),
            includeClasses = Seq(classOf[CometFilterExec]))
        }
      }
    }
  }

  test("detour refused by canHandle still falls back cleanly") {
    // Force `CometBatchKernelCodegen.canHandle` to reject the tree at plan time via the
    // `spark.sql.codegen.maxFields` gate. `sentences(s)` has 2 nested fields (the string input +
    // the array<array<string>> output collapses to 1), so maxFields=1 refuses it. The detour hook
    // must surface that as a clean whole-operator fallback, not a mid-execution Janino failure.
    withStringCol("Hello World.", null) {
      withDetour(enabled = true) {
        withSQLConf("spark.sql.codegen.maxFields" -> "1") {
          CometScalaUDFCodegen.resetStats()
          val df = sql("SELECT sentences(s) FROM t")
          checkSparkAnswer(df)
          assert(
            !hasCometProject(df),
            "expected fallback when canHandle refuses the detoured subtree")
          val after = CometScalaUDFCodegen.stats()
          assert(
            after.compileCount == 0,
            s"expected no kernel compile when the detour is refused, got $after")
        }
      }
    }
  }

  test("the detour gate is scoped: it does not fire outside a projection or filter context") {
    // Serde-level assertion of the `DynamicVariable` scoping. `exprToProto` on an unsupported
    // expression returns None on its own, and only returns a `JvmScalarUdf` proto when the call is
    // wrapped in `withJvmDetour` (which `CometProjectExec` / `CometFilterExec` do) AND the feature
    // is enabled. This is what keeps the detour out of join keys, sort keys, and aggregate
    // expressions in v1 without threading a flag through every serde method.
    val attr = AttributeReference("s", StringType)()
    val expr = Sentences(attr)
    val inputs = Seq(attr)

    withDetour(enabled = true) {
      // Outside a detour-eligible context: no detour, even though the feature is enabled.
      assert(
        QueryPlanSerde.exprToProto(expr, inputs).isEmpty,
        "expected no detour outside withJvmDetour")

      // Inside a detour-eligible context: the hook fires and emits a JvmScalarUdf proto.
      val detoured = QueryPlanSerde.withJvmDetour(QueryPlanSerde.exprToProto(expr, inputs))
      assert(detoured.isDefined, "expected a detour proto inside withJvmDetour")
      assert(detoured.get.hasJvmScalarUdf, s"expected a JvmScalarUdf proto, got ${detoured.get}")
    }

    withDetour(enabled = false) {
      // Feature disabled: even inside a detour-eligible context, no detour.
      assert(
        QueryPlanSerde.withJvmDetour(QueryPlanSerde.exprToProto(expr, inputs)).isEmpty,
        "expected no detour when the feature flag is disabled")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Parity suites: a supported built-in routed through the hook (by disabling its serde) ships a
  // chosen input type across the FFI boundary. Results run through Spark's own `doGenCode` inside
  // the kernel, so parity with Spark is the invariant. Each asserts the operator stays native.
  // ---------------------------------------------------------------------------------------------

  /**
   * Enable the detour + dispatcher and disable the given built-in serdes so they route through
   * it.
   */
  private def withDetouredSerdes(disabled: String*)(f: => Unit): Unit = {
    val confs = Seq(
      CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") ++
      disabled.map(c => CometConf.getExprEnabledConfigKey(c) -> "false")
    withSQLConf(confs: _*)(f)
  }

  test("parity: NULL-heavy input through the detour hook") {
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      val rows = (0 until 300)
        .map(i => if (i % 3 == 0) "(NULL)" else s"(${i - 150})")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $rows")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT abs(x) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("parity: empty input keeps the detour native") {
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet") // no rows: exercises the empty-batch path
      withDetouredSerdes("Abs") {
        // No assertCodegenRan: with zero rows the kernel never compiles. We only assert the plan
        // stays native and produces the same (empty) result as Spark.
        checkSparkAnswerAndOperator(
          sql("SELECT abs(x) FROM t"),
          includeClasses = Seq(classOf[CometProjectExec]))
      }
    }
  }

  test("parity: decimals cross the FFI boundary through the detour hook") {
    withTable("t") {
      sql("CREATE TABLE t (d DECIMAL(18,6), e DECIMAL(38,10)) USING parquet")
      sql(
        "INSERT INTO t VALUES (1.5, 2.5), (-3.25, -4.75), (NULL, NULL), (0, 0), " +
          "(999999999999.999999, 9999999999999999999999999999.0)")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT abs(d), abs(e) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("parity: timestamps cross the FFI boundary through the detour hook") {
    withTable("t") {
      sql("CREATE TABLE t (ts TIMESTAMP) USING parquet")
      sql(
        "INSERT INTO t VALUES (TIMESTAMP'2024-01-01 13:45:00'), " +
          "(TIMESTAMP'1970-01-01 00:00:01'), (NULL), (TIMESTAMP'2024-06-15 23:59:59')")
      withDetouredSerdes("Hour") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT hour(ts) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("parity: nested array/map types cross the FFI boundary through the detour hook") {
    withTable("t") {
      sql("CREATE TABLE t (arr ARRAY<INT>, m MAP<STRING, INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES (array(1, 2, 3), map('a', 1)), (array(), map()), " +
          "(NULL, NULL), (array(5, 6), map('x', 9, 'y', 10))")
      withDetouredSerdes("Size") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT size(arr), size(m) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("a subquery inside the detoured subtree falls back cleanly, not to a kernel failure") {
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (10), (20)")
      withDetouredSerdes("Abs") {
        // The detoured tree is closure-serialized at plan time, before the operator's
        // `waitForSubqueries` populates the `ScalarSubquery` result, so evaluating it in the
        // kernel would throw "Subquery ... has not finished". The detour refuses a subquery-bearing
        // tree, so the projection falls back to Spark and results still match. (A subquery that is
        // a *sibling* of the detoured node is unaffected; it stays native.)
        val df = sql("SELECT abs(x - (SELECT max(v) FROM t2)) FROM t")
        checkSparkAnswer(df)
        assert(
          !hasCometProject(df),
          "expected the projection to fall back when the detoured tree contains a subquery")
      }
    }
  }

  test("a subquery that is a sibling of the detoured node stays native") {
    // With `Abs` disabled, `abs(x)` detours (no subquery inside its tree) while the sibling
    // `ScalarSubquery` under the native `+` is evaluated natively (Comet populates it via
    // `waitForSubqueries`). The whole projection therefore stays native. This pins the
    // sibling-subquery behavior the subquery guard's comment relies on.
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (10), (20)")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT abs(x) + (SELECT max(v) FROM t2) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("a mixed projection falls back whole when one sibling cannot detour") {
    // `abs(x)` detours, but `make_interval(x)` returns CalendarInterval, which `canHandle` rejects
    // (unsupported output type) and which Comet has no serde for, so it cannot be detoured. The
    // per-expression detour of `abs(x)` succeeds, yet `exprs.forall(_.isDefined)` is false, so the
    // whole projection falls back to a Spark `ProjectExec`. Results must still match Spark (the
    // detour never changes results, only whether an operator stays native).
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3)")
      withDetouredSerdes("Abs") {
        val df = sql("SELECT abs(x) AS a, make_interval(x) AS b FROM t")
        checkSparkAnswer(df)
        assert(
          !hasCometProject(df),
          "expected the whole projection to fall back when a sibling cannot detour")
      }
    }
  }

  test("parity: nondeterministic detoured tree stays native with per-partition seeding") {
    withTempPath { dir =>
      // Four partitions so per-partition `Rand` seeding (kernel `init(partitionIndex)`) matters:
      // matching Spark on a fixed seed proves the seed advances per partition, not from 0 each time.
      spark.range(0, 4096, 1, numPartitions = 4).write.parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT id, abs(rand(42)) AS r FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
      spark.catalog.dropTempView("t")
    }
  }

  test("island preservation: the projection stays native with no row transition below it") {
    // Match every columnar->row transition kind by simple name (Spark's `ColumnarToRowExec` plus
    // Comet's `CometColumnarToRowExec` / `CometNativeColumnarToRowExec`) so the check does not
    // depend on which one Comet inserts.
    def isRowTransition(p: SparkPlan): Boolean = p.getClass.getSimpleName match {
      case "ColumnarToRowExec" | "CometColumnarToRowExec" | "CometNativeColumnarToRowExec" => true
      case _ => false
    }
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3)")

      // Detour off: the projection falls back to a Spark `ProjectExec`, so a columnar->row
      // transition feeds it and the native island above the scan is broken.
      withSQLConf(
        CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "false",
        CometConf.getExprEnabledConfigKey("Abs") -> "false") {
        val offPlan = stripAQEPlan(sql("SELECT abs(x) FROM t").queryExecution.executedPlan)
        assert(
          collect(offPlan) { case p: ProjectExec => p }.nonEmpty,
          "expected a Spark ProjectExec (fallback) with the detour off")
        assert(
          collect(offPlan) { case p if isRowTransition(p) => p }.nonEmpty,
          "expected a columnar->row transition feeding the fallback project")
      }

      // Detour on: the projection stays native and no columnar->row transition appears anywhere in
      // its subtree, so the island above the scan is preserved end to end.
      withDetouredSerdes("Abs") {
        val onPlan = stripAQEPlan(sql("SELECT abs(x) FROM t").queryExecution.executedPlan)
        val proj = collect(onPlan) { case p: CometProjectExec => p }
        assert(proj.nonEmpty, "expected the projection to stay native with the detour on")
        assert(
          collect(onPlan) { case p: ProjectExec => p }.isEmpty,
          "expected no Spark ProjectExec with the detour on")
        assert(
          proj.forall(p => collect(p) { case c if isRowTransition(c) => c }.isEmpty),
          "expected no columnar->row transition below the native projection (island preserved)")
      }
    }
  }
}

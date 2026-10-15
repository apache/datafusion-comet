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

import scala.util.Random

import org.scalatest.exceptions.TestFailedException

import org.apache.arrow.vector._
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, ApplyFunctionExpression, AttributeReference, AttributeSeq, BindReferences, BoundReference, Cast, CreateArray, CreateMap, CreateNamedStruct, Expression, GenericInternalRow, Hypot, If, IsNull, LessThan, Literal, MapConcat, Or, Rand, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.comet.{CometFilterExec, CometHashAggregateExec, CometProjectExec}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.codegen.CometBatchKernelCodegen.ArrowColumnSpec
import org.apache.comet.serde.{CometInvokeTargets, CometScalaUDF, QueryPlanSerde}
import org.apache.comet.serde.ExprOuterClass.Expr.ExprStructCase
import org.apache.comet.udf.CometUdfBridge
import org.apache.comet.udf.codegen.CometScalaUDFCodegen
import org.apache.comet.vector.CometVector

/**
 * End-to-end correctness for the Arrow-direct codegen dispatcher. Covers the scalar and complex
 * type surface, composed UDF trees, subquery reuse, `TaskContext` propagation, per-task cache
 * isolation, the `maxFields` plan-time gate, and regressions pinned from fuzz.
 *
 * Tests exercising fallback paths (config disabled, `maxFields` exceeded) use `checkSparkAnswer`
 * rather than `checkSparkAnswerAndOperator` because ScalaUDF has no Comet-native path. Under
 * fallback the project runs on the JVM Spark path.
 */
class CometCodegenSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private def withSubjects(values: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val rows = values
        .map(v => if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("json_array_length routing follows native opt-in and dispatcher settings") {
    withSubjects("[1,2,3]", "[]", "not an array", null) {
      for {
        allowIncompatible <- Seq(false, true)
        codegenEnabled <- Seq(false, true)
      } {
        withSQLConf(
          "spark.comet.expression.LengthOfJsonArray.allowIncompatible" ->
            allowIncompatible.toString,
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> codegenEnabled.toString,
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
          val queries = Seq(
            "SELECT json_array_length(s) FROM t",
            "SELECT json_array_length('[1,2,3]') FROM t") ++
            (if (allowIncompatible) Seq.empty
             else
               Seq(
                 """SELECT json_array_length("[{'key':'value'}]") FROM t""",
                 "SELECT json_array_length('[1,2,3] trailing') FROM t"))

          queries.foreach { query =>
            withClue(s"allowIncompatible=$allowIncompatible, codegen=$codegenEnabled: $query") {
              val df = sql(query)
              if (!allowIncompatible && !codegenEnabled) {
                checkSparkAnswerAndFallbackReasons(
                  df,
                  Set("json_array_length: spark.comet.exec.scalaUDF.codegen.enabled=false"))
              } else {
                val (_, cometPlan) = checkSparkAnswerAndOperator(df)
                // Spark 4 rewrites this expression to StaticInvoke. Inspect the executable
                // expression because the rewrite does not preserve its implementation tags.
                val expr = stripAQEPlan(cometPlan)
                  .collectFirst { case project: CometProjectExec =>
                    project.nativeOp.getProjection.getProjectList(0)
                  }
                  .getOrElse(fail("Expected a Comet projection"))
                if (allowIncompatible) {
                  assert(expr.hasScalarFunc)
                  assert(expr.getScalarFunc.getFunc === "json_array_length")
                } else {
                  assert(expr.hasJvmScalarUdf)
                  assert(
                    expr.getJvmScalarUdf.getClassName === classOf[CometScalaUDFCodegen].getName)
                }
              }
            }
          }
        }
      }
    }
  }

  for {
    (name, configName, nativeKind, expressions) <- Seq(
      (
        "from_json",
        "JsonToStructs",
        ExprStructCase.FROM_JSON,
        Seq(
          "from_json(j, 'a INT, b STRING')" -> true,
          "from_json(j, 'a INT, arr ARRAY<INT>')" -> false)),
      (
        "to_json",
        "StructsToJson",
        ExprStructCase.TO_JSON,
        Seq(
          "to_json(s)" -> true,
          "to_json(a)" -> false,
          "to_json(s, map('ignoreNullFields', 'false'))" -> false)))
  } {
    test(s"$name routing follows native opt-in and dispatcher settings") {
      withTable("json_routing") {
        sql("""CREATE TABLE json_routing(j STRING, s STRUCT<a: INT, b: STRING>, a ARRAY<INT>)
              |USING parquet""".stripMargin)
        sql("""INSERT INTO json_routing VALUES
              |('{"a":1,"b":"x","arr":[1,null,3]}', named_struct('a', 1, 'b', 'x'), array(1, null, 3)),
              |('{"a":null,"b":"","arr":[]}', named_struct('a', null, 'b', ''), array()),
              |('{}', named_struct('a', null, 'b', null), array()),
              |(NULL, NULL, NULL)""".stripMargin)
        for {
          allowIncompatible <- Seq(false, true)
          codegenEnabled <- Seq(false, true)
        } {
          withSQLConf(
            s"spark.comet.expression.$configName.allowIncompatible" ->
              allowIncompatible.toString,
            CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
            expressions.foreach { case (expression, nativeSupported) =>
              withClue(
                s"allowIncompatible=$allowIncompatible, codegen=$codegenEnabled: $expression") {
                val query = s"SELECT $expression FROM json_routing"
                val expectNative = allowIncompatible && nativeSupported
                if (!expectNative && !codegenEnabled) {
                  checkSparkAnswerAndFallbackReason(
                    query,
                    s"$name: spark.comet.exec.scalaUDF.codegen.enabled=false")
                } else {
                  val (_, cometPlan) = checkSparkAnswerAndOperator(sql(query))
                  // Spark 4 rewrites to_json to Invoke without preserving implementation tags.
                  // Inspect the executable expression to distinguish native from dispatch.
                  val expr = stripAQEPlan(cometPlan)
                    .collectFirst { case project: CometProjectExec =>
                      project.nativeOp.getProjection.getProjectList(0)
                    }
                    .getOrElse(fail("Expected a Comet projection"))
                  if (expectNative) {
                    assert(expr.getExprStructCase === nativeKind)
                  } else {
                    assert(expr.hasJvmScalarUdf)
                    assert(
                      expr.getJvmScalarUdf.getClassName === classOf[CometScalaUDFCodegen].getName)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def withTwoStringCols(rows: (String, String)*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (c1 STRING, c2 STRING) USING parquet")
      if (rows.nonEmpty) {
        val tuples = rows.map { case (a, b) =>
          val av = if (a == null) "NULL" else s"'${a.replace("'", "''")}'"
          val bv = if (b == null) "NULL" else s"'${b.replace("'", "''")}'"
          s"($av, $bv)"
        }
        sql(s"INSERT INTO t VALUES ${tuples.mkString(", ")}")
      }
      f
    }
  }

  /**
   * Whether `replace` was routed through the JVM codegen dispatcher. Inspects the expression
   * collection rather than formatted explain text: `rollUpInfoMessages` concatenates sibling
   * names alphabetically (`cast, divide, ..., replace`), so a substring `"JVM codegen dispatcher:
   * replace"` misses the case where `replace` is not first.
   */
  private def replaceIsDispatched(
      plan: org.apache.spark.sql.execution.SparkPlan): (Seq[String], String) = {
    val info = new ExtendedExplainInfo()
    (info.getCodegenDispatchExpressions(plan), info.generateExtendedInfo(plan))
  }

  private def assertReplaceDispatch(
      df: org.apache.spark.sql.DataFrame,
      expectDispatcher: Boolean,
      clue: String): Unit = {
    checkSparkAnswerAndOperator(df)
    val (dispatched, explain) = replaceIsDispatched(df.queryExecution.executedPlan)
    assert(
      dispatched.contains("replace") == expectDispatcher,
      s"$clue, got dispatched expressions: $dispatched\n$explain")
  }

  test("codegen kernel round-trips CalendarIntervalType") {
    val input = new IntervalMonthDayNanoVector("in", CometArrowAllocator)
    val field =
      CometBatchKernelCodegen.toFfiArrowField("out", CalendarIntervalType, nullable = true)
    val output = CometBatchKernelCodegen.allocateOutput(field, 2, 0)
    try {
      input.allocateNew()
      input.setSafe(0, 14, -3, 1234567000L)
      input.setNull(1)
      input.setValueCount(2)

      val expr = BoundReference(0, CalendarIntervalType, nullable = true)
      val spec = ArrowColumnSpec(classOf[IntervalMonthDayNanoVector], nullable = true)
      val kernel = CometBatchKernelCodegen.compile(expr, IndexedSeq(spec)).newInstance()
      kernel.init(0)
      kernel.process(Array(input), output, 2)
      output.setValueCount(2)

      val comet = CometVector.getVector(output, null)
      val actual = comet.getInterval(0)
      assert(actual.months === 14)
      assert(actual.days === -3)
      assert(actual.microseconds === 1234567L)
      assert(comet.getInterval(1) == null)
    } finally {
      output.close()
      input.close()
    }
  }

  test("a closed allocateOutput vector releases all of its memory") {
    // Struct outputs leaked the children that StructVector's writer allocates in its
    // constructor. The List and Map cases make sure that these outputs do not start to leak.
    val pair = StructType(
      Seq(StructField("name", StringType), StructField("age", IntegerType, nullable = false)))
    val outputTypes = Seq(
      pair,
      StructType(
        Seq(StructField("_1", LongType, nullable = false), StructField("_2", StringType))),
      StructType(
        Seq(
          StructField("inner", pair),
          StructField("tags", ArrayType(StringType)),
          StructField("attrs", MapType(StringType, IntegerType)))),
      ArrayType(pair),
      MapType(StringType, pair),
      StringType)
    outputTypes.foreach { dataType =>
      val field = CometBatchKernelCodegen.toFfiArrowField("out", dataType, nullable = true)
      val allocator =
        CometArrowAllocator.newChildAllocator(s"allocateOutput($dataType)", 0, Long.MaxValue)
      try {
        CometBatchKernelCodegen.allocateOutput(field, 4, 0, allocator).close()
        assert(
          allocator.getAllocatedMemory == 0,
          s"the $dataType output did not release all of its memory")
      } finally {
        allocator.close()
      }
    }
  }

  test("ScalaUDF over concat(c1, c2) suppresses the null short-circuit") {
    // Concat is not NullIntolerant. The dispatcher's short-circuit guard inspects every node in
    // the bound tree and must skip the whole-tree null short-circuit because one child is
    // non-NullIntolerant. The kernel therefore delegates null handling to Spark's generated
    // code (which handles Concat(null, x) = x correctly) rather than returning null for any
    // null input. Without the guard, null inputs would produce null outputs even where Spark
    // produces a non-null concatenation.
    spark.udf.register("tag", (s: String) => if (s == null) "N" else s"[${s}]")
    withTwoStringCols(("abc", "123"), ("abc", null), (null, "123"), (null, null), ("zz", "zz")) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT tag(concat(c1, c2)) FROM t"))
      }
    }
  }

  test("disabled mode bypasses the dispatcher") {
    // When the per-feature config is off, `CometScalaUDF.convert` returns None and the enclosing
    // operator falls back to Spark. The dispatcher's counters must not move.
    spark.udf.register("noopStr", (s: String) => s)
    CometScalaUDFCodegen.resetStats()
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withSubjects("disabled_1", null) {
        checkSparkAnswer(sql("SELECT noopStr(s) FROM t"))
      }
    }
    val after = CometScalaUDFCodegen.stats()
    assert(
      after.compileCount == 0 && after.cacheHitCount == 0,
      s"expected no dispatcher activity under disabled config, got $after")
  }

  test("schema exceeding spark.sql.codegen.maxFields falls back to Spark") {
    // `CometBatchKernelCodegen.canHandle` mirrors WSCG's `spark.sql.codegen.maxFields` gate by
    // counting nested input fields plus the output field and refusing once the total exceeds the
    // configured cap. Comet has no mid-execution fallback, so the gate must fire at plan time
    // (in the serde) rather than letting an oversized kernel reach Janino. With 5 input
    // BoundReferences and a 1-field output we have 6 fields total. Setting `maxFields=3` ensures
    // the gate fires here regardless of test ordering or future schema additions.
    spark.udf.register(
      "sumFiveInts",
      (a: Int, b: Int, c: Int, d: Int, e: Int) => a + b + c + d + e)
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT, c INT, d INT, e INT) USING parquet")
      sql("INSERT INTO t VALUES (1, 2, 3, 4, 5), (10, 20, 30, 40, 50)")
      CometScalaUDFCodegen.resetStats()
      withSQLConf("spark.sql.codegen.maxFields" -> "3") {
        checkSparkAnswer(sql("SELECT sumFiveInts(a, b, c, d, e) FROM t"))
      }
      val after = CometScalaUDFCodegen.stats()
      assert(
        after.compileCount == 0 && after.cacheHitCount == 0,
        s"expected dispatcher fallback under maxFields=3, got $after")
    }
  }

  test("explain.codegen.enabled surfaces routed expressions in COMET-INFO") {
    // With the opt-in flag on, `hypot` and `nanvl` (both `CometCodegenDispatch`) roll up
    // into one `[COMET-INFO: JVM codegen dispatcher: hypot, nanvl]` line on the
    // `CometProject`. With the flag off (default), no such line appears.
    withTable("t") {
      sql("CREATE TABLE t (a DOUBLE, b DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (3.0, 4.0)")

      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
        val df = sql("SELECT hypot(a, b), nanvl(a, b) FROM t")
        checkSparkAnswerAndOperator(df)
        val explain =
          new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
        assert(
          explain.contains("[COMET-INFO:"),
          s"expected a [COMET-INFO: segment, got:\n$explain")
        // Names appear alphabetically via `.distinct.sorted` in rollUpInfoMessages.
        assert(
          dispatchedNames(explain).containsSlice(Seq("hypot", "nanvl")),
          s"expected combined codegen-dispatch info, got:\n$explain")
      }

      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
        val df = sql("SELECT hypot(a, b), nanvl(a, b) FROM t")
        checkSparkAnswerAndOperator(df)
        val explain =
          new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
        assert(
          !explain.contains("JVM codegen dispatcher"),
          s"expected NO codegen-dispatch info with the flag off, got:\n$explain")
      }
    }
  }

  test("checkSparkAnswerAndImpl pins the mechanism and fails when the claim is wrong") {
    // The assertion helper is only worth having if it fails. `abs` lowers to a native DataFusion
    // expression and `hypot` is a `CometCodegenDispatch`, so this query exercises both buckets at
    // once and each wrong claim below must be rejected.
    withTable("t") {
      sql("CREATE TABLE t (a DOUBLE, b DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (3.0, 4.0)")
      val query = "SELECT abs(a), hypot(a, b) FROM t"

      checkSparkAnswerAndImpl(sql(query), native = Seq("abs"), dispatched = Seq("hypot"))

      // Claiming the wrong mechanism fails, in both directions.
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), native = Seq("hypot"))
      }
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), dispatched = Seq("abs"))
      }
      // So does naming an expression the query does not contain, which is what a typo in a
      // fixture looks like.
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), native = Seq("no_such_expression"))
      }
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), dispatched = Seq("no_such_expression"))
      }
    }
  }

  test("an expression nested inside a dispatched subtree is classified as dispatched") {
    // The whole of `hypot(abs(b), c)` is bound and closure-serialized into one JVM kernel, so the
    // inner `abs` ran in the JVM even though the `abs(a)` next to it ran natively. Naming only
    // the dispatched root would let `native = Seq("abs")` pass here while an `abs` was running in
    // the kernel, which is the one claim this helper exists to make trustworthy.
    withTable("t") {
      sql("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (3.0, 4.0, 5.0)")
      val query = "SELECT abs(a), hypot(abs(b), c) FROM t"

      // `abs` is genuinely on both sides of the fence, so neither claim about it alone holds.
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), native = Seq("abs"))
      }
      intercept[TestFailedException] {
        checkSparkAnswerAndImpl(sql(query), dispatched = Seq("abs"))
      }
      // `hypot` is unambiguous, and the same query still classifies it correctly.
      checkSparkAnswerAndImpl(sql(query), dispatched = Seq("hypot"))
    }
  }

  /**
   * The expression names listed in the `[COMET-INFO: JVM codegen dispatcher: ...]` segment.
   *
   * Matching the whole segment rather than a `contains` on `"JVM codegen dispatcher: <name>"`,
   * because the segment lists every dispatched expression in the operator sorted by name -
   * including expressions nested inside a dispatched subtree - so a substring match pinned to one
   * name breaks as soon as a query dispatches a second one.
   */
  private def dispatchedNames(explain: String): Seq[String] =
    "JVM codegen dispatcher: ([^\\]]*)".r
      .findFirstMatchIn(explain)
      .map(_.group(1).split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)

  private def withSequenceTable(f: => Unit): Unit = {
    withTable("t") {
      // `stp` carries a sign-correct step so `sequence(a, b, stp)` is legal on both rows:
      // ascending (1, 5, 1) and descending (9, 2, -1). A single literal step would raise
      // `Illegal sequence boundaries` on the mismatched row inside Spark's reference run.
      sql("CREATE TABLE t (a INT, b INT, stp INT, d DATE) USING parquet")
      sql("INSERT INTO t VALUES (1, 5, 1, DATE'2024-01-01'), (9, 2, -1, DATE'2024-03-01')")
      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE)(f)
    }
  }

  test("sequence with leaf integral args runs natively") {
    // Integral sequence with column-reference/literal args lowers to the native spark_sequence
    // kernel; no codegen-dispatch marker should appear. The three-argument form uses the `stp`
    // column so both the ascending and descending rows have a sign-correct step (all args
    // stay leaves, so the native path is exercised).
    withSequenceTable {
      val df = sql("SELECT sequence(a, b), sequence(a, b, stp) FROM t")
      checkSparkAnswerAndOperator(df)
      val explain =
        new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
      assert(
        !explain.contains("JVM codegen dispatcher"),
        s"expected integral sequence with leaf args to run natively, got:\n$explain")
    }
  }

  test("sequence with zero-arg UDF stop routes through the dispatcher") {
    // A zero-argument Scala UDF has empty `children` but still fires on evaluation. The gate
    // must reject it (rather than treating it as a safe leaf) so DataFusion does not call it
    // over the whole batch on rows Spark's per-row null short-circuit would have skipped.
    spark.udf.register("comet_seq_stopper", () => 10)
    withSequenceTable {
      val df = sql("SELECT sequence(a, comet_seq_stopper()) FROM t")
      checkSparkAnswerAndOperator(df)
      val explain =
        new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
      assert(
        dispatchedNames(explain).contains("sequence"),
        s"expected zero-arg-UDF sequence to route through the dispatcher, got:\n$explain")
    }
  }

  test("sequence with non-leaf integral args routes through the dispatcher") {
    // A non-leaf argument (e.g. a `CASE WHEN` step) would be evaluated over the whole batch by
    // DataFusion before the outer kernel runs, breaking Spark's per-row null short-circuit.
    // `CometSequence` reports `Unsupported` for these shapes and hands them to the JVM codegen
    // dispatcher.
    withSequenceTable {
      val df = sql("SELECT sequence(a, b, CASE WHEN a <= b THEN 2 ELSE -2 END) FROM t")
      checkSparkAnswerAndOperator(df)
      val explain =
        new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
      assert(
        dispatchedNames(explain).contains("sequence"),
        s"expected composed-arg sequence to route through the dispatcher, got:\n$explain")
    }
  }

  test("sequence with date element type routes through the dispatcher") {
    // Date/timestamp sequences step through timezone/DST/legacy-calendar arithmetic
    // (issue #5349), so `CometSequence` keeps them on the JVM codegen dispatcher.
    withSequenceTable {
      val df = sql("SELECT sequence(d, DATE'2024-06-01', INTERVAL 1 MONTH) FROM t")
      checkSparkAnswerAndOperator(df)
      val explain =
        new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
      assert(
        dispatchedNames(explain).contains("sequence"),
        s"expected date sequence to route through the dispatcher, got:\n$explain")
    }
  }

  test("expression coverage stats split native from codegen-dispatch expressions") {
    // `abs` and `sqrt` lower to native DataFusion expressions; `hypot` and `nanvl` are
    // `CometCodegenDispatch` and so run Spark's own codegen inside the Comet pipeline. The
    // coverage stats and the accessors report the two groups separately, and they do so
    // regardless of `explain.codegen.enabled` (which only controls the `[COMET-INFO:` line).
    withTable("t") {
      sql("CREATE TABLE t (a DOUBLE, b DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (3.0, 4.0)")

      withSQLConf(
        CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
        val df = sql("SELECT abs(a), sqrt(b), hypot(a, b), nanvl(a, b) FROM t")
        checkSparkAnswerAndOperator(df)
        val plan = df.queryExecution.executedPlan
        val info = new ExtendedExplainInfo()

        assert(info.getNativeExpressions(plan) === Seq("abs", "sqrt"))
        assert(info.getCodegenDispatchExpressions(plan) === Seq("hypot", "nanvl"))

        val explain = info.generateExtendedInfo(plan)
        assert(
          explain.contains("Accelerated expressions: 2 native, 2 codegen dispatch."),
          s"expected expression coverage in the summary, got:\n$explain")
        assert(
          !explain.contains("JVM codegen dispatcher"),
          s"expected NO codegen-dispatch info with the flag off, got:\n$explain")
      }
    }
  }

  test("expression coverage stats survive the decimal promotion rewrite") {
    // `DecimalPrecision.promote` rebuilds the expression tree before serde runs, wrapping decimal
    // arithmetic in a synthesized `CheckOverflow`, so the coverage tags land on a copy the
    // operator does not hold. `QueryPlanSerde.liftCoverageTags` moves them back onto the tree the
    // operator holds, which for a projection is the `Alias`.
    //
    // `checkoverflow` is the name that pins that lift: `promote` reuses the original `Add`
    // instance as the wrapper's child, so `add` stays reachable from the untouched tree and would
    // be reported either way. The wrapper exists only on the rebuilt copy.
    withTable("t") {
      sql("CREATE TABLE t (a DECIMAL(10, 2), b DECIMAL(12, 4)) USING parquet")
      sql("INSERT INTO t VALUES (1.23, 4.5678)")

      withSQLConf(
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
        val df = sql("SELECT a + b FROM t")
        checkSparkAnswerAndOperator(df)
        val native =
          new ExtendedExplainInfo().getNativeExpressions(df.queryExecution.executedPlan)
        assert(native.contains("checkoverflow"), s"expected the promoted wrapper, got: $native")
        assert(native.contains("add"), s"expected the arithmetic expression, got: $native")
      }
    }
  }

  test("codegen dispatch coverage survives the decimal promotion rewrite") {
    val decimal = AttributeReference("amount", DecimalType(10, 2), nullable = false)()
    val dispatched = Hypot(Cast(Add(decimal, decimal), DoubleType), Literal(4.0d))
    val projection = Alias(dispatched, "value")()

    // Promotion rebuilds Hypot as well as the Alias above it. Unlike the original Add, the
    // dispatched copy is not reachable from the original tree, so only the coverage lift can
    // bring its names back to the projection owner.
    //
    // Every expression in the rebuilt subtree is named, not just the dispatched root: the whole
    // subtree was bound into the one kernel, so all of it ran in the JVM. `checkoverflow` is the
    // wrapper promotion added around the decimal `Add`, which is what makes the lifted set
    // evidence that the promoted copy, rather than the original tree, was the one recorded.
    val proto = QueryPlanSerde.exprToProto(projection, Seq(decimal)).get
    assert(proto.hasJvmScalarUdf)
    assert(proto.getJvmScalarUdf.getClassName === classOf[CometScalaUDFCodegen].getName)
    assert(dispatched.getTagValue(CometExplainInfo.DISPATCHED_SELF).isEmpty)
    assert(dispatched.getTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS).isEmpty)
    assert(
      projection
        .getTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS)
        .contains(Set("hypot", "cast", "checkoverflow", "add")))
  }

  test("the serde ships a digest of the serialized expression at arg 0 (#6705)") {
    // The dispatcher trusts a cache hit on the digest without comparing the bytes, so the digest
    // the serde ships must be the digest of the bytes it ships next to it. Calling the dispatcher
    // directly cannot catch a mismatch, because those tests build their own digest.
    val x = AttributeReference("x", DoubleType, nullable = false)()
    def payload(e: Expression): (Array[Byte], Array[Byte]) = {
      val proto = QueryPlanSerde.exprToProto(e, Seq(x)).get
      assert(proto.hasJvmScalarUdf)
      val args = proto.getJvmScalarUdf.getArgsList
      (
        args.get(0).getLiteral.getBytesVal.toByteArray,
        args.get(1).getLiteral.getBytesVal.toByteArray)
    }

    val (hypotDigest, hypotBytes) = payload(Hypot(x, Literal(4.0d)))
    val (otherDigest, otherBytes) = payload(Hypot(x, Literal(5.0d)))
    assert(hypotDigest.sameElements(CometScalaUDFCodegen.digest(hypotBytes)))
    assert(otherDigest.sameElements(CometScalaUDFCodegen.digest(otherBytes)))

    // Expressions that differ only in a literal must not share a kernel.
    assert(!hypotBytes.sameElements(otherBytes))
    assert(!hypotDigest.sameElements(otherDigest))
  }

  test("tags copied onto the shared TrueLiteral do not leak into unrelated plans") {
    // Catalyst copies a rewritten node's tags onto its replacement, so a tagged expression that an
    // earlier query rewrote into `Literal.TrueLiteral` brands that process-wide singleton for the
    // lifetime of the JVM. Planting the tags stands in for that history. The `fact` scan below
    // carries a cleaned-up dynamic pruning filter, `dynamicpruningexpression(true)`, which is that
    // very singleton, so before https://github.com/apache/datafusion-comet/issues/5229 the
    // operator reported a name and an info message belonging to some unrelated query.
    val planted = Literal.TrueLiteral
    planted.setTagValue(CometExplainInfo.EXTENSION_INFO, Set("PLANTED_INFO"))
    planted.setTagValue(CometExplainInfo.NATIVE_EXPRS, Set("plantedexpr"))
    planted.setTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS, Set("planteddispatch"))
    try {
      // Decimal promotion rebuilds this projection. Its coverage lift must not copy the
      // singleton's stale tags onto the Alias, which is a legitimate coverage owner.
      val decimal = AttributeReference("amount", DecimalType(10, 2), nullable = false)()
      val projection = Alias(
        CreateNamedStruct(Seq(Literal("flag"), planted, Literal("sum"), Add(decimal, decimal))),
        "value")()
      assert(QueryPlanSerde.exprToProto(projection, Seq(decimal)).isDefined)
      val native = projection.getTagValue(CometExplainInfo.NATIVE_EXPRS).getOrElse(Set.empty)
      assert(native.contains("checkoverflow"), s"expected lifted decimal coverage, got: $native")
      assert(!native.contains("plantedexpr"))
      assert(projection.getTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS).isEmpty)

      withSQLConf(
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE,
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        withTable("fact", "dim") {
          sql("CREATE TABLE fact (v INT, p INT) USING parquet PARTITIONED BY (p)")
          sql("INSERT INTO fact VALUES (1, 1), (2, 2)")
          sql("CREATE TABLE dim (k INT, s STRING) USING parquet")
          sql("INSERT INTO dim VALUES (1, 'a'), (2, 'b')")

          val plan = sql(
            "SELECT * FROM fact JOIN dim ON fact.p = dim.k WHERE dim.s = 'a'").queryExecution.executedPlan

          // Guard against the test going vacuous if planning stops producing the singleton.
          assert(
            plan.exists(_.expressions.exists(_.exists(_ eq planted))),
            s"expected a plan holding Literal.TrueLiteral, got:\n$plan")

          val info = new ExtendedExplainInfo()
          assert(!info.getNativeExpressions(plan).contains("plantedexpr"))
          assert(!info.getCodegenDispatchExpressions(plan).contains("planteddispatch"))
          val explain = info.generateExtendedInfo(plan)
          assert(!explain.contains("PLANTED_INFO"), s"tag leaked into:\n$explain")
        }
      }
    } finally {
      planted.unsetTagValue(CometExplainInfo.EXTENSION_INFO)
      planted.unsetTagValue(CometExplainInfo.NATIVE_EXPRS)
      planted.unsetTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS)
    }
  }

  test("expression coverage stats count nothing when the plan falls back entirely") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
      val df = sql("SELECT abs(1.0)")
      val plan = df.queryExecution.executedPlan
      val info = new ExtendedExplainInfo()
      assert(info.getNativeExpressions(plan).isEmpty)
      assert(info.getCodegenDispatchExpressions(plan).isEmpty)
      assert(
        info
          .generateExtendedInfo(plan)
          .contains("Accelerated expressions: 0 native, 0 codegen dispatch."))
    }
  }

  test("replace compatibility boundary cases stay on JVM codegen dispatcher") {
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {

      // Malformed search: CometLiteral would normalize 0xFF to U+FFFD, incorrectly matching
      // a well-formed U+FFFD in the source.
      withTable("t") {
        sql("CREATE TABLE t (s STRING) USING parquet")
        sql("INSERT INTO t VALUES ('\uFFFD'), ('ok')")
        assertReplaceDispatch(
          sql("SELECT replace(s, CAST(X'FF' AS STRING), 'x') FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for malformed search literal")
      }

      // Malformed replacement has the same serialization hazard.
      withTable("t") {
        sql("CREATE TABLE t (s STRING) USING parquet")
        sql("INSERT INTO t VALUES ('a'), ('b')")
        assertReplaceDispatch(
          sql("SELECT replace(s, 'a', CAST(X'FF' AS STRING)) FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for malformed replacement literal")
      }

      // Spark skips replacement evaluation when src is NULL; native evaluates every child.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withTable("t") {
          sql("CREATE TABLE t (s STRING, n INT) USING parquet")
          sql("INSERT INTO t VALUES (NULL, 0), ('a', 1)")
          assertReplaceDispatch(
            sql("SELECT replace(s, 'a', CAST(1 / n AS STRING)) FROM t"),
            expectDispatcher = true,
            "expected dispatcher path for throwing replacement expression")
        }
      }

      // A 256 KiB scalar replacement overflows Arrow Utf8 offsets when broadcast to 8192 rows.
      withTable("t") {
        sql("CREATE TABLE t (s STRING) USING parquet")
        sql("INSERT INTO t VALUES ('hello')")
        assertReplaceDispatch(
          sql("SELECT replace(s, 'notfound', repeat('x', 262144)) FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for oversized replacement literal")
      }

      // Source is not on the whitelist: Spark short-circuits inside substring when s is NULL.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withTable("t") {
          sql("CREATE TABLE t (s STRING, n INT) USING parquet")
          sql("INSERT INTO t VALUES (NULL, 0), ('a', 1)")
          assertReplaceDispatch(
            sql("SELECT replace(substring(s, 1, CAST(1 / n AS INT)), 'a', 'x') FROM t"),
            expectDispatcher = true,
            "expected dispatcher path for throwing expression nested in source")
        }
      }

      // Malformed source literal: same CometLiteral byte-normalization as search/replacement.
      withTable("t") {
        sql("CREATE TABLE t (r STRING) USING parquet")
        sql("INSERT INTO t VALUES ('x')")
        assertReplaceDispatch(
          sql("SELECT replace(CAST(X'FF' AS STRING), 'a', r) FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for malformed source literal")
      }

      // Malformed literal nested under concat is still in the source tree.
      withTable("t") {
        sql("CREATE TABLE t (r STRING) USING parquet")
        sql("INSERT INTO t VALUES ('x')")
        assertReplaceDispatch(
          sql("SELECT replace(concat(CAST(X'FF' AS STRING), r), 'a', 'x') FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for malformed literal nested in source")
      }

      // Oversized source literal has the same broadcast / offset-overflow hazard.
      withTable("t") {
        sql("CREATE TABLE t (r STRING) USING parquet")
        sql("INSERT INTO t VALUES ('x')")
        assertReplaceDispatch(
          sql("SELECT replace(repeat('x', 262144), 'notfound', r) FROM t"),
          expectDispatcher = true,
          "expected dispatcher path for oversized source literal")
      }
    }
  }

  test("codegen dispatch fallback reasons name the expression") {
    // Flag-off short-circuit tags the expression `<name>: <reason>` so distinct expressions
    // don't collapse in the `Set[String]` roll-up.
    withTable("t") {
      sql("CREATE TABLE t (a DOUBLE, b DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (3.0, 4.0)")
      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
          CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
        val df = sql("SELECT hypot(a, b) FROM t")
        checkSparkAnswer(df)
        val explain =
          new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
        assert(
          explain.contains("hypot:") &&
            explain.contains(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key + "=false"),
          s"expected 'hypot:' prefix and disabled-flag reason, got:\n$explain")
      }
    }
  }

  test("dispatcher caches the compiled kernel across batches of one query") {
    // Within a single query, the dispatcher compiles a kernel for the (expression, schema) pair
    // once and reuses it across every subsequent batch of the same shape. Force multiple batches
    // by lowering the Comet batch size with a row count well above it, then assert at least one
    // cache hit happened during the query.
    //
    // We deliberately do not assert cross-query cache reuse: Spark's analyzer produces a fresh
    // `ScalaUDF` instance per query resolution, and the encoders embedded in that instance
    // contain `AttributeReference`s with fresh `ExprId`s that our `BindReferences.bindReference`
    // does not recurse into. The closure-serialized cache key bytes therefore drift across
    // queries even when the registered function and schema are identical, so each new query of a
    // ScalaUDF pays one compile up front and amortizes within itself. This is an acceptable
    // amortization story (a few tens of milliseconds per query), not a behavior we can or do
    // promise across queries.
    spark.udf.register("kernelCacheMarker", (s: String) => if (s == null) null else s + "_kc")
    val rows = (0 until 256).map(i => s"row_$i")
    CometScalaUDFCodegen.resetStats()
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "32") {
      withSubjects(rows: _*) {
        checkSparkAnswerAndOperator(sql("SELECT kernelCacheMarker(s) FROM t"))
      }
    }
    val stats = CometScalaUDFCodegen.stats()
    assert(stats.compileCount >= 1, s"expected at least one compile during the query, got $stats")
    assert(
      stats.cacheHitCount >= 1,
      s"expected at least one cache hit across batches of the same query, got $stats")
  }

  test("per-partition kernel preserves Nondeterministic state across batches") {
    // Wrap `monotonically_increasing_id()` as the argument of a ScalaUDF so the whole tree
    // (including the stateful MonotonicallyIncreasingID child) routes through the dispatcher.
    // Per-partition kernel caching means the id counter advances across batches within a
    // partition. Without it, every batch would restart at 0 and the UDF output would disagree
    // with Spark's. The UDF body is a trivial identity. We're testing state correctness of the
    // Nondeterministic child across batches, not the UDF logic.
    spark.udf.register("idPassthrough", (id: Long) => id)
    val rows = (0 until 4096).map(i => s"row_$i")
    withSubjects(rows: _*) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT s, idPassthrough(monotonically_increasing_id()) FROM t"))
      }
    }
  }

  test(
    "same UDF over nullable and non-nullable columns gets distinct kernels with independent state") {
    // Two columns, same type, different schema-declared nullability. Same UDF applied to each
    // alongside a per-projection MonotonicallyIncreasingID. Each projection has its own MII
    // child (a different digest), so each kernel must have its own counter advancing 0..N-1.
    // If the dispatcher collapses them onto one kernel or shares state somehow, the counters
    // would interleave and the output would diverge from Spark.
    spark.udf.register("withId", (s: String, id: Long) => s"${s}_${id}")
    withTempPath { dir =>
      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types.{StringType, StructField, StructType}
      val schema = StructType(
        Seq(
          StructField("a", StringType, nullable = true),
          StructField("b", StringType, nullable = false)))
      val rows = (0 until 64).map(i => Row(s"a_$i", s"b_$i"))
      val rdd = spark.sparkContext.parallelize(rows, numSlices = 1)
      spark.createDataFrame(rdd, schema).write.parquet(dir.getCanonicalPath)
      withTable("t") {
        sql(s"CREATE TABLE t USING parquet LOCATION '${dir.getCanonicalPath}'")
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT withId(a, monotonically_increasing_id()), " +
                "withId(b, monotonically_increasing_id()) FROM t"))
          }
        }
      }
    }
  }

  test("Nondeterministic state persists across nullability flips within a partition") {
    // Regression guard against re-introducing per-batch nullability into the cache key. Force a
    // single parquet file with `spark.range(numPartitions=1)`, large enough that batch size 8
    // produces many batches in one scan partition. Null density varies by row range. If the
    // dispatcher ever started deriving spec nullability from runtime data again, the cache key
    // would flip mid-partition, the kernel would be re-allocated, and MII's counter would reset
    // across the flip.
    spark.udf.register("idPair", (id: Long, s: String) => (id, s))
    withTempPath { dir =>
      spark
        .range(0, 200, 1, numPartitions = 1)
        .selectExpr("CASE WHEN id >= 16 AND id < 32 THEN NULL ELSE concat('row_', id) END AS s")
        .write
        .parquet(dir.getCanonicalPath)
      withTable("t") {
        sql(s"CREATE TABLE t USING parquet LOCATION '${dir.getCanonicalPath}'")
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT idPair(monotonically_increasing_id(), s) FROM t"))
          }
        }
      }
    }
  }

  test("Nondeterministic state persists across two ScalaUDFs in one task") {
    // The dispatcher is one instance per task (keyed by `(taskAttemptId, udfClassName)` in
    // CometUdfBridge), so a plan with two distinct ScalaUDFs shares one CometScalaUDFCodegen.
    // Two distinct closure-serialized expressions hit two cache entries. Per batch the
    // dispatcher is invoked once for each. Each cache entry must stash its own kernel instance,
    // otherwise the two expressions would fight for a shared kernel slot and stateful state
    // (MII counter) would reset on every flip.
    //
    // Small batch size forces multiple batches over a small table so the per-key flip happens
    // several times within one task.
    spark.udf.register("idA", (id: Long) => id)
    spark.udf.register("idB", (id: Long) => -id)
    val rows = (0 until 64).map(i => s"row_$i")
    withSubjects(rows: _*) {
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql(
              "SELECT s, " +
                "idA(monotonically_increasing_id()) AS a, " +
                "idB(monotonically_increasing_id()) AS b FROM t"))
        }
      }
    }
  }

  test("per-task cache isolates UDF state across sequential task runs in one session") {
    // Regression guard for the cache-scoping invariant on CometUdfBridge: instances live for
    // exactly one Spark task and are dropped on task completion, so a stateful kernel sees a
    // fresh instance per task. The query has to actually route through the dispatcher for this
    // to test anything, so wrap `monotonically_increasing_id()` in a ScalaUDF identity. Running
    // it twice in one session must produce results matching Spark each time. Under a cache that
    // outlived a task and got reused by the next one, the counter would continue from the
    // previous run's final value and the second run's IDs would diverge from Spark. Under a
    // cache that was keyed by Tokio worker thread rather than task attempt ID, worker reuse
    // across tasks would cause the same leak whenever the second task happened to be polled by
    // the same worker. Two `checkSparkAnswerAndOperator` calls are stronger than asserting
    // first == second: equality alone could pass if both runs are wrong-but-consistent (e.g.
    // `init(partitionIndex)` never fires); matching Spark on both runs rules that out and
    // implies cross-run equality because Spark is deterministic on the same query.
    spark.udf.register("idPassthrough", (id: Long) => id)
    val rows = (0 until 2048).map(i => s"row_$i")
    withSubjects(rows: _*) {
      val q = "SELECT s, idPassthrough(monotonically_increasing_id()) AS mid FROM t"
      checkSparkAnswerAndOperator(sql(q))
      checkSparkAnswerAndOperator(sql(q))
    }
  }

  /**
   * Scalar ScalaUDF smoke tests. These prove that user-registered UDFs route through the codegen
   * dispatcher rather than forcing a whole-plan Spark fallback. Spark's `ScalaUDF.doGenCode`
   * already emits compilable Java that calls the user function via `ctx.addReferenceObj`, so the
   * dispatcher's compile path picks it up for free. Tests that user-registered UDFs route through
   * the dispatcher rather than forcing whole-plan Spark fallback.
   */

  test("registered string ScalaUDF routes through dispatcher") {
    spark.udf.register("shout", (s: String) => if (s == null) null else s.toUpperCase + "!")
    withSubjects("Abc", "xyz", null, "mixed") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT shout(s) FROM t"))
      }
    }
  }

  test("registered Java UDF1 routes through dispatcher") {
    // Java API path: `spark.udf.register(name, UDF1<...>, returnType)`. Spark wraps the Java
    // functional interface in a Scala function and produces a `ScalaUDF` expression at plan
    // time, so the dispatcher handles it the same as a Scala-registered UDF. Sanity check that
    // both registration paths land on the same routing code.
    spark.udf.register(
      "javaLen",
      new UDF1[String, Integer] {
        override def call(s: String): Integer = if (s == null) -1 else s.length
      },
      IntegerType)
    withSubjects("abc", "hello", null, "x") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT javaLen(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("a primitive ScalaUDF's null guard runs in the UDF's kernel (#6704)") {
    // Spark's `HandleNullInputsForUDF` wraps each call below as
    // `if (isnull(a) or ...) null else f(knownnotnull(a), ...)`. Run natively, the `if` is a
    // `CASE` that splits each batch on the predicate and merges the two halves back, so the guard
    // goes to the kernel with the call, and neither `if` nor `isnull` is native.
    spark.udf.register("plusOne", (x: Long) => x + 1)
    spark.udf.register("combine", (a: Long, b: Int, s: String) => s"$s:${a + b}")
    spark.udf.register("addBoth", (x: Long, y: Long) => x + y)
    spark.udf.register("addThree", (x: Long, y: Int, z: Long) => x + y + z)
    withTable("t") {
      sql("CREATE TABLE t (a BIGINT, b INT, s STRING) USING parquet")
      sql(
        "INSERT INTO t VALUES (1, 10, 'x'), (NULL, 20, 'y'), (3, NULL, 'z'), " +
          "(NULL, NULL, NULL), (-5, 7, NULL)")
      val guard = Seq("if", "isnull")
      checkSparkAnswerAndImpl(sql("SELECT plusOne(a) FROM t"), dispatched = "plusone" +: guard)
      checkSparkAnswerAndImpl(
        sql("SELECT max(plusOne(a)), count(plusOne(a)) FROM t"),
        dispatched = "plusone" +: guard)
      // An argument that is an expression is checked as it is.
      checkSparkAnswerAndImpl(
        sql("SELECT plusOne(a * 2) FROM t"),
        dispatched = "plusone" +: guard)
      // One `isnull` per primitive parameter, joined by `or`. The `String` one is not checked.
      checkSparkAnswerAndImpl(
        sql("SELECT combine(a, b, s) FROM t"),
        dispatched = Seq("combine", "or") ++ guard)
      // The optimizer drops a repeated `isnull`, so a column passed twice is checked once.
      checkSparkAnswerAndImpl(sql("SELECT addBoth(a, a) FROM t"), dispatched = "addboth" +: guard)
      checkSparkAnswerAndImpl(
        sql("SELECT addThree(a, b, a) FROM t"),
        dispatched = Seq("addthree", "or") ++ guard)
      // An argument that cannot be null gets no `isnull`.
      checkSparkAnswerAndImpl(
        sql("SELECT addBoth(a, 1L) FROM t"),
        dispatched = "addboth" +: guard)
    }
  }

  test("a boolean ScalaUDF's null guard runs in the kernel with false for null (#6704)") {
    // In a filter, and in the predicate of a conditional, `ReplaceNullWithFalseInPredicate`
    // rewrites the guard to `if (isnull(a)) false else f(knownnotnull(a))`.
    spark.udf.register("isPositive", (x: Long) => x > 0)
    withTypedCol("BIGINT", "1", "NULL", "-3", "0", "7") {
      checkSparkAnswerAndImpl(
        sql("SELECT c FROM t WHERE isPositive(c)"),
        dispatched = Seq("ispositive", "if", "isnull"))
      // The query's own `if` is native, so only the guard's `isnull` is checked.
      checkSparkAnswerAndImpl(
        sql("SELECT IF(isPositive(c), 'yes', 'no') FROM t"),
        dispatched = Seq("ispositive", "isnull"))
    }
  }

  test("a nondeterministic ScalaUDF under its guard is called only for non-null rows (#6704)") {
    // Each result counts the calls made so far in the task, so a call on a null row would shift
    // every result after it.
    import org.apache.spark.sql.functions.udf
    var calls = 0L
    val countCalls = udf { (x: Long) =>
      calls += 1
      x * 1000 + calls
    }
    spark.udf.register("countCalls", countCalls.asNondeterministic())
    withTypedCol("BIGINT", "1", "NULL", "2", "NULL", "NULL", "3") {
      checkSparkAnswerAndImpl(
        sql("SELECT countCalls(c) FROM t"),
        dispatched = Seq("countcalls", "if", "isnull"))
    }
  }

  test("a null guard the dispatcher cannot take whole stays native around its UDF (#6704)") {
    spark.udf.register("plusOne", (x: Long) => x + 1)
    withTypedCol("BIGINT", "1", "NULL", "-3") {
      // `canHandle` counts each reference to a column against `maxFields`, and the guard reads
      // `c` a second time: the UDF alone counts its output and one input, 2, and the guard 3. At
      // 2 the guard is refused while the UDF is not, so the guard has to stay a native `if`
      // around the dispatched UDF rather than take the projection to Spark.
      withSQLConf("spark.sql.codegen.maxFields" -> "2") {
        checkSparkAnswerAndImpl(
          sql("SELECT plusOne(c) FROM t"),
          native = Seq("if", "isnull"),
          dispatched = Seq("plusone"))
      }
      // With the dispatcher off, the fallback reason is the UDF's own, and none is recorded for
      // a guard that the query does not contain.
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          sql("SELECT plusOne(c) FROM t"),
          s"plusone: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false")
        val reasons = new ExtendedExplainInfo().getFallbackReasons(cometPlan)
        assert(!reasons.exists(_.startsWith("if:")), s"unexpected fallback reasons: $reasons")
      }
    }
  }

  test("a disabled expression in a null guard keeps the guard native (#6704)") {
    // Converting the guard natively checks `spark.comet.expression.<name>.enabled` on the UDF and
    // on each node of the predicate, the guarded argument's included, and a disabled one takes
    // the projection to Spark. Dispatching the guard has to make the same checks.
    spark.udf.register("plusOne", (x: Long) => x + 1)
    spark.udf.register("addBoth", (a: Long, b: Long) => a + b)
    withTable("t") {
      sql("CREATE TABLE t (a BIGINT, b BIGINT) USING parquet")
      sql("INSERT INTO t VALUES (1, 10), (NULL, 20), (-3, NULL)")
      Seq(
        "ScalaUDF" -> "SELECT plusOne(a) FROM t",
        "IsNull" -> "SELECT plusOne(a) FROM t",
        "Or" -> "SELECT addBoth(a, b) FROM t",
        "Multiply" -> "SELECT plusOne(a * 2) FROM t").foreach { case (name, query) =>
        val key = CometConf.getExprEnabledConfigKey(name)
        withSQLConf(key -> "false") {
          val (_, cometPlan) = checkSparkAnswerAndFallbackReason(sql(query), s"Set $key=true")
          assert(
            collect(cometPlan) { case p: CometProjectExec => p }.isEmpty,
            s"$query stayed in Comet with $key=false:\n$cometPlan")
        }
      }
    }
  }

  test("only the null guard Spark builds goes to the kernel with its UDF (#6704)") {
    spark.udf.register("plusOne", (x: Long) => x + 1)
    withTable("t") {
      sql("CREATE TABLE t (a BIGINT, b BIGINT) USING parquet")
      val plan = sql("SELECT plusOne(a), b FROM t").queryExecution.optimizedPlan
      val guard = plan.expressions.flatMap(_.collect { case g: If => g }).head
      val Seq(a, b) = plan.collectLeaves().head.output
      def root(expr: Expression): ExprStructCase =
        QueryPlanSerde.exprToProto(expr, Seq(a, b)).get.getExprStructCase
      assert(root(guard) == ExprStructCase.JVM_SCALAR_UDF)
      // A predicate that checks a column the call does not read, or one besides its arguments.
      assert(root(guard.copy(predicate = IsNull(b))) == ExprStructCase.IF)
      assert(root(guard.copy(predicate = Or(guard.predicate, IsNull(b)))) == ExprStructCase.IF)
      // A branch that is not null.
      assert(root(guard.copy(trueValue = Literal(0L))) == ExprStructCase.IF)
      // A nondeterministic argument, which the guard and the call would each evaluate.
      val random = guard.transformUp { case r: AttributeReference =>
        If(LessThan(Rand(Literal(1L)), Literal(2.0)), r, Literal(null, LongType))
      }
      assert(root(random) == ExprStructCase.IF)
    }
  }

  // Arrow Java ignores ArrowArray.offset on import, so the bridge has to zero a sliced boolean's
  // offset before handing it over, at the top level and inside a struct.
  // https://github.com/apache/datafusion-comet/issues/6288
  private def withSlicedGroups(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(0, 4000)
        .selectExpr(
          "id % 2000 AS k",
          "id % 2000 % 3 = 0 AS b",
          "CAST(id % 2000 AS STRING) AS s",
          "named_struct('x', IF(id % 5 = 0, NULL, id % 2000 % 3 = 0)) AS st")
        .write
        .parquet(dir.getCanonicalPath)
      // The hash aggregate slices its emitted groups into batch-size chunks, so with one
      // partition every output batch after the first is a slice.
      withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "100") {
        withParquetTable(dir.getCanonicalPath, "g")(f)
      }
    }
  }

  test("boolean ScalaUDF argument sliced by an aggregate keeps its values") {
    spark.udf.register("flip", (x: Boolean) => !x)
    withSlicedGroups {
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT k, flip(b) FROM (SELECT k, b, count(*) FROM g GROUP BY k, b)"))
      }
    }
  }

  test("typed Dataset.filter reads sliced booleans from a native aggregate") {
    // https://github.com/apache/datafusion-comet/issues/6424
    import testImplicits._
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withSlicedGroups {
        def filtered = spark
          .table("g")
          .groupBy("k", "b")
          .count()
          .as[(Long, Boolean, Long)]
          .filter(_._2)
          .toDF()

        assertCodegenRan {
          val (_, plan) = checkSparkAnswerAndOperator(filtered)
          val aggregates = plan.collect { case a: CometHashAggregateExec => a }
          assert(aggregates.exists(_.modes.contains(Partial)))
          assert(aggregates.exists(_.modes.contains(Final)))
          assert(plan.collect { case f: CometFilterExec => f }.nonEmpty)
          // Check every retained row, since the offset bug both adds and drops rows.
          checkAnswer(filtered, (0 until 2000 by 3).map(k => Row(k.toLong, true, 2L)))
        }
      }
    }
  }

  test("dispatched regexp_replace reads a sliced boolean with its own values") {
    withSlicedGroups {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("""SELECT k, regexp_replace(IF(b, s, 'zz'), '1', 'y')
            |FROM (SELECT k, b, s, count(*) FROM g GROUP BY k, b, s)""".stripMargin))
      }
    }
  }

  test("struct ScalaUDF input with a sliced boolean child keeps its values") {
    spark.udf.register("flip", (x: Boolean) => !x)
    withSlicedGroups {
      // `st.x` is dispatched along with the UDF, so the kernel's input is the whole struct.
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT k, flip(st.x) FROM (SELECT k, st, count(*) FROM g GROUP BY k, st)"))
      }
    }
  }

  test("multi-arg ScalaUDF over string + literal routes through dispatcher") {
    spark.udf.register(
      "prepend",
      (prefix: String, s: String) => if (s == null) null else prefix + s)
    withSubjects("one", "two", null) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT prepend('[', s) FROM t"))
      }
    }
  }

  test("ScalaUDF as a child of a native Spark expression") {
    // The ScalaUDF routes through the dispatcher as a sub-expression. The surrounding `length`
    // runs through Comet's native scalar function path. This exercises the cross-boundary
    // composition where a dispatcher-compiled kernel returns a UTF8String that a native Comet
    // expression then consumes.
    spark.udf.register("wrap", (s: String) => if (s == null) null else s"|$s|")
    withSubjects("abc", "def", null) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT length(wrap(s)) FROM t"))
      }
    }
  }

  test("composed ScalaUDFs outer(inner(s)) fuse into one kernel") {
    // Two user UDFs stacked, both operating on String. The dispatcher binds the whole tree and
    // Spark's codegen emits two `ctx.addReferenceObj` calls inside one generated method. Races
    // on the `ExpressionEncoder` serializers in `references` would show up here since each UDF
    // contributes its own stateful serializer. The `freshReferences` closure in `CompiledKernel`
    // is what keeps this correct across partitions.
    spark.udf.register("inner", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("outer", (s: String) => if (s == null) null else s"<$s>")
    withSubjects("abc", null, "xyz", "MiXeD") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT outer(inner(s)) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), StringType)
    }
  }

  test("ScalaUDFs of different types compose: isShort(len(s))") {
    // Exercises an input type transition: String -> Int -> Boolean. Two user UDFs with
    // different I/O type shapes in one tree, one Janino compile.
    spark.udf.register("len", (s: String) => if (s == null) -1 else s.length)
    spark.udf.register("isShort", (i: Int) => i < 5)
    withSubjects("ab", "abcdef", null, "hi") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT isShort(len(s)) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BooleanType)
    }
  }

  test("three-deep ScalaUDF composition lvl3(lvl2(lvl1(s)))") {
    // Three user UDFs stacked in one tree: String -> String -> String -> Int. The fused kernel
    // carries three `ctx.addReferenceObj` calls. `assertOneKernelForSubtree` asserts that the
    // whole chain collapses into a single compile rather than one per nesting level.
    // Null handling through composed UDFs is covered by the other composition tests above.
    spark.udf.register("lvl1", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lvl2", (s: String) => if (s == null) null else s.reverse)
    spark.udf.register("lvl3", (s: String) => if (s == null) -1 else s.length)
    withSubjects("abc", "hello world", "x") {
      assertOneKernelForSubtree {
        checkSparkAnswerAndOperator(sql("SELECT lvl3(lvl2(lvl1(s))) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("multi-column ScalaUDF composition join(upperU(c1), lowerU(c2))") {
    // One multi-arg user UDF consuming two other user UDFs, each on a different input column.
    // The bound tree has two BoundReferences, and the kernel is specialized on two VarCharVector
    // columns. `assertOneKernelForSubtree` asserts that the two-branch composition fuses into a
    // single kernel rather than one per branch or one per UDF.
    // Input rows intentionally exclude nulls (see note on the three-deep test above).
    spark.udf.register("upperU", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lowerU", (s: String) => if (s == null) null else s.toLowerCase)
    spark.udf.register(
      "joinU",
      (a: String, b: String) => if (a == null || b == null) null else s"$a-$b")
    withTwoStringCols(("Abc", "XYZ"), ("Foo", "bar"), ("baz", "Bar"), ("Hi", "Lo")) {
      assertOneKernelForSubtree {
        checkSparkAnswerAndOperator(sql("SELECT joinU(upperU(c1), lowerU(c2)) FROM t"))
      }
      assertKernelSignaturePresent(
        Seq(classOf[VarCharVector], classOf[VarCharVector]),
        StringType)
    }
  }

  /**
   * Per-primitive identity-UDF coverage. Each entry registers a `T => T` UDF over a parquet
   * column declared at `sqlType` and asserts the dispatcher compiled a kernel for the matching
   * `(vector class, output type)` pair. Parquet-backed (rather than `spark.range`-cast) tables
   * keep the column's Arrow vector class aligned with the UDF signature.
   */
  private def withTypedCol(sqlType: String, valueLiterals: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (c $sqlType) USING parquet")
      if (valueLiterals.nonEmpty) {
        val rows = valueLiterals.map(v => s"($v)").mkString(", ")
        sql(s"INSERT INTO t VALUES $rows")
      }
      f
    }
  }

  private case class IdentityUdfCase(
      label: String,
      sqlType: String,
      values: Seq[String],
      vec: Class[_ <: ValueVector],
      output: DataType,
      udfName: String,
      register: () => Unit)

  private val identityScalarCases: Seq[IdentityUdfCase] = Seq(
    IdentityUdfCase(
      "Boolean",
      "BOOLEAN",
      Seq("TRUE", "FALSE", "TRUE"),
      classOf[BitVector],
      BooleanType,
      "u_bool",
      () => spark.udf.register("u_bool", (b: Boolean) => !b)),
    IdentityUdfCase(
      "Byte",
      "TINYINT",
      Seq("CAST(1 AS TINYINT)", "CAST(2 AS TINYINT)", "CAST(100 AS TINYINT)"),
      classOf[TinyIntVector],
      ByteType,
      "u_byte",
      () => spark.udf.register("u_byte", (b: Byte) => (b + 1).toByte)),
    IdentityUdfCase(
      "Short",
      "SMALLINT",
      Seq("CAST(1 AS SMALLINT)", "CAST(2 AS SMALLINT)", "CAST(30000 AS SMALLINT)"),
      classOf[SmallIntVector],
      ShortType,
      "u_short",
      () => spark.udf.register("u_short", (s: Short) => (s + 1).toShort)),
    IdentityUdfCase(
      "Int",
      "INT",
      Seq("1", "2", "100"),
      classOf[IntVector],
      IntegerType,
      "u_int",
      () => spark.udf.register("u_int", (i: Int) => i * 2)),
    IdentityUdfCase(
      "Long",
      "BIGINT",
      Seq("1", "2", "100"),
      classOf[BigIntVector],
      LongType,
      "u_long",
      () => spark.udf.register("u_long", (l: Long) => l + 1L)),
    IdentityUdfCase(
      "Float",
      "FLOAT",
      Seq("CAST(1.5 AS FLOAT)", "CAST(2.5 AS FLOAT)"),
      classOf[Float4Vector],
      FloatType,
      "u_float",
      () => spark.udf.register("u_float", (f: Float) => f * 1.5f)),
    IdentityUdfCase(
      "Double",
      "DOUBLE",
      Seq("1.5", "2.5", "100.0"),
      classOf[Float8Vector],
      DoubleType,
      "u_double",
      () => spark.udf.register("u_double", (d: Double) => d / 2.0)),
    IdentityUdfCase(
      "Date",
      "DATE",
      Seq("DATE'2024-01-01'", "DATE'2024-06-15'", "DATE'1970-01-01'"),
      classOf[DateDayVector],
      DateType,
      "u_date",
      () =>
        spark.udf.register(
          "u_date",
          (d: java.sql.Date) =>
            if (d == null) null else new java.sql.Date(d.getTime + 86400000L))),
    IdentityUdfCase(
      "Timestamp",
      "TIMESTAMP",
      Seq("TIMESTAMP'2024-01-01 12:00:00'", "TIMESTAMP'2024-06-15 23:59:59'"),
      classOf[TimeStampMicroTZVector],
      TimestampType,
      "u_ts",
      () =>
        spark.udf.register(
          "u_ts",
          (t: java.sql.Timestamp) =>
            if (t == null) null else new java.sql.Timestamp(t.getTime + 1000L))),
    IdentityUdfCase(
      "TimestampNTZ",
      "TIMESTAMP_NTZ",
      Seq("TIMESTAMP_NTZ'2024-01-01 12:00:00'", "TIMESTAMP_NTZ'2024-06-15 23:59:59'"),
      classOf[TimeStampMicroVector],
      TimestampNTZType,
      "u_tsntz",
      () =>
        spark.udf.register(
          "u_tsntz",
          (ldt: java.time.LocalDateTime) => if (ldt == null) null else ldt.plusDays(1))))

  identityScalarCases.foreach { c =>
    test(s"identity ScalaUDF on ${c.label} routes through dispatcher") {
      c.register()
      withTypedCol(c.sqlType, c.values: _*) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql(s"SELECT ${c.udfName}(c) FROM t"))
        }
        assertKernelSignaturePresent(Seq(c.vec), c.output)
      }
    }
  }

  private val boxedPrimitiveTypes = Seq("bool", "byte", "short", "int", "long", "float", "double")

  /**
   * Boxed primitive UDFs for the #6706 tests, where the kernel drops Spark's encoders. Each
   * `<type>_str` function names the exact value it receives, null included, so a value converted
   * differently from Spark's encoder (a null read as 0, a -0.0 read as 0.0) fails the comparison
   * with Spark. Each `<type>_id` function returns the boxed value it receives, which covers the
   * result side. `long_tag` and `opt_long` pair a boxed parameter with ones that keep their
   * encoders.
   */
  private def registerBoxedPrimitiveUdfs(): Unit = {
    spark.udf.register("bool_str", (x: java.lang.Boolean) => String.valueOf(x))
    spark.udf.register("byte_str", (x: java.lang.Byte) => String.valueOf(x))
    spark.udf.register("short_str", (x: java.lang.Short) => String.valueOf(x))
    spark.udf.register("int_str", (x: java.lang.Integer) => String.valueOf(x))
    spark.udf.register("long_str", (x: java.lang.Long) => String.valueOf(x))
    // Raw bits, so the sign of a zero or a NaN payload would show.
    spark.udf.register(
      "float_str",
      (x: java.lang.Float) =>
        if (x == null) "null" else java.lang.Float.floatToRawIntBits(x).toString)
    spark.udf.register(
      "double_str",
      (x: java.lang.Double) =>
        if (x == null) "null" else java.lang.Double.doubleToRawLongBits(x).toString)
    spark.udf.register("bool_id", (x: java.lang.Boolean) => x)
    spark.udf.register("byte_id", (x: java.lang.Byte) => x)
    spark.udf.register("short_id", (x: java.lang.Short) => x)
    spark.udf.register("int_id", (x: java.lang.Integer) => x)
    spark.udf.register("long_id", (x: java.lang.Long) => x)
    spark.udf.register("float_id", (x: java.lang.Float) => x)
    spark.udf.register("double_id", (x: java.lang.Double) => x)
    spark.udf.register("long_tag", (x: java.lang.Long, s: String) => s"$x:$s")
    spark.udf.register("opt_long", (x: Option[Long]) => x.map(_ + 1).getOrElse(-1L))
  }

  private def withBoxedPrimitiveTable(f: => Unit): Unit = {
    withTable("t") {
      sql(
        "CREATE TABLE t (c_bool BOOLEAN, c_byte TINYINT, c_short SMALLINT, c_int INT, " +
          "c_long BIGINT, c_float FLOAT, c_double DOUBLE, s STRING) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(true, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648, " +
          "-9223372036854775808, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE), 'a'), " +
          "(false, CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), 2147483647, " +
          "9223372036854775807, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), NULL), " +
          "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c'), " +
          "(true, CAST(0 AS TINYINT), CAST(1000 AS SMALLINT), 1000, 1000, " +
          "CAST('Infinity' AS FLOAT), CAST(0.0 AS DOUBLE), 'd'), " +
          "(false, CAST(-1 AS TINYINT), CAST(-1000 AS SMALLINT), -1000, -1000, " +
          "CAST(1.5 AS FLOAT), CAST('-Infinity' AS DOUBLE), NULL)")
      f
    }
  }

  test("the kernel drops only boxed primitive encoders from a ScalaUDF (#6706)") {
    registerBoxedPrimitiveUdfs()
    spark.udf.register("str_id", (x: String) => x)
    spark.udf.register("long_in_prim_out", (x: java.lang.Long) => if (x == null) -1L else x + 1L)
    spark.udf.register("prim_add_one", (x: Long) => x + 1)
    withBoxedPrimitiveTable {
      def boundUdf(call: String): ScalaUDF = {
        val udf = sql(s"SELECT $call FROM t").queryExecution.optimizedPlan.expressions
          .flatMap(_.collect { case u: ScalaUDF => u })
          .head
        val attrs = udf.collect { case a: AttributeReference => a }.distinct
        BindReferences.bindReference(udf, AttributeSeq(attrs))
      }
      // Whether the kernel drops the encoder of each parameter and then of the result.
      def dropped(call: String): Seq[Boolean] = {
        val udf = boundUdf(call)
        val kernelUdf =
          CometBatchKernelCodegen.withoutBoxedPrimitiveEncoders(udf).asInstanceOf[ScalaUDF]
        (udf.inputEncoders :+ udf.outputEncoder)
          .zip(kernelUdf.inputEncoders :+ kernelUdf.outputEncoder)
          .map { case (before, after) => before.isDefined && after.isEmpty }
      }
      boxedPrimitiveTypes.foreach { t =>
        assert(dropped(s"${t}_id(c_$t)") === Seq(true, true), t)
      }
      assert(dropped("long_in_prim_out(c_long)") === Seq(true, false))
      assert(dropped("long_tag(c_long, s)") === Seq(true, false, false))
      assert(dropped("prim_add_one(c_long)") === Seq(false, false))
      assert(dropped("opt_long(c_long)") === Seq(false, false))
      assert(dropped("str_id(s)") === Seq(false, false))

      // Spark's encoder path calls the parameter's converter even for a null input. Without the
      // encoder, `ScalaUDF` tests for null itself, which shows the kernel compiles the rewrite.
      def kernelSource(call: String, vectorClass: Class[_ <: ValueVector]): String =
        CometBatchKernelCodegen
          .generateSource(
            boundUdf(call),
            IndexedSeq(ArrowColumnSpec(vectorClass, nullable = true)))
          .body
      assert(kernelSource("str_id(s)", classOf[VarCharVector]).contains(".apply(null)"))
      assert(!kernelSource("long_id(c_long)", classOf[BigIntVector]).contains(".apply(null)"))
    }
  }

  test("boxed primitive UDF parameters and results match Spark (#6706)") {
    registerBoxedPrimitiveUdfs()
    withBoxedPrimitiveTable {
      val calls = boxedPrimitiveTypes.flatMap(t => Seq(s"${t}_str(c_$t)", s"${t}_id(c_$t)"))
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql(s"SELECT ${calls.mkString(", ")} FROM t"))
      }
    }
  }

  test("boxed primitive UDF parameters next to other parameters match Spark (#6706)") {
    registerBoxedPrimitiveUdfs()
    // Spark guards the call with a null check for the primitive parameter, not the boxed one.
    spark.udf.register("long_or", (x: java.lang.Long, y: Long) => if (x == null) y else x + y)
    withBoxedPrimitiveTable {
      val calls = Seq(
        "long_tag(c_long, s)",
        "opt_long(c_long)",
        "long_or(c_long, c_int)",
        // Spark casts the int column to the `java.lang.Long` parameter's type.
        "long_str(c_int)",
        "long_id(long_id(c_long))",
        // Inside a lambda, Spark evaluates the UDF through `eval` rather than generated code,
        // with the same converters.
        "transform(array(c_int, NULL), x -> int_str(x))")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql(s"SELECT ${calls.mkString(", ")} FROM t"))
      }
    }
  }

  test("ScalaUDF returning a different type than its input") {
    // String -> Int output transition. Identity-loop above keeps input == output. This asserts
    // the writer can switch types per the UDF's declared return.
    spark.udf.register("codePoint", (s: String) => if (s == null) 0 else s.codePointAt(0))
    withSubjects("abc", "A", null, "!") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT codePoint(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("ScalaUDF returning BinaryType") {
    // Binary output writer path, exercised here by a user UDF for the first time. Before this
    // the writer only had direct-compile unit tests.
    spark.udf.register("bytes", (s: String) => if (s == null) null else s.getBytes("UTF-8"))
    withSubjects("abc", null, "hello") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT bytes(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BinaryType)
    }
  }

  test("ScalaUDF on BinaryType") {
    // Binary input getter path: VarBinaryVector with byte[] reads via Spark's `getBinary` getter.
    spark.udf.register("blen", (b: Array[Byte]) => if (b == null) -1 else b.length)
    withTable("t") {
      sql("CREATE TABLE t (b BINARY) USING parquet")
      sql("INSERT INTO t VALUES (CAST('abc' AS BINARY)), (CAST('hello' AS BINARY)), (NULL)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT blen(b) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarBinaryVector]), IntegerType)
    }
  }

  test("ScalaUDF returning ArrayType(StringType)") {
    // First use of the ArrayType output path end-to-end. The UDF returns a `Seq[String]`,
    // which Spark encodes as `ArrayType(StringType, containsNull = true)`. The dispatcher's
    // canHandle accepts it (ArrayType is supported when its element type is supported),
    // allocateOutput builds a ListVector with an inner VarCharVector, and emitWrite recurses
    // into the StringType case for the per-element UTF8 on-heap shortcut. End-to-end answer
    // matches Spark.
    spark.udf.register(
      "splitComma",
      (s: String) => if (s == null) null else s.split(",", -1).toSeq)
    withSubjects("a,b,c", "x", null, "", "one,,three") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT splitComma(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning ArrayType(IntegerType)") {
    // Exercises ArrayType output with a primitive element. emitWrite's ArrayType case
    // recurses into the IntegerType case for the inner write. No byte[] allocation involved.
    spark.udf.register(
      "asLengths",
      (s: String) => if (s == null) null else s.split(",").map(_.length).toSeq)
    withSubjects("a,bb,ccc", null, "xyzzy") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT asLengths(s) FROM t"))
      }
    }
  }

  test("zero-column ScalaUDF produces one row per input row") {
    // Non-deterministic (so Spark doesn't constant-fold) with a deterministic body (so
    // Spark-vs-Comet comparison stays honest). The expression has no `AttributeReference`,
    // so the serde produces an empty data-arg list and the dispatcher has no data column to
    // read the batch size from. Guards the `numRows` path through the JNI bridge.
    import org.apache.spark.sql.functions.udf
    val alwaysHello = udf(() => "hello").asNondeterministic()
    spark.udf.register("helloU", alwaysHello)
    withSubjects("a", "b", null, "c") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT helloU() FROM t"))
      }
    }
  }

  /**
   * Decimal end-to-end: the dispatcher's `getDecimal` specializes per `DecimalType.precision` at
   * source-generation time. Two representative cases here; `CometCodegenFuzzSuite` sweeps every
   * shape across the boundary at varying null densities.
   */
  private def withDecimalTable(decimalType: String, values: Seq[String])(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (d $decimalType) USING parquet")
      val rows = values.map(v => if (v == null) "(NULL)" else s"($v)").mkString(", ")
      if (values.nonEmpty) sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("ScalaUDF over Decimal(18, 9) routes through the unscaled-long fast path") {
    // Boundary precision (18 == `MAX_LONG_DIGITS`) with a non-zero scale exercises the fractional
    // branch of the fast-path encoding.
    spark.udf.register("decIdShort", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(18, 9)",
      Seq("0.000000000", "1.123456789", "-1.123456789", "999999999.999999999", null)) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT decIdShort(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(38, 10) routes through the BigDecimal slow path") {
    // Pin the return type to Decimal(38, 10). TypeTag inference for `BigDecimal` would default to
    // Decimal(38, 18), and under Spark 4 ANSI the encoder's CheckOverflow throws on the 28-digit
    // boundary value below when rescaling 10 -> 18.
    spark.udf.register(
      "decIdLong",
      new UDF1[java.math.BigDecimal, java.math.BigDecimal] {
        override def call(d: java.math.BigDecimal): java.math.BigDecimal = d
      },
      DecimalType(38, 10))
    withDecimalTable(
      "DECIMAL(38, 10)",
      Seq(
        "0.0000000000",
        "1.1234567890",
        "-1.1234567890",
        "9999999999999999999999999999.0000000000",
        null)) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT decIdLong(d) FROM t"))
      }
    }
  }

  test("ScalaUDF sees TaskContext.partitionId() per partition") {
    // Direct probe: register a ScalaUDF that reads TaskContext.partitionId() and returns it.
    // Spark's own task thread has TaskContext set, so each partition's rows carry that
    // partition's index. For the dispatcher to match Spark, the invocation thread must see a
    // live TaskContext. With the `createPlan`-time TaskContext capture + bridge-side
    // `TaskContext.setTaskContext` install (see `CometUdfBridge.evaluate` and
    // `CometTaskContextShim`), Tokio workers see the propagated TaskContext and the UDF
    // returns the real partitionId. Without that propagation, `TaskContext.get()` returns null
    // on the Tokio thread and the sentinel (-1) leaks through, diverging from Spark.
    spark.udf.register(
      "pid",
      (_: Long) => {
        val tc = TaskContext.get()
        if (tc != null) tc.partitionId() else -1
      })
    val df = spark
      .range(0, 1024, 1, numPartitions = 4)
      .selectExpr("id", "pid(id) as p")
    checkSparkAnswerAndOperator(df)
  }

  test("ScalaUDF sees TaskContext from fully-native parquet plan") {
    // The `spark.range`-based test above runs through `CometSparkRowToColumnar`, which executes
    // on a Spark task thread where TaskContext is live even without explicit propagation. The
    // fully-native path through `CometNativeScan` runs the JVM UDF bridge on a Tokio worker
    // thread where TaskContext.get() would otherwise be null. This test forces that path by
    // sourcing from a Parquet table written as multiple files (so the native read produces
    // multiple partitions) and asserting the UDF still sees the per-partition TaskContext via
    // the `createPlan`-time capture + bridge-side install.
    spark.udf.register(
      "pidP",
      (_: Int) => {
        val tc = TaskContext.get()
        if (tc != null) tc.partitionId() else -1
      })
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      // Multiple INSERT statements -> multiple parquet files -> multiple read splits ->
      // multiple partitions.
      sql("INSERT INTO t VALUES (1), (2), (3), (4)")
      sql("INSERT INTO t VALUES (5), (6), (7), (8)")
      sql("INSERT INTO t VALUES (9), (10), (11), (12)")
      sql("INSERT INTO t VALUES (13), (14), (15), (16)")
      checkSparkAnswerAndOperator(sql("SELECT x, pidP(x) AS p FROM t"))
    }
  }

  test("Rand seeded per partition across a multi-partition table") {
    // Rand.doGenCode registers an XORShiftRandom via ctx.addMutableState and seeds it via
    // ctx.addPartitionInitializationStatement. That init statement runs inside our kernel's
    // `init(int partitionIndex)`, called once per kernel allocation. Spark seeds
    // `XORShiftRandom(seed + partitionIndex)` per partition, so different partitions produce
    // different sequences for the same seed. Matching Spark across partitions requires the
    // kernel to see the real partition index, which the native plan passes through the bridge.
    // Composing with a ScalaUDF (identity on Double here) forces the tree through codegen
    // dispatch so the Rand evaluation runs inside our kernel's init rather than via Spark's
    // normal codegen.
    spark.udf.register("dblId", (d: Double) => d)
    val df = spark
      .range(0, 1024, 1, numPartitions = 4)
      .selectExpr("id", "dblId(rand(42)) as r")
    checkSparkAnswerAndOperator(df)
  }

  test("scalar subquery inside a dispatched expression falls back to Spark") {
    assume(isSpark40Plus, "collations are Spark 4.0+")
    withTable("t") {
      sql("CREATE TABLE t (c1 STRING) USING parquet")
      sql("INSERT INTO t VALUES ('a')")
      checkSparkAnswer(sql("SELECT COUNT(CAST((SELECT c1 FROM t) AS STRING COLLATE UTF8_LCASE))"))
      checkSparkAnswer(sql("SELECT CAST((SELECT c1 FROM t) AS STRING COLLATE UTF8_LCASE) = 'A'"))
      checkSparkAnswer(
        sql("SELECT COUNT(CAST((SELECT c1 FROM t) AS STRING COLLATE UTF8_LCASE)) FROM t"))
    }
  }

  test("dispatched kernels see the index of the partition the native plan computes") {
    // Under a union, a coalesce and a cartesian product the native plan computes a partition
    // whose index differs from `TaskContext.partitionId()`, and the coalesce runs two plans in
    // one task. See https://github.com/apache/datafusion-comet/issues/6570.
    val df = spark
      .range(0, 8, 1, numPartitions = 2)
      .selectExpr("id", "map(1, spark_partition_id()) AS m", "round(rand(42), 6) AS r")
    Seq(df.union(df), df.coalesce(1)).foreach { q =>
      assertCodegenRan(checkSparkAnswerAndOperator(q))
    }
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // `CartesianProductExec` stays in Spark, over native children.
      assertCodegenRan(checkSparkAnswer(df.crossJoin(spark.range(0, 4, 1, 2))))
    }
    if (isSpark40Plus) {
      // Spark 4 lowers `make_valid_utf8` to an `Invoke` whose `deterministic` skips its target
      // object, so the partition-seeded input sits under a root that reports deterministic.
      val invoke = spark
        .range(0, 8, 1, numPartitions = 2)
        .selectExpr(
          "id",
          "make_valid_utf8(cast(spark_partition_id() AS STRING)) AS p",
          "make_valid_utf8(cast(round(rand(42), 6) AS STRING)) AS r")
      assertCodegenRan(checkSparkAnswerAndOperator(invoke.coalesce(1)))
    }
  }

  test("plans of a coalesce share deterministic kernels and drop their own when they close") {
    // The coalesce runs four native plans in one task. The deterministic `round` compiles once
    // for all of them and the nondeterministic one once per plan. Each plan's kernel is dropped
    // when the plan closes, so once the rows are read only the shared one is left.
    val df = spark
      .range(0, 16, 1, numPartitions = 4)
      .selectExpr("round(id / 3, 2) AS d", "round(rand(42), 6) AS r")
      .coalesce(1)
    CometScalaUDFCodegen.resetStats()
    val cached = df.queryExecution.toRdd
      .mapPartitions { rows =>
        rows.foreach(_ => ())
        val dispatcher = CometUdfBridge.instanceFor(
          TaskContext.get().taskAttemptId(),
          classOf[CometScalaUDFCodegen].getName)
        Iterator(Option(dispatcher).map(_.asInstanceOf[CometScalaUDFCodegen].cachedPlanIds))
      }
      .collect()
      .toSeq
    assert(
      cached == Seq(Some(List(CometScalaUDFCodegen.NoPlan))),
      s"expected only the shared kernel to outlive the plans, got plan ids $cached")
    val stats = CometScalaUDFCodegen.stats()
    assert(stats.compileCount == 5, s"expected 1 shared and 4 per-plan compiles, got $stats")
  }

  test("ScalaUDF composed with reused scalar subquery across projection and filter") {
    // The same scalar subquery appears in two sites: the projection (which the dispatcher
    // compiles into a fused kernel) and the filter (a separate operator). Each site holds its
    // own `ScalarSubquery` expression instance with its own `@volatile result` field. Each
    // surrounding operator's inherited `SparkPlan.waitForSubqueries` populates its instance's
    // `result` before the dispatcher's bridge serializes the expression. The populated value
    // travels through closure serialization into the cache key's bytes, so different subquery
    // values compile distinct kernels. Exercises the full subquery-correctness invariant
    // documented on `CometBatchKernelCodegen.canHandle`.
    spark.udf.register("addOne", (i: Int) => i + 1)
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (2), (4)")
      checkSparkAnswerAndOperator(
        sql("SELECT addOne(x) + (SELECT max(v) FROM t2) AS r " +
          "FROM t WHERE addOne(x) < (SELECT max(v) FROM t2) * 2"))
    }
  }

  /**
   * ArrayType input. The dispatcher emits a nested `InputArray_col0` final class per array-typed
   * input column; Spark's generated `getArray(ord)` resolves to our kernel's switch which returns
   * the pre-allocated instance after resetting its start/length against the list's offsets.
   * Element reads go through the typed child-vector field with no `ArrayData` copy or boxing.
   *
   * Each smoke test exercises the same serde/transport path at a different element type so the
   * nested getter emitter's scalar-element cases are each covered: `StringType` (zero-copy
   * `UTF8String.fromAddress`), `IntegerType` (primitive direct), and `DecimalType(p <= 18)`
   * (decimal128 fast path).
   */
  private def withArrayTable(colType: String, insertRows: String)(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (a $colType) USING parquet")
      sql(s"INSERT INTO t VALUES $insertRows")
      f
    }
  }

  test("ScalaUDF taking Seq[String] reads element by element") {
    spark.udf.register(
      "headOrNull",
      (arr: Seq[String]) => if (arr == null || arr.isEmpty) null else arr.head)
    withArrayTable(
      "ARRAY<STRING>",
      "(array('a', 'b', 'c')), (array('x')), (null), (array()), (array('alone'))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT headOrNull(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[String] iterating all elements") {
    spark.udf.register(
      "concatArr",
      (arr: Seq[String]) => if (arr == null) null else arr.mkString("|"))
    withArrayTable(
      "ARRAY<STRING>",
      "(array('one', 'two', 'three')), (array('solo')), (null), (array())") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT concatArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[Int] reads primitive elements") {
    spark.udf.register("sumArr", (arr: Seq[Int]) => if (arr == null) -1 else arr.sum)
    withArrayTable(
      "ARRAY<INT>",
      "(array(1, 2, 3)), (array(-5, 5)), (array()), (null), (array(42))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sumArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[BigDecimal] hits short-precision decimal fast path") {
    // DecimalType(10, 2) is well inside p <= 18, so the nested-array `getDecimal` emits the
    // unscaled-long fast path (see `emitNestedArrayElementGetter`). A `BigDecimal` UDF argument
    // forces Spark's encoder to call `getDecimal(i, 10, 2)` on our nested ArrayData for each
    // element, which exercises that code path end to end.
    spark.udf.register(
      "sumDecArr",
      (arr: Seq[java.math.BigDecimal]) =>
        if (arr == null) null
        else {
          var acc = java.math.BigDecimal.ZERO
          arr.foreach(v => if (v != null) acc = acc.add(v))
          acc
        })
    withArrayTable(
      "ARRAY<DECIMAL(10, 2)>",
      "(array(1.23, 4.56)), (array(-9.99)), (null), (array())") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sumDecArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF composes with struct-field access reading Struct<String, Int>.age") {
    // Keeps the UDF arg scalar (Int) but puts a `GetStructField` under it so the codegen
    // dispatcher compiles the struct-input read path (`row.getStruct(0, 2).getInt(1)`).
    spark.udf.register("doubleInt", (i: Int) => i * 2)
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, age: INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'alice', 'age', 30)), " +
          "(named_struct('name', 'bob', 'age', 42)), " +
          "(null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT doubleInt(s.age) FROM t"))
      }
    }
  }

  test("ScalaUDF taking full Struct<String, Int> value (case class arg)") {
    // Case-class UDF arguments: test data must not include null top-level rows.
    // `ScalaUDF.scalaConverter` applies Spark's `ExpressionEncoder.Deserializer` on every row
    // to materialize the case-class instance. The generated deserializer has a
    // `newInstance(NameAgePair)` step that throws `EXPRESSION_DECODING_FAILED` on a null input,
    // independent of the dispatcher. Case-class UDF tests omit null top-level rows. Other
    // tests with plain `Seq` / `Map` args can include nulls because the deserializer hands null
    // to the UDF body which handles it.
    spark.udf.register("fmtPair", (r: NameAgePair) => s"${r.name}:${r.age}")
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, age: INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'alice', 'age', 30)), " +
          "(named_struct('name', 'bob', 'age', 42))")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT fmtPair(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning Struct<String, Int> (case class output)") {
    spark.udf.register("makePair", (i: Int) => NameAgePair(s"n$i", i))
    withTypedCol("INT", "1", "2", "3") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT makePair(c) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Map<String, Int>") {
    spark.udf.register("sumMap", (m: Map[String, Int]) => if (m == null) -1 else m.values.sum)
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, INT>) USING parquet")
      sql("INSERT INTO t VALUES (map('a', 1, 'b', 2)), (map()), (null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sumMap(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<Int, Int> (primitive key and value)") {
    // Map with non-string keys: exercises the primitive-key element getter on the input side
    // and the corresponding writer on the output side. Spark's encoder for `Map[Int, Int]` calls
    // `getInt(0)` / `getInt(1)` on the entries struct, hitting the kernel's typed scalar getter
    // for each side rather than the UTF8 path.
    spark.udf.register(
      "incValues",
      (m: Map[Int, Int]) => if (m == null) null else m.map { case (k, v) => k -> (v + 1) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<INT, INT>) USING parquet")
      sql("INSERT INTO t VALUES (map(1, 10, 2, 20)), (map()), (null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT incValues(m) FROM t"))
      }
    }
  }

  test("ScalaUDF returning Map<String, Int>") {
    spark.udf.register(
      "singletonMap",
      (s: String, i: Int) => if (s == null) null else Map(s -> i))
    withTable("t") {
      sql("CREATE TABLE t (s STRING, i INT) USING parquet")
      sql("INSERT INTO t VALUES ('a', 1), ('b', 2), (null, 3)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT singletonMap(s, i) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Map<String, Seq<Int>> exercises nested composition") {
    spark.udf.register(
      "totalLens",
      (m: Map[String, Seq[Int]]) => if (m == null) -1 else m.values.flatten.sum)
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', array(1, 2, 3), 'b', array(10))), " +
          "(map()), " +
          "(null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT totalLens(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Array<Array<Int>> (nested array input + output)") {
    // Exercises nested-array input reads and nested-list output writes in one call: the inner
    // `InputArray_col0_e` class on the input side and the recursive emitWrite on the output.
    spark.udf.register(
      "reverseRows",
      (arr: Seq[Seq[Int]]) => if (arr == null) null else arr.map(_.reverse))
    withTable("t") {
      sql("CREATE TABLE t (a ARRAY<ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(array(array(1, 2, 3), array(4, 5))), " +
          "(array(array())), " +
          "(null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT reverseRows(a) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Struct<name, items: Array<Int>>") {
    // Struct with a complex field on both sides: input reads go through InputStruct_col0 +
    // InputArray_col0_f1, output writes through StructVector + ListVector.
    // Null top-level rows omitted - case-class arg. See the note on `fmtPair` above.
    spark.udf.register(
      "growItems",
      (r: NameItems) =>
        if (r == null) null else NameItems(r.name, if (r.items == null) null else r.items :+ 0))
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, items: ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'a', 'items', array(1, 2))), " +
          "(named_struct('name', 'b', 'items', array()))")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT growItems(s) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<String, Array<Int>> (nested value both sides)") {
    // Map input read goes through InputMap_col0 + InputArray_col0_v (the complex-value side);
    // output write emits MapVector + entries Struct + per-value ListVector inside the map's
    // entries struct.
    spark.udf.register(
      "sortValues",
      (m: Map[String, Seq[Int]]) =>
        if (m == null) null
        else m.map { case (k, v) => k -> (if (v == null) null else v.sorted) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', array(3, 1, 2), 'b', array(10))), " +
          "(map()), " +
          "(null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sortValues(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<String, Struct<x: Int, y: String>>") {
    // Struct value inside a map, both sides. Null top-level rows omitted - the map value is a
    // case class. See the note on `fmtPair` above.
    spark.udf.register(
      "tagValues",
      (m: Map[String, XyPair]) =>
        if (m == null) null
        else
          m.map { case (k, v) => k -> (if (v == null) null else XyPair(v.x + 1, s"<${v.y}>")) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', named_struct('x', 1, 'y', 'one'))), " +
          "(map())")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT tagValues(m) FROM t"))
      }
    }
  }

  private def kernelMapIntString(expr: Expression): Map[Int, String] =
    runKernel(expr, 1) { v =>
      val map = v.getMap(0)
      val keys = map.keyArray()
      val values = map.valueArray()
      (0 until map.numElements())
        .map(i => keys.getInt(i) -> values.getUTF8String(i).toString)
        .toMap
    }

  test("constant-folded map_concat output round-trips every key through the kernel (#4539)") {
    // map_concat(map(1,'a',2,'b'), map(3,'c')) is all-literal, so Spark's optimizer constant-folds
    // it to a Literal(MapType) holding an ArrayBasedMapData. The MapType output writer must marshal
    // every entry into the Arrow MapVector; the reported bug corrupts the last key (3 -> 0).
    def s(str: String): Literal = Literal(UTF8String.fromString(str), StringType)
    val map1 =
      CreateMap(Seq(Literal(1), s("a"), Literal(2), s("b")), useStringTypeWhenEmpty = false)
    val map2 = CreateMap(Seq(Literal(3), s("c")), useStringTypeWhenEmpty = false)
    val folded =
      Literal.create(MapConcat(Seq(map1, map2)).eval(null), MapType(IntegerType, StringType))
    assert(kernelMapIntString(folded) === Map(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  test("constant-folded array output writes every element past the pre-sized child (#4539)") {
    // A single-row array with far more elements than the list child's numRows-derived initial
    // capacity. The element child is written at a cumulative index, so a bare `set` overflows the
    // pre-sized buffer once the row's element count exceeds it; `setSafe` grows it. Sibling of the
    // map_concat case for ArrayType.
    val n = 16
    val elems = (0 until n).map(i => Literal(i * 10, IntegerType))
    val folded =
      Literal.create(CreateArray(elems).eval(null), ArrayType(IntegerType, containsNull = false))

    val got = runKernel(folded, 1) { v =>
      val arr = v.getArray(0)
      (0 until arr.numElements()).map(arr.getInt)
    }
    assert(got === (0 until n).map(_ * 10))
  }

  test(
    "constant-folded Array<Struct<Int, String>> writes struct fields past the pre-sized child " +
      "(#4539)") {
    // The struct sits inside an array, so its fields inherit the array's cumulative index. The
    // fixed-width Int field would overflow with a bare `set`; propagating `nested` into the struct
    // branch makes it `setSafe`. Guards the struct-nested-in-collection path.
    val n = 16
    def structAt(i: Int): Expression =
      CreateNamedStruct(
        Seq(
          Literal("a"),
          Literal(i, IntegerType),
          Literal("b"),
          Literal(UTF8String.fromString(s"v$i"), StringType)))
    val structType = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", StringType, nullable = false)
    val folded = Literal.create(
      CreateArray((0 until n).map(structAt)).eval(null),
      ArrayType(structType, containsNull = false))

    val got = runKernel(folded, 1) { v =>
      val arr = v.getArray(0)
      (0 until arr.numElements()).map { i =>
        val r = arr.getStruct(i, 2)
        r.getInt(0) -> r.getUTF8String(1).toString
      }
    }
    assert(got === (0 until n).map(i => i -> s"v$i"))
  }

  test("array_distinct on Array<Struct<Int, String>> retains element identity across hash set") {
    // Fuzz signal: cardinality(array_distinct(arr_of_struct)) returns 1 where Spark returns 2.
    // Hypothesis: the kernel's InputStruct wrapper backing array_distinct's element reads is
    // reused without resetting per-element state, so every hashed element looks identical and
    // distinct collapses the array to a single entry.
    spark.udf.register("idIntDistinct", (i: Int) => i)
    withTable("t") {
      sql("CREATE TABLE t (s ARRAY<STRUCT<a: INT, b: STRING>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 1, 'b', 'x'))), " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))), " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'), " +
          "named_struct('a', 1, 'b', 'x')))")
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT idIntDistinct(cardinality(array_distinct(s))) FROM t"))
      }
    }
  }

  test("array_max(flatten(arr)) on Array<Array<Binary>> with mixed null inner arrays") {
    // Fuzz signal: array_max(flatten(arr)) returns empty byte arrays where Spark returns the
    // actual max binary, with the empties sorting to the front of the output. Pattern points at
    // cross-batch state pollution. Generate 100 rows of varied outer/inner shape, longer
    // binaries, mixed nulls. Force multiple batches with a small batch size.
    spark.udf.register("idBinFlat", (b: Array[Byte]) => b)
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "16") {
      withTable("t") {
        sql("CREATE TABLE t (a ARRAY<ARRAY<BINARY>>) USING parquet")
        val rows = (0 until 100).map { i =>
          if (i % 11 == 0) {
            "(NULL)"
          } else {
            val outerSize = (i % 5) + 1
            val inners = (0 until outerSize).map { j =>
              val pick = (i * 7 + j) % 13
              if (pick == 0) "array()"
              else if (pick == 1) "NULL"
              else {
                val innerSize = ((i + j) % 4) + 1
                val bytes = (0 until innerSize).map { k =>
                  val len = ((i + j + k) % 8) + 1
                  val hex = (0 until len)
                    .map(b => f"${(i * 13 + j * 17 + k * 5 + b) & 0xff}%02x")
                    .mkString
                  s"X'$hex'"
                }
                "array(" + bytes.mkString(", ") + ")"
              }
            }
            s"(array(${inners.mkString(", ")}))"
          }
        }
        sql(s"INSERT INTO t VALUES ${rows.mkString(", ")}")
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql("SELECT idBinFlat(array_max(flatten(a))) FROM t"))
        }
      }
    }
  }

  /**
   * Regressions for nested reference-typed getter null handling. Spark's
   * `CodeGenerator.setArrayElement` only emits an `isNullAt` check before `array.update(i,
   * getX(j))` for Java primitives. For reference-typed elements (Binary, String, Decimal, Struct,
   * Array, Map) it relies on the source's `getX` to return `null` itself, matching
   * `ColumnarArray.getBinary`. Without that contract, inner nulls become empty bytes / empty
   * strings / garbage decimals / non-null shells in the flattened output.
   */

  test("array_max(flatten(arr)) on Array<Array<Binary>> with null inner Binary returns null") {
    spark.udf.register("idBin", (b: Array[Byte]) => b)
    withArrayTable(
      "ARRAY<ARRAY<BINARY>>",
      "(array(array(NULL))), " +
        "(array(array(NULL, NULL))), " +
        "(array(array(), array(NULL)))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idBin(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test("array_max(flatten(arr)) on Array<Array<String>> with null inner String returns null") {
    spark.udf.register("idStr", (s: String) => s)
    withArrayTable(
      "ARRAY<ARRAY<STRING>>",
      "(array(array(NULL))), " +
        "(array(array(NULL, NULL))), " +
        "(array(array(), array(NULL)))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idStr(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test(
    "array_max(flatten(arr)) on Array<Array<DECIMAL(10,2)>> with null inner Decimal " +
      "(short-precision fast path)") {
    spark.udf.register("idDec10", (d: java.math.BigDecimal) => d)
    withArrayTable(
      "ARRAY<ARRAY<DECIMAL(10, 2)>>",
      "(array(array(CAST(NULL AS DECIMAL(10, 2))))), " +
        "(array(array(" +
        "CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS DECIMAL(10, 2))))), " +
        "(array(array(), array(CAST(NULL AS DECIMAL(10, 2)))))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idDec10(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test(
    "array_max(flatten(arr)) on Array<Array<DECIMAL(30,2)>> with null inner Decimal " +
      "(long-precision slow path)") {
    spark.udf.register("idDec30", (d: java.math.BigDecimal) => d)
    withArrayTable(
      "ARRAY<ARRAY<DECIMAL(30, 2)>>",
      "(array(array(CAST(NULL AS DECIMAL(30, 2))))), " +
        "(array(array(" +
        "CAST(NULL AS DECIMAL(30, 2)), CAST(NULL AS DECIMAL(30, 2))))), " +
        "(array(array(), array(CAST(NULL AS DECIMAL(30, 2)))))") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idDec30(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test("multi-input NullIntolerant tree does not swallow an ANSI error (#5218)") {
    // `add_months` is NullIntolerant and a plain BinaryExpression, so Spark's `nullSafeCodeGen`
    // emits the LEFT child's code unconditionally before testing the right child's null. On the
    // ('notadate', NULL) row Spark therefore evaluates the cast and raises CAST_INVALID_INPUT.
    // The dispatcher used to short-circuit on the union of input ordinals, see the null on `i`,
    // and return NULL -- silently losing the error. Note `pmod` / `div` are not witnesses here:
    // `DivModLike` deliberately evaluates its right child first, so Spark also returns NULL.
    withTable("t") {
      sql("CREATE TABLE t (s STRING, i INT) USING parquet")
      sql("INSERT INTO t VALUES ('notadate', NULL), ('2024-01-31', 1)")
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        CometScalaUDFCodegen.resetStats()
        val (sparkErr, cometErr) =
          checkSparkAnswerMaybeThrows(sql("SELECT add_months(CAST(s AS DATE), i) FROM t"))
        val stats = CometScalaUDFCodegen.stats()
        assert(
          stats.compileCount + stats.cacheHitCount >= 1,
          s"expected the codegen dispatcher to run for this query, got $stats")
        assert(
          sparkErr.isDefined,
          "expected Spark to raise on the invalid ANSI cast; the test row is no longer a witness")
        assert(
          cometErr.isDefined,
          "Comet returned a value where Spark raised: the null short-circuit swallowed the error")
        assert(
          cometErr.get.getMessage.contains("CAST_INVALID_INPUT"),
          s"expected the same CAST_INVALID_INPUT error Spark raises, got: ${cometErr.get}")
      }
    }
  }

  test("single-input NullIntolerant tree still short-circuits nulls") {
    // Guards against over-correcting #5218: the single-ordinal short-circuit is exact (Spark also
    // evaluates nothing when that one input is null) and must be preserved.
    withSubjects("abc", null, "xyz") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT upper(substring(s, 1, 2)) FROM t"))
      }
    }
  }

  test(
    "single-input short-circuit does not swallow an ANSI error from a foldable subtree (#5608)") {
    // The residual hole left by #5218: `canShortCircuitNulls` assumed a single input ordinal
    // leaves Spark nothing to evaluate ahead of that ordinal's null check. Not true when a
    // foldable subtree sits between the root and the ordinal. `ConstantFolding` refuses to fold
    // `1L DIV 0L` because evaluating it throws and it sits under an `If` branch, so the throwing
    // expression survives into the physical plan. `TernaryExpression.nullSafeCodeGen` then emits
    // `Substring`'s `pos` code -- the division -- before it tests `len`'s null, so Spark raises
    // DIVIDE_BY_ZERO on the (true, NULL) row while the short-circuit returned NULL.
    withTable("t") {
      sql("CREATE TABLE t (flag BOOLEAN, n INT) USING parquet")
      sql("INSERT INTO t VALUES (true, NULL), (false, NULL)")
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        CometScalaUDFCodegen.resetStats()
        val (sparkErr, cometErr) = checkSparkAnswerMaybeThrows(
          sql("SELECT IF(flag, upper(substring('abc', CAST(1L DIV 0L AS INT), n)), NULL) FROM t"))
        val stats = CometScalaUDFCodegen.stats()
        assert(
          stats.compileCount + stats.cacheHitCount >= 1,
          s"expected the codegen dispatcher to run for this query, got $stats")
        assert(
          sparkErr.isDefined,
          "expected Spark to raise DIVIDE_BY_ZERO; the test row is no longer a witness")
        assert(
          cometErr.isDefined,
          "Comet returned a value where Spark raised: the null short-circuit swallowed the error")
        assert(
          cometErr.get.getMessage.contains("DIVIDE_BY_ZERO"),
          s"expected the same DIVIDE_BY_ZERO error Spark raises, got: ${cometErr.get}")
      }
    }
  }

  test("multi-input leaf-only NullIntolerant tree short-circuits nulls correctly (#5218)") {
    // The leaf-only-children shape keeps the union-of-ordinals short-circuit, because the only
    // code Spark runs ahead of its own null checks is `BoundReference` reads. Exercises every
    // null combination across two ordinals to confirm the disjunction matches Spark's per-node
    // left-to-right null handling row for row. Wrapped in a UDF so the argument expression is
    // guaranteed to route through the dispatcher rather than Comet's native path.
    spark.udf.register("idInt", (i: Integer) => i)
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT) USING parquet")
      sql(
        "INSERT INTO t VALUES (7, 3), (NULL, 3), (7, NULL), (NULL, NULL), " +
          "(-7, 3), (7, -3), (0, 3)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idInt(pmod(a, b)) FROM t"))
      }
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idInt(a + b) FROM t"))
      }
    }
  }

  test("leaf-only short-circuit preserves ANSI remainder-by-zero behaviour (#5218)") {
    // The one shape where a `NullIntolerant` root's error check is not simply gated behind
    // "all inputs non-null": `Pmod.doGenCode` evaluates the divisor first and throws on a zero
    // divisor under ANSI. It still tests the dividend's null *before* that throw, so
    // `pmod(NULL, 0)` returns NULL in Spark and the union short-circuit stays exact. The
    // (NULL, 0) row pins the non-raising case, the (7, 0) row the raising one.
    //
    // Spark 4.1 introduced REMAINDER_BY_ZERO; older versions raise DIVIDE_BY_ZERO for `pmod`.
    // The error comes from Spark's own generated code running inside the kernel, so the class
    // tracks the Spark version under test.
    val expectedError = if (isSpark41Plus) "REMAINDER_BY_ZERO" else "DIVIDE_BY_ZERO"
    spark.udf.register("idInt", (i: Integer) => i)
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT) USING parquet")
      sql("INSERT INTO t VALUES (NULL, 0)")
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql("SELECT idInt(pmod(a, b)) FROM t"))
        }
      }
      sql("INSERT INTO t VALUES (7, 0)")
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        val (sparkErr, cometErr) =
          checkSparkAnswerMaybeThrows(sql("SELECT idInt(pmod(a, b)) FROM t"))
        assert(
          sparkErr.isDefined,
          "expected Spark to raise on pmod by zero under ANSI; the test row is no longer a witness")
        assert(
          cometErr.isDefined,
          "Comet returned a value where Spark raised: the null short-circuit swallowed the error")
        assert(
          cometErr.get.getMessage.contains(expectedError),
          s"expected the same $expectedError error Spark raises, got: ${cometErr.get}")
        assert(
          sparkErr.get.getMessage.contains(expectedError),
          s"expected Spark to raise $expectedError, got: ${sparkErr.get}")
      }
    }
  }

  test("TIME input column routes through the dispatcher (#5218)") {
    assume(isSpark41Plus, "TimeType requires Spark 4.1+")
    // `canHandle` accepts TIME (`isSupportedDataType`) and `emitTypedGetters` emits a getLong
    // case for `TimeNanoVector`, but `CometScalaUDFCodegen.specFor` used to omit the vector class
    // and throw `UnsupportedOperationException` at execute time -- after the plan had already
    // committed to the kernel, so there was no fallback. Driven through the dispatcher directly
    // because Spark 4.1 still rejects TIME columns in file-based data sources, so no SQL query
    // can produce a TIME input today.
    val timeVec = new TimeNanoVector("tm", CometArrowAllocator)
    val digestVec = new VarBinaryVector("digest", CometArrowAllocator)
    val exprVec = new VarBinaryVector("expr", CometArrowAllocator)
    var out: ValueVector = null
    try {
      timeVec.allocateNew()
      timeVec.setSafe(0, 45296000000000L) // 12:34:56
      timeVec.setNull(1)
      timeVec.setValueCount(2)

      val timeType = org.apache.spark.sql.comet.util.Utils.fromArrowField(timeVec.getField)
      val expr = BoundReference(0, timeType, nullable = true)
      val serialized = SparkEnv.get.closureSerializer.newInstance().serialize(expr)
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      digestVec.allocateNew()
      digestVec.setSafe(0, CometScalaUDFCodegen.digest(bytes))
      digestVec.setValueCount(1)
      exprVec.allocateNew()
      exprVec.setSafe(0, bytes)
      exprVec.setValueCount(1)

      out = new CometScalaUDFCodegen().evaluate(Array(digestVec, exprVec, timeVec), 2)
      val comet = CometVector.getVector(out.asInstanceOf[FieldVector], null)
      assert(comet.getLong(0) === 45296000000000L)
      assert(comet.isNullAt(1))
    } finally {
      if (out != null) out.close()
      digestVec.close()
      exprVec.close()
      timeVec.close()
    }
  }

  test("dispatcher finds a compiled kernel by the expression digest alone (#6705)") {
    // The serde ships a digest of the serialized expression at arg 0 and the bytes at arg 1, and
    // the dispatcher reads the bytes only to compile on a cache miss. The second call passes a
    // null at arg 1, so it succeeds only if the digest finds the kernel the first call compiled.
    val expr = Add(BoundReference(0, LongType, nullable = true), Literal(1L))
    val serialized = SparkEnv.get.closureSerializer.newInstance().serialize(expr)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)

    def binaryScalar(name: String, value: Array[Byte]): VarBinaryVector = {
      val v = new VarBinaryVector(name, CometArrowAllocator)
      v.allocateNew()
      if (value == null) v.setNull(0) else v.setSafe(0, value)
      v.setValueCount(1)
      v
    }
    val digestVec = binaryScalar("digest", CometScalaUDFCodegen.digest(bytes))
    val exprVec = binaryScalar("expr", bytes)
    val nullExprVec = binaryScalar("expr", null)
    val input = new BigIntVector("x", CometArrowAllocator)
    try {
      input.allocateNew(2)
      input.set(0, 41L)
      input.setNull(1)
      input.setValueCount(2)

      val dispatcher = new CometScalaUDFCodegen()
      Seq(exprVec, nullExprVec).foreach { arg1 =>
        val out =
          dispatcher.evaluate(Array(digestVec, arg1, input), 2).asInstanceOf[BigIntVector]
        try {
          assert(out.get(0) === 42L)
          assert(out.isNull(1))
        } finally out.close()
      }

      // The layout before the digest, with the serialized expression at arg 0, is refused
      // rather than taken for a digest.
      val e = intercept[IllegalArgumentException] {
        dispatcher.evaluate(Array(exprVec, input), 2)
      }
      assert(e.getMessage.contains("expression digest"), e.getMessage)
    } finally {
      Seq(digestVec, exprVec, nullExprVec, input).foreach(_.close())
    }
  }

  // Runtime coverage for nullable nested `getStruct` / `getArray` / `getMap` element reads is
  // exercised through HOFs in `CometCodegenHOFSuite`. Static emitter assertions live in
  // `CometCodegenSourceSuite`.

  /**
   * Dynamically sized collection output (regression family for #4539). Each UDF takes a scalar
   * seed and returns a collection whose per-row size is a function of the seed, so the output
   * writer fills each collection's child vector at a cumulative index that `numRows` does not
   * bound. Before #4539 the fixed-width child writes used a bare `set`, which ran off the end of
   * the pre-sized buffer; with Comet's unsafe Arrow memory the overflow corrupted neighboring
   * entries (or, under NMT, aborted the JVM). Scalar input keeps the read side off the
   * complex-input deserializer, isolating coverage to the writer.
   *
   * A small batch size makes the child's `numRows`-derived pre-size tiny relative to the per-row
   * element counts, so the larger rows reliably push past it. Randomized type/shape coverage of
   * the same writer lives in `CometCodegenFuzzSuite`.
   */
  private val collectionOutputSeeds: Seq[String] = {
    val rng = new Random(42)
    (0 until 256).map { i =>
      if (i % 17 == 0) "NULL" // null result
      else if (i % 13 == 0) "0" // empty collection
      else (rng.nextInt(80) - 39).toString // mix of small and larger-than-batch sizes
    }
  }

  private def withSeedTable(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (seed INT) USING parquet")
      collectionOutputSeeds.grouped(64).foreach { batch =>
        sql(s"INSERT INTO t VALUES ${batch.map(s => s"($s)").mkString(", ")}")
      }
      f
    }
  }

  private case class CollectionOutputCase(label: String, register: () => String)

  private val collectionOutputCases: Seq[CollectionOutputCase] = Seq(
    // Fixed-width element with nulls: the exact nested write #4539 corrupted.
    CollectionOutputCase(
      "Array<Int> with null elements",
      () => {
        val n = "arrout_int"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 4 == 0) null else java.lang.Integer.valueOf(i + j)))
        n
      }),
    CollectionOutputCase(
      "Array<Long>",
      () => {
        val n = "arrout_long"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => (i.toLong + j) * 1000000000L))
        n
      }),
    CollectionOutputCase(
      "Array<String> with null elements",
      () => {
        val n = "arrout_str"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 3 == 0) null else s"v${i}_$j"))
        n
      }),
    CollectionOutputCase(
      "Array<Decimal>",
      () => {
        val n = "arrout_dec"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                java.math.BigDecimal.valueOf((i + j).toLong)))
        n
      }),
    CollectionOutputCase(
      "Array<Binary>",
      () => {
        val n = "arrout_bin"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 5 == 0) null else s"b${i}_$j".getBytes("UTF-8")))
        n
      }),
    CollectionOutputCase(
      "Map<Int, Int>",
      () => {
        val n = "mapout_ii"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => j -> (i + j)).toMap)
        n
      }),
    CollectionOutputCase(
      "Map<String, Int>",
      () => {
        val n = "mapout_si"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => s"k$j" -> (i + j)).toMap)
        n
      }),
    CollectionOutputCase(
      "Array<Array<Int>>",
      () => {
        val n = "arrout_arr"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => (0 to j).map(_ + i)))
        n
      }),
    CollectionOutputCase(
      "Map<Int, Array<Int>>",
      () => {
        val n = "mapout_iarr"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => j -> (0 to j).map(_ + i)).toMap)
        n
      }),
    CollectionOutputCase(
      "Array<Struct<Int, String>>",
      () => {
        val n = "arrout_struct"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => IntStr(i + j, s"v$j")))
        n
      }))

  for (c <- collectionOutputCases) {
    test(s"dynamically-sized ${c.label} output round-trips through codegen dispatch (#4539)") {
      val udf = c.register()
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
        withSeedTable {
          assertCodegenRan {
            checkSparkAnswerAndOperator(sql(s"SELECT $udf(seed) FROM t"))
          }
        }
      }
    }
  }

  test("dispatch falls back cleanly when the bound tree cannot be closure-serialized (#5573)") {
    val attr = AttributeReference("s", StringType)()
    // A UDF whose closure holds an object the closure serializer refuses, such as an open
    // resource.
    val target = new CometCodegenSuite.NotSerializableTarget
    val expr = ScalaUDF(
      (s: String) => target.twice(UTF8String.fromString(s)).toString,
      StringType,
      Seq(attr),
      udfName = Some("twice"))
    // `canHandle` greenlights this tree -- string in, string out, nothing unevaluable -- so the
    // closure serializer is the step that refuses it. Every other failure mode in
    // `emitJvmCodegenDispatch` already degraded to a Spark fallback; without the guard this one
    // throws during planning instead, which is a much worse outcome.
    assert(CometScalaUDF.emitJvmCodegenDispatch(expr, Seq(attr), binding = true).isEmpty)
    val reasons = expr.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
    assert(
      reasons.exists(_.contains("could not be closure-serialized")),
      s"unexpected fallback reasons: $reasons")
  }

  test(
    "unrecognized StaticInvoke routes through the dispatcher instead of falling back (#5575)") {
    withTable("t") {
      sql("CREATE TABLE t (b BINARY) USING parquet")
      sql("INSERT INTO t VALUES (unhex('CAFE')), (unhex('')), (NULL)")
      // `lpad` on binary input lowers to `StaticInvoke(ByteArray, "lpad", ...)` on every supported
      // Spark version, and is not in `CometStaticInvoke`'s allowlist. It has no native path, so
      // before #5575 it failed the whole projection back to Spark.
      val query = "SELECT lpad(b, 8, unhex('FF')) FROM t"
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql(query))
      }
      // With the dispatcher off there is nowhere left to run it, so the operator falls back and
      // says why.
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          query,
          "expression has no native path so the plan falls back to Spark")
      }
    }
  }

  test("Invoke routes through the codegen dispatcher (#5575)") {
    // `Invoke` has no serde of its own beyond the catch-all, so this also pins the registration:
    // reaching a `JvmScalarUdf` proto means `QueryPlanSerde` resolved `CometInvoke`. The target is
    // a Catalyst value, as in Spark 4's `is_valid_utf8`, which lowers to
    // `Invoke(input, "isValid", BooleanType)`.
    val attr = AttributeReference("s", StringType)()
    val proto = QueryPlanSerde.exprToProto(Invoke(attr, "toUpperCase", StringType), Seq(attr))
    assert(proto.exists(_.hasJvmScalarUdf), s"expected a codegen-dispatch proto, got $proto")
    // ...and the emitted method call compiles and evaluates.
    val folded = Invoke(
      Literal(UTF8String.fromString("ab"), StringType),
      "repeat",
      StringType,
      Seq(Literal(2)))
    assert(runKernel(folded, 1)(_.getUTF8String(0).toString) === "abab")
  }

  test("the dispatcher declines calls into code other than Spark's (#6425)") {
    val s = AttributeReference("s", StringType)()
    val i = AttributeReference("i", IntegerType)()
    val b = AttributeReference("b", BinaryType)()
    // `lpad` on binary lowers to the first, and Spark 4's `is_valid_utf8` has the shape of the
    // second. The third is the predicate of a typed `Dataset.filter`: user code, but it returns a
    // boolean.
    val predicate: Int => Boolean = _ > 0
    val function1 = ObjectType(classOf[Int => Boolean])
    val allowed = Seq(
      StaticInvoke(
        classOf[ByteArray],
        BinaryType,
        "lpad",
        Seq(b, Literal(8), Literal(Array[Byte](0)))),
      Invoke(s, "toUpperCase", StringType),
      Invoke(Literal(predicate, function1), "apply", BooleanType, Seq(i)))
    allowed.foreach { call =>
      val reason = CometInvokeTargets.declineReason(call)
      assert(reason.isEmpty, s"$call: $reason")
    }
    val money = CometCodegenSuite.Money
    val asMoney = new CometCodegenSuite.InvokeIntAsDecimalFunction(money)
    val declined = Seq(
      StaticInvoke(classOf[java.lang.Math], IntegerType, "abs", Seq(i)) ->
        "java.lang.Math, which is not part of Spark",
      Invoke(Literal(predicate, function1), "apply", IntegerType, Seq(i)) ->
        "scala.Function1, which is not part of Spark",
      // The three ways Spark lowers a call to a DataSource V2 function.
      StaticInvoke(classOf[StaticAsMoneyFunction], money, "invoke", Seq(i)) ->
        s"the DataSource V2 function ${classOf[StaticAsMoneyFunction].getName}",
      Invoke(Literal(asMoney, ObjectType(asMoney.getClass)), "invoke", money, Seq(i)) ->
        s"the DataSource V2 function ${asMoney.getClass.getName}",
      ApplyFunctionExpression(new CometCodegenSuite.IntAsDecimalFunction(money), Seq(i)) ->
        "the DataSource V2 function")
    for ((call, expected) <- declined) {
      // The kernel runs the whole tree it is given, so a call under its root counts too.
      val nested = CreateMap(Seq(Literal("k"), call), useStringTypeWhenEmpty = false)
      for (tree <- Seq(call, nested)) {
        val reason = CometInvokeTargets.declineReason(tree)
        assert(reason.exists(_.contains(expected)), s"$tree: $reason")
      }
    }
  }

  test("calls to a DataSource V2 function run in Spark (#6425)") {
    import CometCodegenSuite.{IntAsDecimalFunction, InvokeIntAsDecimalFunction, Money}
    // Each function returns `Decimal(i)` at scale 0 for a `DECIMAL(10, 2)`. Spark rescales that
    // value, or writes null when it does not fit, only when it writes a row, and an expression
    // around the call reads it as returned: `IS NULL` is false for 100000000, which needs nine
    // integer digits where the type allows eight, `count` counts it, and a cast to string prints
    // "3", not "3.00". The codegen dispatcher has to write a vector of the declared type, so a
    // native consumer of its output would read something else. So every query here has to fall
    // back, whichever way Spark lowers the call: `as_money` has an instance `invoke` method and
    // lowers to `Invoke`, `static_as_money` has a static one and lowers to `StaticInvoke`, and
    // `apply_as_money` has neither and lowers to `ApplyFunctionExpression`.
    val reason = "calls the DataSource V2 function"
    withSQLConf("spark.sql.catalog.decfn" -> classOf[InMemoryCatalog].getName) {
      val catalog =
        spark.sessionState.catalogManager.catalog("decfn").asInstanceOf[InMemoryCatalog]
      Seq(
        "as_money" -> new InvokeIntAsDecimalFunction(Money),
        "static_as_money" -> new StaticAsMoneyFunction,
        "apply_as_money" -> new IntAsDecimalFunction(Money),
        "money_array" -> new InvokeIntAsDecimalFunction(ArrayType(Money)),
        "money_struct" -> new InvokeIntAsDecimalFunction(new StructType().add("m", Money)))
        .foreach { case (name, function) =>
          catalog.createFunction(Identifier.of(Array("ns"), name), function)
        }
      withTypedCol("INT", "3", "NULL", "100000000", "-100000000") {
        val lowerings = sql(
          "SELECT decfn.ns.as_money(c), decfn.ns.static_as_money(c), " +
            "decfn.ns.apply_as_money(c) FROM t").queryExecution.analyzed.expressions
          .flatMap(_.collect {
            case call @ (_: Invoke | _: StaticInvoke | _: ApplyFunctionExpression) =>
              call.getClass
          })
        assert(
          lowerings == Seq(
            classOf[Invoke],
            classOf[StaticInvoke],
            classOf[ApplyFunctionExpression]))
        val three = new java.math.BigDecimal("3.00")
        val projected = sql(
          "SELECT c, decfn.ns.as_money(c), decfn.ns.static_as_money(c), " +
            "decfn.ns.as_money(c) IS NULL, CAST(decfn.ns.static_as_money(c) AS STRING), " +
            "abs(decfn.ns.as_money(c)) IS NULL, decfn.ns.money_array(c)[0] IS NULL, " +
            "decfn.ns.money_struct(c).m IS NULL FROM t")
        checkSparkAnswerAndFallbackReason(projected, reason)
        checkAnswer(
          projected,
          Seq(
            Row(3, three, three, false, "3", false, false, false),
            Row(null, null, null, true, null, true, true, true),
            Row(100000000, null, null, false, "100000000", false, false, false),
            Row(-100000000, null, null, false, "-100000000", false, false, false)))
        val counted =
          sql("SELECT count(decfn.ns.as_money(c)), count(decfn.ns.static_as_money(c)) FROM t")
        checkSparkAnswerAndFallbackReason(counted, reason)
        checkAnswer(counted, Row(3L, 3L))
        for (query <- Seq(
            "SELECT max(decfn.ns.as_money(c)), sum(decfn.ns.static_as_money(c)) FROM t",
            // A projected alias, which Spark passes on without writing a row.
            "SELECT d, d IS NULL FROM (SELECT decfn.ns.as_money(c) AS d FROM t)",
            "SELECT count(x) FROM t LATERAL VIEW explode(decfn.ns.money_array(c)) e AS x",
            // `map(...)` is itself dispatched, so the call would run in its kernel.
            "SELECT map_values(map('k', decfn.ns.as_money(c)))[0] IS NULL FROM t",
            "SELECT map('k', decfn.ns.apply_as_money(c)) FROM t")) {
          checkSparkAnswerAndFallbackReason(query, reason)
        }
        // A bare `ApplyFunctionExpression` never reaches the dispatcher: only Iceberg's functions
        // have a handler.
        checkSparkAnswerAndFallbackReason(
          "SELECT decfn.ns.apply_as_money(c) FROM t",
          "has no native handler")
        // Spark hashes the value the function returned. AQE would coalesce the partitions.
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          checkSparkAnswerAndFallbackReason(
            "SELECT c, spark_partition_id() FROM " +
              "(SELECT * FROM t DISTRIBUTE BY decfn.ns.as_money(c))",
            reason)
        }
      }
    }
  }
}

/**
 * Fixtures for the closure-serialization and DataSource V2 function tests. Declared inside the
 * companion object so they are static nested classes with no reference to the enclosing suite --
 * otherwise closure-serializing a tree that holds one would drag the whole suite in and the
 * serialization outcome would say nothing about the fixture itself.
 */
object CometCodegenSuite {

  /** Deliberately not `Serializable`, to make the closure serializer refuse the bound tree. */
  class NotSerializableTarget {
    def twice(s: UTF8String): UTF8String = UTF8String.fromString(s.toString + s.toString)
  }

  /** The type the #6425 functions declare. Their values are at scale 0 instead. */
  val Money: DecimalType = DecimalType(10, 2)

  /**
   * Returns its `INT` argument as the unscaled value of a `Decimal` at scale 0, whatever scale
   * `declared` has. For an array or struct type, every decimal in the result holds that value:
   * the array has one element, and each field of the struct has it. It has no `invoke` method, so
   * Spark lowers a call to `ApplyFunctionExpression`. The function binds to itself.
   */
  class IntAsDecimalFunction(declared: DataType)
      extends UnboundFunction
      with ScalarFunction[Any] {
    override def name(): String = "int_as_decimal"
    override def description(): String = s"int -> ${declared.sql}, at scale 0"
    override def bind(inputType: StructType): BoundFunction = this
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = declared
    override def produceResult(input: InternalRow): Any = valueOf(declared, input.getInt(0))

    protected def valueOf(dataType: DataType, v: Int): Any = dataType match {
      case _: DecimalType => Decimal(v)
      case ArrayType(elementType, _) => new GenericArrayData(Array(valueOf(elementType, v)))
      case struct: StructType =>
        new GenericInternalRow(struct.fields.map(f => valueOf(f.dataType, v)))
    }
  }

  /**
   * [[IntAsDecimalFunction]] with an instance `invoke` method, so Spark lowers a call to
   * `Invoke`.
   */
  class InvokeIntAsDecimalFunction(declared: DataType) extends IntAsDecimalFunction(declared) {
    def invoke(v: Int): Any = valueOf(declared, v)
  }
}

/**
 * `as_money` for the #6425 tests, with `invoke` on the companion object. Scala also compiles a
 * top-level companion's methods to static methods on the class, so Spark finds a static `invoke`
 * and lowers a call to `StaticInvoke`.
 */
class StaticAsMoneyFunction
    extends CometCodegenSuite.IntAsDecimalFunction(CometCodegenSuite.Money)

object StaticAsMoneyFunction {
  def invoke(v: Int): Decimal = Decimal(v)
}

/**
 * Case class used by the struct-input / struct-output smoke tests. Must be declared at file scope
 * (not inside the test class) so Spark's TypeTag-based UDF encoder can resolve the Spark
 * `StructType` schema from the Scala class.
 */
private case class NameAgePair(name: String, age: Int)

private case class NameItems(name: String, items: Seq[Int])

private case class XyPair(x: Int, y: String)

/** Element type for the `Array<Struct<Int, String>>` dynamically-sized output case. */
private case class IntStr(a: Int, b: String)

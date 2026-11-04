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
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, AttributeReference, Literal, Multiply}
import org.apache.spark.sql.comet.{CometFilterExec, CometProjectExec}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.hive.CometHiveTestExpressions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometJvmDetour, QueryPlanSerde}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Tests for the JVM expression detour (partial project fallback). When a projection item or a
 * filter condition has no Comet translation, [[CometConf.COMET_EXEC_JVM_DETOUR_ENABLED]] sends
 * the whole item to the codegen dispatcher instead of falling the operator back to Spark. The
 * detour is applied only at the root of an item, never to a node below it; see
 * [[CometJvmDetour]].
 *
 * An expression reaches the detour by being named in
 * [[CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS]], which makes Comet treat a supported
 * expression as having no translation while leaving it eligible for the detour. That keeps the
 * tests stable as Comet's coverage changes (`sentences`, for example, has no serde, but on Spark
 * 4.x it is replaced by a `StaticInvoke` that Comet already dispatches) and lets a test choose
 * the input types that cross to the JVM. Disabling an expression with
 * `spark.comet.expression.<Name>.enabled=false` would not do: that must make it fall back.
 */
class CometPartialProjectFallbackSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  /**
   * Enable the detour and make Comet treat the named supported expressions as having no
   * translation, so they reach the detour.
   */
  private def withDetouredSerdes(unsupported: String*)(f: => Unit): Unit =
    withSQLConf(
      CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true",
      CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> unsupported.mkString(","))(f)

  private def withIntTable(f: => Unit): Unit =
    withTable("t") {
      sql("CREATE TABLE t (x INT, s STRING) USING parquet")
      sql("INSERT INTO t VALUES (-1, 'Hello'), (2, ''), (NULL, NULL), (-7, 'abc')")
      f
    }

  /** Whether the (already-executed) DataFrame's plan kept a `CometProjectExec`. */
  private def hasCometProject(df: DataFrame): Boolean =
    collect(stripAQEPlan(df.queryExecution.executedPlan)) { case p: CometProjectExec =>
      p
    }.nonEmpty

  private def fallbackReasons(df: DataFrame): Seq[String] =
    new ExtendedExplainInfo().getFallbackReasons(stripAQEPlan(df.queryExecution.executedPlan))

  private def nativeExprs(plan: SparkPlan): Seq[String] =
    new ExtendedExplainInfo().getNativeExpressions(plan)

  test("unsupported projection expression stays in the Comet pipeline with the detour enabled") {
    withIntTable {
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT abs(x) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  test("same expression falls the operator back to Spark with the detour disabled (default)") {
    withIntTable {
      withSQLConf(CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Abs") {
        CometScalaUDFCodegen.resetStats()
        val df = sql("SELECT abs(x) FROM t")
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

  // ---------------------------------------------------------------------------------------------
  // Root-only detour. These pin that the detour never leaves a Rust-implemented ancestor over a
  // detoured node, which is what keeps it sound (see `CometJvmDetour`).
  // ---------------------------------------------------------------------------------------------

  test("the detour takes the whole projection item, not just the unsupported node") {
    // `concat` and `upper` have Comet translations, and `upper` converts before the conversion
    // reaches `length` and fails. The whole item must still run in one kernel, and `upper` must
    // not be reported as Rust-implemented from that abandoned attempt.
    withIntTable {
      withDetouredSerdes("Length") {
        assertOneKernelForSubtree {
          val (_, cometPlan) = checkSparkAnswerAndOperator(
            sql("SELECT concat(upper(s), cast(length(s) AS STRING)) FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
          assertExpressionImpl(
            cometPlan,
            native = Seq.empty,
            dispatched = Seq("concat", "length"))
          assert(
            !nativeExprs(cometPlan).contains("upper"),
            s"expected upper in the kernel, native=${nativeExprs(cometPlan)}")
        }
      }
    }
  }

  test("the detour takes the whole filter condition") {
    withIntTable {
      withDetouredSerdes("Length") {
        assertCodegenRan {
          val (_, cometPlan) = checkSparkAnswerAndOperator(
            sql("SELECT s FROM t WHERE upper(s) <> 'X' AND length(s) > 0"),
            includeClasses = Seq(classOf[CometFilterExec]))
          assertExpressionImpl(cometPlan, native = Seq.empty, dispatched = Seq("length"))
          val native = nativeExprs(cometPlan)
          assert(
            !native.contains("upper") && !native.contains("and"),
            s"expected the whole condition in the kernel, native=$native")
        }
      }
    }
  }

  test("a conditional keeps Spark's short-circuit evaluation in a projection") {
    // `10 div x` raises under ANSI for x = 0, and Spark never evaluates it for that row because
    // the CASE takes the first branch. The detour takes the whole CASE, so Spark's own code makes
    // that choice. (A detour of `abs(10 div x)` alone under the Rust CASE also passes today,
    // because DataFusion evaluates a branch only on the rows that reach it; this pins the result
    // without relying on that.)
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (0), (2), (NULL), (5)")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withDetouredSerdes("Abs") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT CASE WHEN x = 0 THEN -1L ELSE abs(10 div x) END FROM t"),
              includeClasses = Seq(classOf[CometProjectExec]))
          }
        }
      }
    }
  }

  test("a conjunction keeps Spark's short-circuit evaluation in a filter") {
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (0), (2), (NULL), (5)")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withDetouredSerdes("Abs") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT x FROM t WHERE x <> 0 AND abs(10 div x) > 1"),
              includeClasses = Seq(classOf[CometFilterExec]))
          }
        }
      }
    }
  }

  test("ancestor compatibility: array_contains over a declined element_at matches Spark") {
    // The reproducer from the review of apache/datafusion-comet#4827. Spark returns true:
    // `array_contains` treats -0.0 and 0.0 as equal. A node-local rescue of `element_at` left
    // `map_keys` and `array_contains` in Rust over its result and returned false. Making
    // `element_at` decline reproduces that shape whatever Comet's coverage of the literal is.
    withTable("t") {
      sql("CREATE TABLE t (k INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (1), (NULL)")
      val queries = Seq(
        "SELECT array_contains(map_keys(element_at(map(1, map(0.0D, 7)), k)), -0.0D) FROM t",
        "SELECT map_contains_key(element_at(map(1, map(0.0D, 7)), k), -0.0D) FROM t")
      for (query <- queries) {
        withClue(s"$query: ") {
          withDetouredSerdes("ElementAt") {
            assertCodegenRan {
              val (_, cometPlan) = checkSparkAnswerAndOperator(
                sql(query),
                includeClasses = Seq(classOf[CometProjectExec]))
              assertExpressionImpl(cometPlan, native = Seq.empty, dispatched = Seq("element_at"))
              val native = nativeExprs(cometPlan)
              assert(
                !native.contains("array_contains") && !native.contains("map_keys"),
                s"expected no Rust-implemented ancestor of the declined element_at, got $native")
            }
          }
          // Without the forced decline, whatever path Comet takes must match Spark too.
          withSQLConf(CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true") {
            checkSparkAnswer(sql(query))
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // `spark.comet.expression.<Name>.enabled=false` must still fall back to Spark.
  // ---------------------------------------------------------------------------------------------

  test("a disabled expression falls back to Spark even with the detour enabled") {
    withIntTable {
      // A Rust-implemented serde, a codegen-dispatch serde, and a disabled node below an
      // expression without a translation. In the third, `abs` stands in for that expression, so
      // the conversion stops there and never reaches the disabled `upper`.
      val cases = Seq(
        "Abs" -> "SELECT abs(x) FROM t",
        "RLike" -> "SELECT s rlike 'a.*' FROM t",
        "Upper" -> "SELECT abs(length(upper(s))) FROM t")
      for ((name, query) <- cases) {
        withClue(s"$name: $query: ") {
          withSQLConf(
            CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true",
            CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Abs",
            CometConf.getExprEnabledConfigKey(name) -> "false") {
            CometScalaUDFCodegen.resetStats()
            val df = sql(query)
            checkSparkAnswer(df)
            assert(!hasCometProject(df), "expected the projection to fall back to Spark")
            val stats = CometScalaUDFCodegen.stats()
            assert(
              stats.compileCount == 0 && stats.cacheHitCount == 0,
              s"expected no dispatcher activity, got $stats")
            assert(
              fallbackReasons(df).exists(_.contains(CometConf.getExprEnabledConfigKey(name))),
              s"expected a reason naming the disabled config, got ${fallbackReasons(df)}")
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Code that is not Spark's stays out of the kernel.
  // ---------------------------------------------------------------------------------------------

  test("an item with a Hive UDF is not detoured") {
    // The spark module has no Hive-enabled session in its tests (spark-hive is on the test
    // classpath only for Iceberg's metastore), so this builds the `HiveGenericUDF` directly and
    // drives the serde.
    val s = AttributeReference("s", StringType)()
    val item = Alias(CometHiveTestExpressions.hiveUpper(s), "u")()
    withSQLConf(CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true") {
      CometExplainInfo.withFallbackReasonScope {
        assert(CometJvmDetour.exprToProtoOrDetour(item, Seq(s)).isEmpty, "expected no detour")
        val reasons = CometExplainInfo.currentFallbackReasons(item)
        assert(
          reasons.exists(_.contains("which is not a Spark Catalyst expression")),
          s"expected the Hive UDF reason, got $reasons")
      }
    }
  }

  test("an item with reflect is not detoured") {
    withIntTable {
      withSQLConf(CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true") {
        CometScalaUDFCodegen.resetStats()
        val df = sql("SELECT reflect('java.lang.Integer', 'toHexString', coalesce(x, 0)) FROM t")
        checkSparkAnswer(df)
        assert(!hasCometProject(df), "expected the projection to fall back to Spark")
        assert(
          fallbackReasons(df).exists(_.contains("calls a Java method named in the query")),
          s"expected the reflect reason, got ${fallbackReasons(df)}")
        val stats = CometScalaUDFCodegen.stats()
        assert(stats.compileCount == 0, s"expected no kernel compile, got $stats")
      }
    }
  }

  test("a built-in expression without a Comet translation is detoured") {
    // Not forced through the testing config. Spark rewrites `session_window` in a projection to
    // a `named_struct` of `PreciseTimestampConversion` and other expressions, and that
    // item has no Comet translation. (On Spark 3.5 Spark also adds a filter, which Comet
    // translates, so `precisetimestampconversion` appears there too; `named_struct` appears only
    // in the detoured item.)
    withTable("t") {
      sql("CREATE TABLE t (ts TIMESTAMP) USING parquet")
      sql(
        "INSERT INTO t VALUES (TIMESTAMP'2024-01-15 10:00:00'), (NULL), " +
          "(TIMESTAMP'1999-12-31 23:59:59')")
      val query = "SELECT session_window(ts, '5 minutes') AS w FROM t"
      val off = sql(query)
      checkSparkAnswer(off)
      assert(!hasCometProject(off), "expected the projection to fall back with the detour off")
      withSQLConf(CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true") {
        assertCodegenRan {
          checkSparkAnswerAndImpl(sql(query), dispatched = Seq("named_struct"))
        }
      }
    }
  }

  test("a detoured item's reasons are not reported when its operator falls back for another") {
    // `abs(x)` detours and `upper(s)` is disabled, so the projection falls back. Only the
    // disabled `upper` explains that: the abandoned reason for `abs` must be cleared.
    withIntTable {
      withSQLConf(
        CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true",
        CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Abs",
        CometConf.getExprEnabledConfigKey("Upper") -> "false") {
        val df = sql("SELECT abs(x) AS a, upper(s) AS b FROM t")
        checkSparkAnswer(df)
        assert(!hasCometProject(df), "expected the projection to fall back to Spark")
        val reasons = fallbackReasons(df)
        assert(
          reasons.exists(_.contains(CometConf.getExprEnabledConfigKey("Upper"))),
          s"expected the disabled upper to be reported, got $reasons")
        assert(
          !reasons.exists(_.contains(CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key)),
          s"expected no reason for the detoured abs, got $reasons")
      }
    }
  }

  test("a detoured item is not reported as a fallback when the operator stays in Comet") {
    withIntTable {
      withDetouredSerdes("Abs") {
        val df = sql("SELECT abs(x) FROM t")
        checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
        assert(
          fallbackReasons(df).isEmpty,
          s"expected no fallback reasons, got ${fallbackReasons(df)}")
      }
    }
  }

  test("a detour keeps explain tags that another item left on a shared subtree") {
    // `CollapseProject` can leave one subtree instance in two projection items. `b` converts to
    // Rust first and tags the shared `x + 1`; detouring `a` afterwards must leave that tag, and a
    // fallback reason another conversion recorded on the node, in place.
    val x = AttributeReference("x", IntegerType)()
    val shared = Add(x, Literal(1))
    val b = Alias(Multiply(shared, Literal(2)), "b")()
    val a = Alias(Abs(shared), "a")()
    withDetouredSerdes("Abs") {
      CometExplainInfo.withFallbackReasonScope {
        assert(CometJvmDetour.exprToProtoOrDetour(b, Seq(x)).isDefined)
        val nativeBefore = shared.getTagValue(CometExplainInfo.NATIVE_EXPRS)
        assert(nativeBefore.exists(_.nonEmpty), "expected b's conversion to tag the shared node")
        withFallbackReason(shared, "reason from another item")

        val detoured = CometJvmDetour.exprToProtoOrDetour(a, Seq(x))
        assert(detoured.exists(_.hasJvmScalarUdf), s"expected a detour, got $detoured")

        assert(
          shared.getTagValue(CometExplainInfo.NATIVE_EXPRS) == nativeBefore,
          "expected the shared node to keep b's coverage tag")
        assert(
          CometExplainInfo.currentFallbackReasons(shared) == Set("reason from another item"),
          "expected the shared node to keep the other item's fallback reason")
        // What `a`'s own abandoned conversion recorded is gone.
        assert(
          CometExplainInfo.currentFallbackReasons(a.child).isEmpty,
          "expected no fallback reason left on the detoured abs")
      }
    }
  }

  test("a refused detour keeps the conversion's fallback reasons") {
    val x = AttributeReference("x", IntegerType)()
    val a = Alias(Abs(x), "a")()
    withDetouredSerdes("Abs") {
      withSQLConf("spark.sql.codegen.maxFields" -> "1") {
        CometExplainInfo.withFallbackReasonScope {
          assert(CometJvmDetour.exprToProtoOrDetour(a, Seq(x)).isEmpty)
          val reasons = CometExplainInfo.currentFallbackReasons(a.child)
          assert(
            reasons.exists(_.contains(CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key)),
            s"expected the conversion's reason, got $reasons")
          assert(
            reasons.exists(_.contains("maxFields")),
            s"expected the dispatcher's reason, got $reasons")
        }
      }
    }
  }

  test("a nondeterministic filter condition is not detoured") {
    // Spark's whole-stage code for `FilterExec` evaluates `y IS NOT NULL` only just before the
    // first conjunct that reads `y`, so `rand(42)` runs for every row, including the rows with a
    // NULL `y`, and its stream advances for them. The kernel would evaluate the condition as one
    // short-circuiting AND, skipping those rows, and keep different rows (62 instead of 56 here,
    // which is what Spark itself returns without whole-stage codegen).
    withTempPath { dir =>
      spark
        .range(0, 200, 1, numPartitions = 1)
        .selectExpr("CAST(id AS INT) AS id", "IF(id % 3 = 0, NULL, CAST(id AS INT)) AS y")
        .write
        .parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")
      withDetouredSerdes("Abs") {
        val df = sql("SELECT id FROM t WHERE y IS NOT NULL AND abs(rand(42)) < 0.5")
        checkSparkAnswer(df)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(
          collect(plan) { case f: CometFilterExec => f }.isEmpty,
          "expected the filter to fall back to Spark")
        assert(
          fallbackReasons(df).exists(_.contains("the filter condition is nondeterministic")),
          s"expected the nondeterministic-filter reason, got ${fallbackReasons(df)}")
      }
      spark.catalog.dropTempView("t")
    }
  }

  test("a detoured item drops the abandoned conversion's info messages") {
    // With `spark.comet.caseConversion.enabled` off (the default), converting `upper` records a
    // hint that a native implementation can be enabled. Here the conversion then fails at
    // `length` and the whole item runs in the kernel, where that switch changes nothing.
    def hints(df: DataFrame): Set[String] =
      collect(stripAQEPlan(df.queryExecution.executedPlan)) { case p: CometProjectExec =>
        p.getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse(Set.empty[String])
      }.flatten.toSet
    def upperHint(msgs: Set[String]): Boolean =
      msgs.exists(_.toLowerCase(java.util.Locale.ROOT).contains("native implementation of upper"))
    withIntTable {
      withDetouredSerdes("Length") {
        val plain = sql("SELECT upper(s) FROM t")
        checkSparkAnswer(plain)
        assert(
          upperHint(hints(plain)),
          s"expected the hint on a plain upper, got ${hints(plain)}")

        val detoured = sql("SELECT concat(upper(s), cast(length(s) AS STRING)) FROM t")
        checkSparkAnswerAndOperator(detoured, includeClasses = Seq(classOf[CometProjectExec]))
        assert(
          !upperHint(hints(detoured)),
          s"expected no hint for the upper inside the detoured item, got ${hints(detoured)}")
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Dispatcher-level hazards raised in review.
  // ---------------------------------------------------------------------------------------------

  test("identical nondeterministic items keep independent state") {
    // Spark gives each `rand(42)` its own generator, so both columns are equal. The two items
    // serialize to the same tree; each must still get its own kernel state.
    withTempPath { dir =>
      spark.range(0, 64, 1, numPartitions = 2).write.parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          val df = sql("SELECT abs(rand(42)) AS a, abs(rand(42)) AS b FROM t")
          checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
          val rows = df.collect()
          assert(rows.forall(r => r.getDouble(0) == r.getDouble(1)), "expected a == b per row")
        }
      }
      spark.catalog.dropTempView("t")
    }
  }

  test("identical seeded uuid items keep independent state (Spark 4.0+)") {
    assume(CometSparkSessionExtensions.isSpark40Plus, "uuid(seed) needs Spark 4.0+")
    withTempPath { dir =>
      spark.range(0, 3, 1, numPartitions = 1).write.parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")
      withDetouredSerdes("Uuid") {
        assertCodegenRan {
          val df = sql("SELECT uuid(42) AS a, uuid(42) AS b FROM t")
          checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
          val rows = df.collect()
          assert(rows.forall(r => r.getString(0) == r.getString(1)), "expected a == b per row")
        }
      }
      spark.catalog.dropTempView("t")
    }
  }

  test("year-month interval output of one detour feeds another") {
    // The nondeterministic `rand` keeps Spark from collapsing the two projections, so the upper
    // detour reads the interval column the lower one wrote.
    withTempPath { dir =>
      spark.range(0, 32, 1, numPartitions = 1).write.parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")
      withDetouredSerdes("MakeYMInterval", "Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql(
              "SELECT abs(ym) FROM " +
                "(SELECT make_ym_interval(CAST(rand(42) * 10 AS INT)) AS ym FROM t)"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
      spark.catalog.dropTempView("t")
    }
  }

  test("detour refused by canHandle still falls back cleanly") {
    // `spark.sql.codegen.maxFields=1` makes `CometBatchKernelCodegen.canHandle` refuse the tree,
    // which counts the input and the output field.
    withIntTable {
      withDetouredSerdes("Abs") {
        withSQLConf("spark.sql.codegen.maxFields" -> "1") {
          CometScalaUDFCodegen.resetStats()
          val df = sql("SELECT abs(x) FROM t")
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

  test("the detour applies only to roots passed to exprToProtoOrDetour") {
    // `exprToProto` never detours, so join keys, sort keys, aggregate and partitioning
    // expressions are unaffected. Only the projection and filter serdes call
    // `exprToProtoOrDetour`.
    val attr = AttributeReference("x", IntegerType)()
    val expr = Alias(Abs(attr), "a")()
    val inputs = Seq(attr)

    withDetouredSerdes("Abs") {
      assert(QueryPlanSerde.exprToProto(expr, inputs).isEmpty, "expected no detour")
      val detoured = CometJvmDetour.exprToProtoOrDetour(expr, inputs)
      assert(detoured.isDefined, "expected a detour proto")
      assert(detoured.get.hasJvmScalarUdf, s"expected a JvmScalarUdf proto, got ${detoured.get}")
    }

    withSQLConf(CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Abs") {
      assert(
        CometJvmDetour.exprToProtoOrDetour(expr, inputs).isEmpty,
        "expected no detour when the feature flag is disabled")
    }
  }

  test("a subquery inside the detoured item falls back cleanly, not to a kernel failure") {
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (10), (20)")
      withDetouredSerdes("Abs") {
        // The detoured tree is closure-serialized at plan time, before the operator's
        // `waitForSubqueries` populates the `ScalarSubquery` result, so `canHandle` refuses it.
        val df = sql("SELECT abs(x - (SELECT max(v) FROM t2)) FROM t")
        checkSparkAnswer(df)
        assert(
          !hasCometProject(df),
          "expected the projection to fall back when the detoured tree contains a subquery")
        val reasons = fallbackReasons(df)
        assert(
          reasons.exists(
            _.contains("JVM detour not attempted: the expression contains the " +
              "subquery")),
          s"expected the subquery reason, got $reasons")
        assert(
          !reasons.exists(_.contains("not a Spark Catalyst expression")),
          s"expected no package reason for the subquery, got $reasons")
      }
    }
  }

  test("a subquery in a sibling projection item stays in the Comet pipeline") {
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (10), (20)")
      withDetouredSerdes("Abs") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql("SELECT abs(x) AS a, x + (SELECT max(v) FROM t2) AS b FROM t"),
            includeClasses = Seq(classOf[CometProjectExec]))
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Parity: a supported expression sent through the detour ships a chosen input type across the
  // FFI boundary. The kernel runs Spark's own code, so the results must match Spark.
  // ---------------------------------------------------------------------------------------------

  test("parity: NULL-heavy input through the detour") {
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

  test("parity: empty input keeps the detour in the Comet pipeline") {
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet") // no rows: exercises the empty-batch path
      withDetouredSerdes("Abs") {
        // No assertCodegenRan: with zero rows the kernel never compiles.
        checkSparkAnswerAndOperator(
          sql("SELECT abs(x) FROM t"),
          includeClasses = Seq(classOf[CometProjectExec]))
      }
    }
  }

  test("parity: decimals cross the FFI boundary through the detour") {
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

  test("parity: timestamps cross the FFI boundary through the detour") {
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

  test("parity: nested array/map types cross the FFI boundary through the detour") {
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

  test("parity: nondeterministic detoured tree with per-partition seeding") {
    withTempPath { dir =>
      // Four partitions so per-partition `Rand` seeding (kernel `init(partitionIndex)`) matters.
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

  test("island preservation: the projection stays in Comet with no row transition below it") {
    def isRowTransition(p: SparkPlan): Boolean = p.getClass.getSimpleName match {
      case "ColumnarToRowExec" | "CometColumnarToRowExec" | "CometNativeColumnarToRowExec" => true
      case _ => false
    }
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3)")

      // Detour off: the projection falls back to a Spark `ProjectExec` fed by a columnar->row
      // transition.
      withSQLConf(CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Abs") {
        val offPlan = stripAQEPlan(sql("SELECT abs(x) FROM t").queryExecution.executedPlan)
        assert(
          collect(offPlan) { case p: ProjectExec => p }.nonEmpty,
          "expected a Spark ProjectExec (fallback) with the detour off")
        assert(
          collect(offPlan) { case p if isRowTransition(p) => p }.nonEmpty,
          "expected a columnar->row transition feeding the fallback project")
      }

      // Detour on: the projection stays in Comet and no columnar->row transition appears below.
      withDetouredSerdes("Abs") {
        val onPlan = stripAQEPlan(sql("SELECT abs(x) FROM t").queryExecution.executedPlan)
        val proj = collect(onPlan) { case p: CometProjectExec => p }
        assert(proj.nonEmpty, "expected the projection to stay in Comet with the detour on")
        assert(
          collect(onPlan) { case p: ProjectExec => p }.isEmpty,
          "expected no Spark ProjectExec with the detour on")
        assert(
          proj.forall(p => collect(p) { case c if isRowTransition(c) => c }.isEmpty),
          "expected no columnar->row transition below the projection")
      }
    }
  }
}

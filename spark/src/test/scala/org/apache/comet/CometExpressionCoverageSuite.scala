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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

/**
 * A runtime coverage audit that finds Spark scalar expressions which Comet does not accelerate.
 *
 * Static grep-based audits are unreliable because Comet dispatches expressions through several
 * mechanisms (serde maps, the `StaticInvoke` method map, `RuntimeReplaceable` rewrites to other
 * handled expressions, and the Spark 4.x expression shims). This suite probes behavior directly:
 * for every function in Spark's registry it runs the function's own documented example over a
 * Comet-scanned table and inspects the physical plan. If the projection stays fully Comet native
 * the expression is covered; otherwise the Comet fallback reason (from [[ExtendedExplainInfo]])
 * is recorded.
 *
 * Two categories of fallback are NOT gaps and are filtered out:
 *   - "all arguments are foldable": several serdes intentionally decline all-constant inputs so
 *     Spark can constant-fold them. The probe examples are all-literal, so this is a probe
 *     artifact, not a gap. `spark.comet.expr.allowIncompatible` is enabled so that expressions
 *     that are merely incompatible-by-default do not show up as gaps either.
 *
 * A gap whose fallback operator is a `Project` is an actionable scalar-expression gap, versus
 * generators, aggregates, and window functions, which fall back through their own operators.
 *
 * The report is written to `spark/target/expression-coverage-report.md`. Run with:
 * {{{
 * ./mvnw test -Dsuites="org.apache.comet.CometExpressionCoverageSuite" -Dtest=none
 * }}}
 */
class CometExpressionCoverageSuite extends CometTestBase {

  private val probe = "comet_coverage_probe"

  private case class Probe(
      function: String,
      className: String,
      status: String,
      operator: String,
      reason: String,
      query: String)

  /**
   * Pull the first self-contained `SELECT` example out of a function's `ExpressionInfo`. Examples
   * that reference tables, values lists, windows, or grouping need setup this probe does not
   * provide, so they are skipped.
   */
  private def firstProjectionExample(examples: String): Option[String] = {
    if (examples == null) return None
    examples.linesIterator
      .map(_.trim)
      .filter(_.startsWith("> SELECT "))
      .map(_.stripPrefix(">").trim.stripSuffix(";").trim)
      .find { q =>
        val u = q.toUpperCase
        !u.contains(" FROM ") && !u.contains(" OVER ") && !u.contains("GROUP BY") &&
        !u.contains("VALUES") && !u.contains(" JOIN ") && !u.contains("LATERAL") &&
        !u.contains("(SELECT")
      }
  }

  test("expression coverage report") {
    withTable(probe) {
      sql(s"CREATE TABLE $probe(x int) USING parquet")
      sql(s"INSERT INTO $probe VALUES (1)")

      val explain = new ExtendedExplainInfo()
      val registry = spark.sessionState.functionRegistry
      val functions = registry.listFunction().sortBy(_.funcName).distinct

      val results: Seq[Probe] = functions.flatMap { fi =>
        registry.lookupFunction(fi).map { info =>
          val className = info.getClassName
          firstProjectionExample(info.getExamples) match {
            case None =>
              Probe(fi.funcName, className, "SKIP", "", "no self-contained example", "")
            case Some(projection) =>
              val query = s"$projection FROM $probe"
              val outcome = Try {
                withSQLConf(
                  CometConf.COMET_ENABLED.key -> "true",
                  CometConf.COMET_EXEC_ENABLED.key -> "true",
                  CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
                  "spark.comet.expr.allowIncompatible" -> "true",
                  SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                  SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
                    "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
                  val plan = spark.sql(query).queryExecution.executedPlan
                  (findFirstNonCometOperator(plan), explain.getFallbackReasons(plan))
                }
              }
              outcome match {
                case Success((None, _)) =>
                  Probe(fi.funcName, className, "NATIVE", "", "", query)
                case Success((Some(op), reasons)) =>
                  val reason = reasons.mkString("; ")
                  val lower = reason.toLowerCase
                  // Reasons that are not real gaps:
                  //  - the probe uses all-literal examples, so serdes that decline all-constant
                  //    inputs (to let Spark constant-fold) or require a columnar argument report a
                  //    fallback that would not happen for column inputs.
                  //  - "not fully compatible"/"correctness" means the expression is supported but
                  //    incompatible by default (opt-in via allowIncompatible).
                  val status =
                    if (lower.contains("foldable") ||
                      lower.contains("all arguments are literals") ||
                      lower.contains("scalar values are not supported")) {
                      "LITERAL_PROBE"
                    } else if (lower.contains("not fully compatible") ||
                      lower.contains("correctness")) {
                      "INCOMPATIBLE"
                    } else {
                      "GAP"
                    }
                  Probe(fi.funcName, className, status, op.nodeName, reason, query)
                case Failure(e) =>
                  Probe(fi.funcName, className, "ERROR", "", e.getClass.getSimpleName, query)
              }
          }
        }
      }

      assert(results.nonEmpty, "expected to probe at least one function")

      val byStatus = results.groupBy(_.status).map { case (k, v) => k -> v.size }
      val gaps = results.filter(_.status == "GAP")
      // A Project fallback means a scalar expression Comet could not convert. Other operators
      // (Generate, HashAggregate, Window, Expand, ...) are non-scalar and expected to appear here.
      val scalarGaps = gaps.filter(_.operator.contains("Project")).sortBy(_.function)
      val otherGaps = gaps.filterNot(_.operator.contains("Project")).sortBy(_.function)

      val sparkVersion = org.apache.spark.SPARK_VERSION
      val sb = new StringBuilder
      sb.append(s"# Comet expression coverage report (Spark $sparkVersion)\n\n")
      sb.append(
        s"Probed ${results.size} registered functions using their documented examples. " +
          "Rerun under each Maven Spark profile (`-Pspark-3.4`, `-Pspark-3.5`, `-Pspark-4.0`, " +
          "`-Pspark-4.1`) to compare coverage across versions.\n\n")
      sb.append("| Status | Count |\n| --- | --- |\n")
      Seq("NATIVE", "INCOMPATIBLE", "LITERAL_PROBE", "GAP", "SKIP", "ERROR").foreach { s =>
        sb.append(s"| $s | ${byStatus.getOrElse(s, 0)} |\n")
      }
      sb.append(
        s"\n## Scalar gaps (Project fallback) - ${scalarGaps.size}\n\n" +
          "Actionable candidates: scalar expressions Comet did not convert.\n\n")
      sb.append("| Function | Expression class | Fallback reason | Probed query |\n")
      sb.append("| --- | --- | --- | --- |\n")
      scalarGaps.foreach(g =>
        sb.append(s"| `${g.function}` | `${g.className}` | ${g.reason} | `${g.query}` |\n"))
      sb.append(
        s"\n## Non-scalar gaps (Generate / Aggregate / Window / etc.) - ${otherGaps.size}\n\n")
      sb.append("| Function | Expression class | Operator |\n| --- | --- | --- |\n")
      otherGaps.foreach(g =>
        sb.append(s"| `${g.function}` | `${g.className}` | `${g.operator}` |\n"))

      val incompatible = results.filter(_.status == "INCOMPATIBLE").sortBy(_.function)
      sb.append(
        s"\n## Incompatible by default (supported, opt-in) - ${incompatible.size}\n\n" +
          "Supported natively but off by default; today they fall back to Spark unless " +
          "`allowIncompatible` is set. Candidates for a codegen-dispatch default.\n\n")
      sb.append("| Function | Expression class | Reason |\n| --- | --- | --- |\n")
      incompatible.foreach(g =>
        sb.append(s"| `${g.function}` | `${g.className}` | ${g.reason} |\n"))

      val literalProbe = results.filter(_.status == "LITERAL_PROBE").sortBy(_.function)
      sb.append(
        s"\n## Supported, probe artifact (all-literal example) - ${literalProbe.size}\n\n")
      sb.append("| Function | Expression class | Reason |\n| --- | --- | --- |\n")
      literalProbe.foreach(g =>
        sb.append(s"| `${g.function}` | `${g.className}` | ${g.reason} |\n"))

      val out = Paths.get("target", s"expression-coverage-report-spark-$sparkVersion.md")
      Files.write(out, sb.toString.getBytes(StandardCharsets.UTF_8))

      // scalastyle:off println
      println("=" * 80)
      println(s"Comet expression coverage for Spark $sparkVersion")
      println(
        "Comet expression coverage: " +
          Seq("NATIVE", "INCOMPATIBLE", "LITERAL_PROBE", "GAP", "SKIP", "ERROR")
            .map(s => s"$s=${byStatus.getOrElse(s, 0)}")
            .mkString(", "))
      println(s"Scalar gaps (Project fallback): ${scalarGaps.size}")
      scalarGaps.foreach(g => println(s"  ${g.function} -> ${g.reason}"))
      println(s"Full report written to ${out.toAbsolutePath}")
      println("=" * 80)
      // scalastyle:on println
    }
  }
}

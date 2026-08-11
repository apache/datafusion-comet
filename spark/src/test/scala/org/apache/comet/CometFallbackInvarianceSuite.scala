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

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Checks that forcing an expression from Comet's native path back to Spark does not change the
 * outcome of a query.
 *
 * Comet exposes `spark.comet.expression.<Name>.enabled` for every registered expression, and
 * `QueryPlanSerde` honours it as forced fallback. For any expression Comet rates compatible, that
 * gives a free invariant: the outcome of a query -- its rows, or the error it raises -- must be
 * identical whether the expression is evaluated natively or by Spark.
 *
 * Two properties of this suite are load-bearing:
 *
 *   1. "Outcome" is three-valued. A leg either produces rows or throws. A value on one leg and an
 *      exception on the other is a failure with a named witness, not an error in the harness --
 *      exception parity is where several historical divergences have lived.
 *
 * 2. Every comparison is gated on evidence that the config flip actually moved execution. A query
 * whose executed plan proves nothing is reported SKIPPED-VACUOUS and never counted as a pass.
 * Without that gate an invariance sweep silently overstates its own coverage.
 *
 * Queries deliberately avoid `ORDER BY`: a shuffle roots the executed plan at
 * `AdaptiveSparkPlanExec`, whose `children` is empty, so plan inspection would read zero for
 * everything and the gate above would be inoperative. Row order is canonicalised in the
 * comparator instead.
 */
class CometFallbackInvarianceSuite extends CometFuzzTestBase {

  /**
   * Expressions Comet documents as not guaranteed to match Spark exactly. A divergence here is
   * reported as EXCUSED: logged for the record, never counted as a pass.
   */
  private val incompatibleRated: Set[String] =
    Set(
      "DateFormatClass",
      "FromUTCTimestamp",
      "GetJsonObject",
      "RLike",
      "StringTranslate",
      "TruncTimestamp")

  private sealed trait Outcome
  private case class Rows(rows: Seq[String]) extends Outcome
  private case class Threw(errorClass: String) extends Outcome

  /**
   * What the executed plan proves about where the projection ran. Captured whether or not
   * execution succeeded, so a leg that throws is still gated rather than waved through.
   *
   * `opaque` is the guard that matters: a plan showing neither a Comet operator nor a Spark
   * `ProjectExec` proves nothing at all, and must never be read as "ran natively".
   */
  private case class Presence(cometOps: Int, cometProjects: Int, sparkProjects: Int) {
    def opaque: Boolean = cometOps == 0 && sparkProjects == 0
    def ranNatively: Boolean = sparkProjects == 0 && cometOps > 0
    def fellBack: Boolean = sparkProjects > 0
    override def toString: String =
      s"cometOps=$cometOps cometProject=$cometProjects sparkProject=$sparkProjects"
  }

  private case class Leg(outcome: Outcome, presence: Presence)

  private def presenceOf(plan: SparkPlan): Presence =
    Presence(
      cometOps = plan.collect {
        case op if op.getClass.getSimpleName.startsWith("Comet") => true
      }.length,
      cometProjects = plan.collect {
        case op if op.getClass.getSimpleName == "CometProjectExec" => true
      }.length,
      sparkProjects = plan.collect { case _: ProjectExec => true }.length)

  private def errorClassOf(e: Throwable): String = {
    var root: Throwable = e
    while (root.getCause != null && root.getCause != root) root = root.getCause
    val message = Option(root.getMessage).getOrElse("")
    "\\[([A-Z_]+)\\]".r
      .findFirstMatchIn(message)
      .map(_.group(1))
      .getOrElse(root.getClass.getSimpleName)
  }

  /** NaN, -0.0 and NULL are distinct tokens; row order is canonicalised by sorting. */
  private def canon(rows: Seq[Row]): Seq[String] =
    rows.map { row =>
      (0 until row.length)
        .map { i =>
          val value = row.get(i)
          if (value == null) "<NULL>"
          else
            value match {
              case d: java.lang.Double if d.isNaN => "<NaN>"
              case f: java.lang.Float if f.isNaN => "<NaN>"
              case d: java.lang.Double if d == 0.0d && (1.0d / d) < 0 => "<NEGZERO>"
              case f: java.lang.Float if f == 0.0f && (1.0f / f) < 0 => "<NEGZERO>"
              case other => other.toString
            }
        }
        .mkString("|")
    }.sorted

  private def runLeg(sql: String): Leg = {
    val df = spark.sql(sql)
    val collected = Try(df.collect())
    // The executed plan is available whether or not collect() succeeded, so both legs are gated.
    val presence = Try(presenceOf(df.queryExecution.executedPlan))
      .getOrElse(Presence(0, 0, 0))
    val outcome = collected match {
      case Success(rows) => Rows(canon(rows.toSeq))
      case Failure(e) => Threw(errorClassOf(e))
    }
    Leg(outcome, presence)
  }

  private def compare(
      label: String,
      expr: String,
      base: Leg,
      forced: Leg,
      tally: mutable.Map[String, Int],
      findings: mutable.ArrayBuffer[String]): Unit = {
    def bump(key: String): Unit = tally(key) = tally.getOrElse(key, 0) + 1
    def skip(why: String): Unit = {
      bump("vacuous")
      findings += s"SKIPPED-VACUOUS [$label] $expr $why " +
        s"(default: ${base.presence}) (forced: ${forced.presence})"
    }

    if (base.presence.opaque || forced.presence.opaque) {
      // Neither "ran natively" nor "fell back" can be established from this plan.
      skip("bind-unprovable-opaque-plan")
    } else if (!base.presence.ranNatively) {
      skip("not-native-by-default")
    } else if (!forced.presence.fellBack) {
      skip("flip-did-not-force-fallback")
    } else {
      (base.outcome, forced.outcome) match {
        case (Rows(a), Rows(b)) =>
          if (a.length != b.length) {
            bump("fail")
            findings += s"FAIL-LEN [$label] $expr default=${a.length} forced=${b.length}"
          } else if (a == b) {
            bump("pass")
          } else {
            val i = a.zip(b).indexWhere { case (x, y) => x != y }
            if (incompatibleRated.contains(expr)) {
              bump("excused")
              findings += s"EXCUSED [$label] $expr row=$i default=[${a(i)}] forced=[${b(i)}]"
            } else {
              bump("fail")
              findings += s"FAIL-VALUE [$label] $expr row=$i default=[${a(i)}] forced=[${b(i)}]"
            }
          }
        case (Rows(a), Threw(k)) =>
          bump("fail")
          findings += s"FAIL-OUTCOME [$label] $expr " +
            s"default=VALUE(${a.headOption.getOrElse("<empty>")}) forced=THREW($k)"
        case (Threw(k), Rows(b)) =>
          bump("fail")
          findings += s"FAIL-OUTCOME [$label] $expr default=THREW($k) " +
            s"forced=VALUE(${b.headOption.getOrElse("<empty>")})"
        case (Threw(k1), Threw(k2)) =>
          if (k1 == k2) {
            bump("pass")
          } else {
            bump("fail")
            findings += s"FAIL-ERRCLASS [$label] $expr default=$k1 forced=$k2"
          }
      }
    }
  }

  /**
   * Runs the query with the expression forced to fall back to Spark. The leg is captured through
   * a local rather than returned from `withSQLConf`, because on Spark 3.x that helper is declared
   * to return `Unit`.
   */
  private def runLegForced(expr: String, sql: String): Leg = {
    var leg: Option[Leg] = None
    withSQLConf(CometConf.getExprEnabledConfigKey(expr) -> "false") {
      leg = Some(runLeg(sql))
    }
    leg.get
  }

  private def sweep(
      label: String,
      cases: Seq[(String, String)],
      tally: mutable.Map[String, Int],
      findings: mutable.ArrayBuffer[String]): Unit =
    cases.foreach { case (expr, sql) =>
      val base = runLeg(sql)
      val forced = runLegForced(expr, sql)
      compare(label, expr, base, forced, tally, findings)
    }

  test("outcome is invariant under forced per-expression fallback") {
    val tally = mutable.Map[String, Int]()
    val findings = mutable.ArrayBuffer[String]()

    // Value parity over the shared fuzz fixture, ANSI off.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = spark.read.parquet(filename)
      df.createOrReplaceTempView("t1")
      val schema = df.schema.fields
      def firstOf(p: String => Boolean): Option[String] =
        schema.find(field => p(field.dataType.typeName)).map(_.name)
      val i = firstOf(t => t == "integer" || t == "long").getOrElse("c0")
      val s = firstOf(_ == "string").getOrElse("c0")
      val d = firstOf(_.startsWith("decimal")).getOrElse(i)
      val f = firstOf(t => t == "double" || t == "float").getOrElse(i)

      sweep(
        "values",
        Seq(
          ("Add", s"SELECT $i + 1 AS r FROM t1"),
          ("Subtract", s"SELECT $i - 1 AS r FROM t1"),
          ("Multiply", s"SELECT $i * 2 AS r FROM t1"),
          ("Divide", s"SELECT $d / 3 AS r FROM t1"),
          ("Remainder", s"SELECT $i % 7 AS r FROM t1"),
          ("Abs", s"SELECT abs($i) AS r FROM t1"),
          ("Cast", s"SELECT CAST($i AS STRING) AS r FROM t1"),
          ("Substring", s"SELECT substring($s, 1, 3) AS r FROM t1"),
          ("Upper", s"SELECT upper($s) AS r FROM t1"),
          ("Lower", s"SELECT lower($s) AS r FROM t1"),
          ("Length", s"SELECT length($s) AS r FROM t1"),
          ("Coalesce", s"SELECT coalesce($i, 0) AS r FROM t1"),
          ("IsNull", s"SELECT $i IS NULL AS r FROM t1"),
          ("EqualTo", s"SELECT $i = 1 AS r FROM t1"),
          ("GreaterThan", s"SELECT $i > 0 AS r FROM t1"),
          ("If", s"SELECT IF($i > 0, 1, 2) AS r FROM t1"),
          ("CaseWhen", s"SELECT CASE WHEN $i > 0 THEN 1 ELSE 2 END AS r FROM t1"),
          ("Sqrt", s"SELECT sqrt(abs($f)) AS r FROM t1"),
          ("StringTranslate", s"SELECT translate($s, 'ab', 'xy') AS r FROM t1"),
          ("RLike", s"SELECT $s RLIKE 'a' AS r FROM t1")),
        tally,
        findings)
    }

    // Error parity, ANSI on: expressions whose divergence shows up as a raised exception rather
    // than a wrong value. ANSI is set explicitly so the section behaves the same on every profile.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("fallback_invariance_t") {
        sql("CREATE TABLE fallback_invariance_t (s STRING, i INT) USING parquet")
        sql("INSERT INTO fallback_invariance_t VALUES ('notadate', NULL), ('2024-01-01', 1)")
        sweep(
          "errors",
          Seq(
            (
              "AddMonths",
              "SELECT add_months(CAST(s AS DATE), i) AS r FROM fallback_invariance_t"),
            (
              "MonthsBetween",
              "SELECT months_between(CAST(s AS DATE), CAST(s AS DATE)) AS r " +
                "FROM fallback_invariance_t"),
            ("Pmod", "SELECT pmod(i, CAST(s AS INT)) AS r FROM fallback_invariance_t"),
            ("Conv", "SELECT conv(s, 16, 10) AS r FROM fallback_invariance_t"),
            ("Cast", "SELECT CAST(s AS DATE) AS r FROM fallback_invariance_t")),
          tally,
          findings)
      }
    }

    val summary =
      s"pass=${tally.getOrElse("pass", 0)} fail=${tally.getOrElse("fail", 0)} " +
        s"excused=${tally.getOrElse("excused", 0)} vacuous=${tally.getOrElse("vacuous", 0)}"
    // scalastyle:off println
    println(s"FALLBACK-INVARIANCE-RESULT $summary")
    findings.foreach(finding => println(s"FALLBACK-INVARIANCE $finding"))
    // scalastyle:on println

    val failures = findings.filter(_.startsWith("FAIL"))
    assert(
      failures.isEmpty,
      s"forced fallback changed the outcome of ${failures.length} quer" +
        s"${if (failures.length == 1) "y" else "ies"} ($summary):\n" +
        failures.mkString("\n"))
  }
}

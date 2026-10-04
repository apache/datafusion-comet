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

import java.io.File

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[SqlFileTestParser]]. Pure text parsing, so no Spark session is needed. The
 * end-to-end behaviour of each query mode is covered by `CometSqlFileTestSuite` running the
 * fixtures themselves.
 */
class SqlFileTestParserSuite extends AnyFunSuite {

  private def parseQueries(lines: String*): Seq[SqlQuery] =
    SqlFileTestParser.parse(lines).records.collect { case q: SqlQuery => q }

  private def modeOf(directive: String): QueryAssertionMode =
    parseQueries(directive, "SELECT 1").head.mode

  test("empty and comment-only fixtures have no records or configuration") {
    Seq(Seq.empty[String], Seq("-- fixture comment", "", "  -- another comment  ")).foreach {
      lines =>
        assert(
          SqlFileTestParser.parse(lines) ===
            SqlTestFile(Seq.empty, Seq.empty, Seq.empty, Seq.empty))
    }
  }

  test("configuration directives preserve order and trim keys and values") {
    val parsed = SqlFileTestParser.parse(
      Seq(
        "-- Config: spark.sql.ansi.enabled = true  ",
        "-- ConfigMatrix: spark.sql.session.timeZone = UTC, America/Los_Angeles ",
        "  -- Config: spark.comet.exec.scalaUDF.codegen.enabled = false",
        "-- ConfigMatrix: spark.sql.optimizer.inSetConversionThreshold = 100, 0"))

    assert(
      parsed.configs === Seq(
        "spark.sql.ansi.enabled" -> "true",
        "spark.comet.exec.scalaUDF.codegen.enabled" -> "false"))
    assert(
      parsed.configMatrix === Seq(
        "spark.sql.session.timeZone" -> Seq("UTC", "America/Los_Angeles"),
        "spark.sql.optimizer.inSetConversionThreshold" -> Seq("100", "0")))
    assert(parsed.records.isEmpty)
    assert(parsed.tables.isEmpty)
  }

  test("minimum and maximum Spark versions are parsed independently") {
    Seq(
      (Seq("-- MinSparkVersion: 3.5 "), Some("3.5"), None),
      (Seq("  -- MaxSparkVersion: 4.0"), None, Some("4.0")),
      (Seq("-- MinSparkVersion: 3.5", "-- MaxSparkVersion: 4.0"), Some("3.5"), Some("4.0")))
      .foreach { case (lines, minVersion, maxVersion) =>
        val parsed = SqlFileTestParser.parse(lines)
        assert(parsed.minSparkVersion === minVersion)
        assert(parsed.maxSparkVersion === maxVersion)
        assert(parsed.records.isEmpty)
      }
  }

  test("statements and queries preserve SQL, source lines and tables for cleanup") {
    val lines = Seq(
      "-- fixture comment",
      "",
      "statement",
      "CREATE TABLE first_table(",
      "  value STRING)",
      "USING parquet",
      "  ",
      "-- another table",
      "statement",
      "create table second_table(value INT) USING parquet",
      "",
      "statement",
      "INSERT INTO first_table VALUES ('café')",
      "",
      "query",
      "SELECT value",
      "FROM first_table")

    // A final record must be collected both at EOF and when followed by a blank line.
    Seq(lines, lines :+ "").foreach { input =>
      val parsed = SqlFileTestParser.parse(input)
      assert(
        parsed.records === Seq(
          SqlStatement("CREATE TABLE first_table(\n  value STRING)\nUSING parquet", 4),
          SqlStatement("create table second_table(value INT) USING parquet", 10),
          SqlStatement("INSERT INTO first_table VALUES ('café')", 13),
          SqlQuery("SELECT value\nFROM first_table", CheckCoverageAndAnswer, 16)))
      assert(parsed.tables === Seq("first_table", "second_table"))
    }
  }

  test("bare query directive defaults to checking coverage and answer") {
    assert(modeOf("query") === CheckCoverageAndAnswer)
  }

  test("expect_dispatch parses a single expression name") {
    assert(modeOf("query expect_dispatch(bit_length)") === ExpectDispatch(Seq("bit_length")))
  }

  test("expect_native parses a single expression name") {
    assert(modeOf("query expect_native(length)") === ExpectNative(Seq("length")))
  }

  test("expect_dispatch parses a comma-separated list and trims whitespace") {
    assert(
      modeOf("query expect_dispatch(rlike,  regexp_replace ,split)") ===
        ExpectDispatch(Seq("rlike", "regexp_replace", "split")))
  }

  test("expect_native tolerates extra whitespace around the directive") {
    assert(modeOf("query   expect_native( round , abs )") === ExpectNative(Seq("round", "abs")))
  }

  test("empty names are dropped rather than becoming unmatchable entries") {
    // A name that is the empty string could never appear in the plan's expression set, so it
    // would fail the assertion for a reason that has nothing to do with the query.
    assert(modeOf("query expect_dispatch(lower,,)") === ExpectDispatch(Seq("lower")))
  }

  test("the new modes do not shadow the existing ones") {
    assert(modeOf("query expect_fallback(some reason)") === ExpectFallback("some reason"))
    assert(modeOf("query expect_error(DIVIDE_BY_ZERO)") === ExpectError("DIVIDE_BY_ZERO"))
    assert(modeOf("query spark_answer_only") === SparkAnswerOnly)
    assert(modeOf("query tolerance=0.001") === WithTolerance(0.001))
    assert(
      modeOf("query ignore(https://example.com/issue)") === Ignore("https://example.com/issue"))
  }

  test("query mode and SQL text are associated with the right record") {
    val queries = parseQueries(
      "query expect_native(abs)",
      "SELECT abs(a) FROM t",
      "",
      "query expect_dispatch(hypot)",
      "SELECT hypot(a, b) FROM t")
    assert(queries.map(_.mode) === Seq(ExpectNative(Seq("abs")), ExpectDispatch(Seq("hypot"))))
    assert(queries.map(_.sql) === Seq("SELECT abs(a) FROM t", "SELECT hypot(a, b) FROM t"))
  }

  // #5702: Spark releases before SPARK-54918 keep -0.0 distinct in array_distinct and array_union,
  // and native distinct/union run by default only on Spark 4.2.0, the one release that normalizes
  // their arguments in the plan. The signed-zero cases therefore live in version-gated fixtures:
  // the Spark 3 and 4.0/4.1 ones assert the fallback, and only the Spark 4.2+ one may claim that
  // Comet runs them natively and agrees, through the native opt-ins.
  test("signed-zero array fixtures are version-gated and claim agreement only on Spark 4.2+") {
    def fixture(name: String): File = {
      val url = getClass.getClassLoader.getResource(s"sql-tests/expressions/array/$name")
      assert(url != null, s"missing fixture $name")
      new File(url.toURI)
    }
    def queries(name: String): Seq[SqlQuery] =
      SqlFileTestParser.parse(fixture(name)).records.collect { case q: SqlQuery => q }
    def hasSignedZero(sql: String): Boolean = sql.contains("'-0.0'")
    def isDistinctOrUnion(sql: String): Boolean =
      sql.contains("array_distinct(") || sql.contains("array_union(")

    val stalePhrases = Seq(
      "both Spark and Comet collapse it and agree here",
      "only rewrites literals, not parquet columns")
    Seq("array_distinct.sql", "array_except.sql", "array_intersect.sql", "array_union.sql")
      .foreach { name =>
        val text = {
          val src = scala.io.Source.fromFile(fixture(name), "UTF-8")
          try src.mkString
          finally src.close()
        }
        stalePhrases.foreach { phrase =>
          assert(!text.contains(phrase), s"$name still claims: $phrase")
        }
        assert(
          !text.contains("'-0.0'"),
          s"$name has signed-zero cases outside the version-gated array_set_signed_zero fixtures")
      }

    // (fixture, MinSparkVersion, MaxSparkVersion, allowed mode for array_distinct/array_union)
    val gated = Seq(
      ("array_set_signed_zero_spark_3.sql", None, Some("3.5"), "expect_fallback"),
      ("array_set_signed_zero_spark_4_0_4_1.sql", Some("4.0"), Some("4.1"), "expect_fallback"),
      ("array_set_signed_zero.sql", Some("4.2"), None, "query"))
    gated.foreach { case (name, minVersion, maxVersion, expectedMode) =>
      val parsed = SqlFileTestParser.parse(fixture(name))
      assert(parsed.minSparkVersion == minVersion, s"$name MinSparkVersion")
      assert(parsed.maxSparkVersion == maxVersion, s"$name MaxSparkVersion")
      if (expectedMode == "query") {
        // 4.2.1+ falls back by default, so only the opt-ins keep this fixture patch-independent.
        Seq("ArrayDistinct", "ArrayUnion").foreach { expr =>
          assert(
            parsed.configs.contains(CometConf.getExprAllowIncompatConfigKey(expr) -> "true"),
            s"$name must opt $expr into native execution")
        }
      }
      val cases = queries(name).filter(q => isDistinctOrUnion(q.sql))
      Seq("array_distinct(", "array_union(").foreach { fn =>
        assert(
          cases.exists(q =>
            q.sql.contains(fn) && hasSignedZero(q.sql) && !q.sql.toLowerCase.contains(" from ")),
          s"$name is missing a signed-zero literal query for $fn")
      }
      cases.foreach { q =>
        val modeOk = (expectedMode, q.mode) match {
          // Before 4.2, no release normalizes in the plan, so the native path must not run.
          case ("expect_fallback", ExpectFallback(_)) => true
          // Spark 4.2+ normalizes flat zeros like the native kernels, so they must run and agree.
          case ("query", CheckCoverageAndAnswer) => true
          case _ => false
        }
        assert(modeOk, s"$name line ${q.line}: ${q.mode} is not $expectedMode for ${q.sql}")
      }
    }
  }
}

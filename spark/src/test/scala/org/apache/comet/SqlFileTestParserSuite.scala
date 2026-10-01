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

  // #5702: NormalizeFloatingNumbers does not rewrite array-function inputs, so a
  // plain SELECT keeps -0.0 literals intact. The fixtures must skip those literal
  // cases for the same reason as the column-sourced ones, and must not claim that
  // Spark and Comet agree on the literal path.
  test("signed-zero array fixtures skip literals and do not claim Spark agreement") {
    val names =
      Seq("array_distinct.sql", "array_except.sql", "array_intersect.sql", "array_union.sql")
    val stalePhrases = Seq(
      "both Spark and Comet collapse it and agree here",
      "only rewrites literals, not parquet columns")
    names.foreach { name =>
      val url = getClass.getClassLoader.getResource(s"sql-tests/expressions/array/$name")
      assert(url != null, s"missing fixture $name")
      val file = new File(url.toURI)
      val text = {
        val src = scala.io.Source.fromFile(file, "UTF-8")
        try src.mkString
        finally src.close()
      }
      stalePhrases.foreach { phrase =>
        assert(!text.contains(phrase), s"$name still claims: $phrase")
      }
      val ignoredLiterals = SqlFileTestParser.parse(file).records.collect {
        case SqlQuery(sql, Ignore(_), _)
            if sql.contains("array(") &&
              (sql.contains("double('-0.0')") || sql.contains("float('-0.0')")) &&
              !sql.toLowerCase.contains(" from ") =>
          sql
      }
      assert(ignoredLiterals.nonEmpty, s"$name is missing an ignored signed-zero literal query")
    }
  }
}

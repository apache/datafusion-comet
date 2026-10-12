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

import scala.io.Source

/** A record in a SQL test file: either a statement (DDL/DML) or a query (SELECT). */
sealed trait SqlTestRecord

/**
 * A SQL statement to execute (CREATE TABLE, INSERT, etc.).
 *
 * @param sql
 *   The SQL text.
 * @param line
 *   1-based line number in the original .sql file where the statement starts.
 */
case class SqlStatement(sql: String, line: Int) extends SqlTestRecord

/**
 * A SQL query whose results are compared between Spark and Comet.
 *
 * @param sql
 *   The SQL text.
 * @param mode
 *   How to validate the query.
 * @param line
 *   1-based line number in the original .sql file where the query starts.
 */
case class SqlQuery(sql: String, mode: QueryAssertionMode = CheckCoverageAndAnswer, line: Int)
    extends SqlTestRecord

sealed trait QueryAssertionMode
case object CheckCoverageAndAnswer extends QueryAssertionMode
case object SparkAnswerOnly extends QueryAssertionMode
case class WithTolerance(tol: Double) extends QueryAssertionMode
case class ExpectFallback(reason: String) extends QueryAssertionMode
case class Ignore(reason: String) extends QueryAssertionMode

/**
 * Checks results and coverage like [[CheckCoverageAndAnswer]], and additionally asserts that
 * Comet ran each named expression through the JVM codegen dispatcher rather than lowering it to a
 * native DataFusion expression.
 *
 * Matching results cannot distinguish the two mechanisms, so without this a serde that gains a
 * native path (or loses its dispatcher route) changes how the query executes while every other
 * assertion in the fixture stays green.
 */
case class ExpectDispatch(names: Seq[String]) extends QueryAssertionMode

/**
 * The native counterpart of [[ExpectDispatch]]: asserts Comet lowered each named expression to a
 * native DataFusion expression rather than routing it through the JVM codegen dispatcher.
 */
case class ExpectNative(names: Seq[String]) extends QueryAssertionMode

/**
 * Asserts that both Spark and Comet raise an error whose message contains `pattern`.
 *
 * Fixtures that combine `ExpectError` with `spark.comet.exec.scalaUDF.codegen.enabled=true` must
 * also include at least one [[CheckCoverageAndAnswer]] sentinel query over valid input. If the
 * dispatcher silently rejects the expression at plan time, the operator falls back to Spark and
 * Spark itself raises the same error, so the `ExpectError` queries would pass vacuously. The
 * sentinel query uses `checkSparkAnswerAndOperator`, which fails when the expression does not run
 * inside Comet. `CometSqlFileTestSuite.requireSentinelForCodegenExpectError` enforces this shape
 * at test-run time.
 */
case class ExpectError(pattern: String) extends QueryAssertionMode

/**
 * Parsed representation of a .sql test file.
 *
 * @param configs
 *   Spark SQL configs to set for this test file.
 * @param configMatrix
 *   Map of config key to list of values. The test will run once per combination.
 * @param records
 *   Ordered list of statements and queries.
 * @param tables
 *   Table names extracted from CREATE TABLE statements (for cleanup).
 * @param minSparkVersion
 *   Optional minimum Spark version required to run this test (e.g. "3.5"). The test is skipped on
 *   older versions.
 * @param maxSparkVersion
 *   Optional maximum Spark version this test applies to (e.g. "3.4"). The test is skipped on
 *   newer versions. Useful for paired fixtures where each version range has its own expected
 *   error class or output format.
 * @param excludedRules
 *   Additional optimizer rule names to exclude, alongside the default ConstantFolding exclusion.
 * @param constantFoldingEnabled
 *   Whether to let Spark fold constants before Comet sees the query.
 */
case class SqlTestFile(
    configs: Seq[(String, String)],
    configMatrix: Seq[(String, Seq[String])],
    records: Seq[SqlTestRecord],
    tables: Seq[String],
    minSparkVersion: Option[String] = None,
    maxSparkVersion: Option[String] = None,
    excludedRules: Seq[String] = Seq.empty,
    constantFoldingEnabled: Boolean = false)

object SqlFileTestParser {

  private val ConfigPattern = """--\s*Config:\s*([^=]+)=(.*)""".r
  private val ConfigMatrixPattern = """--\s*ConfigMatrix:\s*([^=]+)=(.*)""".r
  private val ExcludeRulesPattern = """--\s*ExcludeRules:\s*(.*)""".r
  private val ConstantFoldingPattern = """--\s*ConstantFolding:\s*(.*)""".r
  private val ConfigDirectivePattern =
    """--\s*(Config|ConfigMatrix|ExcludeRules|ConstantFolding):.*""".r
  private val MinSparkVersionPattern = """--\s*MinSparkVersion:\s*(.+)""".r
  private val MaxSparkVersionPattern = """--\s*MaxSparkVersion:\s*(.+)""".r
  private val CreateTablePattern = """(?i)CREATE\s+TABLE\s+(\w+)""".r.unanchored

  def parse(file: File): SqlTestFile = {
    val source = Source.fromFile(file, "UTF-8")
    try {
      parse(source.getLines().toSeq)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"${file.getPath}: ${e.getMessage}", e)
    } finally {
      source.close()
    }
  }

  def parse(lines: Seq[String]): SqlTestFile = {
    var configs = Seq.empty[(String, String)]
    var configMatrix = Seq.empty[(String, Seq[String])]
    var minSparkVersion: Option[String] = None
    var maxSparkVersion: Option[String] = None
    var excludedRules = Seq.empty[String]
    var constantFoldingEnabled: Option[Boolean] = None
    val records = Seq.newBuilder[SqlTestRecord]
    val tables = Seq.newBuilder[String]

    var lineIdx = 0
    while (lineIdx < lines.length) {
      val line = lines(lineIdx).trim

      line match {
        case ConfigPattern(key, value) =>
          require(key.trim.nonEmpty, s"Empty Config key at line ${lineIdx + 1}")
          configs :+= (key.trim -> value.trim)
          lineIdx += 1

        case ConfigMatrixPattern(key, values) =>
          require(key.trim.nonEmpty, s"Empty ConfigMatrix key at line ${lineIdx + 1}")
          val choices = values.split(",", -1).map(_.trim).toSeq
          require(choices.forall(_.nonEmpty), s"Empty ConfigMatrix value at line ${lineIdx + 1}")
          configMatrix :+= (key.trim -> choices)
          lineIdx += 1

        case ExcludeRulesPattern(rules) =>
          val names = splitNames(rules)
          require(names.nonEmpty, s"Empty ExcludeRules at line ${lineIdx + 1}")
          excludedRules ++= names
          lineIdx += 1

        case ConstantFoldingPattern(mode) =>
          require(
            mode == "enabled" || mode == "disabled",
            s"Invalid ConstantFolding mode '$mode' at line ${lineIdx + 1}: " +
              "expected enabled or disabled")
          val enabled = mode == "enabled"
          require(
            constantFoldingEnabled.forall(_ == enabled),
            s"Conflicting ConstantFolding directives at line ${lineIdx + 1}")
          constantFoldingEnabled = Some(enabled)
          lineIdx += 1

        case ConfigDirectivePattern(name) =>
          throw new IllegalArgumentException(s"Malformed $name directive at line ${lineIdx + 1}")

        case MinSparkVersionPattern(version) =>
          minSparkVersion = Some(version.trim)
          lineIdx += 1

        case MaxSparkVersionPattern(version) =>
          maxSparkVersion = Some(version.trim)
          lineIdx += 1

        case "statement" =>
          lineIdx += 1
          val startLine = lineIdx + 1
          val (sql, nextIdx) = collectSql(lines, lineIdx)
          // Extract table names for cleanup
          CreateTablePattern.findFirstMatchIn(sql).foreach(m => tables += m.group(1))
          records += SqlStatement(sql, startLine)
          lineIdx = nextIdx

        case s if s.startsWith("query") =>
          val mode = parseQueryAssertionMode(s)
          lineIdx += 1
          val startLine = lineIdx + 1
          val (sql, nextIdx) = collectSql(lines, lineIdx)
          records += SqlQuery(sql, mode, startLine)
          lineIdx = nextIdx

        case _ =>
          // Skip blank lines and comments
          lineIdx += 1
      }
    }

    SqlTestFile(
      configs,
      configMatrix,
      records.result(),
      tables.result(),
      minSparkVersion,
      maxSparkVersion,
      excludedRules.distinct,
      constantFoldingEnabled.getOrElse(false))
  }

  private val FallbackPattern = """query\s+expect_fallback\((.+)\)""".r
  private val IgnorePattern = """query\s+ignore\((.+)\)""".r
  private val ErrorPattern = """query\s+expect_error\((.+)\)""".r
  private val DispatchPattern = """query\s+expect_dispatch\((.+)\)""".r
  private val NativePattern = """query\s+expect_native\((.+)\)""".r

  private def parseQueryAssertionMode(directive: String): QueryAssertionMode = {
    directive match {
      case FallbackPattern(reason) =>
        ExpectFallback(reason.trim)
      case IgnorePattern(reason) =>
        Ignore(reason.trim)
      case ErrorPattern(pattern) =>
        ExpectError(pattern.trim)
      case DispatchPattern(names) =>
        ExpectDispatch(splitNames(names))
      case NativePattern(names) =>
        ExpectNative(splitNames(names))
      case _ =>
        val parts = directive.split("\\s+")
        if (parts.length == 1) return CheckCoverageAndAnswer
        parts(1) match {
          case "spark_answer_only" => SparkAnswerOnly
          case s if s.startsWith("tolerance=") =>
            WithTolerance(s.stripPrefix("tolerance=").toDouble)
          case _ => CheckCoverageAndAnswer
        }
    }
  }

  /**
   * Split a comma-separated expression-name list, dropping empties so `expect_dispatch(a, b,)`
   * and stray whitespace do not produce a name that can never match.
   */
  private def splitNames(names: String): Seq[String] =
    names.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  /** Collect SQL lines until a blank line or end of file. */
  private def collectSql(lines: Seq[String], start: Int): (String, Int) = {
    val sb = new StringBuilder
    var lineIdx = start
    while (lineIdx < lines.length && lines(lineIdx).trim.nonEmpty) {
      if (sb.nonEmpty) sb.append("\n")
      sb.append(lines(lineIdx))
      lineIdx += 1
    }
    (sb.toString, lineIdx)
  }
}

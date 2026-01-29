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

/** A SQL statement to execute (CREATE TABLE, INSERT, etc.). */
case class SqlStatement(sql: String) extends SqlTestRecord

/** A SQL query whose results are compared between Spark and Comet. */
case class SqlQuery(sql: String, mode: QueryAssertionMode = CheckCoverageAndAnswer)
    extends SqlTestRecord

sealed trait QueryAssertionMode
case object CheckCoverageAndAnswer extends QueryAssertionMode
case object SparkAnswerOnly extends QueryAssertionMode
case class WithTolerance(tol: Double) extends QueryAssertionMode
case class ExpectFallback(reason: String) extends QueryAssertionMode
case class Ignore(reason: String) extends QueryAssertionMode

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
 *   Optional minimum Spark version required to run this test (e.g. "3.5").
 */
case class SqlTestFile(
    configs: Seq[(String, String)],
    configMatrix: Seq[(String, Seq[String])],
    records: Seq[SqlTestRecord],
    tables: Seq[String],
    minSparkVersion: Option[String] = None)

object SqlFileTestParser {

  private val ConfigPattern = """--\s*Config:\s*(.+)=(.+)""".r
  private val ConfigMatrixPattern = """--\s*ConfigMatrix:\s*(.+)=(.+)""".r
  private val MinSparkVersionPattern = """--\s*MinSparkVersion:\s*(.+)""".r
  private val CreateTablePattern = """(?i)CREATE\s+TABLE\s+(\w+)""".r.unanchored

  def parse(file: File): SqlTestFile = {
    val source = Source.fromFile(file, "UTF-8")
    try {
      parse(source.getLines().toSeq)
    } finally {
      source.close()
    }
  }

  def parse(lines: Seq[String]): SqlTestFile = {
    var configs = Seq.empty[(String, String)]
    var configMatrix = Seq.empty[(String, Seq[String])]
    var minSparkVersion: Option[String] = None
    val records = Seq.newBuilder[SqlTestRecord]
    val tables = Seq.newBuilder[String]

    var lineIdx = 0
    while (lineIdx < lines.length) {
      val line = lines(lineIdx).trim

      line match {
        case ConfigPattern(key, value) =>
          configs :+= (key.trim -> value.trim)
          lineIdx += 1

        case ConfigMatrixPattern(key, values) =>
          configMatrix :+= (key.trim -> values.split(",").map(_.trim).toSeq)
          lineIdx += 1

        case MinSparkVersionPattern(version) =>
          minSparkVersion = Some(version.trim)
          lineIdx += 1

        case "statement" =>
          lineIdx += 1
          val (sql, nextIdx) = collectSql(lines, lineIdx)
          // Extract table names for cleanup
          CreateTablePattern.findFirstMatchIn(sql).foreach(m => tables += m.group(1))
          records += SqlStatement(sql)
          lineIdx = nextIdx

        case s if s.startsWith("query") =>
          val mode = parseQueryAssertionMode(s)
          lineIdx += 1
          val (sql, nextIdx) = collectSql(lines, lineIdx)
          records += SqlQuery(sql, mode)
          lineIdx = nextIdx

        case _ =>
          // Skip blank lines and comments
          lineIdx += 1
      }
    }

    SqlTestFile(configs, configMatrix, records.result(), tables.result(), minSparkVersion)
  }

  private val FallbackPattern = """query\s+expect_fallback\((.+)\)""".r
  private val IgnorePattern = """query\s+ignore\((.+)\)""".r

  private def parseQueryAssertionMode(directive: String): QueryAssertionMode = {
    directive match {
      case FallbackPattern(reason) =>
        ExpectFallback(reason.trim)
      case IgnorePattern(reason) =>
        Ignore(reason.trim)
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

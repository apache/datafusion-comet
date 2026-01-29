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
case class SqlQuery(sql: String, mode: QueryMode = CheckOperator) extends SqlTestRecord

sealed trait QueryMode
case object CheckOperator extends QueryMode
case object SparkAnswerOnly extends QueryMode
case class WithTolerance(tol: Double) extends QueryMode
case class ExpectFallback(reason: String) extends QueryMode
case class Ignore(reason: String) extends QueryMode

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
 */
case class SqlTestFile(
    configs: Seq[(String, String)],
    configMatrix: Seq[(String, Seq[String])],
    records: Seq[SqlTestRecord],
    tables: Seq[String])

object SqlFileTestParser {

  private val ConfigPattern = """--\s*Config:\s*(.+)=(.+)""".r
  private val ConfigMatrixPattern = """--\s*ConfigMatrix:\s*(.+)=(.+)""".r
  private val CreateTablePattern = """(?i)CREATE\s+TABLE\s+(\w+)""".r.unanchored

  def parse(file: File): SqlTestFile = {
    val source = Source.fromFile(file)
    try {
      parse(source.getLines().toSeq)
    } finally {
      source.close()
    }
  }

  def parse(lines: Seq[String]): SqlTestFile = {
    var configs = Seq.empty[(String, String)]
    var configMatrix = Seq.empty[(String, Seq[String])]
    val records = Seq.newBuilder[SqlTestRecord]
    val tables = Seq.newBuilder[String]

    var i = 0
    while (i < lines.length) {
      val line = lines(i).trim

      line match {
        case ConfigPattern(key, value) =>
          configs :+= (key.trim -> value.trim)
          i += 1

        case ConfigMatrixPattern(key, values) =>
          configMatrix :+= (key.trim -> values.split(",").map(_.trim).toSeq)
          i += 1

        case "statement" =>
          i += 1
          val (sql, nextIdx) = collectSql(lines, i)
          // Extract table names for cleanup
          CreateTablePattern.findFirstMatchIn(sql).foreach(m => tables += m.group(1))
          records += SqlStatement(sql)
          i = nextIdx

        case s if s.startsWith("query") =>
          val mode = parseQueryMode(s)
          i += 1
          val (sql, nextIdx) = collectSql(lines, i)
          records += SqlQuery(sql, mode)
          i = nextIdx

        case _ =>
          // Skip blank lines and comments
          i += 1
      }
    }

    SqlTestFile(configs, configMatrix, records.result(), tables.result())
  }

  private val FallbackPattern = """query\s+expect_fallback\((.+)\)""".r
  private val IgnorePattern = """query\s+ignore\((.+)\)""".r

  private def parseQueryMode(directive: String): QueryMode = {
    directive match {
      case FallbackPattern(reason) =>
        ExpectFallback(reason.trim)
      case IgnorePattern(reason) =>
        Ignore(reason.trim)
      case _ =>
        val parts = directive.split("\\s+")
        if (parts.length == 1) return CheckOperator
        parts(1) match {
          case "spark_answer_only" => SparkAnswerOnly
          case s if s.startsWith("tolerance=") =>
            WithTolerance(s.stripPrefix("tolerance=").toDouble)
          case _ => CheckOperator
        }
    }
  }

  /** Collect SQL lines until a blank line or end of file. */
  private def collectSql(lines: Seq[String], start: Int): (String, Int) = {
    val sb = new StringBuilder
    var i = start
    while (i < lines.length && lines(i).trim.nonEmpty) {
      if (sb.nonEmpty) sb.append("\n")
      sb.append(lines(i))
      i += 1
    }
    (sb.toString, i)
  }
}

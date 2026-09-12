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
}

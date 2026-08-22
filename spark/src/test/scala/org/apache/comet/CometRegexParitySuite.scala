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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.comet.expressions.{CometRegex, RegexFlavor}
import org.apache.comet.serde.Compatible

/**
 * Differential corpus: every pattern the plan-time analyzer admits is evaluated on Spark (Java
 * `Pattern`) and on Comet's native Rust path, and the results must match. Routing is asserted via
 * extended explain so a dispatcher fallback cannot silently satisfy the comparison.
 */
class CometRegexParitySuite extends CometTestBase with AdaptiveSparkPlanHelper {

  // One pattern per whitelist production, plus the lexer-boundary cases.
  private val admittedPatterns: Seq[String] = Seq(
    "",
    "abc",
    "abc[0-9]+",
    "[a-zA-Z_][a-zA-Z0-9_]*",
    "(foo|bar){1,3}",
    "a\\+b",
    "[0-9_]",
    "[^0-9]",
    "(?:foo|bar)",
    "(foo)",
    "abc|def",
    "a*",
    "a+",
    "a?",
    "a{2}",
    "a{2,}",
    "a{2,4}",
    "[(?=]",
    "\\(\\?=",
    "\\\\d",
    "[.]",
    "a|",
    "|a",
    "[a-]",
    "[-a]",
    "[a\\-z]",
    "a b",
    "(?:(?:foo)|bar)",
    "(ab)+")

  private val subjects: Seq[String] = Seq(
    "abc",
    "abc123",
    "ABC",
    "",
    null,
    "foo",
    "bar",
    "foobar",
    "a+b",
    "\\d",
    "αβγ",
    "١٢٣",
    "😀",
    "e\u0301",
    "\n",
    "\r",
    "\r\n",
    "\u0085",
    "\u2028",
    "\u2029",
    "\nabc",
    "abc\n",
    "\nabc\n",
    "(?=",
    ".",
    "aa",
    "aaaa",
    "-",
    "z",
    "a b",
    "ab",
    "abab",
    "b")

  private def sqlLiteral(pattern: String): String =
    pattern.replace("\\", "\\\\").replace("'", "''")

  private def withSubjectTable(values: Seq[String])(f: => Unit): Unit = {
    val schema = StructType(Seq(StructField("s", StringType, nullable = true)))
    val rows = spark.sparkContext.parallelize(values.map(Row(_)))
    val df = spark.createDataFrame(rows, schema)
    df.createOrReplaceTempView("t")
    f
  }

  private def explainInfo(df: org.apache.spark.sql.DataFrame): String =
    new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)

  test("analyzer admits every corpus pattern") {
    admittedPatterns.foreach { p =>
      val level = CometRegex.supportLevel(p, RegexFlavor.RLike)
      assert(level.isInstanceOf[Compatible], s"corpus pattern must be Compatible: [$p] -> $level")
    }
  }

  test("native rlike matches Spark for every admitted pattern") {
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
      withSubjectTable(subjects) {
        admittedPatterns.foreach { pattern =>
          val lit = sqlLiteral(pattern)
          val projected = sql(s"SELECT s, s rlike '$lit' FROM t")
          checkSparkAnswerAndOperator(projected)
          val projectedExplain = explainInfo(projected)
          assert(
            !projectedExplain.contains("JVM codegen dispatcher: rlike"),
            s"expected native path for [$pattern], got:\n$projectedExplain")

          val filtered = sql(s"SELECT s FROM t WHERE s rlike '$lit'")
          checkSparkAnswerAndOperator(filtered)
          val filteredExplain = explainInfo(filtered)
          assert(
            !filteredExplain.contains("JVM codegen dispatcher: rlike"),
            s"expected native path for filter [$pattern], got:\n$filteredExplain")
        }
      }
    }
  }

  test("native rlike matches Spark across multiple Arrow batches") {
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> "64",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
      val many = (0 until 5000).map(i => if (i % 7 == 0) null else s"row_${i}_abc123")
      withSubjectTable(many) {
        val df = sql("SELECT s, s rlike 'abc[0-9]+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainInfo(df)
        assert(
          !explain.contains("JVM codegen dispatcher: rlike"),
          s"expected native path for multi-batch rlike, got:\n$explain")
      }
    }
  }
}

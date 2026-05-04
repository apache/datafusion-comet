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
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometFilterExec, CometProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometRegExpJvmSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(CometConf.COMET_REGEXP_ENGINE.key, CometConf.REGEXP_ENGINE_JAVA)

  // Patterns that the Rust regex crate cannot handle. Using one of these proves
  // the JVM path was taken: if the pattern reached native, native would have
  // rejected it and the operator would not be Comet.
  private val backreference = "^(\\\\w)\\\\1$"
  private val lookahead = "foo(?=bar)"
  private val lookbehind = "(?<=foo)bar"
  private val embeddedFlags = "(?i)foo"
  private val namedGroup = "(?<digit>\\\\d)"

  private def withSubjects(values: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val rows = values
        .map(v => if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("rlike: projection produces Java regex semantics with null handling") {
    withSubjects("abc123", "no digits", null, "mixed_42_data") {
      val df = sql("SELECT s, s rlike '\\\\d+' AS m FROM t")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("rlike: predicate filters rows using Java regex semantics") {
    withSubjects("abc123", "no digits", null, "mixed_42_data") {
      val df = sql("SELECT s FROM t WHERE s rlike '\\\\d+'")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("rlike: backreference in projection (Java-only construct)") {
    withSubjects("aa", "ab", "xyzzy", null) {
      val df = sql(s"SELECT s, s rlike '$backreference' FROM t")
      checkSparkAnswerAndOperator(df)
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case p: CometProjectExec => p }.nonEmpty,
        s"Expected CometProjectExec in:\n$plan")
    }
  }

  test("rlike: backreference in predicate (Java-only construct)") {
    withSubjects("aa", "ab", "xyzzy", null) {
      val df = sql(s"SELECT s FROM t WHERE s rlike '$backreference'")
      checkSparkAnswerAndOperator(df)
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case f: CometFilterExec => f }.nonEmpty,
        s"Expected CometFilterExec in:\n$plan")
    }
  }

  test("rlike: lookahead pattern (Java-only construct)") {
    withSubjects("foobar", "foobaz", "barfoo", null) {
      checkSparkAnswerAndOperator(sql(s"SELECT s, s rlike '$lookahead' FROM t"))
      checkSparkAnswerAndOperator(sql(s"SELECT s FROM t WHERE s rlike '$lookahead'"))
    }
  }

  test("rlike: lookbehind pattern (Java-only construct)") {
    withSubjects("foobar", "barbar", "foofoo", null) {
      checkSparkAnswerAndOperator(sql(s"SELECT s, s rlike '$lookbehind' FROM t"))
    }
  }

  test("rlike: embedded case-insensitive flag (Java-only construct)") {
    withSubjects("FOO", "foo", "fOO", "bar") {
      checkSparkAnswerAndOperator(sql(s"SELECT s, s rlike '$embeddedFlags' FROM t"))
    }
  }

  test("rlike: named groups (Java-only construct)") {
    withSubjects("a1", "ab", "9z", null) {
      checkSparkAnswerAndOperator(sql(s"SELECT s, s rlike '$namedGroup' FROM t"))
    }
  }

  test("rlike: empty pattern matches every non-null row") {
    withSubjects("abc", "", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, s rlike '' FROM t"))
    }
  }

  test("rlike: empty subject string is handled correctly") {
    withSubjects("", "x", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, s rlike '^$' FROM t"))
    }
  }

  test("rlike: all-null subject column produces all-null result") {
    withSubjects(null, null, null) {
      checkSparkAnswerAndOperator(sql("SELECT s rlike '\\\\d+' FROM t"))
    }
  }

  test("rlike: null literal pattern falls back to Spark") {
    withSubjects("a", "b", null) {
      // Convert path rejects Literal(null) pattern; query must still produce
      // Spark-compatible all-null output via the Spark fallback.
      checkSparkAnswer(sql("SELECT s rlike CAST(NULL AS STRING) FROM t"))
    }
  }

  test("rlike: invalid pattern falls back to Spark") {
    withSubjects("a") {
      // Convert path catches PatternSyntaxException at planning time; Spark's
      // own RLike runs and throws its native error.
      val ex = intercept[Throwable](sql("SELECT s rlike '[' FROM t").collect())
      assert(
        ex.getMessage.toLowerCase.contains("regex") ||
          ex.getMessage.contains("PatternSyntax") ||
          ex.getMessage.contains("Unclosed"),
        s"Unexpected error: ${ex.getMessage}")
    }
  }

  test("rlike: combines with filter, projection, and aggregate") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING, k INT) USING parquet")
      sql("""INSERT INTO t VALUES
            |  ('aa', 1), ('ab', 1), ('aa', 2), ('xyzzy', 2), ('aa', 3), (NULL, 3)""".stripMargin)
      val df = sql(s"""SELECT k, COUNT(*) AS c
           |FROM t
           |WHERE s rlike '$backreference'
           |GROUP BY k
           |ORDER BY k""".stripMargin)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("rlike: many rows spanning multiple batches") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val values = (0 until 5000)
        .map(i => if (i % 7 == 0) "(NULL)" else s"('row_${i}_aa')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $values")
      checkSparkAnswerAndOperator(sql(s"SELECT s, s rlike '$backreference' FROM t"))
      checkSparkAnswerAndOperator(sql(s"SELECT s FROM t WHERE s rlike '$backreference'"))
    }
  }
}

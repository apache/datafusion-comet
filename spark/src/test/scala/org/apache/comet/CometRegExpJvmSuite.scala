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
import org.apache.spark.sql.comet.{CometFilterExec, CometProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

// Regex expressions other than in-subset `rlike` run through the codegen dispatcher by default
// (Spark's own code, enabled by default) rather than the native rust path.
class CometRegExpJvmSuite extends CometTestBase with AdaptiveSparkPlanHelper {

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

  // ========== rlike tests ==========

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

  test("rlike: invalid pattern falls back to Spark") {
    withSubjects("a") {
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

  private def withRLikeExplain(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE)(f)
  }

  private def explainOf(df: org.apache.spark.sql.DataFrame): String =
    new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)

  private def assertSparkRegexError(query: String): Unit = {
    def collectError(): Throwable =
      intercept[Throwable](sql(query).collect())

    def chain(ex: Throwable): List[Throwable] =
      Iterator.iterate(ex)(_.getCause).takeWhile(_ != null).toList

    var sparkEx: Throwable = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkEx = collectError()
    }
    val cometEx = collectError()
    val sparkMsgs = chain(sparkEx).flatMap(e => Option(e.getMessage)).mkString("\n")
    val cometMsgs = chain(cometEx).flatMap(e => Option(e.getMessage)).mkString("\n")
    assert(
      sparkMsgs.toLowerCase.contains("unclosed") || sparkMsgs.contains("PatternSyntax") ||
        sparkMsgs.toLowerCase.contains("regex"),
      s"Spark error did not look like a regex syntax error: $sparkMsgs")
    assert(
      cometMsgs.toLowerCase.contains("unclosed") || cometMsgs.contains("PatternSyntax") ||
        cometMsgs.toLowerCase.contains("regex"),
      s"Comet error did not look like a regex syntax error: $cometMsgs")
    val sparkTypes = chain(sparkEx).map(_.getClass.getName)
    val cometTypes = chain(cometEx).map(_.getClass.getName)
    assert(
      sparkTypes.exists(cometTypes.contains),
      s"Comet exception types $cometTypes did not share a type with Spark $sparkTypes")
  }

  test("rlike: safe literal pattern takes the native path by default") {
    withRLikeExplain {
      withSubjects("abc123", "xyz", null, "abc") {
        val df = sql("SELECT s, s rlike 'abc[0-9]+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          !explain.contains("JVM codegen dispatcher: rlike"),
          s"expected native path for in-subset pattern, got:\n$explain")
      }
    }
  }

  test("rlike: Rust class set operations stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("~", "a", "b", "x", null) {
        Seq("[a~~b]", "[^a~~b]").foreach { pat =>
          val df = sql(s"SELECT s, s rlike '$pat' FROM t")
          checkSparkAnswerAndOperator(df)
          assert(
            explainOf(df).contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for $pat, got:\n${explainOf(df)}")
        }
      }
      withSubjects("b", "a", "z", "-", null) {
        val df = sql("SELECT s, s rlike '[a-z--b]' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for [a-z--b], got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: leading-bracket class ranges stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("_", "-", "]", "a", "z", null) {
        Seq("[]-a]", "[^]-a]").foreach { pat =>
          val df = sql(s"SELECT s, s rlike '$pat' FROM t")
          checkSparkAnswerAndOperator(df)
          assert(
            explainOf(df).contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for [$pat], got:\n${explainOf(df)}")
        }
      }
    }
  }

  test("rlike: raw [ range endpoint stays on the dispatcher and preserves Spark error") {
    withRLikeExplain {
      withSubjects("@", "[", "A", null) {
        Seq("[@-[]", "[^@-[]").foreach { pat =>
          val query = s"SELECT s, s rlike '$pat' FROM t"
          val df = sql(query)
          assert(
            explainOf(df).contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for $pat, got:\n${explainOf(df)}")
          assertSparkRegexError(query)
        }
      }
    }
  }

  test("rlike: over-budget counted repetition stays on the dispatcher") {
    withRLikeExplain {
      withSubjects("a", "aaa", null) {
        val df = sql("SELECT s, s rlike 'a{1000000}' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for a{1000000}, got:\n${explainOf(df)}")
      }
      withSubjects(";", "x", "xx", null) {
        val df = sql("SELECT s, s rlike '[^;]{20000}' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for [^;]{20000}, got:\n${explainOf(df)}")
      }
      withSubjects("a", "aaa", null) {
        val df = sql("SELECT s, s rlike '(a{100}){100}' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for (a{100}){100}, got:\n${explainOf(df)}")
      }
      withSubjects("", "x", ";" * 256, null) {
        val pat = "(([^;]{256}){0,}){256}"
        val df = sql(s"SELECT s, s rlike '$pat' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for $pat, got:\n${explainOf(df)}")
      }
      withSubjects("a", "b", null) {
        val nested = "(" * 33 + "a" + ")" * 33
        val df = sql(s"SELECT s, s rlike '$nested' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for 33 nested groups, got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: compile-budget boundary stays native") {
    withRLikeExplain {
      withSubjects("a", "aaa", null) {
        val pat = "(?:a{64}){64}"
        val df = sql(s"SELECT s, s rlike '$pat' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          !explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected native path for expansion-4096 pattern, got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: exact-zero counted repetitions stay native") {
    withRLikeExplain {
      withSubjects("", "x", ";" * 256, null) {
        Seq("(([^;]{256}){0}){256}", "(([^;]{256}){0,0}){256}").foreach { pat =>
          val df = sql(s"SELECT s, s rlike '$pat' FROM t")
          checkSparkAnswerAndOperator(df)
          assert(
            !explainOf(df).contains("JVM codegen dispatcher: rlike"),
            s"expected native path for exact-zero pattern $pat, got:\n${explainOf(df)}")
        }
      }
    }
  }

  test("rlike: aggregate expansion budget stays on the dispatcher") {
    withRLikeExplain {
      withSubjects("a", "aaa", null) {
        Seq("a{256}" * 17, "a{0}" * 4097).foreach { pat =>
          val df = sql(s"SELECT s, s rlike '$pat' FROM t")
          checkSparkAnswerAndOperator(df)
          assert(
            explainOf(df).contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for aggregate over-budget pattern, got:\n${explainOf(df)}")
        }
      }
    }
  }

  test("rlike: nested quantified stars stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("b", "c", null) {
        val q = (1 to 30).foldLeft("a") { (p, _) => s"($p)*" }
        val pat = s"(($q){255}){16}b"
        val df = sql(s"SELECT s, s rlike '$pat' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for nested quantified stars, got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: capturing groups copied by counted repetition stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("x", ";", null) {
        val q = (1 to 7).foldLeft("[^;]") { (p, _) => s"($p)*" }
        val wrapped = ("(" * 16) + q + (")" * 16)
        val pat = (wrapped + "{256}") * 16
        val df = sql(s"SELECT s, s rlike '$pat' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for capture-cost residual, got:\n${explainOf(df)}")
      }
      withSubjects("x", "y", null) {
        val df = sql("SELECT s, s rlike '[^x]{256}' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          !explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected native path for [^x]{256}, got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: unsafe literal pattern stays on the JVM dispatcher by default") {
    withRLikeExplain {
      withSubjects("abc123", "no digits", null) {
        val df = sql("SELECT s, s rlike '\\\\d+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          explain.contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher path for out-of-subset pattern, got:\n$explain")
      }
    }
  }

  test("rlike: unsafe but Rust-accepted pattern is native after opt-in") {
    withRLikeExplain {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey("RLike") -> "true") {
        withSubjects("abc123", "no digits", null) {
          val df = sql("SELECT s, s rlike '\\\\d+' FROM t")
          checkSparkAnswerAndOperator(df)
          val explain = explainOf(df)
          assert(
            !explain.contains("JVM codegen dispatcher: rlike"),
            s"expected native path after allowIncompatible, got:\n$explain")
        }
      }
    }
  }

  test("rlike: non-literal pattern stays on the dispatcher") {
    withRLikeExplain {
      withTable("t") {
        sql("CREATE TABLE t (s STRING, p STRING) USING parquet")
        sql("INSERT INTO t VALUES ('abc123', 'abc[0-9]+'), ('xyz', 'xyz')")
        val df = sql("SELECT s, s rlike p FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          explain.contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher path for non-literal pattern, got:\n$explain")
      }
    }
  }

  test("rlike: null literal pattern stays on the dispatcher") {
    // NullPropagation would replace `s rlike NULL` with a null literal before serde sees it.
    withRLikeExplain {
      withSQLConf(
        "spark.sql.optimizer.excludedRules" ->
          "org.apache.spark.sql.catalyst.optimizer.NullPropagation") {
        withSubjects("a", "b", null) {
          val df = sql("SELECT s rlike CAST(NULL AS STRING) FROM t")
          checkSparkAnswerAndOperator(df)
          val explain = explainOf(df)
          assert(
            explain.contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher path for null literal pattern, got:\n$explain")
        }
      }
    }
  }

  test("rlike: unsafe pattern falls back to Spark when the dispatcher is disabled") {
    withSubjects("abc123", "no digits", null) {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          sql("SELECT s, s rlike '\\\\d+' FROM t"),
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key + "=false")
      }
    }
  }

  test("rlike: in-subset pattern stays native when the dispatcher is disabled") {
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE) {
      withSubjects("abc123", "xyz", null, "abc") {
        val df = sql("SELECT s, s rlike 'abc[0-9]+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          !explain.contains("JVM codegen dispatcher: rlike"),
          s"expected native path with dispatcher disabled, got:\n$explain")
        assert(
          !explain.contains(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key),
          s"in-subset rlike must not fall back when the dispatcher is off, got:\n$explain")
      }
    }
  }

  test("rlike: invalid pattern preserves Spark exception type and message") {
    withSubjects("a") {
      assertSparkRegexError("SELECT s rlike '[' FROM t")
    }
  }

  test("rlike: Java-only pattern with allowIncompatible keeps existing opt-in behavior") {
    // Pre-existing: opt-in sends a non-null default-collation literal to native. Rust cannot
    // compile lookaround, so native plan construction fails. This PR does not add a fallback
    // for that case.
    withSQLConf(CometConf.getExprAllowIncompatConfigKey("RLike") -> "true") {
      withSubjects("foobar") {
        val ex = intercept[Throwable](sql(s"SELECT s rlike '$lookahead' FROM t").collect())
        val msgs =
          Iterator
            .iterate(ex)(_.getCause)
            .takeWhile(_ != null)
            .flatMap(e => Option(e.getMessage))
            .mkString("\n")
        assert(
          msgs.toLowerCase.contains("pattern") || msgs.toLowerCase.contains("regex") ||
            msgs.toLowerCase.contains("look"),
          s"expected native compile failure for lookaround under opt-in, got: $msgs")
      }
    }
  }

  test("rlike: UTF8_BINARY safe literal is native on Spark 4") {
    assume(isSpark40Plus)
    withRLikeExplain {
      withSubjects("abc123", "xyz") {
        val df = sql("SELECT s rlike 'abc[0-9]+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          !explain.contains("JVM codegen dispatcher: rlike"),
          s"expected native path for UTF8_BINARY, got:\n$explain")
      }
    }
  }

  test("rlike: non-default collation on the subject stays on the dispatcher") {
    assume(isSpark40Plus)
    withRLikeExplain {
      withSubjects("abc123", "ABC123", null) {
        val df = sql("SELECT CAST(s AS STRING COLLATE UTF8_LCASE) rlike 'abc[0-9]+' FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          explain.contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for collated subject, got:\n$explain")
      }
    }
  }

  test("rlike: non-default collation on the pattern stays on the dispatcher") {
    assume(isSpark40Plus)
    withRLikeExplain {
      withSubjects("abc123", "xyz", null) {
        val df = sql("SELECT s rlike CAST('abc[0-9]+' AS STRING COLLATE UTF8_LCASE) FROM t")
        checkSparkAnswerAndOperator(df)
        val explain = explainOf(df)
        assert(
          explain.contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for collated pattern, got:\n$explain")
      }
    }
  }

  test("rlike: collated subject stays on dispatcher even with allowIncompatible") {
    assume(isSpark40Plus)
    withRLikeExplain {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey("RLike") -> "true") {
        withSubjects("abc123", "ABC123", null) {
          val df = sql("SELECT CAST(s AS STRING COLLATE UTF8_LCASE) rlike 'abc[0-9]+' FROM t")
          checkSparkAnswerAndOperator(df)
          val explain = explainOf(df)
          assert(
            explain.contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for collated subject with allowIncompatible, got:\n$explain")
        }
      }
    }
  }

  test("rlike: collated pattern stays on dispatcher even with allowIncompatible") {
    assume(isSpark40Plus)
    withRLikeExplain {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey("RLike") -> "true") {
        withSubjects("abc123", "xyz", null) {
          val df = sql("SELECT s rlike CAST('abc[0-9]+' AS STRING COLLATE UTF8_LCASE) FROM t")
          checkSparkAnswerAndOperator(df)
          val explain = explainOf(df)
          assert(
            explain.contains("JVM codegen dispatcher: rlike"),
            s"expected dispatcher for collated pattern with allowIncompatible, got:\n$explain")
        }
      }
    }
  }

  // ========== regexp_extract tests ==========

  test("regexp_extract: basic group extraction") {
    withSubjects("abc123def", "no match", null, "xyz789") {
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract(s, '([a-z]+)(\\\\d+)', 1) FROM t"))
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract(s, '([a-z]+)(\\\\d+)', 2) FROM t"))
    }
  }

  test("regexp_extract: group 0 returns entire match") {
    withSubjects("hello world", "foo123bar", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract(s, '\\\\d+', 0) FROM t"))
    }
  }

  test("regexp_extract: no match returns empty string") {
    withSubjects("abc", "def", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract(s, '\\\\d+', 0) FROM t"))
    }
  }

  test("regexp_extract: backreference pattern (Java-only)") {
    withSubjects("aa", "ab", "bb", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract(s, '(\\\\w)\\\\1', 0) FROM t"))
    }
  }

  test("regexp_extract: lookahead pattern (Java-only)") {
    withSubjects("foobar", "foobaz", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract(s, 'foo(?=bar)', 0) FROM t"))
    }
  }

  test("regexp_extract: embedded flags (Java-only)") {
    withSubjects("FOO123", "foo456", "bar789") {
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract(s, '(?i)(foo)(\\\\d+)', 2) FROM t"))
    }
  }

  test("regexp_extract: all-null column") {
    withSubjects(null, null, null) {
      checkSparkAnswerAndOperator(sql("SELECT regexp_extract(s, '(\\\\d+)', 1) FROM t"))
    }
  }

  // ========== regexp_extract_all tests ==========

  test("regexp_extract_all: basic extraction of all matches") {
    withSubjects("abc123def456", "no match", null, "x1y2z3") {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract_all(s, '(\\\\d+)', 1) FROM t"))
    }
  }

  test("regexp_extract_all: group 0 returns full matches") {
    withSubjects("cat bat hat", "no vowels", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract_all(s, '[a-z]at', 0) FROM t"))
    }
  }

  test("regexp_extract_all: multiple groups") {
    withSubjects("a1b2c3", "x9y8", null) {
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract_all(s, '([a-z])(\\\\d)', 1) FROM t"))
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract_all(s, '([a-z])(\\\\d)', 2) FROM t"))
    }
  }

  test("regexp_extract_all: no matches returns empty array") {
    withSubjects("abc", "def") {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract_all(s, '\\\\d+', 0) FROM t"))
    }
  }

  test("regexp_extract_all: lookahead pattern (Java-only)") {
    withSubjects("foobar foobaz fooqux") {
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract_all(s, 'foo(?=ba[rz])', 0) FROM t"))
    }
  }

  // ========== regexp_replace tests ==========

  test("regexp_replace: basic replacement") {
    withSubjects("abc123def456", "no digits", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '\\\\d+', 'NUM') FROM t"))
    }
  }

  test("regexp_replace: backreference in pattern (Java-only)") {
    withSubjects("aabbcc", "abcabc", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '(\\\\w)\\\\1', 'X') FROM t"))
    }
  }

  test("regexp_replace: backreference in replacement") {
    withSubjects("hello world", "foo bar", null) {
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_replace(s, '(\\\\w+) (\\\\w+)', '$2 $1') FROM t"))
    }
  }

  test("regexp_replace: lookahead pattern (Java-only)") {
    withSubjects("foobar", "foobaz", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, 'foo(?=bar)', 'XXX') FROM t"))
    }
  }

  test("regexp_replace: empty pattern replaces between characters") {
    withSubjects("abc", "", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '', '-') FROM t"))
    }
  }

  test("regexp_replace: all-null column") {
    withSubjects(null, null, null) {
      checkSparkAnswerAndOperator(sql("SELECT regexp_replace(s, '\\\\d', 'X') FROM t"))
    }
  }

  test("regexp_replace: invalid group reference surfaces the original Spark exception") {
    // Mirrors Spark's StringFunctionsSuite "RegExpReplace throws the right exception when replace
    // fails on a particular row": a replacement that references a group the pattern does not
    // define makes Spark's codegen raise SparkRuntimeException(INVALID_REGEXP_REPLACE). The
    // codegen dispatcher runs that same Spark code inside native execution, so the original
    // SparkRuntimeException must surface unwrapped rather than as a CometNativeException.
    assume(org.apache.comet.CometSparkSessionExtensions.isSpark40Plus)
    withSubjects("first last") {
      val df =
        sql("SELECT regexp_replace(s, '(?<first>[a-zA-Z]+) (?<last>[a-zA-Z]+)', '$3 $1') FROM t")
      val e = intercept[Throwable](df.collect())
      val chain = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toList
      val names = chain.map(_.getClass.getName)
      // The original Spark exception must survive: re-thrown unwrapped, not flattened into a
      // CometNativeException at the JNI boundary.
      assert(
        names.contains("org.apache.spark.SparkRuntimeException"),
        s"expected SparkRuntimeException in the cause chain, got: $names")
      assert(
        !names.contains("org.apache.comet.CometNativeException"),
        s"native exception leaked across the boundary: $names")
      assert(e.getMessage.contains("INVALID_REGEXP_REPLACE"))
    }
  }

  // ========== regexp_instr tests ==========

  test("regexp_instr: basic position finding") {
    withSubjects("abc123def", "no match", null, "456xyz") {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '\\\\d+', 0) FROM t"))
    }
  }

  test("regexp_instr: specific group position") {
    withSubjects("abc123def456", "xyz", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '([a-z]+)(\\\\d+)', 1) FROM t"))
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '([a-z]+)(\\\\d+)', 2) FROM t"))
    }
  }

  test("regexp_instr: no match returns 0") {
    withSubjects("abc", "def", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '\\\\d+', 0) FROM t"))
    }
  }

  test("regexp_instr: lookahead (Java-only)") {
    withSubjects("foobar", "foobaz", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, 'foo(?=bar)', 0) FROM t"))
    }
  }

  // ========== split tests ==========

  test("split: basic regex split") {
    withSubjects("a,b,c", "x,,y", null, "single") {
      checkSparkAnswerAndOperator(sql("SELECT s, split(s, ',') FROM t"))
    }
  }

  test("split: regex pattern") {
    withSubjects("abc123def456ghi", "no-digits", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, split(s, '\\\\d+') FROM t"))
    }
  }

  test("split: with limit") {
    withSubjects("a,b,c,d,e") {
      checkSparkAnswerAndOperator(sql("SELECT s, split(s, ',', 3) FROM t"))
    }
  }

  test("split: limit -1 returns all") {
    withSubjects("a,,b,,c") {
      checkSparkAnswerAndOperator(sql("SELECT s, split(s, ',', -1) FROM t"))
    }
  }

  test("split: lookahead pattern (Java-only)") {
    withSubjects("camelCaseString", "anotherOne", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, split(s, '(?=[A-Z])') FROM t"))
    }
  }

  test("split: all-null column") {
    withSubjects(null, null, null) {
      checkSparkAnswerAndOperator(sql("SELECT split(s, ',') FROM t"))
    }
  }

  // ========== multi-batch and combined tests ==========

  test("regexp_extract: many rows spanning multiple batches") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val values = (0 until 5000)
        .map(i => if (i % 7 == 0) "(NULL)" else s"('item_${i}_value')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $values")
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract(s, 'item_(\\\\d+)_value', 1) FROM t"))
    }
  }

  test("all regexp expressions combined in one query") {
    withSubjects("abc123def456", "hello world", null, "aa") {
      checkSparkAnswerAndOperator(sql("""
          |SELECT
          |  s,
          |  s rlike '\\d+' AS has_digits,
          |  regexp_extract(s, '(\\d+)', 1) AS first_num,
          |  regexp_replace(s, '\\d+', 'N') AS replaced,
          |  regexp_instr(s, '\\d+', 0) AS num_pos
          |FROM t
          |""".stripMargin))
    }
  }

  test("expressions with no native path always run via the JVM dispatcher") {
    withSubjects("abc123def", "no match", null, "xyz789") {
      // regexp_extract / regexp_extract_all / regexp_instr have no native rust path, so they
      // always run on Comet via the JVM codegen dispatcher rather than falling back to Spark.
      checkSparkAnswerAndOperator(
        sql("SELECT s, regexp_extract(s, '([a-z]+)(\\\\d+)', 2) FROM t"))
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_extract_all(s, '\\\\d+', 0) FROM t"))
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '\\\\d+', 0) FROM t"))
    }
  }
}

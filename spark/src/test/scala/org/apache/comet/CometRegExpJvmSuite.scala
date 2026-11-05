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
import org.apache.spark.sql.functions.col

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}

// Regex expressions other than in-subset `rlike` run through the codegen dispatcher by default
// (Spark's own code, enabled by default) rather than the native rust path.
class CometRegExpJvmSuite extends CometTestBase {

  // A Java-only pattern used to check the opt-in behavior when Rust rejects it.
  private val lookahead = "foo(?=bar)"

  // Same structure as Sunchao's 4096-branch / 131,072-capture reproducer
  // (PR #5415). CometRegexSuite asserts the full pattern on the analyzer
  // only. This suite uses a 64-branch shrink that still exceeds MaxExpansion
  // after capture-state cost is charged at group construction (64 * 65 = 4160).
  private def uncountedCapturePattern(branches: Int): String = {
    val q = (1 to 8).foldLeft("a")((p, _) => s"($p)*")
    val branch = "(" * 24 + q + ")" * 24
    Seq.fill(branches)(branch).mkString("|")
  }

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

  private def withRLikeExplain(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.key -> "true",
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE)(f)
  }

  private def explainOf(df: org.apache.spark.sql.DataFrame): String =
    new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)

  private def codegenDispatchExpressions(df: org.apache.spark.sql.DataFrame): Seq[String] =
    new ExtendedExplainInfo().getCodegenDispatchExpressions(df.queryExecution.executedPlan)

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

  test("rlike: uncounted capturing groups stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("", "a", null) {
        val pattern = uncountedCapturePattern(branches = 64)
        val df = spark.table("t").select(col("s"), col("s").rlike(pattern))
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for uncounted capture budget residual, got:\n${explainOf(df)}")
      }
    }
  }

  test("rlike: uncounted capturing groups fall back to Spark when the dispatcher is disabled") {
    withSubjects("", "a", null) {
      withSQLConf(
        CometConf.getExprAllowIncompatConfigKey("RLike") -> "false",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        val pattern = uncountedCapturePattern(branches = 64)
        val df = spark.table("t").select(col("s"), col("s").rlike(pattern))
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          df,
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key + "=false")
        val explain = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
        assert(
          !explain.toLowerCase.contains("compiledtoobig"),
          s"native compile must not run for over-budget captures, got:\n$explain")
        assert(
          !explain.contains("JVM codegen dispatcher: rlike"),
          s"dispatcher is disabled; expected Spark fallback, got:\n$explain")
      }
    }
  }

  test(
    "rlike: full uncounted-capture reproducer falls back to Spark when JVM dispatcher is disabled") {
    assume(isSpark40Plus && !isSpark41Plus, "full 4096-branch reproducer is Spark 4.0-only")
    assume(
      sys.props("java.specification.version") == "21",
      "full 4096-branch reproducer is JDK 21-only")
    withSubjects("", "a", null) {
      withSQLConf(
        CometConf.getExprAllowIncompatConfigKey("RLike") -> "false",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        val pattern = uncountedCapturePattern(branches = 4096)
        assert(pattern.length == 303103)
        val df = spark.table("t").select(col("s"), col("s").rlike(pattern))
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          df,
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key + "=false")
        val explain = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
        assert(
          !explain.toLowerCase.contains("compiledtoobig"),
          "native compilation must not run for the full uncounted-capture reproducer, " +
            s"but the plan contained a compiler-budget failure:\n$explain")
      }
    }
  }

  test("rlike: repeated complex character classes stay on the dispatcher") {
    withRLikeExplain {
      withSubjects("x", ";", null) {
        val cls = """[^ "$&(*,.02468:<>@BDFHJLNPRTVXZ\\\^`bdfhjlnprtvxz|~]"""
        val body = (1 to 7).foldLeft(cls)((p, _) => s"(?:$p)*")
        val pattern = s"(?:$body){256}" * 16
        assert(pattern.length == 1552)
        val df = spark.table("t").select(col("s"), col("s").rlike(pattern))
        checkSparkAnswerAndOperator(df)
        assert(
          explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected dispatcher for complex character-class residual, got:\n${explainOf(df)}")
      }
      withSubjects("x", "y", null) {
        val df = sql("SELECT s, s rlike '[a-zA-Z0-9_]{256}' FROM t")
        checkSparkAnswerAndOperator(df)
        assert(
          !explainOf(df).contains("JVM codegen dispatcher: rlike"),
          s"expected native path for [a-zA-Z0-9_]{256}, got:\n${explainOf(df)}")
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
          codegenDispatchExpressions(df).contains("rlike"),
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
          codegenDispatchExpressions(df).contains("rlike"),
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
            codegenDispatchExpressions(df).contains("rlike"),
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
            codegenDispatchExpressions(df).contains("rlike"),
            s"expected dispatcher for collated pattern with allowIncompatible, got:\n$explain")
        }
      }
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
      val chain = causeChain(e)
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

  test("regexp_replace: invalid group reference in a filter surfaces the original exception") {
    // DataFusion's parquet RowFilter flattens a predicate's error to a string, which would lose
    // the Java throwable. A filter that calls into the JVM must therefore stay out of the scan
    // even with row-level pushdown on, so it fails in the Filter above the scan instead.
    assume(isSpark40Plus)
    withSQLConf(CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withSubjects("first last") {
        val df = sql(
          "SELECT COUNT(*) FROM t " +
            "WHERE regexp_replace(s, '(?<first>[a-zA-Z]+) (?<last>[a-zA-Z]+)', '$3 $1') " +
            "IS NOT NULL")
        val names = causeChain(intercept[Throwable](df.collect())).map(_.getClass.getName)
        assert(
          names.contains("org.apache.spark.SparkRuntimeException"),
          s"expected SparkRuntimeException in the cause chain, got: $names")
        assert(
          !names.contains("org.apache.comet.CometNativeException"),
          s"native exception leaked across the boundary: $names")
      }
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

  // ========== combined tests ==========

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
}

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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.udf.CometCodegenDispatchUDF

/**
 * Randomized tests for the Arrow-direct codegen dispatcher. Generates string inputs at varying
 * null densities and runs a fixed set of regex patterns through both Spark and the codegen
 * dispatcher, asserting results agree. Fixes a seed per test for reproducibility.
 *
 * Scope of this pass: the string surface the dispatcher currently exercises end to end (rlike and
 * regexp_replace). Broader cross-type fuzz, including primitive inputs, multi-column expressions,
 * and view-type variants, lands once more serdes route through codegen dispatch.
 *
 * Pinned to `mode=force` so every eligible query is guaranteed to route through the dispatcher
 * rather than the hand-coded regex UDF, keeping the fuzz focused on the codegen path.
 */
class CometCodegenDispatchFuzzSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_REGEXP_ENGINE.key, CometConf.REGEXP_ENGINE_JAVA)
      .set(CometConf.COMET_CODEGEN_DISPATCH_MODE.key, CometConf.CODEGEN_DISPATCH_FORCE)

  private val RowCount: Int = 512
  private val MaxStringLen: Int = 32

  /**
   * Characters the generator picks from. Mix of digits, lowercase, uppercase, and a couple of
   * non-alphanumerics to exercise classes, anchors, and alternations.
   */
  private val charPalette: Array[Char] =
    ("0123456789" +
      "abcdefghijklmnopqrstuvwxyz" +
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "_-. ").toCharArray

  private def randomString(rng: Random): String = {
    val len = rng.nextInt(MaxStringLen + 1)
    val sb = new StringBuilder(len)
    var i = 0
    while (i < len) {
      sb.append(charPalette(rng.nextInt(charPalette.length)))
      i += 1
    }
    sb.toString
  }

  /**
   * Generate `RowCount` strings with the requested null density. Seeded for determinism. Empty
   * strings and nulls are both part of the distribution when density > 0.
   */
  private def generateSubjects(seed: Long, nullDensity: Double): Seq[String] = {
    val rng = new Random(seed)
    (0 until RowCount).map { _ =>
      if (rng.nextDouble() < nullDensity) null
      else randomString(rng)
    }
  }

  /**
   * Resets dispatcher stats, runs `f`, then asserts the codegen path actually ran for at least
   * one batch. Without this, a silent serde fallback would let the fuzz pass trivially because
   * both Spark and whatever-Comet-ran-instead agree with Spark.
   */
  private def assertCodegenRan(f: => Unit): Unit = {
    CometCodegenDispatchUDF.resetStats()
    f
    val after = CometCodegenDispatchUDF.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected at least one codegen dispatcher invocation during this query, got $after")
  }

  /** Create a temp table `t(s STRING)` populated with the given subjects, run `f`, then drop. */
  private def withSubjectTable(subjects: Seq[String])(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      if (subjects.nonEmpty) {
        val escaped = subjects.map { v =>
          if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')"
        }
        // Insert in chunks so the generated VALUES list doesn't blow the SQL parser.
        escaped.grouped(64).foreach { batch =>
          sql(s"INSERT INTO t VALUES ${batch.mkString(", ")}")
        }
      }
      f
    }
  }

  // Regex patterns chosen to span common rlike shapes and the Java-only backreference feature.
  // All are Spark-compatible under the java regex engine the codegen path uses.
  private val rlikePatterns: Seq[String] =
    Seq("\\\\d+", "^[a-z]", "[A-Z][0-9]+", "(ab){2,}", "^(\\\\w)\\\\1", "_.*\\\\.", "^$")

  // regexp_replace (pattern, replacement) pairs. Mix of no-match, narrow match, wide match.
  private val regexpReplacePatterns: Seq[(String, String)] = Seq(
    "\\\\d+" -> "N",
    "[a-z]+" -> "L",
    "[aeiouAEIOU]" -> "*",
    "xyzzy" -> "<none>",
    "\\\\s+" -> "_")

  private val nullDensities: Seq[Double] = Seq(0.0, 0.1, 0.5, 1.0)

  for {
    density <- nullDensities
    pattern <- rlikePatterns
  } {
    test(s"fuzz rlike pattern='$pattern' nullDensity=$density") {
      val subjects = generateSubjects(seed = pattern.hashCode.toLong ^ density.hashCode, density)
      withSubjectTable(subjects) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
        }
      }
    }
  }

  for {
    density <- nullDensities
    (pattern, replacement) <- regexpReplacePatterns
  } {
    test(
      s"fuzz regexp_replace pattern='$pattern' replacement='$replacement' nullDensity=$density") {
      val seed = (pattern + replacement).hashCode.toLong ^ density.hashCode
      val subjects = generateSubjects(seed = seed, density)
      withSubjectTable(subjects) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql(s"SELECT regexp_replace(s, '$pattern', '$replacement') FROM t"))
        }
      }
    }
  }

  /**
   * Multi-column fuzz via expression composition. The rlike serde is single-input from its own
   * point of view, but its subject can be an arbitrary sub-expression that references multiple
   * columns. `concat(c1, c2) rlike 'pat'` is the simplest such shape, and it exercises the
   * kernel's two-column `inputSchema` path plus the NullIntolerant short-circuit gating (Concat
   * is not NullIntolerant, so the whole-tree guard in `defaultBody` must skip the short-circuit
   * for this shape; Spark's own Concat codegen handles nulls correctly).
   */
  private def withTwoColumnTable(c1Values: Seq[String], c2Values: Seq[String])(
      f: => Unit): Unit = {
    require(
      c1Values.length == c2Values.length,
      s"columns must be same length: c1=${c1Values.length}, c2=${c2Values.length}")
    withTable("t") {
      sql("CREATE TABLE t (c1 STRING, c2 STRING) USING parquet")
      if (c1Values.nonEmpty) {
        val rows = c1Values.zip(c2Values).map { case (a, b) =>
          val av = if (a == null) "NULL" else s"'${a.replace("'", "''")}'"
          val bv = if (b == null) "NULL" else s"'${b.replace("'", "''")}'"
          s"($av, $bv)"
        }
        rows.grouped(64).foreach { batch =>
          sql(s"INSERT INTO t VALUES ${batch.mkString(", ")}")
        }
      }
      f
    }
  }

  private val twoColumnPatterns: Seq[String] = Seq("[0-9]+", "^[a-z]", "[A-Z][0-9]+")
  private val perColumnNullDensities: Seq[Double] = Seq(0.0, 0.25, 1.0)

  for {
    d1 <- perColumnNullDensities
    d2 <- perColumnNullDensities
    pattern <- twoColumnPatterns
  } {
    test(s"fuzz concat(c1,c2) rlike '$pattern' nullDensity=($d1,$d2)") {
      val seed = (pattern.hashCode.toLong ^ d1.hashCode) * 31 + d2.hashCode
      val c1 = generateSubjects(seed, d1)
      val c2 = generateSubjects(seed ^ 0x5f3759df, d2)
      withTwoColumnTable(c1, c2) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql(s"SELECT concat(c1, c2) rlike '$pattern' FROM t"))
        }
      }
    }
  }
}

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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}

/**
 * Tests the tolerance comparison behind `query tolerance=` in Comet SQL tests and the
 * `checkSparkAnswer*WithTol*` helpers. The DataFrame stands in for the Comet result and the rows
 * for the Spark answer.
 */
class CometAnswerToleranceSuite extends CometTestBase {
  import testImplicits._

  private def assertMatch(comet: DataFrame, spark: Row*): Unit =
    checkCometAnswerWithTolerance(comet, spark, 1e-6)

  private def assertMismatch(comet: DataFrame, spark: Row*): Unit = {
    val e = intercept[TestFailedException](checkCometAnswerWithTolerance(comet, spark, 1e-6))
    assert(e.getMessage.startsWith("Results do not match within tolerance"), e.getMessage)
  }

  test("finite values match within the tolerance") {
    assertMatch(Seq(1.0 + 1e-7).toDF("d"), Row(1.0))
    assertMismatch(Seq(1.0 + 1e-5).toDF("d"), Row(1.0))
    assertMatch(Seq(1.0f).toDF("f"), Row(1.0f))
    assertMismatch(Seq(1.001f).toDF("f"), Row(1.0f))
  }

  test("NaN matches only NaN") {
    assertMatch(Seq(Double.NaN).toDF("d"), Row(Double.NaN))
    assertMismatch(Seq(Double.NaN).toDF("d"), Row(1.0))
    assertMismatch(Seq(1.0).toDF("d"), Row(Double.NaN))
    assertMismatch(Seq(Double.NaN).toDF("d"), Row(Double.PositiveInfinity))
    assertMatch(Seq(Float.NaN).toDF("f"), Row(Float.NaN))
    assertMismatch(Seq(Float.NaN).toDF("f"), Row(1.0f))
    assertMismatch(Seq(1.0f).toDF("f"), Row(Float.NaN))
  }

  test("infinities compare with their sign") {
    assertMatch(Seq(Double.PositiveInfinity).toDF("d"), Row(Double.PositiveInfinity))
    assertMatch(Seq(Double.NegativeInfinity).toDF("d"), Row(Double.NegativeInfinity))
    assertMismatch(Seq(Double.PositiveInfinity).toDF("d"), Row(Double.NegativeInfinity))
    assertMismatch(Seq(Double.NegativeInfinity).toDF("d"), Row(Double.PositiveInfinity))
    assertMismatch(Seq(Double.MaxValue).toDF("d"), Row(Double.PositiveInfinity))
    assertMismatch(Seq(Float.PositiveInfinity).toDF("f"), Row(Float.NegativeInfinity))
  }

  test("signed zeros match each other") {
    assertMatch(Seq(-0.0).toDF("d"), Row(0.0))
    assertMatch(Seq(0.0f).toDF("f"), Row(-0.0f))
  }

  test("other values compare exactly") {
    assertMatch(Seq(("a", 1.0)).toDF("s", "d"), Row("a", 1.0 + 1e-7))
    assertMismatch(Seq(("a", 1.0)).toDF("s", "d"), Row("b", 1.0))
    // Only top-level floating-point values get the tolerance.
    assertMismatch(Seq(Seq(1.0 + 1e-7)).toDF("a"), Row(Seq(1.0)))
    assertMismatch(Seq(Seq(-0.0)).toDF("a"), Row(Seq(0.0)))
    assertMatch(Seq(Seq(Double.NaN)).toDF("a"), Row(Seq(Double.NaN)))
  }

  test("rows are sorted before they are paired") {
    assertMatch(
      Seq(Some(Double.NaN), Some(1.0), None, Some(Double.NegativeInfinity), Some(-0.0)).toDF("d"),
      Row(0.0),
      Row(Double.NegativeInfinity),
      Row(null),
      Row(1.0),
      Row(Double.NaN))
    // Sorting by `toString` would put 1.2E-16 after 0.5 but 0.0 before it, and pair 0.5 with 0.0.
    assertMatch(Seq(0.5, 1.2246467991473532e-16).toDF("d"), Row(0.0), Row(0.5))
    assertMismatch(Seq(1.0, 2.0).toDF("d"), Row(1.0), Row(Double.NaN))
    assertMismatch(Seq(1.0, 2.0).toDF("d"), Row(1.0))
  }

  test("rows of an unordered query pair one-to-one") {
    // A leading value within tolerance must not pair rows whose other fields differ.
    assertMatch(Seq((1e-7, "a"), (0.0, "b")).toDF("d", "s"), Row(0.0, "a"), Row(0.0, "b"))
    assertMismatch(Seq((1e-7, "b"), (0.0, "b")).toDF("d", "s"), Row(0.0, "a"), Row(0.0, "b"))
    // The same with a second floating-point field, which sorting alone cannot pair.
    assertMatch(Seq((1e-7, 1.0), (0.0, 2.0)).toDF("x", "y"), Row(0.0, 1.0), Row(0.0, 2.0))
    // The first Spark row matches both Comet rows, the second only the first Comet row. Pairing
    // in order, or giving each Spark row the first free Comet row it matches, pairs the first
    // rows and leaves the second Spark row without a partner.
    assertMatch(Seq((2e-7, 5e-7), (3e-7, -8e-7)).toDF("x", "y"), Row(0.0, 0.0), Row(1e-7, 9e-7))
    // Every row matches a row on the other side, but not one of its own.
    assertMismatch(
      Seq((0.0, 0.0), (1.0, 1.0), (1.0, 1.0)).toDF("x", "y"),
      Row(0.0, 0.0),
      Row(0.0, 0.0),
      Row(1.0, 1.0))
    // Maps match whatever the order of their entries, which their string form does not ignore.
    assertMatch(
      Seq(Map("b" -> 2, "a" -> 1), Map("a" -> 2, "b" -> 1)).toDF("m"),
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("b" -> 1, "a" -> 2)))
    assertMismatch(
      Seq(Map("b" -> 2, "a" -> 1), Map("a" -> 1, "b" -> 2)).toDF("m"),
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("b" -> 1, "a" -> 2)))
  }

  test("rows of a sorted query are paired in order") {
    assertMatch(Seq(1.0, 2.0).toDF("d").orderBy($"d".desc), Row(2.0), Row(1.0))
    assertMismatch(Seq(1.0, 2.0).toDF("d").orderBy($"d".desc), Row(1.0), Row(2.0))
  }
}

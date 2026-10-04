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

  test("rows of a sorted query are paired in order") {
    assertMatch(Seq(1.0, 2.0).toDF("d").orderBy($"d".desc), Row(2.0), Row(1.0))
    assertMismatch(Seq(1.0, 2.0).toDF("d").orderBy($"d".desc), Row(1.0), Row(2.0))
  }
}

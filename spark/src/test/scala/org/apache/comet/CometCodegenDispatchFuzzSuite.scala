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
 * Randomized tests for the Arrow-direct codegen dispatcher. Generates inputs at varying null
 * densities and runs them through ScalaUDFs that route through the dispatcher, asserting Comet
 * results agree with Spark. Fixes a seed per test for reproducibility.
 */
class CometCodegenDispatchFuzzSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private val RowCount: Int = 512

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

  private val nullDensities: Seq[Double] = Seq(0.0, 0.1, 0.5, 1.0)

  /**
   * Randomized decimal identity UDF. Spans both sides of the `Decimal.MAX_LONG_DIGITS` (18)
   * boundary so each test hits one of the two specialized branches in the generated `getDecimal`
   * getter. Precisions are chosen to exercise: small short-precision, boundary short-precision
   * with varying scale, just-past-boundary long precision, and the max decimal128 precision.
   */
  private def generateDecimals(
      seed: Long,
      precision: Int,
      scale: Int,
      nullDensity: Double): Seq[java.math.BigDecimal] = {
    val rng = new Random(seed)
    val intDigits = precision - scale
    // `BigInt.apply(bits, rng)` samples uniformly on `[0, 2^bits - 1]`; bound to the decimal's
    // integer-part range (10^intDigits - 1) so the result fits the schema. `BigInteger.bitLength`
    // would overshoot slightly; min with the exact max is cheap insurance.
    val intMax = BigInt(10).pow(intDigits) - 1
    val bits = math.max(intMax.bitLength, 1)
    (0 until RowCount).map { _ =>
      if (rng.nextDouble() < nullDensity) null
      else {
        val mag = BigInt(bits, rng).min(intMax)
        val signed = if (rng.nextBoolean()) -mag else mag
        new java.math.BigDecimal(signed.bigInteger, scale)
      }
    }
  }

  private def withDecimalTable(decimalType: String, values: Seq[java.math.BigDecimal])(
      f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (d $decimalType) USING parquet")
      if (values.nonEmpty) {
        val rows = values.map { v =>
          if (v == null) "(NULL)" else s"(${v.toPlainString})"
        }
        rows.grouped(64).foreach { batch =>
          sql(s"INSERT INTO t VALUES ${batch.mkString(", ")}")
        }
      }
      f
    }
  }

  // (precision, scale) pairs spanning both sides of the MAX_LONG_DIGITS=18 boundary.
  private val decimalShapes: Seq[(Int, Int)] = Seq((9, 2), (18, 0), (18, 9), (19, 0), (38, 10))

  for {
    density <- nullDensities
    (precision, scale) <- decimalShapes
  } {
    test(s"decimal identity precision=$precision scale=$scale nullDensity=$density") {
      // Reuse one registered UDF name across iterations; Spark replaces by name. The Scala-side
      // signature uses `BigDecimal`, which Spark encodes as DecimalType(38, 18); an implicit Cast
      // from the column's DecimalType to the UDF's parameter type runs inside Spark's generated
      // code, but the column read still goes through our kernel's `getDecimal` which is the path
      // we're fuzzing.
      spark.udf.register("dec_id_fuzz", (d: java.math.BigDecimal) => d)
      val seed = ((precision * 31L) + scale) * 31L + density.hashCode
      val values = generateDecimals(seed, precision, scale, density)
      withDecimalTable(s"DECIMAL($precision, $scale)", values) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql("SELECT dec_id_fuzz(d) FROM t"))
        }
      }
    }
  }
}

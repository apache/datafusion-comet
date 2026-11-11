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

package org.apache.comet.serde

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.tags.ExpressionsGroup

/**
 * Pins the version table of every fix in `SparkFixes`, so that the boundaries are checked on all
 * Spark versions in one run and not only on the one a test suite runs against.
 */
@ExpressionsGroup
class SparkFixesSuite extends AnyFunSuite {

  private def checkVersions(name: String, fixed: String => Boolean)(
      expectations: (String, Boolean)*): Unit =
    expectations.foreach { case (version, expected) =>
      assert(fixed(version) == expected, s"$name for Spark $version should be $expected")
    }

  test("fixes by Spark version") {
    // SPARK-58821 and SPARK-60118 are in 4.0.5, 4.1.4 and 4.2.1.
    Seq(
      "3.4.3" -> false,
      "3.5.9" -> false,
      "4.0.4" -> false,
      "4.1.3" -> false,
      "4.2.0" -> false,
      "4.2.0-SNAPSHOT" -> false,
      "4.0.5" -> true,
      "4.1.4" -> true,
      "4.2.1" -> true,
      "4.3.0" -> true,
      "5.0.0" -> true).foreach { case (version, fixed) =>
      assert(SparkFixes.sequenceRecomputesOverflowedLength(version) == fixed, version)
      assert(SparkFixes.castsZeroWithAnyExponentToDecimal(version) == fixed, version)
      val sequence = if (fixed) "spark_sequence_recomputed" else "spark_sequence"
      assert(CometSequence.nativeFunction(version) == sequence, version)
    }
  }

  test("array set functions normalize floats by Spark version") {
    // 4.2.0 normalizes the arguments in the plan (SPARK-54918), and 4.0.5+, 4.1.4+ and 4.2.1+
    // normalize during evaluation (SPARK-59602). Earlier releases keep flat signed zeros apart.
    checkVersions("array set float normalization", SparkFixes.arraySetFunctionsNormalizeFloats)(
      "3.4.3" -> false,
      "3.5.9" -> false,
      "4.0.0" -> false,
      "4.0.4" -> false,
      "4.0.4-SNAPSHOT" -> false,
      "4.1.0" -> false,
      "4.1.3" -> false,
      "4.0.5" -> true,
      "4.0.10" -> true,
      "4.1.4" -> true,
      "4.1.10" -> true,
      "4.2.0" -> true,
      "4.2.0-SNAPSHOT" -> true,
      "4.2.1" -> true,
      "4.2.10" -> true,
      "4.3.0" -> true,
      "5.0.0" -> true)
  }

  test("regression aggregate flags follow the Spark patch release that changed them") {
    // Both fixes shipped mid-line, so the flag must flip at the exact patch and stay off for
    // earlier patches of the same minor version. A vendor suffix parses and a missing patch
    // reads as 0.
    checkVersions("slope pair-null filtering", SparkFixes.slopeFiltersVarByPairNulls)(
      "3.4.3" -> false,
      "3.5.0" -> false,
      "3.5.1" -> false,
      "3.5.2" -> true,
      "3.5.9" -> true,
      "3.5.10" -> true,
      "4.0.0" -> true,
      "4.1.3" -> true,
      "4.2.0-SNAPSHOT" -> true,
      "4.1.3-amzn-1" -> true,
      "3.5" -> false)

    checkVersions("regr_r2 degenerate-case swap", SparkFixes.r2DegenerateCasesSwapped)(
      "3.4.3" -> false,
      "3.5.3" -> false,
      "3.5.8" -> false,
      "3.5.9" -> true,
      "3.5.10" -> true,
      "4.0.1" -> false,
      "4.0.2" -> false,
      "4.0.3" -> true,
      "4.0.4" -> true,
      "4.1.0" -> false,
      "4.1.1" -> false,
      "4.1.2" -> true,
      "4.1.3" -> true,
      "4.2.0" -> true,
      "4.2.0-SNAPSHOT" -> true,
      "3.6.0" -> true,
      "5.0.0" -> true,
      "4.1.3-amzn-1" -> true,
      "3.5" -> false)
  }

  test("percentile keeps signed-zero keys from the Spark release that fixed it") {
    // SPARK-45599 is in 3.5.2 and 4.0.0.
    checkVersions("percentile signed-zero keys", SparkFixes.keepsSignedZeroKeys)(
      "3.4.3" -> false,
      "3.5.0" -> false,
      "3.5.1" -> false,
      "3.5" -> false,
      "3.5.2" -> true,
      "3.5.9" -> true,
      "4.0.0" -> true,
      "4.1.3" -> true,
      "4.2.0-SNAPSHOT" -> true,
      "4.1.3-amzn-1" -> true,
      "5.0.0" -> true)
  }

  test("a version that parses with a vendor suffix is read by its numbers") {
    // Spark's own parser ignores what follows the patch version after a '.' or a '-'.
    Seq("4.1.4-amzn-1", "4.0.5.7.3.2.30000-15", "4.2.1-SNAPSHOT").foreach { version =>
      assert(SparkFixes.sequenceRecomputesOverflowedLength(version), version)
      assert(SparkFixes.castsZeroWithAnyExponentToDecimal(version), version)
      assert(SparkFixes.arraySetFunctionsNormalizeFloats(version), version)
      assert(SparkFixes.slopeFiltersVarByPairNulls(version), version)
      assert(SparkFixes.r2DegenerateCasesSwapped(version), version)
      assert(SparkFixes.keepsSignedZeroKeys(version), version)
    }
  }

  test("a version that does not parse counts as unfixed") {
    // These are not versions a real build reports. Every check reads the version with the same
    // parser, so none of them takes a different rule for these strings than the others.
    val fixes: Seq[(String, String => Boolean)] = Seq(
      "sequenceRecomputesOverflowedLength" -> SparkFixes.sequenceRecomputesOverflowedLength,
      "castsZeroWithAnyExponentToDecimal" -> SparkFixes.castsZeroWithAnyExponentToDecimal,
      "arraySetFunctionsNormalizeFloats" -> SparkFixes.arraySetFunctionsNormalizeFloats,
      "slopeFiltersVarByPairNulls" -> SparkFixes.slopeFiltersVarByPairNulls,
      "r2DegenerateCasesSwapped" -> SparkFixes.r2DegenerateCasesSwapped,
      "keepsSignedZeroKeys" -> SparkFixes.keepsSignedZeroKeys)
    Seq("", "unknown", "v3.5.9", "v4.2.1", "4.0.5_vendor", "4.1.4x").foreach { version =>
      fixes.foreach { case (name, fixed) =>
        assert(!fixed(version), s"$name should count Spark '$version' as unfixed")
      }
    }
  }
}

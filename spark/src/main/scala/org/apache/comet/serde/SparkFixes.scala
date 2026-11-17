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

import org.apache.spark.sql.comet.util.Utils

/**
 * The Spark fixes that shipped in patch releases and change results that Comet's Rust code
 * reproduces, and the releases that carry them. A fix leaves nothing in the plan for Comet to
 * detect, so the checks go by the version. Every check reads it with
 * `Utils.majorMinorPatchVersion`, and a version that does not parse counts as unfixed. Each fix
 * has its own predicate and its own table of releases, so a fork that backports one fix ahead of
 * a release changes one table and leaves the others alone.
 *
 * A check for a feature release, such as `isSpark42Plus`, does not belong here.
 */
private[comet] object SparkFixes {

  /**
   * SPARK-58821: when `stop - start` overflows a long but the length is small, sequence() returns
   * the exact length instead of raising INTERNAL_ERROR("Unreachable code reached.").
   */
  def sequenceRecomputesOverflowedLength(sparkVersion: String): Boolean =
    // On branch-4.0, branch-4.1, branch-4.2 and master, and in the 4.0.5-rc3, 4.1.4-rc4 and
    // 4.2.1-rc3 release candidates, so the first releases that carry it are 4.0.5, 4.1.4 and
    // 4.2.1.
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (4, 0, patch) => patch >= 5
      case (4, 1, patch) => patch >= 4
      case (4, 2, patch) => patch >= 1
      case (major, minor, _) => major > 4 || (major == 4 && minor >= 3)
    }

  /**
   * SPARK-60118: casting a string to decimal no longer counts integral digits for zero, so zero
   * with a large exponent ("0e39") casts to 0 instead of NULL (or NUMERIC_OUT_OF_SUPPORTED_RANGE
   * under ANSI).
   */
  def castsZeroWithAnyExponentToDecimal(sparkVersion: String): Boolean =
    // On branch-4.0, branch-4.1, branch-4.2 and master, and in the 4.0.5-rc3, 4.1.4-rc4 and
    // 4.2.1-rc3 release candidates, so the first releases that carry it are 4.0.5, 4.1.4 and
    // 4.2.1.
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (4, 0, patch) => patch >= 5
      case (4, 1, patch) => patch >= 4
      case (4, 2, patch) => patch >= 1
      case (major, minor, _) => major > 4 || (major == 4 && minor >= 3)
    }

  /**
   * SPARK-60119: `Decimal.changePrecision` skips `BigDecimal.setScale` when the value's magnitude
   * already decides the result, so a string far below the target scale's ulp casts to 0 where
   * `setScale` used to throw a raw `ArithmeticException`. That includes a scale difference that
   * does not fit in an int, such as `'1e-2147483647'` to `DECIMAL(10,-2)`.
   */
  def castsTinyStringToDecimalZero(sparkVersion: String): Boolean =
    // On branch-4.0, branch-4.1, branch-4.2 and master, and in the 4.0.5-rc3, 4.1.4-rc4 and
    // 4.2.1-rc3 release candidates, so the first releases that carry it are 4.0.5, 4.1.4 and
    // 4.2.1.
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (4, 0, patch) => patch >= 5
      case (4, 1, patch) => patch >= 4
      case (4, 2, patch) => patch >= 1
      case (major, minor, _) => major > 4 || (major == 4 && minor >= 3)
    }

  /**
   * SPARK-54918 and SPARK-59602: Spark 4.2.0 normalizes the arguments of the array set functions
   * in the plan (SPARK-54918), and 4.0.5, 4.1.4 and 4.2.1 normalize while evaluating them
   * (SPARK-59602). Either way, Spark treats -0.0 and 0.0, and every NaN, as one value at any
   * depth, which the spark_ variants match. Earlier releases keep -0.0 and 0.0 apart in a flat
   * array. Both changes cover arrays_overlap too (see CometArraysOverlap.nativeFunction).
   *
   * The check reads the version rather than a KnownFloatingPointNormalized marker, because
   * SPARK-59602 adds no marker, and SPARK-54918 normalizes CreateArray, If, CaseWhen and Coalesce
   * without wrapping the resulting array.
   */
  def arraySetFunctionsNormalizeFloats(sparkVersion: String): Boolean =
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (4, 0, patch) => patch >= 5
      case (4, 1, patch) => patch >= 4
      case (major, minor, _) => major > 4 || (major == 4 && minor >= 2)
    }

  /**
   * SPARK-48719 made regr_slope and regr_intercept count VariancePop(x) only over rows where both
   * y and x are non-null. Present from 3.5.2 and 4.0.0; earlier 3.5 patches and 3.4 count every
   * row where x is non-null.
   */
  def slopeFiltersVarByPairNulls(sparkVersion: String): Boolean =
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (3, 5, patch) => patch >= 2
      case (major, minor, _) => major > 3 || (major == 3 && minor > 5)
    }

  /**
   * SPARK-55969 swapped regr_r2's degenerate cases: a constant dependent variable yields 1.0 (was
   * null) and a constant independent variable yields null (was 1.0). Present from 3.5.9, 4.0.3,
   * 4.1.2 and 4.2.0; earlier patches of those lines and 3.4 keep the old cases.
   */
  def r2DegenerateCasesSwapped(sparkVersion: String): Boolean =
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (3, 5, patch) => patch >= 9
      case (4, 0, patch) => patch >= 3
      case (4, 1, patch) => patch >= 2
      case (major, minor, _) => major > 3 || (major == 3 && minor > 5)
    }

  /**
   * SPARK-45599: Spark 3.5.2 keeps 0.0 and -0.0 as separate keys in the percentile buffer.
   * Earlier releases can merge the two, which the Rust-implemented percentile does not reproduce.
   */
  def keepsSignedZeroKeys(sparkVersion: String): Boolean =
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (3, 5, patch) => patch >= 2
      case (major, minor, _) => major > 3 || (major == 3 && minor > 5)
    }
}

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
 * SPARK-58821 and SPARK-60118, which change results Comet reproduces natively, and the Spark
 * releases that carry them. The fixes leave nothing in the plan for Comet to detect, so the
 * checks go by the version. Other fixes Comet picks by version keep their own checks, such as
 * `RegrSparkVersions` and `ArraySetSupport.normalizesFloats`.
 */
private[comet] object SparkFixes {

  /**
   * SPARK-58821: when `stop - start` overflows a long but the length is small, sequence() returns
   * the exact length instead of raising INTERNAL_ERROR("Unreachable code reached.").
   */
  def sequenceRecomputesOverflowedLength(sparkVersion: String): Boolean =
    fixedIn(sparkVersion)

  /**
   * SPARK-60118: casting a string to decimal no longer counts integral digits for zero, so zero
   * with a large exponent ("0e39") casts to 0 instead of NULL (or NUMERIC_OUT_OF_SUPPORTED_RANGE
   * under ANSI).
   */
  def castsZeroWithAnyExponentToDecimal(sparkVersion: String): Boolean =
    fixedIn(sparkVersion)

  // Both fixes are on branch-4.0, branch-4.1, branch-4.2 and master, and in the 4.0.5-rc3,
  // 4.1.4-rc4 and 4.2.1-rc3 release candidates, so the first releases that carry them are 4.0.5,
  // 4.1.4 and 4.2.1.
  private def fixedIn(sparkVersion: String): Boolean =
    Utils.majorMinorPatchVersion(sparkVersion).exists {
      case (4, 0, patch) => patch >= 5
      case (4, 1, patch) => patch >= 4
      case (4, 2, patch) => patch >= 1
      case (major, minor, _) => major > 4 || (major == 4 && minor >= 3)
    }
}

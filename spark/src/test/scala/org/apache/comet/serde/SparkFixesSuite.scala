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

class SparkFixesSuite extends AnyFunSuite {

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
}

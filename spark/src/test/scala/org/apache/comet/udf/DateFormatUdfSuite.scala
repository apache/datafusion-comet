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

package org.apache.comet.udf

import java.nio.charset.StandardCharsets
import java.time.LocalDate

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.shaded.arrow.vector.{DateDayVector, VarCharVector}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.udf.builtin.DateFormatUdf

/**
 * Smoke test verifying that a `CometUDF` implementation in the spark module can directly
 * manipulate Arrow vectors when the imports target Comet's shaded Arrow namespace
 * (`org.apache.comet.shaded.arrow.*`).
 */
class DateFormatUdfSuite extends AnyFunSuite {

  private def daysSinceEpoch(s: String): Int = LocalDate.parse(s).toEpochDay.toInt

  test("formats dates with yyyy-MM-dd pattern, propagates nulls") {
    val dates = new DateDayVector("d", CometArrowAllocator)
    val pattern = new VarCharVector("p", CometArrowAllocator)
    try {
      dates.allocateNew(3)
      dates.setSafe(0, daysSinceEpoch("2024-01-15"))
      dates.setNull(1)
      dates.setSafe(2, daysSinceEpoch("1999-12-31"))
      dates.setValueCount(3)

      pattern.allocateNew(1)
      pattern.setSafe(0, "yyyy-MM-dd".getBytes(StandardCharsets.UTF_8))
      pattern.setValueCount(1)

      val out = new DateFormatUdf().evaluate(Array(dates, pattern), 3).asInstanceOf[VarCharVector]
      try {
        assert(out.getValueCount === 3)
        assert(new String(out.get(0), StandardCharsets.UTF_8) === "2024-01-15")
        assert(out.isNull(1))
        assert(new String(out.get(2), StandardCharsets.UTF_8) === "1999-12-31")
      } finally {
        out.close()
      }
    } finally {
      dates.close()
      pattern.close()
    }
  }
}

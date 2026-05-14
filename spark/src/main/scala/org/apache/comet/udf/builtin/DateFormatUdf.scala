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

package org.apache.comet.udf.builtin

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.arrow.vector.{DateDayVector, ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.udf.CometUDF

/**
 * Comet JVM UDF that formats a DateType column using a literal Java SimpleDateFormat-style
 * pattern. Inputs: (date column, literal format string). Output: VarCharVector of formatted
 * strings.
 *
 * This exists as a smoke test for the JVM UDF framework: production routing of `DateFormatClass`
 * still goes through `CometDateFormat` -> DataFusion `to_char`.
 */
class DateFormatUdf extends CometUDF {

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(inputs.length == 2, s"DateFormatUdf expects 2 inputs, got ${inputs.length}")

    val dates = inputs(0).asInstanceOf[DateDayVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    require(patternVec.getValueCount >= 1, "format pattern vector must have at least one row")
    val pattern = new String(patternVec.get(0), StandardCharsets.UTF_8)
    val fmt = DateTimeFormatter.ofPattern(pattern)

    val out = new VarCharVector("date_format", CometArrowAllocator)
    out.allocateNew(numRows)
    var i = 0
    while (i < numRows) {
      if (dates.isNull(i)) {
        out.setNull(i)
      } else {
        val days = dates.get(i)
        val formatted = LocalDate.ofEpochDay(days.toLong).format(fmt)
        out.setSafe(i, formatted.getBytes(StandardCharsets.UTF_8))
      }
      i += 1
    }
    out.setValueCount(numRows)
    out
  }
}

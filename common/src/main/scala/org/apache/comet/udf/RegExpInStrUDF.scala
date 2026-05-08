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
import java.util
import java.util.regex.Pattern

import org.apache.arrow.vector.{IntVector, ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator

/**
 * `regexp_instr(subject, pattern, idx)` implemented with java.util.regex.Pattern.
 *
 * Returns the 1-based position of the start of the first match of the idx-th capturing group, or
 * 0 if no match. idx=0 means the entire match.
 *
 * Inputs:
 *   - inputs(0): VarCharVector subject column
 *   - inputs(1): VarCharVector pattern (scalar, length-1)
 *   - inputs(2): IntVector group index (scalar, length-1)
 *
 * Output: IntVector, same length as subject.
 */
class RegExpInStrUDF extends CometUDF {

  private val patternCache =
    new util.LinkedHashMap[String, Pattern](RegExpInStrUDF.PatternCacheCapacity, 0.75f, true) {
      override def removeEldestEntry(eldest: util.Map.Entry[String, Pattern]): Boolean =
        size() > RegExpInStrUDF.PatternCacheCapacity
    }

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(inputs.length == 3, s"RegExpInStrUDF expects 3 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    val idxVec = inputs(2).asInstanceOf[IntVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "RegExpInStrUDF requires a non-null scalar pattern")
    require(
      idxVec.getValueCount >= 1 && !idxVec.isNull(0),
      "RegExpInStrUDF requires a non-null scalar group index")

    val patternStr = new String(patternVec.get(0), StandardCharsets.UTF_8)
    val pattern = {
      val cached = patternCache.get(patternStr)
      if (cached != null) cached
      else {
        val compiled = Pattern.compile(patternStr)
        patternCache.put(patternStr, compiled)
        compiled
      }
    }
    idxVec.get(0)

    val n = subject.getValueCount
    val out = new IntVector("regexp_instr_result", CometArrowAllocator)
    out.allocateNew(n)

    var i = 0
    while (i < n) {
      if (subject.isNull(i)) {
        out.setNull(i)
      } else {
        val s = new String(subject.get(i), StandardCharsets.UTF_8)
        val matcher = pattern.matcher(s)
        if (matcher.find()) {
          // Spark uses 1-based positions; matcher.start() is 0-based.
          out.set(i, matcher.start() + 1)
        } else {
          out.set(i, 0)
        }
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}

object RegExpInStrUDF {
  private val PatternCacheCapacity: Int = 128
}

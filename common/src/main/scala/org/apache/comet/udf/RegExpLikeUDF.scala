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

import org.apache.arrow.vector.{BitVector, ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator

/**
 * `regexp` / `RLike` implemented with java.util.regex.Pattern (Java semantics).
 *
 * Inputs:
 *   - inputs(0): VarCharVector subject column
 *   - inputs(1): VarCharVector pattern, 1-row scalar (serde guarantees this)
 *
 * Output: BitVector (Arrow boolean), same length as the subject vector.
 */
class RegExpLikeUDF extends CometUDF {

  // Bounded LRU so a workload with many distinct patterns does not retain
  // Pattern objects for the executor's lifetime.
  private val patternCache =
    new util.LinkedHashMap[String, Pattern](RegExpLikeUDF.PatternCacheCapacity, 0.75f, true) {
      override def removeEldestEntry(eldest: util.Map.Entry[String, Pattern]): Boolean =
        size() > RegExpLikeUDF.PatternCacheCapacity
    }

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"RegExpLikeUDF expects 2 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "RegExpLikeUDF requires a non-null scalar pattern")

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

    val n = subject.getValueCount
    val out = new BitVector("rlike_result", CometArrowAllocator)
    out.allocateNew(n)

    var i = 0
    while (i < n) {
      if (subject.isNull(i)) {
        out.setNull(i)
      } else {
        val s = new String(subject.get(i), StandardCharsets.UTF_8)
        out.set(i, if (pattern.matcher(s).find()) 1 else 0)
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}

object RegExpLikeUDF {
  private val PatternCacheCapacity: Int = 128
}

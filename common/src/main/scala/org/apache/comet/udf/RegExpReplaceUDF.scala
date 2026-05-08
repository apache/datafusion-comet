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
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import org.apache.arrow.vector.{ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator

/**
 * `regexp_replace(subject, pattern, replacement)` implemented with java.util.regex.Pattern.
 *
 * Replaces all occurrences of pattern in subject with replacement.
 *
 * Inputs:
 *   - inputs(0): VarCharVector subject column
 *   - inputs(1): VarCharVector pattern (scalar, length-1)
 *   - inputs(2): VarCharVector replacement (scalar, length-1)
 *
 * Output: VarCharVector, same length as subject.
 */
class RegExpReplaceUDF extends CometUDF {

  private val patternCache = new ConcurrentHashMap[String, Pattern]()

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 3, s"RegExpReplaceUDF expects 3 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    val replacementVec = inputs(2).asInstanceOf[VarCharVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "RegExpReplaceUDF requires a non-null scalar pattern")
    require(
      replacementVec.getValueCount >= 1 && !replacementVec.isNull(0),
      "RegExpReplaceUDF requires a non-null scalar replacement")

    val patternStr = new String(patternVec.get(0), StandardCharsets.UTF_8)
    val pattern = patternCache.computeIfAbsent(patternStr, Pattern.compile)
    val replacement = new String(replacementVec.get(0), StandardCharsets.UTF_8)

    val n = subject.getValueCount
    val out = new VarCharVector("regexp_replace_result", CometArrowAllocator)
    out.allocateNew(n)

    var i = 0
    while (i < n) {
      if (subject.isNull(i)) {
        out.setNull(i)
      } else {
        val s = new String(subject.get(i), StandardCharsets.UTF_8)
        val result = pattern.matcher(s).replaceAll(replacement)
        out.setSafe(i, result.getBytes(StandardCharsets.UTF_8))
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}

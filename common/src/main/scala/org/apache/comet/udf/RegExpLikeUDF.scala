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
import java.util.regex.Pattern

import scala.collection.mutable

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BitVector, ValueVector, VarCharVector}

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

  private val allocator = new RootAllocator(Long.MaxValue)
  private val patternCache = mutable.Map.empty[String, Pattern]

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"RegExpLikeUDF expects 2 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "RegExpLikeUDF requires a non-null scalar pattern")

    val patternStr = new String(patternVec.get(0), StandardCharsets.UTF_8)
    val pattern = patternCache.getOrElseUpdate(patternStr, Pattern.compile(patternStr))

    val n = subject.getValueCount
    val out = new BitVector("rlike_result", allocator)
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

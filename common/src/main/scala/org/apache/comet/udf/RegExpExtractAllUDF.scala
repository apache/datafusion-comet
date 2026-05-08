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
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.comet.CometArrowAllocator

/**
 * `regexp_extract_all(subject, pattern, idx)` implemented with java.util.regex.Pattern.
 *
 * Returns an array of strings: for every match of pattern in subject, extracts the idx-th
 * capturing group. idx=0 returns the entire match.
 *
 * Inputs:
 *   - inputs(0): VarCharVector subject column
 *   - inputs(1): VarCharVector pattern (scalar, length-1)
 *   - inputs(2): IntVector group index (scalar, length-1)
 *
 * Output: ListVector of VarChar, same length as subject.
 */
class RegExpExtractAllUDF extends CometUDF {

  private val patternCache =
    new util.LinkedHashMap[String, Pattern](
      RegExpExtractAllUDF.PatternCacheCapacity,
      0.75f,
      true) {
      override def removeEldestEntry(eldest: util.Map.Entry[String, Pattern]): Boolean =
        size() > RegExpExtractAllUDF.PatternCacheCapacity
    }

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(inputs.length == 3, s"RegExpExtractAllUDF expects 3 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    val idxVec = inputs(2).asInstanceOf[IntVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "RegExpExtractAllUDF requires a non-null scalar pattern")
    require(
      idxVec.getValueCount >= 1 && !idxVec.isNull(0),
      "RegExpExtractAllUDF requires a non-null scalar group index")

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
    val idx = idxVec.get(0)

    val n = subject.getValueCount
    val out = ListVector.empty("regexp_extract_all_result", CometArrowAllocator)
    out.addOrGetVector[VarCharVector](new FieldType(true, ArrowType.Utf8.INSTANCE, null))
    val writer = out.getWriter

    var i = 0
    while (i < n) {
      if (subject.isNull(i)) {
        out.setNull(i)
      } else {
        val s = new String(subject.get(i), StandardCharsets.UTF_8)
        val matcher = pattern.matcher(s)
        writer.setPosition(i)
        writer.startList()
        while (matcher.find()) {
          if (idx <= matcher.groupCount()) {
            val group = matcher.group(idx)
            val bytes =
              if (group == null) "".getBytes(StandardCharsets.UTF_8)
              else group.getBytes(StandardCharsets.UTF_8)
            val buf = CometArrowAllocator.buffer(bytes.length)
            buf.writeBytes(bytes)
            writer.varChar().writeVarChar(0, bytes.length, buf)
            buf.close()
          } else {
            val bytes = "".getBytes(StandardCharsets.UTF_8)
            val buf = CometArrowAllocator.buffer(bytes.length)
            buf.writeBytes(bytes)
            writer.varChar().writeVarChar(0, bytes.length, buf)
            buf.close()
          }
        }
        writer.endList()
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}

object RegExpExtractAllUDF {
  private val PatternCacheCapacity: Int = 128
}

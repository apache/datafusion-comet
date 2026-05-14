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

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.comet.CometArrowAllocator

/**
 * `split(subject, pattern, limit)` implemented with java.util.regex.Pattern.
 *
 * Splits the subject string around matches of the pattern, up to the specified limit.
 *
 * Inputs:
 *   - inputs(0): VarCharVector subject column
 *   - inputs(1): VarCharVector pattern (scalar, length-1)
 *   - inputs(2): IntVector limit (scalar, length-1)
 *
 * Output: ListVector of VarChar, same length as subject.
 */
class StringSplitUDF extends CometUDF {

  private val patternCache = new ConcurrentHashMap[String, Pattern]()

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(inputs.length == 3, s"StringSplitUDF expects 3 inputs, got ${inputs.length}")
    val subject = inputs(0).asInstanceOf[VarCharVector]
    val patternVec = inputs(1).asInstanceOf[VarCharVector]
    val limitVec = inputs(2).asInstanceOf[org.apache.arrow.vector.IntVector]
    require(
      patternVec.getValueCount >= 1 && !patternVec.isNull(0),
      "StringSplitUDF requires a non-null scalar pattern")
    require(
      limitVec.getValueCount >= 1 && !limitVec.isNull(0),
      "StringSplitUDF requires a non-null scalar limit")

    val patternStr = new String(patternVec.get(0), StandardCharsets.UTF_8)
    val pattern = patternCache.computeIfAbsent(patternStr, Pattern.compile)
    val limit = limitVec.get(0)

    val n = subject.getValueCount
    val out = ListVector.empty("string_split_result", CometArrowAllocator)
    out.addOrGetVector[VarCharVector](new FieldType(true, ArrowType.Utf8.INSTANCE, null))
    val writer = out.getWriter

    var i = 0
    while (i < n) {
      if (subject.isNull(i)) {
        out.setNull(i)
      } else {
        val s = new String(subject.get(i), StandardCharsets.UTF_8)
        // Spark semantics: limit <= 0 means no limit (split returns all)
        val parts = if (limit <= 0) pattern.split(s, -1) else pattern.split(s, limit)
        writer.setPosition(i)
        writer.startList()
        var j = 0
        while (j < parts.length) {
          val bytes = parts(j).getBytes(StandardCharsets.UTF_8)
          val buf = CometArrowAllocator.buffer(bytes.length)
          buf.writeBytes(bytes)
          writer.varChar().writeVarChar(0, bytes.length, buf)
          buf.close()
          j += 1
        }
        writer.endList()
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}

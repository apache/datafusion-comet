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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BitVector, ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator

class RegExpLikeUDFSuite extends AnyFunSuite {

  // Use the same allocator that RegExpLikeUDF allocates its output from. A
  // per-test RootAllocator interacts poorly with shared Arrow accounting state
  // when other suites have already exercised CometArrowAllocator in the same
  // surefire JVM (observed under Spark 3.5 Scala 2.13 / Spark 4.x in CI).
  private def allocator: BufferAllocator = CometArrowAllocator

  private def varchar(values: Seq[String]): VarCharVector = {
    val v = new VarCharVector("subject", allocator)
    v.allocateNew()
    values.zipWithIndex.foreach { case (s, i) =>
      if (s == null) v.setNull(i)
      else v.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
    }
    v.setValueCount(values.length)
    v
  }

  private def scalarPattern(pattern: String): VarCharVector = {
    val v = new VarCharVector("pattern", allocator)
    v.allocateNew()
    v.setSafe(0, pattern.getBytes(StandardCharsets.UTF_8))
    v.setValueCount(1)
    v
  }

  /** Verify that everything allocated within `body` is released by the time it returns. */
  private def assertNoLeak(body: => Unit): Unit = {
    val before = allocator.getAllocatedMemory
    body
    assert(
      allocator.getAllocatedMemory === before,
      s"test leaked Arrow memory: ${allocator.getAllocatedMemory - before} bytes")
  }

  test("matches Java regex semantics including null handling") {
    assertNoLeak {
      val subject = varchar(Seq("abc123", "no-digits", null, "X"))
      val pattern = scalarPattern("\\d+")

      val udf = new RegExpLikeUDF
      val out = udf.evaluate(Array[ValueVector](subject, pattern)).asInstanceOf[BitVector]

      assert(out.getValueCount === 4)
      assert(out.get(0) === 1)
      assert(out.get(1) === 0)
      assert(out.isNull(2))
      assert(out.get(3) === 0)
      out.close()
      subject.close()
      pattern.close()
    }
  }

  test("compiled Pattern is cached across evaluate calls") {
    assertNoLeak {
      val udf = new RegExpLikeUDF
      val pattern = scalarPattern("[a-z]+")
      val s1 = varchar(Seq("hello"))
      val s2 = varchar(Seq("WORLD"))

      val r1 = udf.evaluate(Array[ValueVector](s1, pattern)).asInstanceOf[BitVector]
      val r2 = udf.evaluate(Array[ValueVector](s2, pattern)).asInstanceOf[BitVector]

      assert(r1.get(0) === 1)
      assert(r2.get(0) === 0)
      r1.close(); r2.close()
      s1.close(); s2.close(); pattern.close()
    }
  }

  test("empty subject vector yields empty result") {
    assertNoLeak {
      val subject = varchar(Seq.empty)
      val pattern = scalarPattern("\\d+")

      val out = new RegExpLikeUDF()
        .evaluate(Array[ValueVector](subject, pattern))
        .asInstanceOf[BitVector]

      assert(out.getValueCount === 0)
      out.close(); subject.close(); pattern.close()
    }
  }

  test("all-null subject column produces all-null bitmap") {
    assertNoLeak {
      val subject = varchar(Seq(null, null, null))
      val pattern = scalarPattern(".*")

      val out = new RegExpLikeUDF()
        .evaluate(Array[ValueVector](subject, pattern))
        .asInstanceOf[BitVector]

      assert(out.getValueCount === 3)
      assert(out.isNull(0) && out.isNull(1) && out.isNull(2))
      out.close(); subject.close(); pattern.close()
    }
  }
}

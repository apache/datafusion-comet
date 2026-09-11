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

package org.apache.comet.vector

import java.io.IOException
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, IntVector, VarCharVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}

/** Check failure cleanup without transferring ownership of the helper's source vectors. */
class CometVectorUtilsSuite extends AnyFunSuite with Matchers {

  for (missingDictionary <- Seq(true, false)) {
    val failurePoint = if (missingDictionary) "a later dictionary lookup" else "the callback"
    test(s"withDecodedVectors releases temporaries when $failurePoint fails") {
      val sourceAllocator = new RootAllocator(Long.MaxValue)
      val decodedAllocator = new RootAllocator(Long.MaxValue)
      val indexType = new ArrowType.Int(32, true)
      val firstEncoding = new DictionaryEncoding(11L, false, indexType)
      val secondEncoding = new DictionaryEncoding(22L, false, indexType)
      val firstValues = new VarCharVector("first_values", sourceAllocator)
      val secondValues = new VarCharVector("second_values", sourceAllocator)
      val firstIndices =
        new IntVector("first", new FieldType(true, indexType, firstEncoding), sourceAllocator)
      val secondIndices =
        new IntVector("second", new FieldType(true, indexType, secondEncoding), sourceAllocator)
      val plain = new BigIntVector("plain", sourceAllocator)
      val sources =
        Seq[FieldVector](firstValues, secondValues, firstIndices, secondIndices, plain)
      try {
        firstValues.allocateNew()
        firstValues.setSafe(0, "alpha".getBytes(StandardCharsets.UTF_8))
        firstValues.setSafe(1, "λ".getBytes(StandardCharsets.UTF_8))
        firstValues.setValueCount(2)
        secondValues.allocateNew()
        secondValues.setSafe(0, "beta".getBytes(StandardCharsets.UTF_8))
        secondValues.setValueCount(1)
        firstIndices.allocateNew(3)
        firstIndices.set(0, 0)
        firstIndices.setNull(1)
        firstIndices.set(2, 1)
        firstIndices.setValueCount(3)
        secondIndices.allocateNew(3)
        secondIndices.set(0, 0)
        secondIndices.set(1, 0)
        secondIndices.setNull(2)
        secondIndices.setValueCount(3)
        plain.allocateNew(3)
        (0 until 3).foreach(row => plain.set(row, 100L + row))
        plain.setValueCount(3)

        val firstDictionary = new Dictionary(firstValues, firstEncoding)
        val secondDictionary = new Dictionary(secondValues, secondEncoding)
        val provider = new DictionaryProvider {

          /** Borrow the fixture's dictionaries, returning null for its deliberately absent id. */
          override def lookup(id: Long): Dictionary = {
            if (id == firstEncoding.getId) firstDictionary
            else if (id == secondEncoding.getId && !missingDictionary) secondDictionary
            else null
          }

          /** Return the available ids; the provider does not own or close either value vector. */
          override def getDictionaryIds: java.util.Set[java.lang.Long] = {
            val ids = if (missingDictionary) Seq(11L) else Seq(11L, 22L)
            ids.map(id => java.lang.Long.valueOf(id)).toSet.asJava
          }
        }
        val columns = Seq[CometVector](
          new CometDictionaryVector(
            new CometPlainVector(firstIndices),
            new CometDictionary(new CometPlainVector(firstValues)),
            provider),
          new CometPlainVector(plain),
          new CometDictionaryVector(
            new CometPlainVector(secondIndices),
            new CometDictionary(new CometPlainVector(secondValues)),
            provider))
        val sourceBuffers = sources.flatMap(_.getFieldBuffers.asScala)
        val sourceRefs = sourceBuffers.map(_.refCnt())
        val sourceBytes = sourceAllocator.getAllocatedMemory
        val callbackFailure = new IOException("injected logical-vector callback failure")
        var callbackEntered = false

        val error = intercept[Exception] {
          CometVectorUtils.withDecodedVectors(columns, decodedAllocator) { vectors =>
            callbackEntered = true
            vectors(0).getField.getDictionary shouldBe null
            vectors(0).getObject(0).toString shouldBe "alpha"
            vectors(0).isNull(1) shouldBe true
            vectors(0).getObject(2).toString shouldBe "λ"
            vectors(1) should be theSameInstanceAs plain
            vectors(2).getObject(1).toString shouldBe "beta"
            vectors(2).isNull(2) shouldBe true
            throw callbackFailure
          }
        }

        if (missingDictionary) {
          error shouldBe a[IllegalStateException]
          error.getMessage shouldBe "Missing dictionary 22 for column 'second'"
          callbackEntered shouldBe false
        } else {
          error should be theSameInstanceAs callbackFailure
          callbackEntered shouldBe true
        }
        // A nonzero peak proves the earlier dictionary was decoded before either failure.
        decodedAllocator.getPeakMemoryAllocation should be > 0L
        decodedAllocator.getAllocatedMemory shouldBe 0L
        sourceAllocator.getAllocatedMemory shouldBe sourceBytes
        sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
        firstValues.getObject(1).toString shouldBe "λ"
        secondValues.getObject(0).toString shouldBe "beta"
        firstIndices.get(2) shouldBe 1
        firstIndices.isNull(1) shouldBe true
        secondIndices.isNull(2) shouldBe true
        plain.get(2) shouldBe 102L
      } finally {
        // Close each fixture-owned source vector once; the helper only borrowed its wrapper.
        sources.reverseIterator.foreach(_.close())
        decodedAllocator.close()
        sourceAllocator.close()
      }
    }
  }
}

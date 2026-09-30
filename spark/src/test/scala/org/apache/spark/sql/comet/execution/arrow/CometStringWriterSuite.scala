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

package org.apache.spark.sql.comet.execution.arrow

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.util.Using

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.c.{ArrowArray, Data}
import org.apache.arrow.memory.{ArrowBuf, OutOfMemoryException, RootAllocator}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.util.OversizedAllocationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class CometStringWriterSuite extends AnyFunSuite with Matchers {
  private val sourceOffset = 7
  private val schema = StructType(Seq(StructField("text", StringType)))

  private def offHeapString(source: ArrowBuf, bytes: Array[Byte]): UTF8String = {
    source.setZero(0, source.capacity())
    source.setBytes(sourceOffset.toLong, bytes)
    val value = UTF8String.fromAddress(null, source.memoryAddress() + sourceOffset, bytes.length)
    value.getBaseObject shouldBe null
    value
  }

  private def row(value: UTF8String): InternalRow = new GenericInternalRow(Array[Any](value))

  private def assertValues(vector: VarCharVector, expected: Seq[Option[Array[Byte]]]): Unit = {
    vector.getValueCount shouldBe expected.size
    var offset = 0
    expected.zipWithIndex.foreach { case (value, index) =>
      vector.getOffsetBuffer.getInt(index * 4L) shouldBe offset
      vector.isNull(index) shouldBe value.isEmpty
      value.foreach { bytes =>
        vector.get(index).toSeq shouldBe bytes.toSeq
        offset += bytes.length
      }
      vector.getOffsetBuffer.getInt((index + 1) * 4L) shouldBe offset
    }
    vector.getNullCount shouldBe expected.count(_.isEmpty)
  }

  Seq("off-heap", "heap array", "heap slice").foreach { storage =>
    test(s"$storage strings preserve bytes, nulls and offsets through data growth") {
      Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
        Using.Manager { use =>
          val source = use(allocator.buffer(1024))
          val actual = use(new VarCharVector("actual", allocator))
          val baseline = use(new VarCharVector("baseline", allocator))
          actual.allocateNew(8, 32)
          baseline.allocateNew(8, 32)
          val initialDataCapacity = actual.getDataBuffer.capacity()
          val initialValueCapacity = actual.getValueCapacity
          val expected = Seq(
            None,
            Some(Array.emptyByteArray),
            Some("prefix".getBytes(UTF_8)),
            None,
            Some(Array.tabulate[Byte](initialDataCapacity.toInt + 33)(i => (i % 251).toByte)),
            Some("a\u0000λ😀".getBytes(UTF_8)),
            Some(Array[Byte](0xc3.toByte, 0x28, 0xff.toByte)),
            Some(Array.emptyByteArray),
            None,
            Some("tail".getBytes(UTF_8)),
            None)
          val writer = new StringWriter(actual)
          expected.zipWithIndex.foreach { case (bytes, index) =>
            val backing = bytes.map { value =>
              if (storage == "heap slice") {
                Array.fill[Byte](sourceOffset)(-1) ++ value ++ Array[Byte](-1)
              } else {
                value.clone()
              }
            }
            val value = bytes.map { payload =>
              storage match {
                case "off-heap" => offHeapString(source, payload)
                case "heap array" => UTF8String.fromBytes(backing.get)
                case "heap slice" =>
                  UTF8String.fromBytes(backing.get, sourceOffset, payload.length)
              }
            }
            writer.write(row(value.orNull), 0)
            value match {
              case Some(utf8) =>
                val buffer = utf8.getByteBuffer
                baseline.setSafe(index, buffer, buffer.position(), utf8.numBytes())
              case None => baseline.setNull(index)
            }
            // Reusing Spark's source storage after every value must not change Arrow's bytes.
            source.setZero(0, source.capacity())
            backing.foreach(bytes => java.util.Arrays.fill(bytes, 0.toByte))
          }
          writer.finish()
          baseline.setValueCount(expected.size)
          actual.getDataBuffer.capacity() should be > initialDataCapacity
          actual.getValueCapacity shouldBe initialValueCapacity
          assertValues(actual, expected)
          assertValues(baseline, expected)
        }.get
        allocator.getAllocatedMemory shouldBe 0L
      }
    }
  }

  test("off-heap empty strings grow offsets and validity without growing the data buffer") {
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.Manager { use =>
        val source = use(allocator.buffer(sourceOffset + 1L))
        val vector = use(new VarCharVector("text", allocator))
        vector.allocateNew(8, 1)
        val initialDataCapacity = vector.getDataBuffer.capacity()
        val initialOffsetCapacity = vector.getOffsetBuffer.capacity()
        val initialValidityCapacity = vector.getValidityBuffer.capacity()
        val count = (initialValidityCapacity * 8).toInt + 2
        val expected = (0 until count).map { i =>
          if (i % 3 == 0) None else Some(Array.emptyByteArray)
        }
        val writer = new StringWriter(vector)
        expected.foreach { bytes =>
          writer.write(row(bytes.map(offHeapString(source, _)).orNull), 0)
          source.setZero(0, source.capacity())
        }
        writer.finish()
        vector.getOffsetBuffer.capacity() should be > initialOffsetCapacity
        vector.getValidityBuffer.capacity() should be > initialValidityCapacity
        vector.getDataBuffer.capacity() shouldBe initialDataCapacity
        assertValues(vector, expected)
      }.get
      allocator.getAllocatedMemory shouldBe 0L
    }
  }

  test("C Data consumers retain off-heap string batches after source reuse and reader close") {
    // Maven points java.io.tmpdir at target/tmp. This suite does not start Spark, which normally
    // creates that directory before Arrow extracts its C Data JNI library there.
    Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.Manager { use =>
        val source = use(allocator.buffer(1024))
        val expected = Seq(
          Some("first".getBytes(UTF_8)),
          None,
          Some(Array.emptyByteArray),
          Some(Array.fill[Byte](257)(42)),
          Some("λ😀".getBytes(UTF_8)),
          None)
        val rows = expected.iterator.map { bytes =>
          row(bytes.map(offHeapString(source, _)).orNull)
        }
        val arrowSchema = Utils.toArrowSchema(schema, "UTC")
        val reader = use(new RowArrowReader(allocator, arrowSchema, rows, 3))
        val retained = expected
          .grouped(3)
          .map { batch =>
            reader.loadNextBatch() shouldBe true
            val root = reader.getVectorSchemaRoot
            root.getRowCount shouldBe batch.size
            val output = use(VectorSchemaRoot.create(arrowSchema, allocator))
            Using.resource(ArrowArray.allocateNew(allocator)) { array =>
              Data.exportVectorSchemaRoot(allocator, root, null, array)
              Data.importIntoVectorSchemaRoot(allocator, array, output, null)
            }
            // Import retains the exported buffers, so another batch must not reuse their storage.
            output.getVector(0).getDataBuffer.memoryAddress() shouldBe
              root.getVector(0).getDataBuffer.memoryAddress()
            source.setZero(0, source.capacity())
            output
          }
          .toVector
        reader.loadNextBatch() shouldBe false
        reader.close()
        retained.zip(expected.grouped(3).toVector).foreach { case (root, batch) =>
          root.getRowCount shouldBe batch.size
          assertValues(root.getVector(0).asInstanceOf[VarCharVector], batch)
        }
      }.get
      allocator.getAllocatedMemory shouldBe 0L
    }
  }

  test("failed off-heap string reservation emits no batch and releases partial allocations") {
    Using.resource(new RootAllocator(Long.MaxValue)) { allocator =>
      Using.Manager { use =>
        val destination = use(allocator.newChildAllocator("destination", 0, 128 * 1024))
        val source = use(allocator.buffer(256L * 1024 + sourceOffset))
        val large = offHeapString(source, Array.fill[Byte](256 * 1024)(42))
        var largeValueRead = false
        val failingRow = new GenericInternalRow(Array[Any](large)) {
          override def getUTF8String(ordinal: Int): UTF8String = {
            largeValueRead = true
            super.getUTF8String(ordinal)
          }
        }
        val batches = CometArrowConverters.rowToArrowBatchIter(
          Iterator(row(UTF8String.fromString("first")), failingRow),
          schema,
          2,
          "UTC",
          destination)
        intercept[OutOfMemoryException] {
          val batch = batches.next()
          try fail("A partially written batch must not be emitted")
          finally batch.close()
        }
        largeValueRead shouldBe true
        destination.getAllocatedMemory shouldBe 0L
      }.get
      allocator.getAllocatedMemory shouldBe 0L
    }
  }

  Seq("off-heap", "heap array", "heap slice").foreach { storage =>
    test(s"$storage string offset overflow throws Arrow's oversized allocation exception") {
      Seq(false, true).foreach { growOffsets =>
        Using.resource(new RootAllocator(1024 * 1024)) { allocator =>
          Using.Manager { use =>
            val source = use(allocator.buffer(sourceOffset + 4L))
            val vector = use(new VarCharVector("text", allocator))
            vector.allocateNew(8, 4)
            vector.setSafe(0, Array[Byte](42))
            // Simulate a nearly full 32-bit offset range without allocating gigabytes.
            val start = Int.MaxValue - 1
            vector.getOffsetBuffer.setInt(4L, start)
            val writer = new StringWriter(vector)
            val index = if (growOffsets) vector.getValueCapacity else 1
            writer.count = index
            val payload = Array[Byte](1, 2, 3, 4)
            val value = storage match {
              case "off-heap" => offHeapString(source, payload)
              case "heap array" => UTF8String.fromBytes(payload)
              case "heap slice" =>
                val backing = Array.fill[Byte](sourceOffset)(-1) ++ payload ++ Array[Byte](-1)
                UTF8String.fromBytes(backing, sourceOffset, payload.length)
            }
            intercept[OversizedAllocationException] {
              writer.write(row(value), 0)
            }
            // Arrow may grow metadata before rejecting the data size, but must not commit a row.
            writer.count shouldBe index
            vector.getLastSet shouldBe 0
            vector.getOffsetBuffer.getInt(4L) shouldBe start
            vector.getDataBuffer.getByte(0L) shouldBe 42.toByte
            if (index < vector.getValueCapacity) vector.isNull(index) shouldBe true
          }.get
          allocator.getAllocatedMemory shouldBe 0L
        }
      }
    }
  }

  test("off-heap negative string length is rejected before reserving or copying") {
    Using.resource(new RootAllocator(1024 * 1024)) { allocator =>
      Using.Manager { use =>
        val source = use(allocator.buffer(8))
        val vector = use(new VarCharVector("text", allocator))
        vector.allocateNew(8, 4)
        vector.setSafe(0, Array[Byte](42))
        val writer = new StringWriter(vector)
        writer.count = 1
        val allocated = allocator.getAllocatedMemory
        val value = UTF8String.fromAddress(null, source.memoryAddress(), -1)
        intercept[IllegalArgumentException] {
          writer.write(row(value), 0)
        }.getMessage should include("String length must be non-negative")
        allocator.getAllocatedMemory shouldBe allocated
        writer.count shouldBe 1
        vector.getLastSet shouldBe 0
        vector.getOffsetBuffer.getInt(4L) shouldBe 1
        vector.getDataBuffer.getByte(0L) shouldBe 42.toByte
        vector.isNull(1) shouldBe true
      }.get
      allocator.getAllocatedMemory shouldBe 0L
    }
  }
}

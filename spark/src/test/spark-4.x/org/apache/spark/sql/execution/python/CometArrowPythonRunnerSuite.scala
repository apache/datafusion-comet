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

package org.apache.spark.sql.execution.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{FieldVector, IntVector, NullVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.execution.python.CometArrowPythonRunnerBase.{writeDirectBatch, DirectArrowStreamWriter}

class CometArrowPythonRunnerSuite extends AnyFunSuite with Matchers {

  private def withWriter(
      childFields: Seq[Field],
      allocator: BufferAllocator,
      channel: WritableByteChannel)(f: DirectArrowStreamWriter => Unit): Unit = {
    val structField = new Field(
      "struct",
      new FieldType(false, ArrowType.Struct.INSTANCE, null),
      childFields.asJava)
    val root = VectorSchemaRoot.create(new Schema(Seq(structField).asJava), allocator)
    val writer = new DirectArrowStreamWriter(root, channel)
    try {
      writer.start()
      f(writer)
      writer.end()
    } finally {
      writer.close()
      root.close()
    }
  }

  private def withReader(bytes: Array[Byte])(f: ArrowStreamReader => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    try {
      f(reader)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("direct batches retain borrowed buffers without copying them into the writer allocator") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(1024)
    val vector = new VarCharVector("source_name", sourceAllocator)
    val output = new ByteArrayOutputStream()
    try {
      val payload = Array.fill[Byte](16 * 1024)('x'.toByte)
      vector.allocateNew(payload.length.toLong, 2)
      vector.setSafe(0, payload)
      vector.setNull(1)
      vector.setValueCount(2)

      val field = new Field("payload", vector.getField.getFieldType, vector.getField.getChildren)
      val buffers = vector.getFieldBuffers.asScala.toSeq
      val originalReferenceCounts = buffers.map(_.refCnt())
      val originalLastSet = vector.getLastSet

      withWriter(Seq(field), writerAllocator, Channels.newChannel(output)) { writer =>
        val originalWriterAllocation = writerAllocator.getAllocatedMemory
        writeDirectBatch(writer, Seq(vector), 2, writerAllocator)

        writerAllocator.getAllocatedMemory shouldBe originalWriterAllocation
        buffers.map(_.refCnt()) shouldBe originalReferenceCounts
        vector.getLastSet shouldBe originalLastSet
        vector.getValueCount shouldBe 2
        vector.get(0) shouldBe payload
        vector.isNull(1) shouldBe true
      }

      withReader(output.toByteArray) { reader =>
        reader.loadNextBatch() shouldBe true
        val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
        struct.getNullCount shouldBe 0
        val result = struct.getChild("payload").asInstanceOf[VarCharVector]
        result.get(0) shouldBe payload
        result.isNull(1) shouldBe true
        reader.loadNextBatch() shouldBe false
      }
    } finally {
      vector.close()
      writerAllocator.close()
      sourceAllocator.close()
    }
  }

  test("direct batches preserve nested list, struct, map, and null field layouts") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(Long.MaxValue)
    val list = ListVector.empty("items", sourceAllocator)
    val struct = StructVector.empty("details", sourceAllocator)
    val map = MapVector.empty("mapping", sourceAllocator, false)
    val nulls = new NullVector("nulls", 3)
    val output = new ByteArrayOutputStream()
    try {
      val listWriter = list.getWriter
      listWriter.setPosition(0)
      listWriter.startList()
      listWriter.integer().writeInt(11)
      listWriter.integer().writeInt(12)
      listWriter.endList()
      listWriter.setPosition(1)
      listWriter.writeNull()
      listWriter.setPosition(2)
      listWriter.startList()
      listWriter.integer().writeInt(13)
      listWriter.endList()
      listWriter.setValueCount(3)

      val structWriter = struct.getWriter
      structWriter.setPosition(0)
      structWriter.start()
      structWriter.integer("count").writeInt(21)
      structWriter.end()
      structWriter.setPosition(1)
      structWriter.writeNull()
      structWriter.setPosition(2)
      structWriter.start()
      structWriter.integer("count").writeNull()
      structWriter.end()
      structWriter.setValueCount(3)

      val mapWriter = map.getWriter
      mapWriter.setPosition(0)
      mapWriter.startMap()
      mapWriter.startEntry()
      mapWriter.key().integer().writeInt(31)
      mapWriter.value().integer().writeInt(32)
      mapWriter.endEntry()
      mapWriter.endMap()
      mapWriter.setPosition(1)
      mapWriter.writeNull()
      mapWriter.setPosition(2)
      mapWriter.startMap()
      mapWriter.startEntry()
      mapWriter.key().integer().writeInt(33)
      mapWriter.value().integer().writeNull()
      mapWriter.endEntry()
      mapWriter.endMap()
      mapWriter.setValueCount(3)

      val vectors = Seq[FieldVector](list, struct, map, nulls)
      withWriter(vectors.map(_.getField), writerAllocator, Channels.newChannel(output)) {
        writer =>
          writeDirectBatch(writer, vectors, 3, writerAllocator)
      }

      withReader(output.toByteArray) { reader =>
        reader.loadNextBatch() shouldBe true
        val result = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
        result.getNullCount shouldBe 0

        val resultList = result.getChild("items").asInstanceOf[ListVector]
        resultList.getObject(0).asScala.toSeq shouldBe Seq(11, 12)
        resultList.isNull(1) shouldBe true
        resultList.getObject(2).asScala.toSeq shouldBe Seq(13)

        val resultStruct = result.getChild("details").asInstanceOf[StructVector]
        resultStruct.getChild("count").asInstanceOf[IntVector].get(0) shouldBe 21
        resultStruct.isNull(1) shouldBe true
        resultStruct.getChild("count").isNull(2) shouldBe true

        val resultMap = result.getChild("mapping").asInstanceOf[MapVector]
        val entries = resultMap.getDataVector.asInstanceOf[StructVector]
        entries.getChildByOrdinal(0).getField.getName shouldBe MapVector.KEY_NAME
        entries.getChildByOrdinal(1).getField.getName shouldBe MapVector.VALUE_NAME
        entries.getChildByOrdinal(0).asInstanceOf[IntVector].get(0) shouldBe 31
        entries.getChildByOrdinal(1).asInstanceOf[IntVector].get(0) shouldBe 32
        resultMap.isNull(1) shouldBe true
        entries.getChildByOrdinal(1).isNull(1) shouldBe true

        val resultNulls = result.getChild("nulls").asInstanceOf[NullVector]
        resultNulls.getNullCount shouldBe 3
        reader.loadNextBatch() shouldBe false
      }
    } finally {
      nulls.close()
      map.close()
      struct.close()
      list.close()
      writerAllocator.close()
      sourceAllocator.close()
    }
  }

  test("direct batches preserve zero-row batches between populated batches") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(Long.MaxValue)
    val first = new IntVector("value", sourceAllocator)
    val empty = new IntVector("value", sourceAllocator)
    val last = new IntVector("value", sourceAllocator)
    val output = new ByteArrayOutputStream()
    try {
      first.allocateNew(2)
      first.setSafe(0, 41)
      first.setSafe(1, 42)
      first.setValueCount(2)
      empty.setValueCount(0)
      last.allocateNew(1)
      last.setSafe(0, 43)
      last.setValueCount(1)

      withWriter(Seq(first.getField), writerAllocator, Channels.newChannel(output)) { writer =>
        writeDirectBatch(writer, Seq(first), 2, writerAllocator)
        writeDirectBatch(writer, Seq(empty), 0, writerAllocator)
        writeDirectBatch(writer, Seq(last), 1, writerAllocator)
      }

      withReader(output.toByteArray) { reader =>
        reader.loadNextBatch() shouldBe true
        reader.getVectorSchemaRoot.getRowCount shouldBe 2
        reader.loadNextBatch() shouldBe true
        reader.getVectorSchemaRoot.getRowCount shouldBe 0
        reader.loadNextBatch() shouldBe true
        reader.getVectorSchemaRoot.getRowCount shouldBe 1
        val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
        struct.getChild("value").asInstanceOf[IntVector].get(0) shouldBe 43
        reader.loadNextBatch() shouldBe false
      }
    } finally {
      last.close()
      empty.close()
      first.close()
      writerAllocator.close()
      sourceAllocator.close()
    }
  }

  test("direct batches represent non-null structs with no child columns") {
    val allocator = new RootAllocator(Long.MaxValue)
    val output = new ByteArrayOutputStream()
    try {
      withWriter(Seq.empty, allocator, Channels.newChannel(output)) { writer =>
        writeDirectBatch(writer, Seq.empty, 3, allocator)
      }

      withReader(output.toByteArray) { reader =>
        reader.loadNextBatch() shouldBe true
        val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
        struct.getValueCount shouldBe 3
        struct.getNullCount shouldBe 0
        struct.getChildrenFromFields.isEmpty shouldBe true
        reader.loadNextBatch() shouldBe false
      }
    } finally {
      allocator.close()
    }
  }

  test("direct batches release temporary references when writing the stream fails") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(Long.MaxValue)
    val source = new IntVector("value", sourceAllocator)
    val output = new ByteArrayOutputStream()
    var failWrites = false
    val channel = new WritableByteChannel {
      private var open = true

      override def isOpen: Boolean = open

      override def close(): Unit = open = false

      override def write(buffer: ByteBuffer): Int = {
        if (failWrites) {
          throw new IOException("injected Arrow IPC write failure")
        }
        val bytes = new Array[Byte](buffer.remaining())
        buffer.get(bytes)
        output.write(bytes)
        bytes.length
      }
    }
    try {
      source.allocateNew(1)
      source.setSafe(0, 51)
      source.setValueCount(1)

      withWriter(Seq(source.getField), writerAllocator, channel) { writer =>
        val originalReferenceCounts = source.getFieldBuffers.asScala.map(_.refCnt()).toSeq
        val originalWriterAllocation = writerAllocator.getAllocatedMemory
        failWrites = true
        try {
          val error = intercept[IOException] {
            writeDirectBatch(writer, Seq(source), 1, writerAllocator)
          }
          error.getMessage shouldBe "injected Arrow IPC write failure"
        } finally {
          failWrites = false
        }
        source.getFieldBuffers.asScala.map(_.refCnt()).toSeq shouldBe originalReferenceCounts
        writerAllocator.getAllocatedMemory shouldBe originalWriterAllocation
        source.get(0) shouldBe 51
      }
    } finally {
      source.close()
      writerAllocator.close()
      sourceAllocator.close()
    }
  }
}

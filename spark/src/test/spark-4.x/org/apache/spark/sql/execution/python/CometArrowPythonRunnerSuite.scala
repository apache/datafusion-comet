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
import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, OutOfMemoryException, RootAllocator}
import org.apache.arrow.vector.{FieldVector, FixedSizeBinaryVector, IntVector, LargeVarBinaryVector, LargeVarCharVector, NullVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType, Schema}
import org.apache.spark.sql.execution.python.CometArrowPythonRunnerBase.{hasCompatibleSchema, serializeBatch, withLargeVarTypes}

class CometArrowPythonRunnerSuite extends AnyFunSuite with Matchers {

  private def withWriter(
      childFields: Seq[Field],
      allocator: BufferAllocator,
      channel: WritableByteChannel)(f: WritableByteChannel => Unit): Unit = {
    val structField = new Field(
      "struct",
      new FieldType(false, ArrowType.Struct.INSTANCE, null),
      childFields.asJava)
    val root = VectorSchemaRoot.create(new Schema(Seq(structField).asJava), allocator)
    val writer = new ArrowStreamWriter(root, null, channel)
    try {
      writer.start()
      f(channel)
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

  test("input schema compatibility preserves physical types and nested layouts") {
    val intType = new ArrowType.Int(32, true)
    def nested(dataType: ArrowType): Seq[Field] = Seq(
      new Field(
        "outer",
        FieldType.nullable(ArrowType.Struct.INSTANCE),
        Seq(new Field("value", FieldType.nullable(dataType), null)).asJava))

    val renamed = Seq(
      new Field(
        "renamed",
        FieldType.notNullable(ArrowType.Struct.INSTANCE),
        Seq(new Field("other", FieldType.notNullable(intType), null)).asJava))
    hasCompatibleSchema(nested(intType), renamed) shouldBe true
    hasCompatibleSchema(nested(intType), Seq.empty) shouldBe false
    hasCompatibleSchema(
      nested(intType),
      Seq(new Field("outer", FieldType.nullable(ArrowType.Struct.INSTANCE), null))) shouldBe false

    val incompatibleTypes: Seq[(ArrowType, ArrowType)] = Seq(
      (intType, new ArrowType.Int(64, true)),
      (intType, new ArrowType.Int(32, false)),
      (ArrowType.Utf8.INSTANCE, ArrowType.LargeUtf8.INSTANCE),
      (ArrowType.Binary.INSTANCE, ArrowType.LargeBinary.INSTANCE),
      (new ArrowType.Decimal(10, 2, 128), new ArrowType.Decimal(10, 3, 128)))
    incompatibleTypes.foreach { case (expected, actual) =>
      hasCompatibleSchema(nested(expected), nested(actual)) shouldBe false
    }
  }

  test("input schema compatibility preserves extension and dictionary interpretation") {
    val intType = new ArrowType.Int(32, true)
    def fields(
        metadata: Map[String, String] = Map.empty,
        dictionary: DictionaryEncoding = null): Seq[Field] =
      Seq(new Field("value", new FieldType(true, intType, dictionary, metadata.asJava), null))

    hasCompatibleSchema(
      fields(Map("PARQUET:field_id" -> "1")),
      fields(Map("PARQUET:field_id" -> "2"))) shouldBe true
    Seq(
      ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
      ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA).foreach { key =>
      hasCompatibleSchema(
        fields(Map(key -> "before")),
        fields(Map(key -> "after"))) shouldBe false
    }
    val dictionary = new DictionaryEncoding(1L, false, intType)
    hasCompatibleSchema(fields(dictionary = dictionary), fields()) shouldBe false
    hasCompatibleSchema(
      fields(dictionary = dictionary),
      fields(dictionary = new DictionaryEncoding(2L, false, intType))) shouldBe false
  }

  test("input schema compatibility accepts UTC timestamp aliases with matching units") {
    def fields(unit: TimeUnit, timezone: String): Seq[Field] =
      Seq(new Field("ts", FieldType.nullable(new ArrowType.Timestamp(unit, timezone)), null))
    def nested(children: Seq[Field]): Seq[Field] =
      Seq(new Field("outer", FieldType.nullable(ArrowType.Struct.INSTANCE), children.asJava))

    for (unit <- TimeUnit.values()) {
      val utc = fields(unit, "UTC")
      val alias = fields(unit, "Etc/UTC")
      hasCompatibleSchema(utc, alias) shouldBe true
      hasCompatibleSchema(alias, utc) shouldBe true
      hasCompatibleSchema(nested(utc), nested(alias)) shouldBe true
      hasCompatibleSchema(nested(alias), nested(utc)) shouldBe true
    }

    for {
      expectedUnit <- TimeUnit.values()
      actualUnit <- TimeUnit.values()
      if expectedUnit != actualUnit
    } {
      hasCompatibleSchema(
        fields(expectedUnit, "UTC"),
        fields(actualUnit, "Etc/UTC")) shouldBe false
      hasCompatibleSchema(
        fields(expectedUnit, "Etc/UTC"),
        fields(actualUnit, "UTC")) shouldBe false
    }
  }

  test("UTC timestamp aliases preserve timezone, dictionary and extension constraints") {
    def fields(
        timezone: String,
        metadata: Map[String, String] = Map.empty,
        dictionary: DictionaryEncoding = null): Seq[Field] = Seq(
      new Field(
        "ts",
        new FieldType(
          true,
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone),
          dictionary,
          metadata.asJava),
        null))

    for {
      utc <- Seq("UTC", "Etc/UTC")
      other <- Seq(null, "", "GMT", "+00:00", "America/Los_Angeles")
    } {
      hasCompatibleSchema(fields(utc), fields(other)) shouldBe false
      hasCompatibleSchema(fields(other), fields(utc)) shouldBe false
    }
    hasCompatibleSchema(fields(null), fields(null)) shouldBe true
    hasCompatibleSchema(
      fields("America/Los_Angeles"),
      fields("America/Los_Angeles")) shouldBe true

    val dictionary = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true))
    val otherDictionary = new DictionaryEncoding(2L, false, dictionary.getIndexType)
    hasCompatibleSchema(fields("UTC", dictionary = dictionary), fields("Etc/UTC")) shouldBe false
    hasCompatibleSchema(
      fields("UTC", dictionary = dictionary),
      fields("Etc/UTC", dictionary = otherDictionary)) shouldBe false
    Seq(
      ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
      ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA).foreach { key =>
      hasCompatibleSchema(
        fields("UTC", Map(key -> "before")),
        fields("Etc/UTC", Map(key -> "after"))) shouldBe false
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

      withWriter(Seq(field), writerAllocator, Channels.newChannel(output)) { channel =>
        val originalWriterAllocation = writerAllocator.getAllocatedMemory
        serializeBatch(
          new WriteChannel(channel),
          Seq(vector),
          2,
          writerAllocator,
          useLargeVarTypes = false)

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

  for {
    failSerialization <- Seq(false, true)
    useLargeVarTypes <- Seq(false, true)
  } {
    test(
      s"direct FFI batches release references (failure: $failSerialization, large: $useLargeVarTypes)") {
      // Arrow's JNI loader extracts its library here; Maven's target/tmp may not exist yet.
      Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
      val sourceAllocator = new RootAllocator(Long.MaxValue)
      val importAllocator = new RootAllocator(Long.MaxValue)
      val writerAllocator = new RootAllocator(1024)
      val source = new VarCharVector("payload", sourceAllocator)
      val array = ArrowArray.allocateNew(sourceAllocator)
      val schema = ArrowSchema.allocateNew(sourceAllocator)
      var imported: VarCharVector = null
      var failWrites = false
      val output = new ByteArrayOutputStream() {
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
          if (failWrites) {
            throw new IOException("injected Arrow IPC write failure")
          }
          super.write(bytes, offset, length)
        }
      }
      try {
        val payload = Array.fill[Byte](16 * 1024)('x'.toByte)
        source.allocateNew(payload.length.toLong, 2)
        source.setSafe(0, payload)
        source.setNull(1)
        source.setValueCount(2)

        Data.exportVector(sourceAllocator, source, null, array, schema)
        imported =
          Data.importVector(importAllocator, array, schema, null).asInstanceOf[VarCharVector]
        imported.getDataBuffer.memoryAddress() shouldBe source.getDataBuffer.memoryAddress()
        // Only the C Data Interface release callback now keeps the original buffers alive.
        source.close()

        val buffers = imported.getFieldBuffers.asScala.toSeq
        val originalReferenceCounts = buffers.map(_.refCnt())
        val originalImportAllocation = importAllocator.getAllocatedMemory
        val originalSourceAllocation = sourceAllocator.getAllocatedMemory
        originalSourceAllocation should be > 0L

        val field =
          if (useLargeVarTypes) withLargeVarTypes(imported.getField) else imported.getField
        withWriter(Seq(field), writerAllocator, Channels.newChannel(output)) { channel =>
          val originalWriterAllocation = writerAllocator.getAllocatedMemory
          failWrites = failSerialization
          try {
            if (failSerialization) {
              val error = intercept[IOException] {
                serializeBatch(
                  new WriteChannel(channel),
                  Seq(imported),
                  2,
                  writerAllocator,
                  useLargeVarTypes)
              }
              error.getMessage shouldBe "injected Arrow IPC write failure"
            } else {
              serializeBatch(
                new WriteChannel(channel),
                Seq(imported),
                2,
                writerAllocator,
                useLargeVarTypes)
            }
          } finally {
            failWrites = false
          }

          buffers.map(_.refCnt()) shouldBe originalReferenceCounts
          importAllocator.getAllocatedMemory shouldBe originalImportAllocation
          sourceAllocator.getAllocatedMemory shouldBe originalSourceAllocation
          writerAllocator.getAllocatedMemory shouldBe originalWriterAllocation
          imported.getValueCount shouldBe 2
          imported.get(0) shouldBe payload
          imported.isNull(1) shouldBe true
        }

        if (!failSerialization) {
          withReader(output.toByteArray) { reader =>
            reader.loadNextBatch() shouldBe true
            val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
            val result = struct.getChild("payload")
            result.getField.getType shouldBe field.getType
            result.getObject(0).toString shouldBe new String(payload, "UTF-8")
            result.isNull(1) shouldBe true
            reader.loadNextBatch() shouldBe false
          }
        }

        imported.close()
        imported = null
        importAllocator.getAllocatedMemory shouldBe 0L
        sourceAllocator.getAllocatedMemory shouldBe 0L
        writerAllocator.getAllocatedMemory shouldBe 0L
      } finally {
        if (imported != null) {
          imported.close()
        }
        schema.close()
        array.close()
        source.close()
        writerAllocator.close()
        importAllocator.close()
        sourceAllocator.close()
      }
    }
  }

  test("large input types widen offsets without copying string or binary payloads") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(1024)
    val text = new VarCharVector("text", sourceAllocator)
    val binary = new VarBinaryVector("binary", sourceAllocator)
    val output = new ByteArrayOutputStream()
    try {
      val strings = Seq(
        Array.fill[Byte](16 * 1024)('x'.toByte),
        Array.emptyByteArray,
        "λ中文".getBytes("UTF-8"))
      val bytes =
        Seq(Array.fill[Byte](16 * 1024)(0xff.toByte), Array.emptyByteArray, Array[Byte](0, 1, -1))
      text.allocateNew()
      binary.allocateNew()
      Seq(0, 2, 3).zipWithIndex.foreach { case (row, i) =>
        text.setSafe(row, strings(i))
        binary.setSafe(row, bytes(i))
      }
      text.setNull(1)
      binary.setNull(1)
      text.setValueCount(4)
      binary.setValueCount(4)
      val vectors = Seq[FieldVector](text, binary)
      val buffers = vectors.flatMap(_.getFieldBuffers.asScala)
      val refs = buffers.map(_.refCnt())
      val sourceBytes = sourceAllocator.getAllocatedMemory

      withWriter(
        vectors.map(v => withLargeVarTypes(v.getField)),
        writerAllocator,
        Channels.newChannel(output)) { channel =>
        serializeBatch(
          new WriteChannel(channel),
          vectors,
          4,
          writerAllocator,
          useLargeVarTypes = true)
        writerAllocator.getAllocatedMemory shouldBe 0L
        sourceAllocator.getAllocatedMemory shouldBe sourceBytes
        buffers.map(_.refCnt()) shouldBe refs
        text.getLastSet shouldBe 3
        binary.getLastSet shouldBe 3
        text.get(3) shouldBe strings(2)
        binary.get(3) shouldBe bytes(2)
      }
      withReader(output.toByteArray) { reader =>
        reader.loadNextBatch() shouldBe true
        val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
        val resultText = struct.getChild("text").asInstanceOf[LargeVarCharVector]
        val resultBinary = struct.getChild("binary").asInstanceOf[LargeVarBinaryVector]
        Seq(0, 2, 3).zipWithIndex.foreach { case (row, i) =>
          resultText.get(row) shouldBe strings(i)
          resultBinary.get(row) shouldBe bytes(i)
        }
        resultText.isNull(1) shouldBe true
        resultBinary.isNull(1) shouldBe true
        reader.loadNextBatch() shouldBe false
      }
    } finally {
      binary.close()
      text.close()
      writerAllocator.close()
      sourceAllocator.close()
    }
  }

  test("large input types preserve empty batches and reuse already-large offsets") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    val writerAllocator = new RootAllocator(64)
    val first = new VarCharVector("value", sourceAllocator)
    val empty = new VarCharVector("value", sourceAllocator)
    val last = new LargeVarCharVector("value", sourceAllocator)
    val output = new ByteArrayOutputStream()
    try {
      first.allocateNew()
      first.setSafe(0, "first".getBytes("UTF-8"))
      first.setValueCount(1)
      last.allocateNew()
      last.setSafe(0, "last".getBytes("UTF-8"))
      last.setValueCount(1)
      val largeBuffers = last.getFieldBuffers.asScala.toSeq
      val largeRefs = largeBuffers.map(_.refCnt())
      val largeField = withLargeVarTypes(first.getField)
      hasCompatibleSchema(Seq(largeField), Seq(withLargeVarTypes(last.getField))) shouldBe true

      withWriter(Seq(largeField), writerAllocator, Channels.newChannel(output)) { channel =>
        serializeBatch(
          new WriteChannel(channel),
          Seq(first),
          1,
          writerAllocator,
          useLargeVarTypes = true)
        serializeBatch(
          new WriteChannel(channel),
          Seq(empty),
          0,
          writerAllocator,
          useLargeVarTypes = true)
        // Only the wrapping bitmap fits: an already-large input must not allocate new offsets.
        writerAllocator.setLimit(8L)
        serializeBatch(
          new WriteChannel(channel),
          Seq(last),
          1,
          writerAllocator,
          useLargeVarTypes = true)
        largeBuffers.map(_.refCnt()) shouldBe largeRefs
        writerAllocator.getAllocatedMemory shouldBe 0L
      }
      withReader(output.toByteArray) { reader =>
        Seq(Some("first"), None, Some("last")).foreach { expected =>
          reader.loadNextBatch() shouldBe true
          reader.getVectorSchemaRoot.getRowCount shouldBe expected.size
          val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
          val value = struct.getChild("value").asInstanceOf[LargeVarCharVector]
          expected.foreach(v => value.getObject(0).toString shouldBe v)
        }
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

  test("large input conversion releases earlier offsets when a later allocation fails") {
    val sourceAllocator = new RootAllocator(Long.MaxValue)
    // The wrapping bitmap and one 32-byte offset buffer fit, but the second offset buffer cannot.
    val writerAllocator = new RootAllocator(40)
    val vectors = Seq(
      new VarCharVector("first", sourceAllocator),
      new VarCharVector("second", sourceAllocator))
    val output = new ByteArrayOutputStream()
    try {
      vectors.foreach { vector =>
        vector.allocateNew()
        (0 until 3).foreach(i => vector.setSafe(i, s"value-$i".getBytes("UTF-8")))
        vector.setValueCount(3)
      }
      val buffers = vectors.flatMap(_.getFieldBuffers.asScala)
      val refs = buffers.map(_.refCnt())
      val sourceBytes = sourceAllocator.getAllocatedMemory
      withWriter(
        vectors.map(v => withLargeVarTypes(v.getField)),
        writerAllocator,
        Channels.newChannel(output)) { channel =>
        intercept[OutOfMemoryException] {
          serializeBatch(
            new WriteChannel(channel),
            vectors,
            3,
            writerAllocator,
            useLargeVarTypes = true)
        }
        writerAllocator.getPeakMemoryAllocation should be > 8L
        writerAllocator.getAllocatedMemory shouldBe 0L
        buffers.map(_.refCnt()) shouldBe refs
        sourceAllocator.getAllocatedMemory shouldBe sourceBytes
        vectors.foreach(_.getObject(2).toString shouldBe "value-2")
      }
    } finally {
      vectors.foreach(_.close())
      writerAllocator.close()
      sourceAllocator.close()
    }
  }

  for (useLargeVarTypes <- Seq(false, true)) {
    test(s"direct batches preserve mixed nested layouts (large: $useLargeVarTypes)") {
      val sourceAllocator = new RootAllocator(Long.MaxValue)
      val writerAllocator = new RootAllocator(Long.MaxValue)
      val details = StructVector.empty("details", sourceAllocator)
      val texts = ListVector.empty("texts", sourceAllocator)
      val mapping = MapVector.empty("mapping", sourceAllocator, false)
      val records = ListVector.empty("records", sourceAllocator)
      val nulls = new NullVector("nulls", 3)
      val fixed = new FixedSizeBinaryVector("fixed", sourceAllocator, 4)
      val trailing = new VarCharVector("trailing", sourceAllocator)
      val output = new ByteArrayOutputStream()
      try {
        val detailsWriter = details.getWriter
        detailsWriter.setPosition(0)
        detailsWriter.start()
        detailsWriter.varChar("text").writeVarChar("alpha")
        detailsWriter.integer("count").writeInt(21)
        detailsWriter.varBinary("data").writeVarBinary(Array[Byte](1, 2))
        detailsWriter.end()
        detailsWriter.setPosition(1)
        detailsWriter.writeNull()
        detailsWriter.setPosition(2)
        detailsWriter.start()
        detailsWriter.varChar("text").writeVarChar("")
        detailsWriter.integer("count").writeNull()
        detailsWriter.varBinary("data").writeVarBinary(Array[Byte](3, 4))
        detailsWriter.end()
        detailsWriter.setValueCount(3)

        val textsWriter = texts.getWriter
        textsWriter.setPosition(0)
        textsWriter.startList()
        textsWriter.varChar().writeVarChar("one")
        textsWriter.varChar().writeVarChar("")
        textsWriter.endList()
        textsWriter.setPosition(1)
        textsWriter.writeNull()
        textsWriter.setPosition(2)
        textsWriter.startList()
        textsWriter.varChar().writeVarChar("three")
        textsWriter.endList()
        textsWriter.setValueCount(3)

        val mapWriter = mapping.getWriter
        mapWriter.setPosition(0)
        mapWriter.startMap()
        mapWriter.startEntry()
        mapWriter.key().varChar().writeVarChar("key-0")
        mapWriter.value().varChar().writeVarChar("value-0")
        mapWriter.endEntry()
        mapWriter.endMap()
        mapWriter.setPosition(1)
        mapWriter.writeNull()
        mapWriter.setPosition(2)
        mapWriter.startMap()
        mapWriter.startEntry()
        mapWriter.key().varChar().writeVarChar("key-2")
        mapWriter.value().varChar().writeVarChar("value-2")
        mapWriter.endEntry()
        mapWriter.endMap()
        mapWriter.setValueCount(3)

        val recordsWriter = records.getWriter
        val recordWriter = recordsWriter.struct()
        recordsWriter.setPosition(0)
        recordsWriter.startList()
        recordWriter.start()
        recordWriter.varChar("text").writeVarChar("record-0")
        recordWriter.end()
        recordsWriter.endList()
        recordsWriter.setPosition(1)
        recordsWriter.writeNull()
        recordsWriter.setPosition(2)
        recordsWriter.startList()
        recordWriter.start()
        recordWriter.varChar("text").writeVarChar("record-2")
        recordWriter.end()
        recordsWriter.endList()
        recordsWriter.setValueCount(3)

        fixed.allocateNew()
        fixed.setSafe(0, Array[Byte](5, 6, 7, 8))
        fixed.setNull(1)
        fixed.setSafe(2, Array[Byte](9, 10, 11, 12))
        fixed.setValueCount(3)

        trailing.allocateNew()
        trailing.setSafe(0, "tail-0".getBytes("UTF-8"))
        trailing.setNull(1)
        trailing.setSafe(2, "tail-2".getBytes("UTF-8"))
        trailing.setValueCount(3)

        val vectors = Seq[FieldVector](details, texts, mapping, records, nulls, fixed, trailing)
        def buffers(vector: FieldVector): Seq[ArrowBuf] =
          vector.getFieldBuffers.asScala.toSeq ++
            vector.getChildrenFromFields.asScala.toSeq.flatMap(buffers)
        val sourceBuffers = vectors.flatMap(buffers)
        val sourceRefs = sourceBuffers.map(_.refCnt())
        val sourceBytes = sourceAllocator.getAllocatedMemory
        val streamFields = vectors.map { vector =>
          if (useLargeVarTypes) withLargeVarTypes(vector.getField) else vector.getField
        }

        withWriter(streamFields, writerAllocator, Channels.newChannel(output)) { channel =>
          serializeBatch(new WriteChannel(channel), vectors, 3, writerAllocator, useLargeVarTypes)
          writerAllocator.getAllocatedMemory shouldBe 0L
          sourceAllocator.getAllocatedMemory shouldBe sourceBytes
          sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
        }

        withReader(output.toByteArray) { reader =>
          reader.loadNextBatch() shouldBe true
          val result = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
          result.getNullCount shouldBe 0
          val expectedUtf8 =
            if (useLargeVarTypes) ArrowType.LargeUtf8.INSTANCE else ArrowType.Utf8.INSTANCE
          val expectedBinary =
            if (useLargeVarTypes) ArrowType.LargeBinary.INSTANCE else ArrowType.Binary.INSTANCE

          val resultDetails = result.getChild("details").asInstanceOf[StructVector]
          val detailText = resultDetails.getChild("text")
          val detailCount = resultDetails.getChild("count").asInstanceOf[IntVector]
          val detailData = resultDetails.getChild("data")
          detailText.getField.getType shouldBe expectedUtf8
          detailData.getField.getType shouldBe expectedBinary
          detailText.getObject(0).toString shouldBe "alpha"
          detailCount.get(0) shouldBe 21
          detailData.getObject(0).asInstanceOf[Array[Byte]] shouldBe Array[Byte](1, 2)
          resultDetails.isNull(1) shouldBe true
          detailText.getObject(2).toString shouldBe ""
          detailCount.isNull(2) shouldBe true
          detailData.getObject(2).asInstanceOf[Array[Byte]] shouldBe Array[Byte](3, 4)

          val resultTexts = result.getChild("texts").asInstanceOf[ListVector]
          resultTexts.getDataVector.getField.getType shouldBe expectedUtf8
          resultTexts.getObject(0).asScala.map(_.toString).toSeq shouldBe Seq("one", "")
          resultTexts.isNull(1) shouldBe true
          resultTexts.getObject(2).asScala.map(_.toString).toSeq shouldBe Seq("three")

          val resultMap = result.getChild("mapping").asInstanceOf[MapVector]
          val entries = resultMap.getDataVector.asInstanceOf[StructVector]
          val keys = entries.getChildByOrdinal(0)
          val values = entries.getChildByOrdinal(1)
          keys.getField.getName shouldBe MapVector.KEY_NAME
          values.getField.getName shouldBe MapVector.VALUE_NAME
          keys.getField.getType shouldBe expectedUtf8
          values.getField.getType shouldBe expectedUtf8
          keys.getObject(0).toString shouldBe "key-0"
          values.getObject(0).toString shouldBe "value-0"
          resultMap.isNull(1) shouldBe true
          keys.getObject(1).toString shouldBe "key-2"
          values.getObject(1).toString shouldBe "value-2"

          val resultRecords = result.getChild("records").asInstanceOf[ListVector]
          val recordStruct = resultRecords.getDataVector.asInstanceOf[StructVector]
          val recordText = recordStruct.getChild("text")
          recordText.getField.getType shouldBe expectedUtf8
          resultRecords.getObject(0).size() shouldBe 1
          recordText.getObject(0).toString shouldBe "record-0"
          resultRecords.isNull(1) shouldBe true
          resultRecords.getObject(2).size() shouldBe 1
          recordText.getObject(1).toString shouldBe "record-2"

          result.getChild("nulls").asInstanceOf[NullVector].getNullCount shouldBe 3
          val resultFixed = result.getChild("fixed").asInstanceOf[FixedSizeBinaryVector]
          resultFixed.get(0) shouldBe Array[Byte](5, 6, 7, 8)
          resultFixed.isNull(1) shouldBe true
          resultFixed.get(2) shouldBe Array[Byte](9, 10, 11, 12)
          val resultTrailing = result.getChild("trailing")
          resultTrailing.getField.getType shouldBe expectedUtf8
          resultTrailing.getObject(0).toString shouldBe "tail-0"
          resultTrailing.isNull(1) shouldBe true
          resultTrailing.getObject(2).toString shouldBe "tail-2"
          reader.loadNextBatch() shouldBe false
        }
      } finally {
        trailing.close()
        fixed.close()
        nulls.close()
        records.close()
        mapping.close()
        texts.close()
        details.close()
        writerAllocator.close()
        sourceAllocator.close()
      }
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

      withWriter(Seq(first.getField), writerAllocator, Channels.newChannel(output)) { channel =>
        serializeBatch(
          new WriteChannel(channel),
          Seq(first),
          2,
          writerAllocator,
          useLargeVarTypes = false)
        serializeBatch(
          new WriteChannel(channel),
          Seq(empty),
          0,
          writerAllocator,
          useLargeVarTypes = false)
        serializeBatch(
          new WriteChannel(channel),
          Seq(last),
          1,
          writerAllocator,
          useLargeVarTypes = false)
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
      withWriter(Seq.empty, allocator, Channels.newChannel(output)) { channel =>
        serializeBatch(
          new WriteChannel(channel),
          Seq.empty,
          3,
          allocator,
          useLargeVarTypes = false)
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

      withWriter(Seq(source.getField), writerAllocator, channel) { channel =>
        val originalReferenceCounts = source.getFieldBuffers.asScala.map(_.refCnt()).toSeq
        val originalWriterAllocation = writerAllocator.getAllocatedMemory
        failWrites = true
        try {
          val error = intercept[IOException] {
            serializeBatch(
              new WriteChannel(channel),
              Seq(source),
              1,
              writerAllocator,
              useLargeVarTypes = false)
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

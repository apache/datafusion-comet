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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{FieldVector, IntVector, NullVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType, Schema}
import org.apache.spark.sql.execution.python.CometArrowPythonRunnerBase.{hasCompatibleSchema, serializeBatch, withMaterializedInputVectors}

import org.apache.comet.vector.{CometDecodedVector, CometDictionary, CometDictionaryVector, CometPlainVector}

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
        serializeBatch(new WriteChannel(channel), Seq(vector), 2, writerAllocator)

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

  for (failSerialization <- Seq(false, true)) {
    test(s"direct FFI batches release temporary references (write failure: $failSerialization)") {
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

        withWriter(Seq(imported.getField), writerAllocator, Channels.newChannel(output)) {
          channel =>
            val originalWriterAllocation = writerAllocator.getAllocatedMemory
            failWrites = failSerialization
            try {
              if (failSerialization) {
                val error = intercept[IOException] {
                  serializeBatch(new WriteChannel(channel), Seq(imported), 2, writerAllocator)
                }
                error.getMessage shouldBe "injected Arrow IPC write failure"
              } else {
                serializeBatch(new WriteChannel(channel), Seq(imported), 2, writerAllocator)
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
            val result = struct.getChild("payload").asInstanceOf[VarCharVector]
            result.get(0) shouldBe payload
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

  for (failSerialization <- Seq(false, true)) {
    test(s"dictionary inputs materialize logical values (failure: $failSerialization)") {
      val sourceAllocator = new RootAllocator(Long.MaxValue)
      val writerAllocator = new RootAllocator(Long.MaxValue)
      val intType = new ArrowType.Int(32, true)
      val textEncoding = new DictionaryEncoding(11L, false, intType)
      val binaryEncoding = new DictionaryEncoding(12L, false, intType)
      val textValues = new VarCharVector("text", sourceAllocator)
      val binaryValues = new VarBinaryVector("data", sourceAllocator)
      val textIndices =
        new IntVector("text", new FieldType(true, intType, textEncoding), sourceAllocator)
      val binaryIndices =
        new IntVector("data", new FieldType(true, intType, binaryEncoding), sourceAllocator)
      val dictionaries = Map(
        textEncoding.getId -> new Dictionary(textValues, textEncoding),
        binaryEncoding.getId -> new Dictionary(binaryValues, binaryEncoding))
      val provider = new DictionaryProvider {
        override def lookup(id: Long): Dictionary = dictionaries(id)

        override def getDictionaryIds: java.util.Set[java.lang.Long] =
          dictionaries.keys.map(id => java.lang.Long.valueOf(id)).toSet.asJava
      }
      val columns = Seq[CometDecodedVector](
        new CometDictionaryVector(
          new CometPlainVector(textIndices),
          new CometDictionary(new CometPlainVector(textValues)),
          provider),
        new CometDictionaryVector(
          new CometPlainVector(binaryIndices),
          new CometDictionary(new CometPlainVector(binaryValues)),
          provider))
      var failWrites = false
      val output = new ByteArrayOutputStream() {
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
          if (failWrites) {
            throw new IOException("injected dictionary IPC write failure")
          }
          super.write(bytes, offset, length)
        }
      }
      try {
        textValues.allocateNew()
        Seq("same", "", "λ中文").zipWithIndex.foreach { case (value, index) =>
          textValues.setSafe(index, value.getBytes(StandardCharsets.UTF_8))
        }
        textValues.setValueCount(3)
        binaryValues.allocateNew()
        Seq(Array[Byte](1, 2), Array.emptyByteArray, Array[Byte](0, -1)).zipWithIndex.foreach {
          case (value, index) => binaryValues.setSafe(index, value)
        }
        binaryValues.setValueCount(3)
        textIndices.allocateNew()
        Seq(0, 1, 0, 2).zipWithIndex.foreach { case (value, index) =>
          textIndices.setSafe(index, value)
        }
        textIndices.setNull(2)
        textIndices.setValueCount(4)
        binaryIndices.allocateNew()
        Seq(2, 0, 0, 1).zipWithIndex.foreach { case (value, index) =>
          binaryIndices.setSafe(index, value)
        }
        binaryIndices.setNull(2)
        binaryIndices.setValueCount(4)

        val sourceVectors = Seq(textValues, binaryValues, textIndices, binaryIndices)
        val sourceBuffers = sourceVectors.flatMap(_.getFieldBuffers.asScala)
        val sourceRefs = sourceBuffers.map(_.refCnt())
        val sourceBytes = sourceAllocator.getAllocatedMemory

        def writeDictionaryBatch(): Unit =
          withMaterializedInputVectors(columns, writerAllocator) { vectors =>
            vectors.map(_.getField.getDictionary) shouldBe Seq(null, null)
            vectors.head.getObject(0).toString shouldBe "same"
            vectors.head.getObject(1).toString shouldBe ""
            vectors.head.isNull(2) shouldBe true
            vectors.head.getObject(3).toString shouldBe "λ中文"
            vectors(1).getObject(0).asInstanceOf[Array[Byte]] shouldBe Array[Byte](0, -1)
            vectors(1).isNull(2) shouldBe true

            withWriter(vectors.map(_.getField), writerAllocator, Channels.newChannel(output)) {
              channel =>
                failWrites = failSerialization
                try {
                  serializeBatch(new WriteChannel(channel), vectors, 4, writerAllocator)
                } finally {
                  failWrites = false
                }
            }
          }

        if (failSerialization) {
          val error = intercept[IOException](writeDictionaryBatch())
          error.getMessage shouldBe "injected dictionary IPC write failure"
        } else {
          writeDictionaryBatch()
          withReader(output.toByteArray) { reader =>
            reader.loadNextBatch() shouldBe true
            val struct = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[StructVector]
            val resultText = struct.getChild("text")
            val resultData = struct.getChild("data")
            resultText.getField.getType shouldBe ArrowType.Utf8.INSTANCE
            resultData.getField.getType shouldBe ArrowType.Binary.INSTANCE
            resultText.getObject(0).toString shouldBe "same"
            resultText.getObject(1).toString shouldBe ""
            resultText.isNull(2) shouldBe true
            resultText.getObject(3).toString shouldBe "λ中文"
            resultData.getObject(0).asInstanceOf[Array[Byte]] shouldBe Array[Byte](0, -1)
            resultData.isNull(2) shouldBe true
            reader.loadNextBatch() shouldBe false
          }
        }

        writerAllocator.getAllocatedMemory shouldBe 0L
        sourceAllocator.getAllocatedMemory shouldBe sourceBytes
        sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
        textValues.getObject(0).toString shouldBe "same"
        binaryValues.getObject(2).asInstanceOf[Array[Byte]] shouldBe Array[Byte](0, -1)
      } finally {
        columns.foreach(_.close())
        writerAllocator.close()
        sourceAllocator.close()
      }
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
        channel =>
          serializeBatch(new WriteChannel(channel), vectors, 3, writerAllocator)
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

      withWriter(Seq(first.getField), writerAllocator, Channels.newChannel(output)) { channel =>
        serializeBatch(new WriteChannel(channel), Seq(first), 2, writerAllocator)
        serializeBatch(new WriteChannel(channel), Seq(empty), 0, writerAllocator)
        serializeBatch(new WriteChannel(channel), Seq(last), 1, writerAllocator)
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
        serializeBatch(new WriteChannel(channel), Seq.empty, 3, allocator)
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
            serializeBatch(new WriteChannel(channel), Seq(source), 1, writerAllocator)
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

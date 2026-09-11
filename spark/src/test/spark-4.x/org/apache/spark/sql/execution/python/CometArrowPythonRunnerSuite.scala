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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BigIntVector, FieldVector, IntVector, NullVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType, Schema}
import org.apache.spark.{SparkConf, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.CometArrowPythonRunnerBase.{foreachInputBatch, hasCompatibleSchema, inputBatchRanges, serializeBatch, withMaterializedInputVectors}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.DirectByteBufferOutputStream

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.{CometDecodedVector, CometDictionary, CometDictionaryVector, CometPlainVector, CometStructVector}

class CometArrowPythonRunnerSuite extends AnyFunSuite with Matchers {

  /**
   * Expose the production input writer without opening a worker socket. The fixture borrows its
   * input batches; the task context owns the writer allocator and must be completed after use.
   * The BasePythonRunner constructor is common to all supported Spark 4.x versions.
   */
  private class InputWriterRunner(functions: Seq[ChainedPythonFunctions])
      extends BasePythonRunner[Iterator[ColumnarBatch], ColumnarBatch](
        functions,
        PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
        Array(Array(0)),
        None,
        Map.empty)
      with CometArrowPythonRunnerBase {
    override protected val workerConf: Map[String, String] = Map.empty
    override protected val pythonMetrics: Map[String, SQLMetric] =
      Map("pythonDataSent" -> new SQLMetric("size", 0L))
    override protected val schema: StructType = StructType(
      Seq(StructField("struct", StructType(Seq(StructField("text", StringType))))))
    override protected val arrowMaxRecordsPerBatch: Int = 1
    override protected val arrowMaxBytesPerBatch: Long = Long.MaxValue

    /** Command serialization is unused because this fixture never opens a Python worker. */
    override protected def writeUDF(dataOut: DataOutputStream): Unit = ()

    /** Create a production writer borrowing the input, with cleanup owned by the given task. */
    def inputWriter(input: Iterator[Iterator[ColumnarBatch]], context: TaskContext): Writer =
      newWriter(SparkEnv.get, null, input, 0, context)

    /** Return the accumulated IPC byte metric without changing writer state. */
    def bytesSent: Long = pythonMetrics("pythonDataSent").value

    /** Return Spark's byte threshold for filling the transport buffer between socket writes. */
    def transportBufferSize: Int = bufferSize
  }

  /**
   * Install a configuration-only Spark environment, run a writer test, and complete its task even
   * on failure. No Spark services or worker processes are created. Restore the caller's
   * environment after allocator cleanup; source batches remain owned by the caller throughout.
   */
  private def withInputWriterRunner(body: (InputWriterRunner, TaskContextImpl) => Unit): Unit = {
    val previousEnv = SparkEnv.get
    val env = new SparkEnv(
      "python-writer-test",
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      new SparkConf(false))
    val context = TaskContext.empty()
    SparkEnv.set(env)
    try {
      val function = SimplePythonFunction(
        Seq.empty[Byte],
        new java.util.HashMap[String, String](),
        new java.util.ArrayList[String](),
        "python",
        "3",
        java.util.Collections.emptyList(),
        null)
      body(new InputWriterRunner(Seq(ChainedPythonFunctions(Seq(function)))), context)
    } finally {
      try {
        context.markTaskCompleted(None)
      } finally {
        SparkEnv.set(previousEnv)
      }
    }
  }

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

  private case class LargeDictionaryInput(
      columns: Seq[CometDecodedVector],
      values: VarCharVector,
      indices: IntVector,
      expected: Seq[String],
      allocator: BufferAllocator)
      extends AutoCloseable {
    override def close(): Unit = {
      columns.foreach(_.close())
      allocator.close()
    }
  }

  private def largeDictionaryInput(): LargeDictionaryInput = {
    val allocator = new RootAllocator(Long.MaxValue)
    val intType = new ArrowType.Int(32, true)
    val encoding = new DictionaryEncoding(21L, false, intType)
    val values = new VarCharVector("text", allocator)
    val indices =
      new IntVector("text", new FieldType(true, intType, encoding), allocator)
    val dictionary = new Dictionary(values, encoding)
    val provider = new DictionaryProvider {
      override def lookup(id: Long): Dictionary = {
        require(id == encoding.getId)
        dictionary
      }

      override def getDictionaryIds: java.util.Set[java.lang.Long] =
        Set(java.lang.Long.valueOf(encoding.getId)).asJava
    }

    val dictionaryValues = Seq("a" * (64 * 1024), "b" * (64 * 1024))
    values.allocateNew()
    dictionaryValues.zipWithIndex.foreach { case (value, index) =>
      values.setSafe(index, value.getBytes(StandardCharsets.UTF_8))
    }
    values.setValueCount(dictionaryValues.size)
    indices.allocateNew()
    val expected = (0 until 10).map { index =>
      val valueIndex = index % dictionaryValues.size
      indices.setSafe(index, valueIndex)
      dictionaryValues(valueIndex)
    }
    indices.setValueCount(expected.size)

    val columns = Seq[CometDecodedVector](
      new CometDictionaryVector(
        new CometPlainVector(indices),
        new CometDictionary(new CometPlainVector(values)),
        provider))
    LargeDictionaryInput(columns, values, indices, expected, allocator)
  }

  /** Borrow an allocated index vector and count scalar reads without changing its values. */
  private class CountingIndices(vector: IntVector) extends CometPlainVector(vector) {
    var reads: Int = 0

    /** Return the index at the zero-based row and increment the observable read count. */
    override def getInt(row: Int): Int = {
      reads += 1
      super.getInt(row)
    }
  }

  /** Own one dictionary column and its allocator; callers must close the fixture after use. */
  private case class DictionaryInput(
      column: CometDictionaryVector,
      values: VarCharVector,
      indices: CountingIndices,
      allocator: BufferAllocator)
      extends AutoCloseable {

    /** Release the index and dictionary buffers before closing their owning allocator. */
    override def close(): Unit = {
      column.close()
      allocator.close()
    }
  }

  /**
   * Allocate an owned UTF-8 dictionary with nullable row indices. Optional reported byte lengths
   * install synthetic offsets beyond the real payload, allowing range tests to read large lengths
   * without allocating data. These fixtures must never be decoded or serialized; closing still
   * releases the actual owned buffers normally. Reported lengths must match the dictionary size,
   * be nonnegative, and sum to at most Int.MaxValue. The caller owns the returned fixture.
   */
  private def dictionaryInput(
      dictionaryValues: Seq[String],
      rowIndices: Seq[Option[Int]],
      reportedLengths: Option[Seq[Int]] = None): DictionaryInput = {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = new VarCharVector("text", allocator)
    val intType = new ArrowType.Int(32, true)
    val encoding = new DictionaryEncoding(31L, false, intType)
    val indices = new IntVector("text", new FieldType(true, intType, encoding), allocator)
    val dictionary = new Dictionary(values, encoding)
    val provider = new DictionaryProvider {

      /** Return the fixture's borrowed dictionary, rejecting any unexpected dictionary ID. */
      override def lookup(id: Long): Dictionary = {
        require(id == encoding.getId)
        dictionary
      }

      /** Return the single dictionary ID without transferring ownership of its buffers. */
      override def getDictionaryIds: java.util.Set[java.lang.Long] =
        Set(java.lang.Long.valueOf(encoding.getId)).asJava
    }
    try {
      values.allocateNew()
      dictionaryValues.zipWithIndex.foreach { case (value, index) =>
        values.setSafe(index, value.getBytes(StandardCharsets.UTF_8))
      }
      values.setValueCount(dictionaryValues.size)
      reportedLengths.foreach { lengths =>
        require(lengths.size == dictionaryValues.size)
        require(lengths.forall(_ >= 0))
        require(lengths.map(_.toLong).sum <= Int.MaxValue.toLong)
        // Planning reads offsets and validity only. Leave real payload capacity and ownership
        // untouched, and never pass this deliberately synthetic layout to decoding or IPC.
        var offset = 0
        values.getOffsetBuffer.setInt(0L, offset)
        lengths.zipWithIndex.foreach { case (length, index) =>
          offset += length
          values.getOffsetBuffer.setInt((index + 1L) * 4L, offset)
        }
      }
      indices.allocateNew(math.max(1, rowIndices.size))
      rowIndices.zipWithIndex.foreach { case (value, row) =>
        value match {
          case Some(index) => indices.setSafe(row, index)
          case None => indices.setNull(row)
        }
      }
      indices.setValueCount(rowIndices.size)
      val countedIndices = new CountingIndices(indices)
      DictionaryInput(
        new CometDictionaryVector(
          countedIndices,
          new CometDictionary(new CometPlainVector(values)),
          provider),
        values,
        countedIndices,
        allocator)
    } catch {
      case error: Throwable =>
        indices.close()
        values.close()
        allocator.close()
        throw error
    }
  }

  /**
   * Model Spark's soft byte limit from logical UTF-8 payload lengths (None means a null row).
   * Each returned range is contiguous and nonempty, except the single range for empty input. This
   * independent oracle includes offsets and validity bytes and checks the soft limit before
   * adding the next row; all randomized payloads are far below Arrow's hard ceiling.
   */
  private def expectedDictionaryRanges(
      payloadLengths: Seq[Option[Int]],
      maxRecords: Int,
      maxBytes: Long): Seq[(Int, Int)] = {
    if (payloadLengths.isEmpty) {
      return Seq(0 -> 0)
    }
    val ranges = ArrayBuffer.empty[(Int, Int)]
    val recordLimit = if (maxRecords > 0) maxRecords else Int.MaxValue
    val byteLimit = if (maxBytes > 0) maxBytes else Long.MaxValue
    var start = 0
    while (start < payloadLengths.size) {
      var length = 0
      var bytes = 4L
      while (start + length < payloadLengths.size && length < recordLimit &&
        (length == 0 || bytes < byteLimit)) {
        bytes += payloadLengths(start + length).getOrElse(0).toLong + 4L
        if (length % 8 == 0) {
          bytes += 1L
        }
        length += 1
      }
      ranges += start -> length
      start += length
    }
    ranges.toSeq
  }

  Seq((64 * 1024, 256), (16, 600)).foreach { case (valueBytes, firstRows) =>
    test(
      s"input writer bounds Spark transport buffering for $valueBytes-byte dictionary values") {
      val firstValue = "a" * valueBytes
      val secondValue = "b" * valueBytes
      val secondRows = 3
      val first = dictionaryInput(Seq(firstValue), Seq.fill(firstRows)(Some(0)))
      val second = dictionaryInput(Seq(secondValue), Seq.fill(secondRows)(Some(0)))
      val transport = new DirectByteBufferOutputStream()
      val wireBytes = new ByteArrayOutputStream()
      try {
        withInputWriterRunner { (runner, context) =>
          var emittedBatches = 0

          /**
           * Borrow one source until all its expected writer calls finish. Checking hasNext after
           * delivery models an upstream iterator that can close/reuse the current batch there.
           */
          def guardedSource(
              input: DictionaryInput,
              rows: Int,
              expectedCalls: Int): Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
            private var delivered = false

            /** Report availability, rejecting source advancement while a slice is pending. */
            override def hasNext: Boolean = {
              if (delivered) {
                emittedBatches shouldBe expectedCalls
              }
              !delivered
            }

            /** Return the borrowed batch once; the fixture retains ownership of its vectors. */
            override def next(): ColumnarBatch = {
              assert(!delivered)
              delivered = true
              new ColumnarBatch(Array[ColumnVector](input.column), rows)
            }
          }

          val input = Iterator(
            Iterator.empty,
            guardedSource(first, firstRows, firstRows),
            Iterator.empty,
            guardedSource(second, secondRows, firstRows + secondRows),
            Iterator.empty)
          val writer = runner.inputWriter(input, context)
          val sourceBytes = Seq(first, second).map(_.allocator.getAllocatedMemory)
          val sourceBuffers = Seq(first, second).flatMap { source =>
            Seq(source.indices.getValueVector.getDataBuffer, source.values.getDataBuffer)
          }
          val sourceRefs = sourceBuffers.map(_.refCnt())
          val writerBytes = CometArrowAllocator.getAllocatedMemory
          var hasInput = true
          var peakPending = 0
          var maxCallsPerDrain = 0
          var countedBytes = 0L
          while (hasInput) {
            // Spark fills this direct buffer until its byte threshold, then drains to the socket.
            // Reset only after capturing every pending byte; small slices may share one drain.
            transport.reset()
            val dataOut = new DataOutputStream(transport)
            val callsBeforeDrain = emittedBatches
            while (transport.size() < runner.transportBufferSize && hasInput) {
              val bytesBeforeCall = transport.size()
              hasInput = writer.writeNextInputToStream(dataOut)
              if (hasInput) {
                emittedBatches += 1
                countedBytes += transport.size() - bytesBeforeCall
              }
              CometArrowAllocator.getAllocatedMemory shouldBe writerBytes
              Seq(first, second).map(_.allocator.getAllocatedMemory) shouldBe sourceBytes
              sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
            }
            peakPending = math.max(peakPending, transport.size())
            maxCallsPerDrain = math.max(maxCallsPerDrain, emittedBatches - callsBeforeDrain)
            // One call may cross Spark's soft threshold by one row plus Arrow IPC metadata.
            transport.size() should be <= (runner.transportBufferSize + valueBytes + 1024)
            val pending = transport.toByteBuffer
            val bytes = new Array[Byte](pending.remaining())
            pending.get(bytes)
            wireBytes.write(bytes)
          }
          emittedBatches shouldBe firstRows + secondRows
          runner.bytesSent shouldBe countedBytes
          peakPending.toLong should be < (firstRows.toLong * (valueBytes + 128L))
          if (valueBytes < runner.transportBufferSize) {
            maxCallsPerDrain should be > 1
          }
          withReader(wireBytes.toByteArray) { reader =>
            val root = reader.getVectorSchemaRoot
            var rowsRead = 0
            while (reader.loadNextBatch()) {
              root.getRowCount shouldBe 1
              val struct = root.getVector(0).asInstanceOf[StructVector]
              val text = struct.getChild("text").asInstanceOf[VarCharVector]
              val expected = if (rowsRead < firstRows) firstValue else secondValue
              text.getObject(0).toString shouldBe expected
              rowsRead += 1
            }
            rowsRead shouldBe firstRows + secondRows
          }
        }
      } finally {
        transport.close()
        second.close()
        first.close()
      }
    }
  }

  Seq(false, true).foreach { failWrite =>
    val interruption = if (failWrite) "serialization failure" else "task interruption"
    test(s"input writer releases temporaries with pending slices after $interruption") {
      val value = "x" * (64 * 1024)
      val input = dictionaryInput(Seq(value), Seq.fill(8)(Some(0)))
      val expectedFailure = new IOException("injected transport failure")
      var rejectWrites = false
      val transport = new DirectByteBufferOutputStream() {

        /**
         * Fail an enabled IPC write before mutating the direct buffer; otherwise copy normally.
         */
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
          if (rejectWrites) {
            throw expectedFailure
          }
          super.write(bytes, offset, length)
        }
      }
      try {
        withInputWriterRunner { (runner, context) =>
          var delivered = false
          val source = new Iterator[ColumnarBatch] {

            /**
             * Reject any upstream access after delivery while the test's ranges remain pending.
             */
            override def hasNext: Boolean = {
              assert(!delivered, "the source must stay borrowed while slices remain")
              true
            }

            /** Return a borrowed eight-row source whose remaining slices outlive this test. */
            override def next(): ColumnarBatch = {
              assert(!delivered)
              delivered = true
              new ColumnarBatch(Array[ColumnVector](input.column), 8)
            }
          }
          val initialChildren = CometArrowAllocator.getChildAllocators.asScala.toSet
          val writer = runner.inputWriter(Iterator(source), context)
          val childAllocator =
            (CometArrowAllocator.getChildAllocators.asScala.toSet -- initialChildren).head
          val sourceBytes = input.allocator.getAllocatedMemory
          val sourceBuffers =
            Seq(input.indices.getValueVector.getDataBuffer, input.values.getDataBuffer)
          val sourceRefs = sourceBuffers.map(_.refCnt())
          val dataOut = new DataOutputStream(transport)
          writer.writeNextInputToStream(dataOut) shouldBe true
          childAllocator.getAllocatedMemory shouldBe 0L
          childAllocator.getPeakMemoryAllocation should be < (8L * value.length)
          if (failWrite) {
            transport.reset()
            rejectWrites = true
            val failure = intercept[IOException] {
              writer.writeNextInputToStream(new DataOutputStream(transport))
            }
            (failure eq expectedFailure) shouldBe true
          }
          childAllocator.getAllocatedMemory shouldBe 0L
          input.allocator.getAllocatedMemory shouldBe sourceBytes
          sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
          context.markInterrupted("stop with pending Python input slices")
          context.markTaskCompleted(if (failWrite) Some(expectedFailure) else None)
          CometArrowAllocator.getChildAllocators.asScala.toSet shouldBe initialChildren
          // Task cleanup must release only writer-owned resources, including on a failed write.
          input.allocator.getAllocatedMemory shouldBe sourceBytes
          sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
          input.column.getUTF8String(7).toString shouldBe value
        }
      } finally {
        transport.close()
        input.close()
      }
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

  test("dictionary inputs are sliced before decoding to the Arrow batch limits") {
    val input = largeDictionaryInput()
    val sourceVectors = Seq(input.values, input.indices)
    val sourceBuffers = sourceVectors.flatMap(_.getFieldBuffers.asScala)
    val sourceRefs = sourceBuffers.map(_.refCnt())
    val sourceBytes = input.allocator.getAllocatedMemory
    val fullDecodedDataBytes =
      input.expected.map(_.getBytes(StandardCharsets.UTF_8).length.toLong).sum
    try {
      val cases = Seq(
        (2, Int.MaxValue.toLong, Seq.fill(5)(2)),
        (100, 100L * 1024L, Seq.fill(5)(2)),
        (100, 4096L, Seq.fill(10)(1)))
      cases.foreach { case (maxRecords, maxBytes, expectedBatchSizes) =>
        val writerAllocator = new RootAllocator(256 * 1024)
        val output = new ByteArrayOutputStream()
        try {
          withWriter(Seq(input.values.getField), writerAllocator, Channels.newChannel(output)) {
            channel =>
              foreachInputBatch(
                input.columns,
                input.expected.size,
                maxRecords,
                maxBytes,
                writerAllocator) { (vectors, numRows) =>
                serializeBatch(new WriteChannel(channel), vectors, numRows, writerAllocator)
              }
          }

          val actualBatchSizes = ArrayBuffer.empty[Int]
          val actualValues = ArrayBuffer.empty[String]
          withReader(output.toByteArray) { reader =>
            while (reader.loadNextBatch()) {
              val root = reader.getVectorSchemaRoot
              actualBatchSizes += root.getRowCount
              val struct = root.getVector(0).asInstanceOf[StructVector]
              val text = struct.getChild("text").asInstanceOf[VarCharVector]
              (0 until root.getRowCount).foreach { row =>
                actualValues += text.getObject(row).toString
              }
            }
          }

          actualBatchSizes.toSeq shouldBe expectedBatchSizes
          actualValues.toSeq shouldBe input.expected
          writerAllocator.getAllocatedMemory shouldBe 0L
          writerAllocator.getPeakMemoryAllocation should be < fullDecodedDataBytes
          input.allocator.getAllocatedMemory shouldBe sourceBytes
          sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
          input.indices.get(9) shouldBe 1
          input.values.getObject(0).toString shouldBe input.expected.head
        } finally {
          writerAllocator.close()
        }
      }
    } finally {
      input.close()
    }
  }

  test("dictionary input slices are released when a later write fails") {
    val input = largeDictionaryInput()
    val sourceVectors = Seq(input.values, input.indices)
    val sourceBuffers = sourceVectors.flatMap(_.getFieldBuffers.asScala)
    val sourceRefs = sourceBuffers.map(_.refCnt())
    val sourceBytes = input.allocator.getAllocatedMemory
    val writerAllocator = new RootAllocator(256 * 1024)
    var failWrites = false
    var slicesEntered = 0
    val output = new ByteArrayOutputStream() {
      override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
        if (failWrites) {
          throw new IOException("injected sliced dictionary IPC write failure")
        }
        super.write(bytes, offset, length)
      }
    }
    try {
      val error = intercept[IOException] {
        withWriter(Seq(input.values.getField), writerAllocator, Channels.newChannel(output)) {
          channel =>
            foreachInputBatch(
              input.columns,
              input.expected.size,
              maxRecordsPerBatch = 2,
              maxBytesPerBatch = Int.MaxValue.toLong,
              allocator = writerAllocator) { (vectors, numRows) =>
              slicesEntered += 1
              failWrites = slicesEntered == 2
              try {
                serializeBatch(new WriteChannel(channel), vectors, numRows, writerAllocator)
              } finally {
                failWrites = false
              }
            }
        }
      }
      error.getMessage shouldBe "injected sliced dictionary IPC write failure"
      slicesEntered shouldBe 2
      writerAllocator.getAllocatedMemory shouldBe 0L
      input.allocator.getAllocatedMemory shouldBe sourceBytes
      sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
      input.indices.get(8) shouldBe 0
      input.values.getObject(1).toString shouldBe input.expected(1)
    } finally {
      failWrites = false
      writerAllocator.close()
      input.close()
    }
  }

  test("dictionary slices keep plain nullable and nested columns aligned through IPC") {
    val input = largeDictionaryInput()
    val number = new BigIntVector("number", input.allocator)
    val plainText = new VarCharVector("plain_text", input.allocator)
    val details = StructVector.empty("details", input.allocator)
    val writerAllocator = new RootAllocator(Long.MaxValue)
    val output = new ByteArrayOutputStream()
    try {
      number.allocateNew(10)
      plainText.allocateNew()
      val structWriter = details.getWriter
      (0 until 10).foreach { row =>
        if (row % 3 == 0) number.setNull(row) else number.setSafe(row, 100L + row)
        plainText.setSafe(row, s"plain-$row".getBytes(StandardCharsets.UTF_8))
        structWriter.setPosition(row)
        structWriter.start()
        if (row == 7) structWriter.bigInt("child").writeNull()
        else structWriter.bigInt("child").writeBigInt(1000L + row)
        structWriter.end()
      }
      number.setValueCount(10)
      plainText.setValueCount(10)
      structWriter.setValueCount(10)
      val plainColumns = Seq[CometDecodedVector](
        new CometPlainVector(number),
        new CometPlainVector(plainText),
        new CometStructVector(details, null))
      val fields =
        Seq(input.values.getField, number.getField, plainText.getField, details.getField)
      val sourceBuffers = Seq[FieldVector](
        input.values,
        input.indices,
        number,
        plainText,
        details,
        details.getChild("child")).flatMap(_.getFieldBuffers.asScala)
      val sourceRefs = sourceBuffers.map(_.refCnt())
      val sourceBytes = input.allocator.getAllocatedMemory

      withWriter(fields, writerAllocator, Channels.newChannel(output)) { channel =>
        foreachInputBatch(
          input.columns ++ plainColumns,
          10,
          maxRecordsPerBatch = 3,
          maxBytesPerBatch = Int.MaxValue.toLong,
          allocator = writerAllocator) { (vectors, numRows) =>
          serializeBatch(new WriteChannel(channel), vectors, numRows, writerAllocator)
        }
      }

      val batchSizes = ArrayBuffer.empty[Int]
      var offset = 0
      withReader(output.toByteArray) { reader =>
        while (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          val result = root.getVector(0).asInstanceOf[StructVector]
          val resultNumber = result.getChild("number").asInstanceOf[BigIntVector]
          val resultDetails = result.getChild("details").asInstanceOf[StructVector]
          val resultChild = resultDetails.getChild("child").asInstanceOf[BigIntVector]
          batchSizes += root.getRowCount
          (0 until root.getRowCount).foreach { row =>
            val sourceRow = offset + row
            result.getChild("text").getObject(row).toString shouldBe input.expected(sourceRow)
            resultNumber.isNull(row) shouldBe (sourceRow % 3 == 0)
            if (sourceRow % 3 != 0) resultNumber.get(row) shouldBe 100L + sourceRow
            result.getChild("plain_text").getObject(row).toString shouldBe s"plain-$sourceRow"
            resultDetails.isNull(row) shouldBe false
            resultChild.isNull(row) shouldBe (sourceRow == 7)
            if (sourceRow != 7) resultChild.get(row) shouldBe 1000L + sourceRow
          }
          offset += root.getRowCount
        }
      }
      batchSizes.toSeq shouldBe Seq(3, 3, 3, 1)
      offset shouldBe 10
      writerAllocator.getAllocatedMemory shouldBe 0L
      input.allocator.getAllocatedMemory shouldBe sourceBytes
      sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
      number.get(8) shouldBe 108L
      details.getChild("child").asInstanceOf[BigIntVector].get(9) shouldBe 1009L
    } finally {
      details.close()
      plainText.close()
      number.close()
      writerAllocator.close()
      input.close()
    }
  }

  for (numRows <- Seq(0, 10)) {
    test(s"dictionary IPC preserves $numRows all-null rows without reading dictionary indices") {
      val input = dictionaryInput(Seq("unused"), Seq.fill(numRows)(None))
      val writerAllocator = new RootAllocator(Long.MaxValue)
      val output = new ByteArrayOutputStream()
      try {
        val sizes = ArrayBuffer.empty[Int]
        withWriter(Seq(input.values.getField), writerAllocator, Channels.newChannel(output)) {
          channel =>
            foreachInputBatch(
              Seq(input.column),
              numRows,
              maxRecordsPerBatch = 3,
              maxBytesPerBatch = 0,
              allocator = writerAllocator) { (vectors, length) =>
              serializeBatch(new WriteChannel(channel), vectors, length, writerAllocator)
            }
        }
        withReader(output.toByteArray) { reader =>
          while (reader.loadNextBatch()) {
            val root = reader.getVectorSchemaRoot
            val text = root.getVector(0).asInstanceOf[StructVector].getChild("text")
            sizes += root.getRowCount
            text.getField.getDictionary shouldBe null
            text.getField.getType shouldBe ArrowType.Utf8.INSTANCE
            text.getNullCount shouldBe root.getRowCount
          }
        }
        sizes.toSeq shouldBe (if (numRows == 0) Seq(0) else Seq(3, 3, 3, 1))
        input.indices.reads shouldBe 0
        writerAllocator.getAllocatedMemory shouldBe 0L
      } finally {
        writerAllocator.close()
        input.close()
      }
    }
  }

  test("non-positive dictionary limits disable only their corresponding soft limit") {
    val input = dictionaryInput(Seq("abcd"), Seq.fill(10)(Some(0)))
    try {
      for (records <- Seq(0, -1); bytes <- Seq(0L, -1L)) {
        inputBatchRanges(Seq(input.column), 10, records, bytes) shouldBe Seq(0 -> 10)
      }
      for (records <- Seq(0, -1)) {
        inputBatchRanges(Seq(input.column), 10, records, 20L) shouldBe
          Seq(0 -> 2, 2 -> 2, 4 -> 2, 6 -> 2, 8 -> 2)
      }
      for (bytes <- Seq(0L, -1L)) {
        inputBatchRanges(Seq(input.column), 10, 3, bytes) shouldBe
          Seq(0 -> 3, 3 -> 3, 6 -> 3, 9 -> 1)
      }
    } finally {
      input.close()
    }
  }

  test("randomized dictionary ranges match the soft-limit oracle and cover every input row") {
    val random = new Random(5560L)
    (0 until 100).foreach { trial =>
      val dictionaryValues = Seq.fill(1 + random.nextInt(8)) {
        "λ" * random.nextInt(40)
      }
      val rowIndices = Seq.fill(random.nextInt(80)) {
        if (random.nextInt(4) == 0) None else Some(random.nextInt(dictionaryValues.size))
      }
      val maxRecords = Seq(0, -1, 1, 3, 8, 17, 100)(random.nextInt(7))
      val maxBytes = Seq(0L, -1L, 1L, 4L, 9L, 64L, 257L, 10000L)(random.nextInt(8))
      val lengths = rowIndices.map(
        _.map(index => dictionaryValues(index).getBytes(StandardCharsets.UTF_8).length))
      val input = dictionaryInput(dictionaryValues, rowIndices)
      try {
        val ranges = inputBatchRanges(Seq(input.column), rowIndices.size, maxRecords, maxBytes)
        withClue(s"trial=$trial, records=$maxRecords, bytes=$maxBytes, lengths=$lengths: ") {
          ranges shouldBe expectedDictionaryRanges(lengths, maxRecords, maxBytes)
          ranges.flatMap { case (offset, length) => offset until offset + length } shouldBe
            rowIndices.indices
          if (rowIndices.nonEmpty) {
            ranges.foreach { case (_, length) =>
              length should be > 0
              if (maxRecords > 0) length should be <= maxRecords
            }
          }
        }
      } finally {
        input.close()
      }
    }
  }

  test("a conservative dictionary bound accepts small batches without reading row indices") {
    val input = dictionaryInput(Seq("", "λ", "longest"), Seq.fill(100)(Some(2)))
    try {
      inputBatchRanges(Seq(input.column), 100, 100, 4096L) shouldBe Seq(0 -> 100)
      input.indices.reads shouldBe 0

      val ranges = inputBatchRanges(Seq(input.column), 100, 3, 4096L)
      ranges.map(_._2) shouldBe (Seq.fill(33)(3) :+ 1)
      input.indices.reads shouldBe 100
    } finally {
      input.close()
    }
  }

  test("plain and zero-column inputs obey record limits without estimating decoded bytes") {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = new BigIntVector("number", allocator)
    try {
      values.allocateNew(10)
      (0 until 10).foreach(row => values.setSafe(row, row.toLong))
      values.setValueCount(10)
      val column = new CometPlainVector(values)
      for (columns <- Seq(Seq(column), Seq.empty[CometDecodedVector])) {
        inputBatchRanges(columns, 10, 3, 1L) shouldBe
          Seq(0 -> 3, 3 -> 3, 6 -> 3, 9 -> 1)
        inputBatchRanges(columns, 10, 0, 1L) shouldBe Seq(0 -> 10)
        inputBatchRanges(columns, 0, 3, 1L) shouldBe Seq(0 -> 0)
      }
    } finally {
      values.close()
      allocator.close()
    }
  }

  test("the hard Arrow ceiling splits gigabyte values with soft limits raised or disabled") {
    val input = dictionaryInput(
      Seq("x"),
      Seq.fill(3)(Some(0)),
      reportedLengths = Some(Seq(1100 * 1024 * 1024)))
    try {
      val sourceBytes = input.allocator.getAllocatedMemory
      sourceBytes should be < 1024L * 1024L
      for (byteLimit <- Seq(0L, -1L, Int.MaxValue.toLong, Long.MaxValue)) {
        inputBatchRanges(Seq(input.column), 3, 0, byteLimit) shouldBe
          Seq(0 -> 1, 1 -> 1, 2 -> 1)
      }
      input.allocator.getAllocatedMemory shouldBe sourceBytes
    } finally {
      input.close()
    }
  }

  test("nested dictionaries fail with their full column path before materializing input") {
    val input = dictionaryInput(Seq("value"), Seq(Some(0)))
    val writerAllocator = new RootAllocator(Long.MaxValue)
    val outer = StructVector.empty("outer", input.allocator)
    try {
      val inner = outer.addOrGet(
        "inner",
        input.column.getValueVector.getField.getFieldType,
        classOf[IntVector])
      outer.allocateNew()
      inner.setSafe(0, 0)
      outer.setIndexDefined(0)
      outer.setValueCount(1)
      val nested = new CometStructVector(outer, input.column.getDictionaryProvider)
      val sourceBuffers = Seq[FieldVector](
        input.values,
        input.column.getValueVector.asInstanceOf[FieldVector],
        outer,
        inner).flatMap(_.getFieldBuffers.asScala)
      val sourceRefs = sourceBuffers.map(_.refCnt())
      val sourceBytes = input.allocator.getAllocatedMemory
      var entered = false
      val error = intercept[IllegalArgumentException] {
        withMaterializedInputVectors(Seq(input.column, nested), writerAllocator) { _ =>
          entered = true
        }
      }
      error.getMessage should include("outer.inner")
      entered shouldBe false
      writerAllocator.getAllocatedMemory shouldBe 0L
      input.allocator.getAllocatedMemory shouldBe sourceBytes
      sourceBuffers.map(_.refCnt()) shouldBe sourceRefs
      input.values.getObject(0).toString shouldBe "value"
    } finally {
      outer.close()
      writerAllocator.close()
      input.close()
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

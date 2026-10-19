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

package org.apache.spark.sql.comet.util

import java.nio.channels.ClosedByInterruptException
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseLargeVariableWidthVector, BitVectorHelper, IntVector, LargeVarBinaryVector, LargeVarCharVector, ValueVector, VarBinaryVector, VarCharVector, VariableWidthFieldVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType}
import org.apache.spark.SparkException
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

class UtilsSuite extends CometTestBase {

  test("serializeBatches preserves row count for a zero-column batch") {
    val numRows = 5
    val batch = new ColumnarBatch(Array.empty[ColumnVector], numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val decoded = Utils.decodeBatches(buf, "test").toSeq
    assert(decoded.map(_.numRows()).sum == numRows)
  }

  test("coalesceBroadcastBatches preserves row count across zero-column inputs") {
    val numRows = 5
    val numBatches = 3
    val batches =
      (0 until numBatches).map(_ => new ColumnarBatch(Array.empty[ColumnVector], numRows))

    val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
    val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)

    val expected = numRows.toLong * numBatches
    assert(batchCount == numBatches)
    assert(totalRows == expected)

    val decoded = coalesced.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
    assert(decoded.map(_.numRows()).sum == expected)
  }

  test("serializeBatches rebases offsets that start past zero") {
    // A slice of a native string array reaches the JVM with its original offsets, which start
    // past zero, and with the bytes of the rows before the slice at the front of its data buffer.
    // Coalescing such batches for a broadcast used to fold those bytes into each batch's first
    // value (#6779).
    val values = (0 until 24).map(i => if (i % 5 == 0) null else s"value-$i")
    val skips = Seq(8, 16)

    def serialize(vectors: Seq[VarCharVector]): Seq[ChunkedByteBuffer] = {
      val batches = vectors.map { v =>
        val column = CometVector.getVector(v, new CDataDictionaryProvider)
        new ColumnarBatch(Array[ColumnVector](column), v.getValueCount)
      }
      try Utils.serializeBatches(batches.iterator).map(_._2).toList
      finally batches.foreach(_.close())
    }

    val sliced = serialize(skips.map(slicedStrings(values, _)))
    val (coalesced, batchCount, _) = Utils.coalesceBroadcastBatches(sliced.iterator)
    assert(batchCount == skips.length)
    val it = Utils.decodeBatches(coalesced.head, "test")
    val out = it.next()
    val got = (0 until out.numRows()).map { i =>
      if (out.column(0).isNullAt(i)) null else out.column(0).getUTF8String(i).toString
    }
    assert(!it.hasNext)
    assert(got == skips.flatMap(values.drop))

    // The bytes before a slice are not written either: it takes as many bytes as the same values
    // in a vector of their own.
    val fresh = serialize(skips.map(skip => strings(values.drop(skip))))
    assert(sliced.map(_.size) == fresh.map(_.size))
  }

  test("serializeBatches closes the slice it writes when the write fails") {
    // A cancelled broadcast interrupts its task, so the next write to the interruptible channel
    // serializeBatches writes through fails. The slice holds its own references on the batch's
    // buffers, and they would outlive the batch unless the slice is closed on that path too.
    val allocator = CometArrowAllocator.newChildAllocator("sliced-batch", 0, Long.MaxValue)
    try {
      val vector = slicedStrings((0 until 24).map(i => s"value-$i"), 8, allocator)
      val column = CometVector.getVector(vector, new CDataDictionaryProvider)
      val batch = new ColumnarBatch(Array[ColumnVector](column), vector.getValueCount)
      Thread.currentThread().interrupt()
      try {
        intercept[ClosedByInterruptException] {
          Utils.serializeBatches(Iterator(batch)).next()
        }
      } finally {
        Thread.interrupted()
        batch.close()
      }
      assert(allocator.getAllocatedMemory == 0)
    } finally {
      allocator.close()
    }
  }

  test("serializeBatches materializes ConstantColumnVector columns") {
    // Spark wraps file-source partition columns and other per-batch constants in
    // ConstantColumnVector. When such a batch reaches Comet's serialization/export path
    // (getBatchFieldVectors), it must be materialized to an Arrow vector rather than
    // rejected with "Comet execution only takes Arrow Arrays".
    val numRows = 4

    val valueCol = new ConstantColumnVector(numRows, IntegerType)
    valueCol.setInt(42)
    val nullCol = new ConstantColumnVector(numRows, IntegerType)
    nullCol.setNull()
    val batch = new ColumnarBatch(Array[ColumnVector](valueCol, nullCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    // Read the decoded values eagerly: ArrowReaderIterator releases a batch's buffers once the
    // iterator advances past it (hasNext closes the previous batch), so values must be read from
    // the current batch before calling hasNext/next again.
    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val values = (0 until numRows).map(i => out.column(0).getInt(i))
    val nulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(values.forall(_ == 42), s"expected all 42, got $values")
    assert(nulls.forall(identity), s"expected all null, got $nulls")
  }

  test("serializeBatches materializes a TimestampType ConstantColumnVector") {
    // Covers the TimestampType materialize path (TimestampWriter -> TimeStampMicroTZVector) and
    // pins down the "UTC" timezone choice in materializeConstantColumnVector: Spark stores
    // TimestampType as micros in UTC, and Comet tags its timestamp Arrow vectors "UTC", so the
    // constant micros round-trip unchanged. This guards against anyone later swapping the zone
    // argument, which would make the materialised constant's Arrow field metadata diverge from the
    // sibling non-constant timestamp columns it shares a VectorSchemaRoot with.
    val numRows = 3
    // 2023-11-14T22:13:20Z in micros since epoch.
    val micros = 1700000000000000L

    val tsCol = new ConstantColumnVector(numRows, TimestampType)
    tsCol.setLong(micros)
    val batch = new ColumnarBatch(Array[ColumnVector](tsCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 1)
    assert(out.numRows() == numRows)
    val got = (0 until numRows).map(i => out.column(0).getLong(i))
    assert(!it.hasNext)

    assert(got.forall(_ == micros), s"expected all $micros, got $got")
  }

  test("serializeBatches materializes a nullable StructType ConstantColumnVector") {
    // Exercises a different ArrowFieldWriter path than the scalar cases: a struct constant is
    // written via getStruct(rowId) -> getChild(ordinal). Covers both a non-null struct (with a
    // null nested field) and a wholly-null struct constant.
    val numRows = 3
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType, nullable = true)))

    // Non-null struct whose `name` field is null, proving nested nullability round-trips.
    val structCol = new ConstantColumnVector(numRows, schema)
    structCol.setNotNull()
    val idChild = new ConstantColumnVector(numRows, IntegerType)
    idChild.setInt(7)
    val nameChild = new ConstantColumnVector(numRows, StringType)
    nameChild.setNull()
    structCol.setChild(0, idChild)
    structCol.setChild(1, nameChild)

    // A wholly-null struct constant.
    val nullStructCol = new ConstantColumnVector(numRows, schema)
    nullStructCol.setNull()
    nullStructCol.setChild(0, new ConstantColumnVector(numRows, IntegerType))
    nullStructCol.setChild(1, new ConstantColumnVector(numRows, StringType))

    val batch =
      new ColumnarBatch(Array[ColumnVector](structCol, nullStructCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val ids = (0 until numRows).map(i => out.column(0).getStruct(i).getInt(0))
    val nameNulls = (0 until numRows).map(i => out.column(0).getStruct(i).isNullAt(1))
    val structNulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(ids.forall(_ == 7), s"expected all id 7, got $ids")
    assert(nameNulls.forall(identity), s"expected all name null, got $nameNulls")
    assert(structNulls.forall(identity), s"expected all struct null, got $structNulls")
  }

  test("isArrowBacked rejects large-offset Arrow vectors") {
    // A CometPlainVector can wrap a LargeVarCharVector or LargeVarBinaryVector -- an accelerated
    // mapInArrow returning pa.large_string() produces one -- but getFieldVector rejects both, and
    // getBatchFieldVectors writes a 32-bit copy of them rather than the vector itself. A caller
    // taking the direct write path expects the batch's own vectors, so isArrowBacked must report
    // false and let the caller convert the batch instead. The cache's direct write path also asks
    // CachedBatchIpc.matchesReaderLayout, which refuses LargeUtf8 against the reader's Utf8, so
    // either check alone keeps such a batch off it.
    val values = Seq("hello", "world")
    Seq[ValueVector](
      fill(new LargeVarCharVector("s", CometArrowAllocator), values),
      fill(new LargeVarBinaryVector("b", CometArrowAllocator), values)).foreach { vector =>
      try {
        val batch = new ColumnarBatch(Array(cometVector(vector)), values.length)
        assert(
          !Utils.isArrowBacked(batch),
          s"${vector.getClass.getSimpleName} must not be reported as directly writable")
      } finally {
        vector.close()
      }
    }
  }

  test("serializeBatches writes large-offset string and binary vectors with 32-bit offsets") {
    // A PyArrow UDF returning pa.large_string() or pa.large_binary() hands Comet a
    // LargeVarCharVector or LargeVarBinaryVector. The column is still StringType or BinaryType,
    // so it is written as the Utf8 or Binary vector every reader of the bytes expects for those.
    val values = Seq("hello", null, "", "wörld")
    val strings = fill(new LargeVarCharVector("s", CometArrowAllocator), values)
    val binaries = fill(new LargeVarBinaryVector("b", CometArrowAllocator), values)
    try {
      val batch =
        new ColumnarBatch(Array(cometVector(strings), cometVector(binaries)), values.length)
      assert(
        roundTrip(batch) == Seq(
          classOf[VarCharVector] -> values,
          classOf[VarBinaryVector] -> values))
    } finally {
      strings.close()
      binaries.close()
    }
  }

  test("serializeBatches rebases large-offset vectors whose offsets start past zero") {
    // Offsets need not start at zero. A slice imported over the C Data interface keeps its
    // parent's offsets and data buffer, so they start past bytes that no value covers. The copy
    // holds only the bytes the values cover, with offsets counted from the first of them.
    val values = Seq("hello", null, "wörld")
    val strings = startingPast("xx", new LargeVarCharVector("s", CometArrowAllocator), values)
    val binaries = startingPast("xx", new LargeVarBinaryVector("b", CometArrowAllocator), values)
    try {
      assert(strings.getOffsetBuffer.getLong(0) == 2)
      assert(binaries.getOffsetBuffer.getLong(0) == 2)
      val batch =
        new ColumnarBatch(Array(cometVector(strings), cometVector(binaries)), values.length)
      assert(
        roundTrip(batch) == Seq(
          classOf[VarCharVector] -> values,
          classOf[VarBinaryVector] -> values))
    } finally {
      strings.close()
      binaries.close()
    }
  }

  test("serializeBatches writes empty large-offset vectors with 32-bit offsets") {
    // A vector that holds no values need not have an offset buffer, so none is read.
    val strings = new LargeVarCharVector("s", CometArrowAllocator)
    val binaries = new LargeVarBinaryVector("b", CometArrowAllocator)
    try {
      strings.setValueCount(0)
      binaries.setValueCount(0)
      assert(strings.getOffsetBuffer.capacity == 0)
      val batch = new ColumnarBatch(Array(cometVector(strings), cometVector(binaries)), 0)
      assert(
        roundTrip(batch) == Seq(
          classOf[VarCharVector] -> Seq.empty,
          classOf[VarBinaryVector] -> Seq.empty))
    } finally {
      strings.close()
      binaries.close()
    }
  }

  test("coalesceBroadcastBatches coalesces batches of mixed offset widths") {
    // The coalescer appends every batch to a root built from the first one's schema, and leaves
    // the broadcast uncoalesced when a batch does not match it. A producer can hand back
    // large_string for one batch and string for the next, so both must serialize alike.
    val large = fill(new LargeVarCharVector("s", CometArrowAllocator), Seq("a", null))
    val regular = fill(new VarCharVector("s", CometArrowAllocator), Seq("b", "c"))
    try {
      val batches = Seq(large, regular).map(v => new ColumnarBatch(Array(cometVector(v)), 2))
      val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
      val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)
      // A batch count of zero is the fallback to the uncoalesced buffers.
      assert(batchCount == 2)
      assert(totalRows == 4)

      val it = coalesced.iterator.flatMap(Utils.decodeBatches(_, "test"))
      val out = it.next()
      val got = stringValues(out.column(0), out.numRows())
      assert(!it.hasNext)
      assert(got == Seq("a", null, "b", "c"))
    } finally {
      large.close()
      regular.close()
    }
  }

  test("serializeBatches releases the vectors it made when a later column fails") {
    // A materialized constant and a 32-bit copy of a large-offset column are both made before the
    // last column is refused. Nothing outside serializeBatches ever holds them, so it has to
    // close them itself, while the batch's own vectors stay with their owner.
    val before = CometArrowAllocator.getAllocatedMemory
    val constant = new ConstantColumnVector(1, IntegerType)
    constant.setInt(42)
    val small = fill(new LargeVarCharVector("small", CometArrowAllocator), Seq("a"))
    val tooBig = tooBigForIntOffsets("big")
    try {
      val batch = new ColumnarBatch(Array(constant, cometVector(small), cometVector(tooBig)), 1)
      val e = intercept[SparkException](Utils.serializeBatches(Iterator(batch)).next())
      assert(e.getMessage.contains("more than 32-bit offsets can address"), e.getMessage)
      assert(small.getValueCount == 1 && small.getObject(0).toString == "a")
    } finally {
      constant.close()
      small.close()
      tooBig.close()
    }
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("serializeBatches releases the vectors it made when the dictionaries of a batch clash") {
    // The vectors are all made before the dictionaries of the columns are combined, which is where
    // two columns sharing an ID without sharing a dictionary are refused. By then the caller has
    // not got them either, and a copy of a large-offset column can be close to 2 GiB.
    val before = CometArrowAllocator.getAllocatedMemory
    val large = fill(new LargeVarCharVector("large", CometArrowAllocator), Seq("a"))
    val (first, firstProvider) = dictionaryEncoded("first", 7L, "x")
    val (second, secondProvider) = dictionaryEncoded("second", 7L, "y")
    try {
      val batch = new ColumnarBatch(
        Array(
          cometVector(large),
          CometVector.getVector(first, firstProvider),
          CometVector.getVector(second, secondProvider)),
        1)
      val e = intercept[SparkException](Utils.serializeBatches(Iterator(batch)).next())
      assert(e.getMessage.contains("different dictionaries under ID 7"), e.getMessage)
      assert(large.getValueCount == 1 && large.getObject(0).toString == "a")
    } finally {
      large.close()
      Seq(first, second).foreach(_.close())
      Seq(firstProvider, secondProvider).foreach(_.lookup(7L).getVector.close())
    }
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  /** A dictionary-encoded index column holding the one entry of a dictionary holding `value`. */
  private def dictionaryEncoded(
      name: String,
      id: Long,
      value: String): (IntVector, DictionaryProvider) = {
    val encoding = new DictionaryEncoding(id, false, null)
    val dictionary =
      new Dictionary(fill(new VarCharVector("dict", CometArrowAllocator), Seq(value)), encoding)
    val indices = new IntVector(
      new Field(name, new FieldType(true, new ArrowType.Int(32, true), encoding), null),
      CometArrowAllocator)
    indices.allocateNew(1)
    indices.set(0, 0)
    indices.setValueCount(1)
    (indices, new DictionaryProvider.MapDictionaryProvider(dictionary))
  }

  /** Writes `values` into `vector`, leaving a null entry unset so that it reads back as null. */
  private def fill[V <: VariableWidthFieldVector](vector: V, values: Seq[String]): V = {
    vector.allocateNew(values.length)
    values.zipWithIndex.foreach { case (value, i) =>
      if (value != null) vector.setSafe(i, value.getBytes(StandardCharsets.UTF_8))
    }
    vector.setValueCount(values.length)
    vector
  }

  /**
   * Loads `values` into `vector` the way an imported slice holds them: the data buffer starts
   * with the bytes of `skipped`, which no value covers, so the offsets start past zero.
   */
  private def startingPast[V <: BaseLargeVariableWidthVector](
      skipped: String,
      vector: V,
      values: Seq[String]): V = {
    val skippedBytes = skipped.getBytes(StandardCharsets.UTF_8)
    val valueBytes =
      values.map(v => if (v == null) Array.emptyByteArray else v.getBytes(StandardCharsets.UTF_8))
    val offsets = valueBytes.scanLeft(skippedBytes.length.toLong)(_ + _.length)
    val width = BaseLargeVariableWidthVector.OFFSET_WIDTH.toLong

    val validity =
      CometArrowAllocator.buffer(BitVectorHelper.getValidityBufferSize(values.length).toLong)
    val offsetBuffer = CometArrowAllocator.buffer(offsets.length * width)
    val data = CometArrowAllocator.buffer(offsets.last)
    val buffers = Seq(validity, offsetBuffer, data)
    try {
      validity.setZero(0, validity.capacity)
      values.indices.filter(values(_) != null).foreach { i =>
        BitVectorHelper.setBit(validity, i.toLong)
      }
      offsets.zipWithIndex.foreach { case (offset, i) => offsetBuffer.setLong(i * width, offset) }
      data.setBytes(0, skippedBytes ++ valueBytes.flatten)
      vector.loadFieldBuffers(
        new ArrowFieldNode(values.length.toLong, values.count(_ == null).toLong),
        buffers.asJava)
      vector
    } finally {
      // The vector takes references of its own.
      buffers.foreach(_.close())
    }
  }

  /** A one-value vector whose offsets claim more data than 32-bit offsets can address. */
  private def tooBigForIntOffsets(name: String): LargeVarCharVector = {
    val vector = fill(new LargeVarCharVector(name, CometArrowAllocator), Seq("a"))
    vector.getOffsetBuffer.setLong(BaseLargeVariableWidthVector.OFFSET_WIDTH, Int.MaxValue + 1L)
    vector
  }

  private def cometVector(vector: ValueVector): ColumnVector =
    CometVector.getVector(vector, new CDataDictionaryProvider)

  /**
   * Serializes `batch` on its own and decodes it as the broadcast consumers do, returning each
   * column's vector class and values.
   */
  private def roundTrip(batch: ColumnarBatch): Seq[(Class[_], Seq[String])] = {
    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == batch.numRows())

    // Everything is read before the iterator moves on, which releases the batch it returned.
    val it = Utils.decodeBatches(buf, "test")
    val out = it.next()
    val columns = (0 until out.numCols()).map { i =>
      val column = out.column(i)
      val vectorClass = column.asInstanceOf[CometVector].getValueVector.getClass
      (vectorClass, stringValues(column, out.numRows()))
    }
    assert(!it.hasNext)
    columns
  }

  /** The values of a string or binary column, with binary values read as UTF-8. */
  private def stringValues(column: ColumnVector, numRows: Int): Seq[String] =
    (0 until numRows).map { i =>
      if (column.isNullAt(i)) {
        null
      } else if (column.dataType == BinaryType) {
        new String(column.getBinary(i), StandardCharsets.UTF_8)
      } else {
        column.getUTF8String(i).toString
      }
    }

  private def strings(
      values: Seq[String],
      allocator: BufferAllocator = CometArrowAllocator): VarCharVector = {
    val vector = new VarCharVector("s", allocator)
    vector.allocateNew()
    values.zipWithIndex.foreach {
      case (null, i) => vector.setNull(i)
      case (value, i) => vector.setSafe(i, value.getBytes(StandardCharsets.UTF_8))
    }
    vector.setValueCount(values.length)
    vector
  }

  /**
   * `values.drop(skip)` in the shape the C Data import gives a slice of a native string array:
   * the offsets are the original ones from `skip` on, and the data buffer still starts with the
   * bytes of the first `skip` values. `skip` is a multiple of 8, so the validity bitmap slices on
   * a byte boundary.
   */
  private def slicedStrings(
      values: Seq[String],
      skip: Int,
      allocator: BufferAllocator = CometArrowAllocator): VarCharVector = {
    val full = strings(values, allocator)
    try {
      val length = values.length - skip
      val sliced = new VarCharVector("s", allocator)
      sliced.loadFieldBuffers(
        new ArrowFieldNode(length.toLong, values.drop(skip).count(_ == null).toLong),
        Seq(
          full.getValidityBuffer.slice(skip / 8L, (length + 7) / 8L),
          full.getOffsetBuffer.slice(skip * 4L, (length + 1) * 4L),
          full.getDataBuffer).asJava)
      sliced
    } finally {
      full.close()
    }
  }
}

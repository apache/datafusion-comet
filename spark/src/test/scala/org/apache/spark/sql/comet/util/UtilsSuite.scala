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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.vector.{FieldVector, IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, NullType, StringType, StructField, StructType, TimestampType}
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
    // mapInArrow returning pa.large_string() produces one -- but getFieldVector rejects both. If
    // isArrowBacked accepted them, a caller would take the direct write path and then fail, so it
    // must report false and let the caller convert the batch instead.
    val numRows = 2
    Seq[org.apache.arrow.vector.FieldVector](
      {
        val v = new org.apache.arrow.vector.LargeVarCharVector("s", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }, {
        val v = new org.apache.arrow.vector.LargeVarBinaryVector("b", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }).foreach { vector =>
      try {
        val col = CometVector.getVector(vector, new CDataDictionaryProvider)
        val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)
        assert(
          !Utils.isArrowBacked(batch),
          s"${vector.getClass.getSimpleName} must not be reported as directly writable")
      } finally {
        vector.close()
      }
    }
  }

  /**
   * One map column of `numRows` rows. With an `IntegerType` key every row is a single entry `i ->
   * NULL` (a `NullVector` map value); with a `NullType` key every row is an empty map, as `map()`
   * produces (a `NullVector` map key). Both nest a `NullVector` inside the entries struct.
   */
  private def nullTypeMapBatch(numRows: Int, keyType: DataType): ColumnarBatch = {
    val field = Utils.toArrowField("m", MapType(keyType, NullType), nullable = true, "UTC")
    val vector = field.createVector(CometArrowAllocator).asInstanceOf[MapVector]
    vector.allocateNew()
    val entries = vector.getDataVector.asInstanceOf[StructVector]
    (0 until numRows).foreach { i =>
      vector.startNewValue(i)
      keyType match {
        case NullType =>
          vector.endValue(i, 0)
        case _ =>
          entries.setIndexDefined(i)
          entries.getChild(MapVector.KEY_NAME).asInstanceOf[IntVector].setSafe(i, i)
          vector.endValue(i, 1)
      }
    }
    entries.setValueCount(if (keyType == NullType) 0 else numRows)
    vector.setValueCount(numRows)
    new ColumnarBatch(Array[ColumnVector](CometVector.getVector(vector, null)), numRows)
  }

  private def mapKeyField(field: Field): Field = field.getChildren.get(0).getChildren.get(0)

  /** One `array<null>` column; row `i` holds `i` nulls. */
  private def nullListBatch(numRows: Int): ColumnarBatch = {
    val field = Utils.toArrowField("l", ArrayType(NullType), nullable = true, "UTC")
    val vector = field.createVector(CometArrowAllocator).asInstanceOf[ListVector]
    vector.allocateNew()
    (0 until numRows).foreach { i =>
      vector.startNewValue(i)
      vector.endValue(i, i)
    }
    vector.setValueCount(numRows)
    new ColumnarBatch(Array[ColumnVector](CometVector.getVector(vector, null)), numRows)
  }

  /** One `array<struct<a: null>>` column; every row holds one struct. */
  private def nullStructListBatch(numRows: Int): ColumnarBatch = {
    val elementType = StructType(Seq(StructField("a", NullType)))
    val field = Utils.toArrowField("l", ArrayType(elementType), nullable = true, "UTC")
    val vector = field.createVector(CometArrowAllocator).asInstanceOf[ListVector]
    vector.allocateNew()
    val elements = vector.getDataVector.asInstanceOf[StructVector]
    (0 until numRows).foreach { i =>
      vector.startNewValue(i)
      elements.setIndexDefined(i)
      vector.endValue(i, 1)
    }
    elements.setValueCount(numRows)
    vector.setValueCount(numRows)
    new ColumnarBatch(Array[ColumnVector](CometVector.getVector(vector, null)), numRows)
  }

  test("withNonNullableMapKeys restores the non-nullable key flag a NullVector drops") {
    val batch = nullTypeMapBatch(2, NullType)
    val field = batch.column(0).asInstanceOf[CometVector].getValueVector.getField
    // `toArrowField` declares the key non-nullable, but Arrow's `MinorType.NULL` factory builds the
    // key `NullVector` from the name alone, so the vector reports a nullable key. If this assertion
    // starts failing, Arrow fixed that and `withNonNullableMapKeys` can go.
    assert(mapKeyField(field).isNullable)

    val repaired = Utils.withNonNullableMapKeys(field)
    assert(!mapKeyField(repaired).isNullable)
    assert(mapKeyField(repaired).getType.isInstanceOf[ArrowType.Null])
    assert(repaired.getName == field.getName)
    assert(repaired.getFieldType == field.getFieldType)
    assert(repaired.getChildren.get(0).getFieldType == field.getChildren.get(0).getFieldType)
    // Idempotent, and a no-op on fields that already satisfy the invariant.
    assert(Utils.withNonNullableMapKeys(repaired) eq repaired)
    batch.close()
  }

  test("newArrowStreamWriter keeps a root whose declared schema is already valid") {
    val batch = nullTypeMapBatch(2, NullType)
    val vector =
      batch.column(0).asInstanceOf[CometVector].getValueVector.asInstanceOf[FieldVector]
    val declared = Utils.withNonNullableMapKeys(vector.getField)
    // The live vector still reports a nullable key, so a root declared from it would be swapped
    // for a repaired copy. One declared from `declared` must be kept as-is: the row count is set
    // only after the writer exists, and a swapped root would not see it.
    val root = new VectorSchemaRoot(Seq(declared).asJava, Seq(vector).asJava, 0)
    val out = new ByteArrayOutputStream()
    val (bound, writer) = Utils.newArrowStreamWriter(root, null, Channels.newChannel(out))
    assert(bound eq root)
    root.setRowCount(2)
    writer.start()
    writer.writeBatch()
    writer.end()

    val reader =
      new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray), CometArrowAllocator)
    assert(reader.loadNextBatch())
    assert(reader.getVectorSchemaRoot.getRowCount == 2)
    assert(!mapKeyField(reader.getVectorSchemaRoot.getSchema.getFields.get(0)).isNullable)
    reader.close()
    batch.close()
  }

  test("newArrowStreamWriter returns the root a later row count must be set on") {
    val batch = nullTypeMapBatch(2, NullType)
    val vector =
      batch.column(0).asInstanceOf[CometVector].getValueVector.asInstanceOf[FieldVector]
    // Declared from the live vector, so the key is nullable and the root must be swapped. Setting
    // the row count on the returned root has to reach the writer; setting it on the original one
    // would ship an empty batch ("Array length did not match record batch length" downstream).
    val root = new VectorSchemaRoot(Seq(vector).asJava)
    val out = new ByteArrayOutputStream()
    val (bound, writer) = Utils.newArrowStreamWriter(root, null, Channels.newChannel(out))
    assert(bound ne root)
    bound.setRowCount(2)
    writer.start()
    writer.writeBatch()
    writer.end()

    val reader =
      new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray), CometArrowAllocator)
    assert(reader.loadNextBatch())
    assert(reader.getVectorSchemaRoot.getRowCount == 2)
    assert(!mapKeyField(reader.getVectorSchemaRoot.getSchema.getFields.get(0)).isNullable)
    reader.close()
    batch.close()
  }

  test("serializeBatches round-trips a NullType map key through Arrow IPC") {
    // The IPC reader rebuilds a MapVector from the stream's schema and rejects a nullable key
    // ("Map data key type should be a non-nullable"), which is exactly what a NullVector key
    // reports unless the written schema is repaired.
    val numRows = 3
    val batch = nullTypeMapBatch(numRows, NullType)
    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val decoded = Utils.decodeBatches(buf, "test").toSeq
    assert(decoded.map(_.numRows()).sum == numRows)
    decoded.foreach(_.close())
  }

  test("coalesceBroadcastBatches ships struct-nested NullType uncoalesced") {
    // VectorSchemaRootAppender cannot grow a NullVector nested in a struct, including the map
    // entries struct (NullVector.reAlloc is a no-op), so such buffers must be passed through,
    // not appended. The list case pins that a struct below a list is still a struct.
    val cases: Seq[(String, Int => ColumnarBatch)] = Seq(
      "map<int, null>" -> (nullTypeMapBatch(_, IntegerType)),
      "map<null, null>" -> (nullTypeMapBatch(_, NullType)),
      "array<struct<a: null>>" -> nullStructListBatch)
    cases.foreach { case (name, batch) =>
      val numRows = 4
      val numBatches = 3
      val batches = (0 until numBatches).map(_ => batch(numRows))
      val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq

      val (result, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs.iterator)
      // The pass-through signature: original buffers, nothing coalesced.
      assert(batchCount == 0 && totalRows == 0, name)
      assert(result.length == numBatches, name)

      val decoded = result.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
      assert(decoded.map(_.numRows()).sum == numRows.toLong * numBatches, name)
      decoded.foreach(_.close())
    }
  }

  test("coalesceBroadcastBatches bypasses exactly the schemas with a NullType under a struct") {
    // Exhaustive over the shape space of the bypass rule: VectorAppender hangs only when a
    // NullVector is a *direct* child of a struct (see `Utils.hasNullDirectlyUnderStruct` for
    // the Arrow mechanics). Each shape runs the real appender under a timeout, so a rule that
    // is too narrow shows up as a timeout on the hanging shapes instead of a hung build, and
    // one that is too wide shows up as a needless bypass.
    val nullStruct = StructType(Seq(StructField("a", NullType)))
    val shapes: Seq[(DataType, Any)] = Seq(
      NullType -> null,
      ArrayType(NullType) -> new GenericArrayData(Array[Any](null)),
      ArrayType(ArrayType(NullType)) ->
        new GenericArrayData(Array[Any](new GenericArrayData(Array[Any](null)))),
      nullStruct -> InternalRow(null),
      ArrayType(nullStruct) -> new GenericArrayData(Array[Any](InternalRow(null))),
      StructType(Seq(StructField("l", ArrayType(NullType)))) ->
        InternalRow(new GenericArrayData(Array[Any](null))),
      MapType(IntegerType, NullType) -> ArrayBasedMapData(Array[Any](1), Array[Any](null)),
      // `map(k, array(NULL))`: the entry struct's direct child is a list, not a NullVector, so
      // this still coalesces.
      MapType(IntegerType, ArrayType(NullType)) ->
        ArrayBasedMapData(Array[Any](1), Array[Any](new GenericArrayData(Array[Any](null)))),
      MapType(NullType, NullType) -> ArrayBasedMapData(Array.empty[Any], Array.empty[Any]))
    // A list insulates whatever is below it, so `inStruct` resets when descending into one.
    def nullUnderStruct(dt: DataType, inStruct: Boolean): Boolean = dt match {
      case NullType => inStruct
      case ArrayType(element, _) => nullUnderStruct(element, inStruct = false)
      case StructType(fields) => fields.exists(f => nullUnderStruct(f.dataType, inStruct = true))
      case MapType(k, v, _) =>
        nullUnderStruct(k, inStruct = true) || nullUnderStruct(v, inStruct = true)
      case _ => false
    }

    val numRows = 4
    val numBatches = 3
    shapes.foreach { case (dataType, value) =>
      val name = dataType.simpleString
      val schema = StructType(Seq(StructField("c", dataType)))
      val batches = (0 until numBatches).map { _ =>
        CometArrowConverters
          .rowToArrowBatchIter(
            Iterator.fill(numRows)(InternalRow(value)),
            schema,
            numRows,
            "UTC",
            CometArrowAllocator)
          .next()
      }
      val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq
      batches.foreach(_.close())

      val (result, batchCount, totalRows) = Await.result(
        Future(Utils.coalesceBroadcastBatches(bufs.iterator))(ExecutionContext.global),
        10.seconds)

      val expectBypass = nullUnderStruct(dataType, inStruct = false)
      assert(
        (batchCount == 0) == expectBypass,
        s"$name: batchCount=$batchCount but bypass expected=$expectBypass")
      assert(result.length == (if (expectBypass) numBatches else 1), name)
      if (!expectBypass) assert(totalRows == numRows.toLong * numBatches, name)

      val decoded = result.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
      assert(decoded.map(_.numRows()).sum == numRows.toLong * numBatches, name)
      decoded.foreach(_.close())
    }
  }

  test("coalesceBroadcastBatches keeps coalescing plain null lists") {
    // A NullVector directly under a list is safe to append: the list's capacity loop only looks
    // at its own offset and validity buffers, and ListVector.setValueCount sets the child's
    // count from the last offset. Bypassing coalescing here would cost every consuming task one
    // IPC stream per original buffer instead of one.
    val numRows = 4
    val numBatches = 3
    val batches = (0 until numBatches).map(_ => nullListBatch(numRows))
    val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq

    val (result, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs.iterator)
    assert(batchCount == numBatches)
    assert(totalRows == numRows.toLong * numBatches)
    assert(result.length == 1)

    // Every list keeps its length across the append. Read each batch before the stream moves
    // on, since the reader reclaims a batch's buffers when it loads the next one.
    def listLengths(bufs: Iterator[ChunkedByteBuffer]): Seq[Int] =
      bufs
        .flatMap(b => Utils.decodeBatches(b, "test"))
        .flatMap { batch =>
          val lengths =
            (0 until batch.numRows()).map(r => batch.column(0).getArray(r).numElements())
          batch.close()
          lengths
        }
        .toSeq
    val expected = Seq.fill(numBatches)(0 until numRows).flatten
    assert(listLengths(bufs.iterator) == expected, "uncoalesced input")
    assert(listLengths(result.iterator) == expected, "coalesced output")
  }
}

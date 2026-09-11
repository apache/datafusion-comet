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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, IsNotNull, IsNull}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.{DefaultCachedBatch, DefaultCachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

/**
 * Cached batch format used when Comet writes Spark in-memory cache data.
 *
 * `columns` holds one compressed Arrow stream per cached column, in cache-schema order, produced
 * by `Utils.serializeBatchColumns`. Storing columns separately is what lets a scan decode only
 * the ones it projected; a single stream covering the whole batch would have to be inflated in
 * full before any projection could be applied. The cache manager still owns storage and eviction;
 * this class only changes the cached payload. `deltaEncoded` marks numeric streams whose values
 * need a prefix sum after decoding; the validity bits and logical schema remain the same as the
 * source column.
 */
private case class CometCachedBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    override val stats: InternalRow,
    columns: Array[ChunkedByteBuffer],
    deltaEncoded: Array[Boolean])
    extends SimpleMetricsCachedBatch

/**
 * Cache serializer that stores Comet-compatible Arrow batches in Spark's in-memory cache.
 *
 * The cached payload format is decided by the schema alone. A relation whose schema Comet's Arrow
 * writer supports is stored as `CometCachedBatch`, and every other relation is delegated in full
 * to Spark's `DefaultCachedBatchSerializer`. The format deliberately does not depend on any
 * runtime config: `spark.sql.cache.serializer` is a static conf, so installing this serializer is
 * already a per-application decision, and a relation whose format could flip mid-session cannot
 * be read back reliably. `spark.comet.exec.inMemoryCache.enabled` still governs whether a scan
 * over the cache runs natively, and its value at startup is what makes `CometDriverPlugin`
 * install this serializer in the first place.
 *
 * Reads of `CometCachedBatch` keep working when the native scan is disabled, because Spark then
 * reads the same cached data through the SparkToColumnar fallback path.
 */
class ArrowCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {

  import ArrowCachedBatchSerializer.supportsSchema

  private val fallback = new DefaultCachedBatchSerializer()

  // Bounds and null counts per column, gathered before the batch is serialized: serializing
  // clears the batch's vectors, and the per-column byte sizes that complete the statistics row
  // are only known afterwards. See statsRow.
  private[sql] def gatherColumnStats(
      batch: ColumnarBatch,
      attrs: Seq[Attribute]): (Array[Any], Array[Any], Array[Int]) = {
    val numCols = attrs.length
    val lower = new Array[Any](numCols)
    val upper = new Array[Any](numCols)
    val nulls = new Array[Int](numCols)
    val numRows = batch.numRows()

    var c = 0
    while (c < numCols) {
      val col = batch.column(c)
      val (min, max, nullCount) = attrs(c).dataType match {
        case BooleanType => gatherBooleanStats(col, numRows)
        case ByteType => gatherByteStats(col, numRows)
        case ShortType => gatherShortStats(col, numRows)
        case IntegerType | DateType => gatherIntStats(col, numRows)
        case LongType | TimestampType | TimestampNTZType => gatherLongStats(col, numRows)
        case FloatType => gatherFloatStats(col, numRows)
        case DoubleType => gatherDoubleStats(col, numRows)
        case d: DecimalType => gatherDecimalStats(col, numRows, d)
        case StringType => gatherStringStats(col, numRows)
        case other =>
          assert(!tracksBounds(other), s"Missing cache bounds implementation for $other")
          var nullCount = 0
          var r = 0
          while (r < numRows) {
            if (col.isNullAt(r)) nullCount += 1
            r += 1
          }
          (null, null, nullCount)
      }
      if (nullCount < numRows) {
        lower(c) = min
        upper(c) = max
      }
      nulls(c) = nullCount
      c += 1
    }

    (lower, upper, nulls)
  }

  // Keep each loop specialized and box only its final bounds. r == nullCount identifies
  // the first non-null value, including when a column starts with nulls.
  private def gatherBooleanStats(col: ColumnVector, numRows: Int): (Boolean, Boolean, Int) = {
    var min = false
    var max = false
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getBoolean(r)
        if (r == nullCount || JBoolean.compare(value, min) < 0) min = value
        if (r == nullCount || JBoolean.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherByteStats(col: ColumnVector, numRows: Int): (Byte, Byte, Int) = {
    var min = 0.toByte
    var max = 0.toByte
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getByte(r)
        if (r == nullCount || JByte.compare(value, min) < 0) min = value
        if (r == nullCount || JByte.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherShortStats(col: ColumnVector, numRows: Int): (Short, Short, Int) = {
    var min = 0.toShort
    var max = 0.toShort
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getShort(r)
        if (r == nullCount || JShort.compare(value, min) < 0) min = value
        if (r == nullCount || JShort.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherIntStats(col: ColumnVector, numRows: Int): (Int, Int, Int) = {
    var min = 0
    var max = 0
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getInt(r)
        if (r == nullCount || JInteger.compare(value, min) < 0) min = value
        if (r == nullCount || JInteger.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherLongStats(col: ColumnVector, numRows: Int): (Long, Long, Int) = {
    var min = 0L
    var max = 0L
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getLong(r)
        if (r == nullCount || JLong.compare(value, min) < 0) min = value
        if (r == nullCount || JLong.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherFloatStats(col: ColumnVector, numRows: Int): (Float, Float, Int) = {
    var min = 0.0f
    var max = 0.0f
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getFloat(r)
        if (r == nullCount || JFloat.compare(value, min) < 0) min = value
        if (r == nullCount || JFloat.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherDoubleStats(col: ColumnVector, numRows: Int): (Double, Double, Int) = {
    var min = 0.0d
    var max = 0.0d
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getDouble(r)
        if (r == nullCount || JDouble.compare(value, min) < 0) min = value
        if (r == nullCount || JDouble.compare(value, max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherDecimalStats(
      col: ColumnVector,
      numRows: Int,
      dt: DecimalType): (Decimal, Decimal, Int) = {
    var min: Decimal = null
    var max: Decimal = null
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getDecimal(r, dt.precision, dt.scale)
        if (r == nullCount || value.compare(min) < 0) min = value
        if (r == nullCount || value.compare(max) > 0) max = value
      }
      r += 1
    }
    (min, max, nullCount)
  }

  private def gatherStringStats(
      col: ColumnVector,
      numRows: Int): (UTF8String, UTF8String, Int) = {
    val ordering = TypeUtils.getInterpretedOrdering(StringType)
    var min: UTF8String = null
    var max: UTF8String = null
    var nullCount = 0
    var r = 0
    while (r < numRows) {
      if (col.isNullAt(r)) {
        nullCount += 1
      } else {
        val value = col.getUTF8String(r)
        // Borrow the value for comparison, but retain owned copies of the bounds.
        if (r == nullCount || ordering.compare(value, min) < 0) min = value.copy()
        if (r == nullCount || ordering.compare(value, max) > 0) max = value.copy()
      }
      r += 1
    }
    (min, max, nullCount)
  }

  // Build the statistics row expected by SimpleMetricsCachedBatchSerializer.
  // For each cached column Spark expects five values in this order:
  // lower bound, upper bound, null count, row count, and size in bytes.
  private def statsRow(
      lower: Array[Any],
      upper: Array[Any],
      nulls: Array[Int],
      numRows: Int,
      columnSizes: Array[Long]): InternalRow = {
    val numCols = lower.length
    val values = new Array[Any](numCols * 5)
    var c = 0
    while (c < numCols) {
      val base = c * 5
      values(base) = lower(c)
      values(base + 1) = upper(c)
      values(base + 2) = nulls(c)
      values(base + 3) = numRows
      // Each column is its own compressed stream, so its size is known exactly. Cache pruning
      // uses bounds/null-count/row-count rather than this field, but Spark reserves it and
      // reports it, so record the real value.
      values(base + 4) = columnSizes(c)
      c += 1
    }

    new GenericInternalRow(values)
  }

  // Spark can prune cache batches only for types whose bounds can be compared.
  // Other types still report null count and row count but leave bounds as null.
  private def tracksBounds(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        _: DecimalType | StringType | DateType | TimestampType | TimestampNTZType =>
      true
    case _ => false
  }

  // Compute Spark-compatible cache stats before serializing each batch to Arrow.
  // The stats are stored beside the Arrow bytes so Spark's cache filter can prune
  // CometCachedBatch without decoding the batch first.
  //
  // A columnar input batch is not guaranteed to be Arrow-backed; see supportsColumnarInput for
  // why. Batches that are not get copied into Arrow first, since Utils.serializeBatches only
  // writes CometVector columns.
  private def encodeBatches(
      batches: Iterator[ColumnarBatch],
      attrs: Seq[Attribute]): Iterator[CachedBatch] = {
    val arrowSchema =
      Utils.toArrowSchema(Utils.fromAttributes(attrs), CometArrowStream.NATIVE_TIMEZONE)

    batches.map { batch =>
      // Bounds and null counts are read from the input batch, which serializing then clears, so
      // they have to be gathered first. The row is only assembled once the per-column sizes are
      // known.
      val (lower, upper, nulls) = gatherColumnStats(batch, attrs)
      val numRows = batch.numRows()

      val (columns, deltaEncoded) = if (Utils.isArrowBacked(batch)) {
        Utils.serializeBatchColumns(batch)
      } else {
        val arrowBatch =
          CometArrowConverters.columnarBatchToArrowBatch(batch, arrowSchema, CometArrowAllocator)
        try Utils.serializeBatchColumns(arrowBatch)
        finally arrowBatch.close()
      }

      val columnSizes = columns.map(_.size)
      CometCachedBatch(
        numRows = numRows,
        sizeInBytes = columnSizes.sum,
        stats = statsRow(lower, upper, nulls, numRows, columnSizes),
        columns = columns,
        deltaEncoded = deltaEncoded)
    }
  }

  // Resolve requested columns by exprId, not by name, because aliases may reuse names.
  //
  // An empty selection stays empty rather than expanding to every column. Spark asks for no
  // columns when the query only needs the row count (SELECT count(*)), and since projection now
  // decides what gets decoded, expanding it would turn the cheapest possible read into the most
  // expensive one.
  private def selectedIndices(
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute]): Array[Int] = {
    val byExprId = cacheAttributes.zipWithIndex.map { case (attr, idx) =>
      attr.exprId -> idx
    }.toMap

    selectedAttributes.map { attr =>
      byExprId.getOrElse(
        attr.exprId,
        throw new IllegalStateException(
          s"Could not resolve selected attribute ${attr.name} from cache attributes"))
    }.toArray
  }

  // Spark's SimpleMetricsCachedBatchSerializer prunes a batch when the generated partition filter
  // does not evaluate to true against the stats row. Bounds are only computed for the types
  // tracksBounds accepts, and for every other column the lower and upper bounds stay null, which
  // makes a comparison against them evaluate to null and therefore prune the batch. That would
  // silently drop rows, so predicates over columns without bounds are not pushed down at all.
  // Null counts and row counts are recorded for every column, so IsNull and IsNotNull stay safe.
  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val prunable = cachedAttributes.collect {
      case a if tracksBounds(a.dataType) => a.exprId
    }.toSet

    val prunablePredicates = predicates.filter {
      case _: IsNull | _: IsNotNull => true
      case p => p.references.forall(a => prunable.contains(a.exprId))
    }

    super.buildFilter(prunablePredicates, cachedAttributes)
  }

  // Comet's Arrow writer only handles the types listed in supportsSchema. Reporting false here
  // sends the relation down the row path, where it is delegated to Spark's default serializer,
  // instead of failing at cache materialization inside Utils.serializeBatches.
  //
  // This answer is schema-only, because attributes are all Spark gives us; it says nothing about
  // the vectors. Returning true also makes InMemoryRelation strip the ColumnarToRow above the
  // cached plan, so convertColumnarBatchToCachedBatch then receives whatever that plan produces:
  // a Comet scan's CometVectors, but equally Spark's vectorized Parquet/ORC reader or a
  // connector's own vectors. encodeBatches converts the non-Arrow ones; that conversion is load
  // bearing, not defensive.
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = supportsSchema(schema)

  // A relation Comet stores is always readable as columnar Arrow. Anything else holds
  // DefaultCachedBatch, so defer to Spark, which only claims columnar output for the primitive
  // types its ColumnAccessor.decompress path can actually decode.
  override def supportsColumnarOutput(schema: StructType): Boolean = {
    if (schema.fields.forall(f => ArrowCachedBatchSerializer.supportsType(f.dataType))) {
      true
    } else {
      fallback.supportsColumnarOutput(schema)
    }
  }

  // Columnar Comet output is stored as compressed Arrow stream bytes. Spark only calls this when
  // supportsColumnarInput returned true, so the schema is known to be Comet-writable here.
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    input.mapPartitions { batches =>
      encodeBatches(batches, schema)
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!supportsSchema(cacheAttributes)) {
      return fallback.convertCachedBatchToColumnarBatch(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    }

    val indices = selectedIndices(cacheAttributes, selectedAttributes)

    input.mapPartitions { it =>
      // Codec factories are reusable; each selected column still gets its own input stream.
      lazy val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      // A ColumnReaders closes its readers (releasing the vectors they are holding) only when the
      // batch it produced has been consumed. A consumer that stops early -- LIMIT, take(), or a
      // cancelled task -- leaves the readers for the batch in flight open, so close them on task
      // completion. Spark's own ArrowCachedBatchSerializer registers a listener for the same
      // reason.
      //
      // flatMap consumes each inner iterator fully before building the next, so at most one batch
      // is open at a time and tracking the current one is enough. close() is idempotent, so
      // closing one that already released itself is a no-op.
      @volatile var current: ColumnReaders = null
      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskCompletionListener[Unit] { _ =>
          val readers = current
          current = null
          if (readers != null) {
            readers.close()
          }
        }
      }

      it.flatMap {
        case cb: CometCachedBatch =>
          if (indices.isEmpty) {
            // Nothing to decode: the row count is the whole answer, and it is already here.
            Iterator.single(new ColumnarBatch(Array.empty[ColumnVector], cb.numRows))
          } else {
            val readers = new ColumnReaders(
              indices.map(i => cb.columns(i)),
              indices.map(i => cb.deltaEncoded(i)),
              cb.numRows,
              codec)
            current = readers
            readers.batches
          }

        case other =>
          throw new IllegalStateException(
            s"Unsupported cached batch type ${other.getClass.getName}")
      }
    }
  }

  // Decodes one selected column stream apiece and stitches the results back into a single batch.
  //
  // Each stream is self-contained, so the columns a scan did not select are never inflated. The
  // decoded vectors stay owned by their readers: closing them releases the batch, which is why
  // this yields a single-element iterator that closes on exhaustion, matching what
  // ArrowReaderIterator did when the payload was one stream.
  private class ColumnReaders(
      buffers: Array[ChunkedByteBuffer],
      deltaEncoded: Array[Boolean],
      numRows: Int,
      codec: CompressionCodec) {
    // decodeBatches opens a reader and eagerly decodes its first batch, so it allocates. If a
    // later column throws, the readers already opened here are unreachable: the task-completion
    // listener cannot release them because `current` is only assigned once this constructor
    // returns, so they would leak off-heap for the life of the executor.
    private val readers: Array[Iterator[ColumnarBatch]] = {
      val opened = new Array[Iterator[ColumnarBatch]](buffers.length)
      var i = 0
      try {
        while (i < buffers.length) {
          opened(i) = Utils.decodeBatches(buffers(i), "CometCache", codec)
          i += 1
        }
      } catch {
        case NonFatal(e) =>
          var j = 0
          while (j < i) {
            opened(j) match {
              case reader: ArrowReaderIterator =>
                try reader.close()
                catch { case NonFatal(closeError) => e.addSuppressed(closeError) }
              case _ => ()
            }
            j += 1
          }
          throw e
      }
      opened
    }
    private var closed = false

    def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        readers.foreach {
          case reader: ArrowReaderIterator => reader.close()
          case _ => ()
        }
      }
    }

    private def assemble(): ColumnarBatch = {
      val columns = new Array[ColumnVector](readers.length)
      var i = 0
      while (i < readers.length) {
        val reader = readers(i)
        if (!reader.hasNext) {
          throw new IllegalStateException(
            s"Cached column stream $i of ${readers.length} decoded to no batch")
        }
        val decoded = reader.next()
        // Each stream holds exactly one single-column record batch, and every column of a cached
        // batch covers the same rows. Check rather than trust: a mismatch would otherwise build a
        // batch whose columns disagree on length, which reads as corrupt data far from here.
        if (decoded.numCols() != 1) {
          throw new IllegalStateException(
            s"Cached column stream $i decoded to ${decoded.numCols()} columns, expected 1")
        }
        if (decoded.numRows() != numRows) {
          throw new IllegalStateException(
            s"Cached column stream $i decoded ${decoded.numRows()} rows, expected $numRows")
        }
        val column = decoded.column(0)
        if (deltaEncoded(i)) {
          Utils.decodeDeltaLongs(column.asInstanceOf[CometVector].getValueVector)
        }
        columns(i) = column
        i += 1
      }
      new ColumnarBatch(columns, numRows)
    }

    def batches: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      private var emitted = false

      override def hasNext: Boolean = {
        if (emitted) {
          close()
          false
        } else {
          true
        }
      }

      override def next(): ColumnarBatch = {
        if (emitted) {
          throw new NoSuchElementException
        }
        emitted = true
        assemble()
      }
    }
  }

  // Row input is cached in Comet format by converting rows to Arrow batches first.
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    if (!supportsSchema(schema)) {
      fallback.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
    } else {
      val batchSize = conf.columnBatchSize

      input.mapPartitions { rows =>
        val iter = CometArrowConverters.rowToArrowBatchIter(
          rows,
          Utils.fromAttributes(schema),
          batchSize,
          // NATIVE_TIMEZONE ("UTC"), not conf.sessionLocalTimeZone, so both write paths produce
          // the same physical format: the columnar path above already encodes with
          // NATIVE_TIMEZONE. Unlike Spark's Arrow cache, whose RecordBatch is deliberately
          // schema-less, CometCachedBatch stores a full IPC stream including the schema, so a
          // session-local label would persist the writing session's mutable timezone into cached
          // data. This is a label only: Spark's internal timestamp representation is micros since
          // the Unix epoch regardless of session timezone, so no values are converted. It also
          // matches Comet's native schema, avoiding a cast at the native boundary.
          CometArrowStream.NATIVE_TIMEZONE,
          CometArrowAllocator)

        encodeBatches(iter, schema)
      }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!supportsSchema(cacheAttributes)) {
      return fallback.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    }

    convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      .mapPartitions { batches =>
        new CachedBatchRowIterator(selectedAttributes).createObject(batches)
      }
  }
}

object ArrowCachedBatchSerializer {

  /**
   * Whether Comet's Arrow cache format can store this type.
   *
   * This mirrors the vectors `Utils.getFieldVector` accepts. A type missing from that list throws
   * during cache materialization, so it has to be delegated to Spark's default cache format
   * instead. Interval types are the notable omission.
   */
  def supportsType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        DateType | TimestampType | TimestampNTZType | BinaryType | NullType =>
      true
    case _: DecimalType => true
    case _: StringType => true
    case ArrayType(elementType, _) => supportsType(elementType)
    case MapType(keyType, valueType, _) => supportsType(keyType) && supportsType(valueType)
    case StructType(fields) => fields.forall(f => supportsType(f.dataType))
    case _ => false
  }

  def supportsSchema(schema: Seq[Attribute]): Boolean =
    schema.forall(a => supportsType(a.dataType))

  /**
   * The classes a `CometCachedBatch` adds on top of [[org.apache.comet.CometKryoRegistrator]]'s
   * shared Arrow-bytes classes.
   *
   * Spark serializes a `CachedBatch` with `spark.serializer` whenever the block leaves the heap:
   * the disk half of `MEMORY_AND_DISK`, the `_SER` levels, replication, and cross-executor
   * fetches. Under `spark.kryo.registrationRequired=true` Kryo rejects any class it has not been
   * told about, so an ordinary `df.cache()` that spills would fail with "Class is not registered"
   * rather than anything naming this feature. Spark registers its own `ArrowCachedBatch` in
   * `KryoSerializer.loadableSparkClasses` for the same reason; Comet cannot add to that list, so
   * `CometKryoRegistrator` registers these instead.
   */
  def kryoClasses: Seq[Class[_]] = Seq(
    classOf[CometCachedBatch],
    // The statistics row, whose values are bounds in Spark's internal representation: boxed
    // primitives, which Kryo registers by default, plus UTF8String and Decimal, which it does not.
    // A Decimal above Long precision holds a scala.math.BigDecimal, which Chill's Scala registrar
    // already covers with a serializer that writes the java.math.BigDecimal inside it as a
    // class-and-object, so that one has to be registered here.
    classOf[GenericInternalRow],
    classOf[Array[Any]],
    classOf[UTF8String],
    classOf[Decimal],
    classOf[java.math.BigDecimal],
    classOf[java.math.BigInteger],
    // A relation whose schema this serializer cannot store is delegated to Spark's
    // DefaultCachedBatchSerializer, so its payload has to survive Kryo too. Spark registers
    // DefaultCachedBatch itself only from 4.1 onwards, so on 3.4, 3.5 and 4.0 the delegated path
    // fails without this. Registering it twice on 4.1 is a no-op.
    classOf[DefaultCachedBatch])
}

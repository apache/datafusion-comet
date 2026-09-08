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

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, IsNotNull, IsNull, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.{DefaultCachedBatch, DefaultCachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometArrowAllocator, CometConf}
import org.apache.comet.vector.NativeUtil

/**
 * Cached batch format used when Comet writes Spark in-memory cache data.
 *
 * `bytes` is one encapsulated Arrow IPC RecordBatch message and its body, with no Schema message
 * and no end-of-stream marker, produced by `CachedBatchIpc.serialize`. Compression is applied per
 * Arrow buffer rather than over the payload as a whole, which is what lets a scan decompress only
 * the columns it projected: the message records every buffer's offset and length, so
 * `CachedBatchIpc.readProjected` copies out just the selected columns' byte ranges. The cache
 * manager still owns storage and eviction; this class only changes the cached payload.
 */
private case class CometCachedBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    override val stats: InternalRow,
    bytes: Array[Byte])
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

  /**
   * How each cached column's bounds are compared, or null for a column that records none.
   *
   * Spark's own interpreted ordering for the type, which is the comparison its expressions use,
   * so bounds recorded with it order the same way a predicate over the column does. That matters
   * for collated strings, where it resolves to the collation's comparator rather than byte order,
   * and it is why this needs no per-Spark-version shim: the collation awareness comes from Spark.
   *
   * Resolved once per partition rather than per row -- `getInterpretedOrdering` walks the type
   * and, for a collated string, looks the collation up by id.
   */
  private def boundsOrderings(attrs: Seq[Attribute]): Array[Ordering[Any]] =
    attrs.map { attr =>
      if (tracksBounds(attr.dataType)) TypeUtils.getInterpretedOrdering(attr.dataType) else null
    }.toArray

  // Bounds and null counts per column, gathered before the batch is serialized: serializing
  // clears the batch's vectors, and the per-column byte sizes that complete the statistics row
  // are only known afterwards. See statsRow.
  private def gatherColumnStats(
      batch: ColumnarBatch,
      attrs: Seq[Attribute],
      orderings: Array[Ordering[Any]]): (Array[Any], Array[Any], Array[Int]) = {
    val numCols = attrs.length
    val lower = new Array[Any](numCols)
    val upper = new Array[Any](numCols)
    val nulls = Array.fill[Int](numCols)(0)
    val numRows = batch.numRows()

    var c = 0
    while (c < numCols) {
      val dt = attrs(c).dataType
      val col = batch.column(c)
      val ordering = orderings(c)
      var r = 0
      while (r < numRows) {
        if (col.isNullAt(r)) {
          nulls(c) += 1
        } else if (ordering != null) {
          val value = readValue(col, dt, r)
          if (lower(c) == null || ordering.compare(value, lower(c)) < 0) {
            lower(c) = value
          }
          if (upper(c) == null || ordering.compare(value, upper(c)) > 0) {
            upper(c) = value
          }
        }
        r += 1
      }
      c += 1
    }

    (lower, upper, nulls)
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
      // The stored size of the column's own Arrow buffers, taken from the message's buffer
      // layout, so it is exact rather than an estimate. Cache pruning uses
      // bounds/null-count/row-count rather than this field, but Spark reserves it and reports it,
      // so record the real value. The per-batch message framing is not attributed to any column,
      // so these sum to slightly less than sizeInBytes.
      values(base + 4) = columnSizes(c)
      c += 1
    }

    new GenericInternalRow(values)
  }

  // Spark can prune cache batches only for types whose bounds can be compared.
  // Other types still report null count and row count but leave bounds as null.
  //
  // Every StringType qualifies, collated ones included. Matching the bare `StringType` object
  // instead would exclude them, since a collated StringType is not equal to the default one, and
  // they would then get null bounds and no pruning at all. See boundsOrderings for how a collated
  // column's bounds are compared.
  private def tracksBounds(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        _: DecimalType | _: StringType | DateType | TimestampType | TimestampNTZType =>
      true
    case _ => false
  }

  // Read a non-null value from a ColumnVector using Spark's internal value type
  // for the corresponding DataType.
  private def readValue(col: ColumnVector, dt: DataType, rowId: Int): Any = dt match {
    case BooleanType => col.getBoolean(rowId)
    case ByteType => col.getByte(rowId)
    case ShortType => col.getShort(rowId)
    case IntegerType | DateType => col.getInt(rowId)
    case LongType | TimestampType | TimestampNTZType => col.getLong(rowId)
    case FloatType => col.getFloat(rowId)
    case DoubleType => col.getDouble(rowId)
    case d: DecimalType => col.getDecimal(rowId, d.precision, d.scale)
    case _: StringType => col.getUTF8String(rowId).copy()
    case _ => null
  }

  // Compute Spark-compatible cache stats before serializing each batch to Arrow.
  // The stats are stored beside the Arrow bytes so Spark's cache filter can prune
  // CometCachedBatch without decoding the batch first.
  //
  // A columnar input batch is not guaranteed to be Arrow-backed; see supportsColumnarInput for
  // why. Batches that are not get copied into Arrow first, since Utils.serializeBatches only
  // writes CometVector columns.
  /**
   * The configured write codec, read on the driver.
   *
   * Both write paths resolve this here rather than inside their `mapPartitions` closure: the
   * closure ships to the executors, where `CometConf` would resolve against whatever `SQLConf`
   * happens to be current on that thread rather than against this session's.
   */
  private def codecSettings(conf: SQLConf): (String, Int) =
    (
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_CODEC.get(conf),
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_ZSTD_LEVEL.get(conf))

  private def encodeBatches(
      batches: Iterator[ColumnarBatch],
      attrs: Seq[Attribute],
      codecSetting: (String, Int)): Iterator[CachedBatch] = {
    val arrowSchema =
      Utils.toArrowSchema(Utils.fromAttributes(attrs), CometArrowStream.NATIVE_TIMEZONE)
    val codec = CachedBatchIpc.compressionCodec(codecSetting._1, codecSetting._2)
    val orderings = boundsOrderings(attrs)

    batches.map { batch =>
      // Bounds and null counts are read from the input batch before it is serialized, and the row
      // is only assembled once the per-column sizes the message reports are known.
      val (lower, upper, nulls) = gatherColumnStats(batch, attrs, orderings)
      val numRows = batch.numRows()

      val (bytes, columnSizes) = if (Utils.isArrowBacked(batch)) {
        CachedBatchIpc.serialize(batch, codec, CometArrowAllocator)
      } else {
        val arrowBatch =
          CometArrowConverters.columnarBatchToArrowBatch(batch, arrowSchema, CometArrowAllocator)
        try CachedBatchIpc.serialize(arrowBatch, codec, CometArrowAllocator)
        finally arrowBatch.close()
      }

      CometCachedBatch(
        numRows = numRows,
        sizeInBytes = bytes.length.toLong,
        stats = statsRow(lower, upper, nulls, numRows, columnSizes),
        bytes = bytes)
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

  // Columnar Comet output is stored as one Arrow IPC record batch message per cached batch. Spark
  // only calls this when supportsColumnarInput returned true, so the schema is known to be
  // Comet-writable here.
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    val codec = codecSettings(conf)

    input.mapPartitions { batches =>
      encodeBatches(batches, schema, codec)
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
    // Captured as a StructType rather than the attributes themselves: this closure ships to the
    // executors, and the Arrow schema is rebuilt there from the same mapping the writer used.
    val cacheSchema = Utils.fromAttributes(cacheAttributes)

    input.mapPartitions { it =>
      // Built once per partition: resolving the Arrow schema and the projection's buffer layout
      // walks every field of the cached relation, which would otherwise be paid per batch.
      val projection = new CachedBatchIpc.Projection(
        Utils
          .toArrowSchema(cacheSchema, CometArrowStream.NATIVE_TIMEZONE)
          .getFields
          .asScala
          .toIndexedSeq,
        indices)

      // A ProjectedBatch owns the vectors of the batch it produced, and releases them only when
      // that batch has been consumed. A consumer that stops early -- LIMIT, take(), or a
      // cancelled task -- leaves the batch in flight open, so close it on task completion.
      // Spark's own ArrowCachedBatchSerializer registers a listener for the same reason.
      //
      // flatMap consumes each inner iterator fully before building the next, so at most one batch
      // is open at a time and tracking the current one is enough. close() is idempotent, so
      // closing one that already released itself is a no-op.
      @volatile var current: ProjectedBatch = null
      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskCompletionListener[Unit] { _ =>
          val open = current
          current = null
          if (open != null) {
            open.close()
          }
        }
      }

      it.flatMap {
        case cb: CometCachedBatch =>
          if (indices.isEmpty) {
            // Nothing to decode: the row count is the whole answer, and it is already here.
            Iterator.single(new ColumnarBatch(Array.empty[ColumnVector], cb.numRows))
          } else {
            val projected = new ProjectedBatch(cb, projection)
            current = projected
            projected.batches
          }

        case other =>
          throw new IllegalStateException(
            s"Unsupported cached batch type ${other.getClass.getName}")
      }
    }
  }

  /**
   * Owns the Arrow vectors decoded for one cached batch.
   *
   * The decode itself belongs to `CachedBatchIpc.Projection`, which is where knowledge of the
   * payload format lives; what is left here is ownership. The vectors stay owned by this object
   * -- closing it releases them -- which is why this yields a single-element iterator that closes
   * on exhaustion.
   */
  private class ProjectedBatch(cached: CometCachedBatch, projection: CachedBatchIpc.Projection) {

    // Decoding happens during construction, so `batches` below can hand out the root directly.
    // `load` releases everything it allocated if it throws, so there is nothing to unwind here.
    private val root = projection.load(cached.bytes, CometArrowAllocator)
    private var closed = false

    // A cached batch's columns all cover the same rows. Check rather than trust: a mismatch would
    // otherwise build a batch whose columns disagree with the row count recorded beside them,
    // which reads as corrupt data far from here.
    if (root.getRowCount != cached.numRows) {
      val decoded = root.getRowCount
      close()
      throw new IllegalStateException(
        s"Cached batch decoded $decoded rows, expected ${cached.numRows}")
    }

    def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        root.close()
      }
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
        NativeUtil.rootAsBatch(root)
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
      val codec = codecSettings(conf)

      input.mapPartitions { rows =>
        val iter = CometArrowConverters.rowToArrowBatchIter(
          rows,
          Utils.fromAttributes(schema),
          batchSize,
          // NATIVE_TIMEZONE ("UTC"), not conf.sessionLocalTimeZone. The payload stores no schema,
          // so the read path rebuilds one with toArrowSchema(cacheSchema, NATIVE_TIMEZONE); a
          // write that labelled its timestamps with the writing session's timezone would be read
          // back under a different label. Both write paths therefore have to agree on this, and
          // the columnar path above encodes with NATIVE_TIMEZONE too. This is a label only:
          // Spark's internal timestamp representation is micros since the Unix epoch regardless
          // of session timezone, so no values are converted. It also matches Comet's native
          // schema, avoiding a cast at the native boundary.
          CometArrowStream.NATIVE_TIMEZONE,
          CometArrowAllocator)

        encodeBatches(iter, schema, codec)
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
        val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)

        batches.flatMap { batch =>
          batch.rowIterator().asScala.map(row => toUnsafe(row).copy())
        }
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
    // The payload itself. Kryo registers Array[Byte] by default, but registering it here is what
    // keeps that true if the payload type ever changes again.
    classOf[Array[Byte]],
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

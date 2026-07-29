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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, IsNotNull, IsNull, UnsafeProjection}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometArrowAllocator

/**
 * Cached batch format used when Comet writes Spark in-memory cache data.
 *
 * `bytes` contains compressed Arrow stream data produced by `Utils.serializeBatches`. The cache
 * manager still owns storage and eviction; this class only changes the cached payload.
 */
private case class CometCachedBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    override val stats: InternalRow,
    bytes: ChunkedByteBuffer)
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

  // Row-to-Arrow conversion needs a StructType, while cache APIs pass attributes.
  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map { attr =>
      StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    })
  }

  // Build the statistics row expected by SimpleMetricsCachedBatchSerializer.
  // For each cached column Spark expects five values in this order:
  // lower bound, upper bound, null count, row count, and size in bytes.
  private def computeStats(batch: ColumnarBatch, attrs: Seq[Attribute]): InternalRow = {
    val numCols = attrs.length
    val lower = new Array[Any](numCols)
    val upper = new Array[Any](numCols)
    val nulls = Array.fill[Int](numCols)(0)
    val numRows = batch.numRows()

    var c = 0
    while (c < numCols) {
      val dt = attrs(c).dataType
      val col = batch.column(c)
      var r = 0
      while (r < numRows) {
        if (col.isNullAt(r)) {
          nulls(c) += 1
        } else if (tracksBounds(dt)) {
          val value = readValue(col, dt, r)
          if (lower(c) == null || compare(dt, value, lower(c)) < 0) {
            lower(c) = value
          }
          if (upper(c) == null || compare(dt, value, upper(c)) > 0) {
            upper(c) = value
          }
        }
        r += 1
      }
      c += 1
    }

    val values = new Array[Any](numCols * 5)
    c = 0
    while (c < numCols) {
      val base = c * 5
      values(base) = lower(c)
      values(base + 1) = upper(c)
      values(base + 2) = nulls(c)
      values(base + 3) = numRows
      // Spark reserves the fifth field for per-column size. Comet stores the whole
      // Arrow stream as one compressed buffer, so per-column size is not tracked here.
      // Cache pruning uses bounds/null-count/row-count, not this size field.
      values(base + 4) = 0L
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
    case StringType => col.getUTF8String(rowId).copy()
    case _ => null
  }

  // Compare values using the same physical representation used in the stats row.
  private def compare(dt: DataType, left: Any, right: Any): Int = dt match {
    case BooleanType =>
      java.lang.Boolean.compare(left.asInstanceOf[Boolean], right.asInstanceOf[Boolean])
    case ByteType =>
      java.lang.Byte.compare(left.asInstanceOf[Byte], right.asInstanceOf[Byte])
    case ShortType =>
      java.lang.Short.compare(left.asInstanceOf[Short], right.asInstanceOf[Short])
    case IntegerType | DateType =>
      java.lang.Integer.compare(left.asInstanceOf[Int], right.asInstanceOf[Int])
    case LongType | TimestampType | TimestampNTZType =>
      java.lang.Long.compare(left.asInstanceOf[Long], right.asInstanceOf[Long])
    case FloatType =>
      java.lang.Float.compare(left.asInstanceOf[Float], right.asInstanceOf[Float])
    case DoubleType =>
      java.lang.Double.compare(left.asInstanceOf[Double], right.asInstanceOf[Double])
    case _: DecimalType =>
      left.asInstanceOf[Decimal].compare(right.asInstanceOf[Decimal])
    case StringType =>
      ByteArray.compareBinary(
        left.asInstanceOf[UTF8String].getBytes,
        right.asInstanceOf[UTF8String].getBytes)
    case other =>
      throw new IllegalStateException(s"compare called for unsupported type $other")
  }

  // Compute Spark-compatible cache stats before serializing each batch to Arrow.
  // The stats are stored beside the Arrow bytes so Spark's cache filter can prune
  // CometCachedBatch without decoding the batch first.
  //
  // Spark decides the input path from `supportsColumnarInput`, which only sees the schema, so a
  // columnar input batch is not guaranteed to be Arrow-backed: any Spark or third-party columnar
  // leaf (Spark's vectorized Parquet/ORC reader, a connector's own vectors) reaches
  // `convertColumnarBatchToCachedBatch` with `On/OffHeapColumnVector`-style columns, which
  // `Utils.serializeBatches` cannot write. Copy those into Arrow first.
  private def encodeBatches(
      batches: Iterator[ColumnarBatch],
      attrs: Seq[Attribute]): Iterator[CachedBatch] = {
    lazy val structType = toStructType(attrs)

    batches.flatMap { batch =>
      val stats = computeStats(batch, attrs)

      val (arrowBatch, ownsBatch) =
        if (CometArrowConverters.isArrowBacked(batch)) {
          (batch, false)
        } else {
          val converted = CometArrowConverters.columnarBatchToArrowBatch(
            batch,
            structType,
            CometArrowStream.NATIVE_TIMEZONE,
            CometArrowAllocator)
          (converted, true)
        }

      try {
        // `Utils.serializeBatches` is lazy and clears the batch's vectors as it writes, so the
        // result has to be materialized before a converted batch is closed.
        Utils
          .serializeBatches(Iterator.single(arrowBatch))
          .map { case (rows, buffer) =>
            CometCachedBatch(
              numRows = rows.toInt,
              sizeInBytes = buffer.size,
              stats = stats,
              bytes = buffer)
          }
          .toList
      } finally {
        if (ownsBatch) {
          arrowBatch.close()
        }
      }
    }
  }

  // Resolve requested columns by exprId, not by name, because aliases may reuse names.
  private def selectedIndices(
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute]): Array[Int] = {
    if (selectedAttributes.isEmpty) {
      cacheAttributes.indices.toArray
    } else {
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
  }

  // A full-width projection is only an identity projection if every selected index
  // is already in column order. For example, [1, 0] must still be projected.
  private def isIdentityProjection(indices: Array[Int], numCols: Int): Boolean =
    indices.length == numCols && indices.indices.forall(i => indices(i) == i)

  private def projectBatch(batch: ColumnarBatch, indices: Array[Int]): ColumnarBatch = {
    if (isIdentityProjection(indices, batch.numCols())) {
      batch
    } else {
      val cols = indices.map(i => batch.column(i).asInstanceOf[ColumnVector])
      new ColumnarBatch(cols, batch.numRows())
    }
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
      it.flatMap {
        case cb: CometCachedBatch =>
          Utils.decodeBatches(cb.bytes, "CometCache").map { batch =>
            projectBatch(batch, indices)
          }

        case other =>
          throw new IllegalStateException(
            s"Unsupported cached batch type ${other.getClass.getName}")
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
      val sessionTz = conf.sessionLocalTimeZone

      input.mapPartitions { rows =>
        val iter = CometArrowConverters.rowToArrowBatchIter(
          rows,
          toStructType(schema),
          batchSize,
          sessionTz,
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
}

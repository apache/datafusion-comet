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
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.{ColumnAccessor, DefaultCachedBatch, DefaultCachedBatchSerializer}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometConf

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
 * Writes use Comet's Arrow cache format only when Comet and the native in-memory cache path are
 * enabled. Reads of CometCachedBatch are still supported even if the native scan is disabled
 * later, because Spark may then read the same cached data through the SparkToColumnar fallback
 * path.
 */
class ArrowCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {

  private val fallback = new DefaultCachedBatchSerializer()

  // Cache writes use Comet format only when both Comet and the in-memory cache scan are enabled.
  private def enabled(conf: SQLConf): Boolean = {
    CometConf.COMET_ENABLED.get(conf) &&
    CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.get(conf)
  }

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
  private def encodeBatches(
      batches: Iterator[ColumnarBatch],
      attrs: Seq[Attribute]): Iterator[CachedBatch] = {
    batches.flatMap { batch =>
      val stats = computeStats(batch, attrs)

      Utils.serializeBatches(Iterator.single(batch)).map { case (rows, buffer) =>
        CometCachedBatch(
          numRows = rows.toInt,
          sizeInBytes = buffer.size,
          stats = stats,
          bytes = buffer)
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

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    val activeConf = SQLConf.get
    activeConf != null && enabled(activeConf)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = true

  // Columnar Comet output is stored as compressed Arrow stream bytes.
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    input.mapPartitions { batches =>
      encodeBatches(batches, schema)
    }
  }

  // A cached relation can contain DefaultCachedBatch when this serializer is installed
  // but spark.comet.exec.inMemoryCache.enabled was disabled while the table was cached.
  // Decode Spark's default cache format here so the read path stays symmetric with the
  // fallback write path without launching another Spark job from inside a task.
  private def decodeDefaultCachedBatch(
      batch: DefaultCachedBatch,
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): ColumnarBatch = {
    val schema = DataTypeUtils.fromAttributes(selectedAttributes)
    val indices = selectedIndices(cacheAttributes, selectedAttributes)
    val numRows = batch.numRows

    // This fallback path is used only for Spark's DefaultCachedBatch format. Use on-heap
    // vectors here to avoid reading SQLConf inside executor-side cache decode code.
    val vectors = OnHeapColumnVector.allocateColumns(numRows, schema)

    val columnarBatch = new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]])
    columnarBatch.setNumRows(numRows)

    var i = 0
    while (i < selectedAttributes.length) {
      ColumnAccessor.decompress(
        batch.buffers(indices(i)),
        columnarBatch.column(i).asInstanceOf[WritableColumnVector],
        schema.fields(i).dataType,
        numRows)
      i += 1
    }

    Option(TaskContext.get()).foreach { taskContext =>
      taskContext.addTaskCompletionListener[Unit](_ => columnarBatch.close())
    }

    columnarBatch
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val indices = selectedIndices(cacheAttributes, selectedAttributes)

    input.mapPartitions { it =>
      it.flatMap {
        case cb: CometCachedBatch =>
          Utils.decodeBatches(cb.bytes, "CometCache").map { batch =>
            projectBatch(batch, indices)
          }

        case cb: DefaultCachedBatch =>
          Iterator(decodeDefaultCachedBatch(cb, cacheAttributes, selectedAttributes, conf))

        case other =>
          throw new IllegalStateException(
            s"Unsupported cached batch type ${other.getClass.getName}")
      }
    }
  }

  // Row input can still be cached in Comet format by converting rows to Arrow batches first.
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    if (!enabled(conf)) {
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
          TaskContext.get())

        encodeBatches(iter, schema)
      }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      .mapPartitions { batches =>
        val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)

        batches.flatMap { batch =>
          batch.rowIterator().asScala.map(row => toUnsafe(row).copy())
        }
      }
  }
}

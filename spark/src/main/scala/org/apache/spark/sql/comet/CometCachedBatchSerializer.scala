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

package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatch
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.ByteArray
import org.apache.spark.unsafe.types.UTF8String

/**
 * A cached batch holding one compressed Arrow IPC message plus Spark-format column stats.
 *
 * @param numRows
 *   number of rows in this batch
 * @param bytes
 *   compressed Arrow IPC bytes for a single record batch
 * @param stats
 *   InternalRow laid out as ColumnStatisticsSchema expects: per column [lowerBound, upperBound,
 *   nullCount, count, sizeInBytes]
 */
case class CometCachedBatch(numRows: Int, bytes: Array[Byte], stats: InternalRow)
    extends SimpleMetricsCachedBatch {
  // Used by InMemoryRelation to estimate the cached relation size; must reflect real bytes.
  override def sizeInBytes: Long = bytes.length.toLong
}

/**
 * Accumulates per-column min/max/null/count for a set of rows and emits the stats InternalRow in
 * the exact layout Spark's ColumnStatisticsSchema / SimpleMetricsCachedBatchSerializer expects.
 *
 * For column data types where a total ordering is not implemented here, the lower/upper bounds
 * are left null. Null bounds mean "cannot prune" and are always correct (this is how Spark itself
 * encodes unknown stats).
 */
class CometCacheColumnStats(attributes: Seq[Attribute]) {
  private val numCols = attributes.length
  private val lower = new Array[Any](numCols)
  private val upper = new Array[Any](numCols)
  private val nulls = new Array[Int](numCols)
  private var rowCount = 0

  /** Update column `ordinal` with one value. `value` is in Catalyst internal form (or null). */
  def update(ordinal: Int, dt: DataType, isNull: Boolean, value: Any): Unit = {
    if (isNull) {
      nulls(ordinal) += 1
      return
    }
    if (!ordered(dt)) return // leave bounds null for unsupported-stat types
    if (lower(ordinal) == null || compare(dt, value, lower(ordinal)) < 0) lower(ordinal) = value
    if (upper(ordinal) == null || compare(dt, value, upper(ordinal)) > 0) upper(ordinal) = value
  }

  def setRowCount(n: Int): Unit = rowCount = n

  def toInternalRow: InternalRow = {
    val values = new Array[Any](numCols * 5)
    var i = 0
    while (i < numCols) {
      val base = i * 5
      values(base) = lower(i) // lowerBound (column data type or null)
      values(base + 1) = upper(i) // upperBound
      values(base + 2) = nulls(i) // nullCount (Int)
      values(base + 3) = rowCount // count (Int)
      values(base + 4) = 0L // sizeInBytes (Long); not used by buildFilter
      i += 1
    }
    new GenericInternalRow(values)
  }

  private def ordered(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        _: DecimalType | StringType | DateType | TimestampType =>
      true
    case _ => false
  }

  private def compare(dt: DataType, x: Any, y: Any): Int = dt match {
    case BooleanType =>
      java.lang.Boolean.compare(x.asInstanceOf[Boolean], y.asInstanceOf[Boolean])
    case ByteType => java.lang.Byte.compare(x.asInstanceOf[Byte], y.asInstanceOf[Byte])
    case ShortType => java.lang.Short.compare(x.asInstanceOf[Short], y.asInstanceOf[Short])
    case IntegerType | DateType =>
      java.lang.Integer.compare(x.asInstanceOf[Int], y.asInstanceOf[Int])
    case LongType | TimestampType =>
      java.lang.Long.compare(x.asInstanceOf[Long], y.asInstanceOf[Long])
    case FloatType => java.lang.Float.compare(x.asInstanceOf[Float], y.asInstanceOf[Float])
    case DoubleType => java.lang.Double.compare(x.asInstanceOf[Double], y.asInstanceOf[Double])
    case _: DecimalType =>
      x.asInstanceOf[org.apache.spark.sql.types.Decimal]
        .compare(y.asInstanceOf[org.apache.spark.sql.types.Decimal])
    case StringType =>
      ByteArray.compareBinary(
        x.asInstanceOf[UTF8String].getBytes,
        y.asInstanceOf[UTF8String].getBytes)
    case other => throw new IllegalStateException(s"compare called for unordered type $other")
  }
}

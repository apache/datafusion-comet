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

package org.apache.comet.udf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import org.apache.comet.shims.CometInternalRowShim

/**
 * Shim base for Comet-owned [[InternalRow]] accessors used by the Arrow-direct codegen kernel.
 *
 * Provides `throw new UnsupportedOperationException` defaults for every abstract method declared
 * by `InternalRow` and `SpecializedGetters`. Concrete subclasses (`CometBatchKernel` and its
 * generated subclasses) override only the getters they actually support for their input shape.
 *
 * Purpose: keep subclasses free of boilerplate throws, and absorb forward-compat breakage if
 * Spark adds abstract methods to `InternalRow` in a future version. Add the defaulted override
 * here once, all subclasses recompile.
 */
abstract class CometInternalRow extends InternalRow with CometInternalRowShim {

  override def numFields: Int = unsupported("numFields")
  override def isNullAt(ordinal: Int): Boolean = unsupported("isNullAt")

  override def getBoolean(ordinal: Int): Boolean = unsupported("getBoolean")
  override def getByte(ordinal: Int): Byte = unsupported("getByte")
  override def getShort(ordinal: Int): Short = unsupported("getShort")
  override def getInt(ordinal: Int): Int = unsupported("getInt")
  override def getLong(ordinal: Int): Long = unsupported("getLong")
  override def getFloat(ordinal: Int): Float = unsupported("getFloat")
  override def getDouble(ordinal: Int): Double = unsupported("getDouble")
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    unsupported("getDecimal")
  override def getUTF8String(ordinal: Int): UTF8String = unsupported("getUTF8String")
  override def getBinary(ordinal: Int): Array[Byte] = unsupported("getBinary")
  override def getInterval(ordinal: Int): CalendarInterval = unsupported("getInterval")
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = unsupported("getStruct")
  override def getArray(ordinal: Int): ArrayData = unsupported("getArray")
  override def getMap(ordinal: Int): MapData = unsupported("getMap")

  /**
   * Generic `get(ordinal, dataType)` dispatcher. Required because `SpecializedGetters` declares
   * it abstract and some Spark codegen paths (notably `SafeProjection` for deserializing
   * `ScalaUDF` struct arguments) call it instead of the typed getter. Dispatches to the typed
   * getter matching `dataType`; a null entry returns `null` outright. Unsupported types fall
   * through to the shared throw.
   */
  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    if (isNullAt(ordinal)) return null
    dataType match {
      case BooleanType => java.lang.Boolean.valueOf(getBoolean(ordinal))
      case ByteType => java.lang.Byte.valueOf(getByte(ordinal))
      case ShortType => java.lang.Short.valueOf(getShort(ordinal))
      case IntegerType | DateType => java.lang.Integer.valueOf(getInt(ordinal))
      case LongType | TimestampType | TimestampNTZType =>
        java.lang.Long.valueOf(getLong(ordinal))
      case FloatType => java.lang.Float.valueOf(getFloat(ordinal))
      case DoubleType => java.lang.Double.valueOf(getDouble(ordinal))
      case _: StringType => getUTF8String(ordinal)
      case BinaryType => getBinary(ordinal)
      case dt: DecimalType => getDecimal(ordinal, dt.precision, dt.scale)
      case st: StructType => getStruct(ordinal, st.size)
      case _: ArrayType => getArray(ordinal)
      case _: MapType => getMap(ordinal)
      case other => unsupported(s"get for dataType $other")
    }
  }

  override def setNullAt(i: Int): Unit = unsupported("setNullAt")
  override def update(i: Int, value: Any): Unit = unsupported("update")
  override def copy(): InternalRow = unsupported("copy")

  protected def unsupported(method: String): Nothing =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: $method not implemented for this row shape")
}

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

package org.apache.comet.codegen

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import org.apache.comet.shims.CometInternalRowShim

/**
 * Shim base for Comet-owned [[ArrayData]] views used by the Arrow-direct codegen kernel.
 *
 * Provides `UnsupportedOperationException` defaults for every abstract method on `ArrayData` and
 * `SpecializedGetters`. Codegen emits a concrete subclass per complex-typed input column,
 * overriding only the small set of getters the element type requires (e.g. `numElements`,
 * `isNullAt`, and `getUTF8String` for an `ArrayType(StringType)` input).
 *
 * Pattern mirrors [[CometInternalRow]]: centralize the boilerplate throws so the codegen- emitted
 * subclasses stay short, and absorb forward-compat breakage if Spark adds abstract methods to
 * `ArrayData` in a future version.
 *
 * Mixes in [[CometInternalRowShim]] for the same reason `CometInternalRow` does: Spark 4.x adds
 * new abstract getters (`getVariant`, `getGeography`, `getGeometry`) on `SpecializedGetters` that
 * both `InternalRow` and `ArrayData` inherit. The shim is per-profile and provides throwing
 * defaults only on the profiles that declare those methods abstract.
 */
abstract class CometArrayData extends ArrayData with CometInternalRowShim {

  override def getInterval(ordinal: Int): CalendarInterval = unsupported("getInterval")

  /**
   * Generic `get(ordinal, dataType)` dispatcher. Spark codegen sometimes calls this rather than
   * the typed getter (`SafeProjection` uses it when deserializing struct-valued ScalaUDF args,
   * for example); leaving it as a throw leaks NPEs once callers catch the
   * `UnsupportedOperationException` and propagate null. Dispatches to the typed getter matching
   * `dataType`; a null entry returns `null` outright.
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

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = unsupported("getStruct")

  override def getArray(ordinal: Int): ArrayData = unsupported("getArray")

  override def getMap(ordinal: Int): MapData = unsupported("getMap")

  override def setNullAt(i: Int): Unit = unsupported("setNullAt")

  protected def unsupported(method: String): Nothing =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: $method not implemented for this array shape")

  override def update(i: Int, value: Any): Unit = unsupported("update")

  override def copy(): ArrayData = unsupported("copy")

  override def array: Array[Any] = unsupported("array")

  override def toString(): String = {
    val n =
      try numElements().toString
      catch {
        case _: Throwable => "?"
      }
    s"${getClass.getSimpleName}(numElements=$n)"
  }

  override def numElements(): Int = unsupported("numElements")
}

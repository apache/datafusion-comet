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
 * Throwing-default base for [[ArrayData]] in the Arrow-direct codegen kernel. Subclasses override
 * only the getters their element type needs (e.g. `numElements`, `isNullAt`, `getUTF8String` for
 * an `ArrayType(StringType)` input).
 *
 * Consumer: `InputArray_${path}` nested classes the input emitter generates per `ArrayType` input
 * column. They back `getArray(ord)` plus the recursion for `Array<Array<...>>` and array-typed
 * map keys / struct fields.
 *
 * `ArrayData` and [[CometInternalRow]]'s [[InternalRow]] are sibling abstract classes in Spark
 * (both extend `SpecializedGetters`, neither inherits the other), so a base aimed at one cannot
 * serve the other. The dispatch body that '''is''' shared between them lives in
 * [[CometSpecializedGettersDispatch]]. The third sibling, [[CometMapData]], backs `InputMap_*`
 * and routes `keyArray()` / `valueArray()` through `CometArrayData` instances.
 *
 * Mixes in [[CometInternalRowShim]] for the same reason `CometInternalRow` does: Spark 4.x adds
 * abstract `SpecializedGetters` methods (`getVariant`, `getGeography`, `getGeometry`) that both
 * `InternalRow` and `ArrayData` inherit; the per-profile shim provides throwing defaults.
 */
abstract class CometArrayData extends ArrayData with CometInternalRowShim {

  override def getInterval(ordinal: Int): CalendarInterval = unsupported("getInterval")

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    CometSpecializedGettersDispatch.get(this, ordinal, dataType)

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

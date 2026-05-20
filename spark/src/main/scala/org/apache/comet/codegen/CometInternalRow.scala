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
 * Throwing-default `InternalRow` base for the codegen kernel. Subclasses override only the
 * getters their input shape needs. Centralizing the throws absorbs forward-compat breakage when
 * Spark adds abstract methods.
 *
 * Two consumers: the compiled kernel (`ctx.INPUT_ROW = "row"` aliases `this`) and per-column
 * `InputStruct_${path}` nested classes that back `getStruct(ord, n)`.
 */
abstract class CometInternalRow extends InternalRow with CometInternalRowShim {

  override def numFields: Int = unsupported("numFields")

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

  override def update(i: Int, value: Any): Unit = unsupported("update")

  override def copy(): InternalRow = unsupported("copy")

  protected def unsupported(method: String): Nothing =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: $method not implemented for this row shape")
}

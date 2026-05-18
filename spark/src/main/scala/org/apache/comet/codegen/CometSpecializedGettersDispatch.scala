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

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

/**
 * Shared `SpecializedGetters.get(ordinal, dataType)` dispatch used by [[CometInternalRow]] and
 * [[CometArrayData]]. Spark codegen paths (notably `SafeProjection` for ScalaUDF struct args) and
 * interpreted-eval fallbacks (`ArrayDistinct.nullSafeEval` etc.) call the generic `get` instead
 * of the typed getter, so both kernel-side bases need a non-throwing implementation.
 *
 * For complex types, the typed getter allocates a fresh `InputStruct_*` / `InputArray_*` /
 * `InputMap_*` per call (`ColumnarRow`-style), so retain-by-reference consumers like
 * `OpenHashSet` get distinct identities.
 */
private[codegen] object CometSpecializedGettersDispatch {

  def get(g: SpecializedGetters, ordinal: Int, dataType: DataType): AnyRef = {
    if (g.isNullAt(ordinal)) return null
    dataType match {
      case BooleanType => java.lang.Boolean.valueOf(g.getBoolean(ordinal))
      case ByteType => java.lang.Byte.valueOf(g.getByte(ordinal))
      case ShortType => java.lang.Short.valueOf(g.getShort(ordinal))
      case IntegerType | DateType => java.lang.Integer.valueOf(g.getInt(ordinal))
      case LongType | TimestampType | TimestampNTZType =>
        java.lang.Long.valueOf(g.getLong(ordinal))
      case FloatType => java.lang.Float.valueOf(g.getFloat(ordinal))
      case DoubleType => java.lang.Double.valueOf(g.getDouble(ordinal))
      case _: StringType => g.getUTF8String(ordinal)
      case BinaryType => g.getBinary(ordinal)
      case dt: DecimalType => g.getDecimal(ordinal, dt.precision, dt.scale)
      case st: StructType => g.getStruct(ordinal, st.size)
      case _: ArrayType => g.getArray(ordinal)
      case _: MapType => g.getMap(ordinal)
      case other =>
        throw new UnsupportedOperationException(
          s"${g.getClass.getSimpleName}: get for dataType $other not implemented")
    }
  }
}

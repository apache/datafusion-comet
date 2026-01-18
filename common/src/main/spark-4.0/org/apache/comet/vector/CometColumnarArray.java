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

package org.apache.comet.vector;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * A mutable implementation of ArrayData backed by a ColumnVector. Unlike Spark's ColumnarArray
 * which has final fields, this class allows updating the offset and length to enable object reuse
 * across rows, reducing GC pressure.
 */
public class CometColumnarArray extends ArrayData {
  private ColumnVector data;
  private int offset;
  private int length;

  public CometColumnarArray(ColumnVector data) {
    this.data = data;
    this.offset = 0;
    this.length = 0;
  }

  public CometColumnarArray(ColumnVector data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  /** Updates this array to point to a new slice of the underlying data. */
  public void update(int offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  /** Updates both the data vector and the slice. */
  public void update(ColumnVector data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int numElements() {
    return length;
  }

  @Override
  public ArrayData copy() {
    Object[] values = new Object[length];
    for (int i = 0; i < length; i++) {
      if (!isNullAt(i)) {
        values[i] = get(i, data.dataType());
      }
    }
    return new GenericArrayData(values);
  }

  @Override
  public Object[] array() {
    DataType dt = data.dataType();
    Object[] values = new Object[length];
    for (int i = 0; i < length; i++) {
      if (!isNullAt(i)) {
        values[i] = get(i, dt);
      }
    }
    return values;
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException("CometColumnarArray is read-only");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException("CometColumnarArray is read-only");
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return data.isNullAt(offset + ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return data.getBoolean(offset + ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return data.getByte(offset + ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return data.getShort(offset + ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return data.getInt(offset + ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return data.getLong(offset + ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return data.getFloat(offset + ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return data.getDouble(offset + ordinal);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getDecimal(offset + ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getUTF8String(offset + ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getBinary(offset + ordinal);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getInterval(offset + ordinal);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return data.getArray(offset + ordinal);
  }

  @Override
  public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal) {
    return data.getMap(offset + ordinal);
  }

  @Override
  public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields) {
    return data.getStruct(offset + ordinal);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    throw new UnsupportedOperationException("Variant type is not supported");
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal)) {
      return null;
    }
    return org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader.read(
        this, ordinal, dataType, true, true);
  }
}

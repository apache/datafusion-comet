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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A mutable implementation of InternalRow backed by a ColumnVector for struct types. Unlike Spark's
 * ColumnarRow which has final fields, this class allows updating the rowId to enable object reuse
 * across rows, reducing GC pressure.
 *
 * <p>This class also implements wrapper pooling for nested complex types (arrays, maps, structs) to
 * avoid per-field allocations when accessing nested data.
 */
public class CometColumnarRow extends InternalRow {
  private ColumnVector data;
  private int rowId;
  private int numFields;

  // Reusable wrappers for nested complex types, indexed by field ordinal
  private CometColumnarArray[] reusableNestedArrays;
  private CometColumnarMap[] reusableNestedMaps;
  private CometColumnarRow[] reusableNestedRows;

  public CometColumnarRow(ColumnVector data) {
    this.data = data;
    this.rowId = 0;
    this.numFields = ((StructType) data.dataType()).size();
  }

  public CometColumnarRow(ColumnVector data, int rowId) {
    this.data = data;
    this.rowId = rowId;
    this.numFields = ((StructType) data.dataType()).size();
  }

  /** Updates this row to point to a different row in the underlying data. */
  public void update(int rowId) {
    this.rowId = rowId;
  }

  @Override
  public int numFields() {
    return numFields;
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException("CometColumnarRow is read-only");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException("CometColumnarRow is read-only");
  }

  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(numFields);
    for (int i = 0; i < numFields; i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = data.getChild(i).dataType();
        row.update(i, get(i, dt));
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return data.getChild(ordinal).isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return data.getChild(ordinal).getBoolean(rowId);
  }

  @Override
  public byte getByte(int ordinal) {
    return data.getChild(ordinal).getByte(rowId);
  }

  @Override
  public short getShort(int ordinal) {
    return data.getChild(ordinal).getShort(rowId);
  }

  @Override
  public int getInt(int ordinal) {
    return data.getChild(ordinal).getInt(rowId);
  }

  @Override
  public long getLong(int ordinal) {
    return data.getChild(ordinal).getLong(rowId);
  }

  @Override
  public float getFloat(int ordinal) {
    return data.getChild(ordinal).getFloat(rowId);
  }

  @Override
  public double getDouble(int ordinal) {
    return data.getChild(ordinal).getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getChild(ordinal).getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getChild(ordinal).getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getChild(ordinal).getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getChild(ordinal).getInterval(rowId);
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    ColumnVector child = data.getChild(ordinal);
    if (child instanceof CometStructVector) {
      if (reusableNestedRows == null) {
        reusableNestedRows = new CometColumnarRow[this.numFields];
      }
      if (reusableNestedRows[ordinal] == null) {
        reusableNestedRows[ordinal] = new CometColumnarRow(child);
      }
      reusableNestedRows[ordinal].update(rowId);
      return reusableNestedRows[ordinal];
    }
    return child.getStruct(rowId);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    ColumnVector child = data.getChild(ordinal);
    if (child instanceof CometListVector) {
      CometListVector listVector = (CometListVector) child;
      long offsetAddr = listVector.getOffsetBufferAddress();
      int start = Platform.getInt(null, offsetAddr + (long) rowId * 4L);
      int end = Platform.getInt(null, offsetAddr + (long) (rowId + 1) * 4L);
      int len = end - start;

      if (reusableNestedArrays == null) {
        reusableNestedArrays = new CometColumnarArray[numFields];
      }
      if (reusableNestedArrays[ordinal] == null) {
        reusableNestedArrays[ordinal] = new CometColumnarArray(listVector.getDataColumnVector());
      }
      reusableNestedArrays[ordinal].update(start, len);
      return reusableNestedArrays[ordinal];
    }
    return child.getArray(rowId);
  }

  @Override
  public MapData getMap(int ordinal) {
    ColumnVector child = data.getChild(ordinal);
    if (child instanceof CometMapVector) {
      CometMapVector mapVector = (CometMapVector) child;
      long offsetAddr = mapVector.getOffsetBufferAddress();
      int start = Platform.getInt(null, offsetAddr + (long) rowId * 4L);
      int end = Platform.getInt(null, offsetAddr + (long) (rowId + 1) * 4L);
      int len = end - start;

      if (reusableNestedMaps == null) {
        reusableNestedMaps = new CometColumnarMap[numFields];
      }
      if (reusableNestedMaps[ordinal] == null) {
        reusableNestedMaps[ordinal] =
            new CometColumnarMap(mapVector.getKeysVector(), mapVector.getValuesVector());
      }
      reusableNestedMaps[ordinal].update(start, len);
      return reusableNestedMaps[ordinal];
    }
    return child.getMap(rowId);
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

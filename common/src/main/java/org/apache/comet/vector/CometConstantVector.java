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

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A CometVector that stores a single constant value. For native export, it lazily creates a
 * 1-element Arrow vector, avoiding the cost of materializing N identical elements. The native side
 * detects the 1-element array and expands it to the actual batch size.
 *
 * <p>For Spark-direct consumption (e.g., ColumnarToRow), all getters return the constant value
 * regardless of rowId.
 */
public class CometConstantVector extends CometVector {
  private final BufferAllocator allocator = new RootAllocator();

  /** Whether the constant value is null */
  private final boolean isNull;

  /** The constant value (null if isNull is true) */
  private final Object value;

  /** The Spark data type */
  private final DataType sparkType;

  /** The Arrow field for creating the 1-element vector */
  private final Field arrowField;

  /** Logical number of rows this vector represents */
  private int numValues;

  /** Lazily created 1-element Arrow vector for native export */
  private ValueVector lazyVector;

  public CometConstantVector(
      DataType sparkType,
      Field arrowField,
      boolean useDecimal128,
      Object value,
      boolean isNull,
      int numValues) {
    super(sparkType, useDecimal128);
    this.sparkType = sparkType;
    this.arrowField = arrowField;
    this.value = value;
    this.isNull = isNull;
    this.numValues = numValues;
  }

  @Override
  public void setNumNulls(int numNulls) {
    // No-op: null status is determined by the constant isNull flag
  }

  @Override
  public void setNumValues(int numValues) {
    this.numValues = numValues;
  }

  @Override
  public int numValues() {
    return numValues;
  }

  @Override
  public boolean hasNull() {
    return isNull;
  }

  @Override
  public int numNulls() {
    return isNull ? numValues : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isNull;
  }

  @Override
  public boolean isFixedLength() {
    return !(sparkType == DataTypes.StringType || sparkType == DataTypes.BinaryType);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return (Boolean) value;
  }

  @Override
  public byte getByte(int rowId) {
    return (Byte) value;
  }

  @Override
  public short getShort(int rowId) {
    return (Short) value;
  }

  @Override
  public int getInt(int rowId) {
    return (Integer) value;
  }

  @Override
  public long getLong(int rowId) {
    return (Long) value;
  }

  @Override
  public long getLongDecimal(int rowId) {
    return (Long) value;
  }

  @Override
  public float getFloat(int rowId) {
    return (Float) value;
  }

  @Override
  public double getDouble(int rowId) {
    return (Double) value;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNull) return null;
    return (UTF8String) value;
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNull) return null;
    return (byte[]) value;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNull) return null;
    return (Decimal) value;
  }

  @Override
  public byte[] copyBinaryDecimal(int i, byte[] dest) {
    // Override to avoid the memory-address code path in CometVector.
    // For constant decimals, we go through getDecimal() instead.
    throw new UnsupportedOperationException(
        "CometConstantVector does not support copyBinaryDecimal; use getDecimal() instead");
  }

  @Override
  public ValueVector getValueVector() {
    if (lazyVector == null) {
      lazyVector = createOneElementVector();
    }
    return lazyVector;
  }

  @Override
  public CometVector slice(int offset, int length) {
    return new CometConstantVector(sparkType, arrowField, useDecimal128, value, isNull, length);
  }

  @Override
  public void close() {
    if (lazyVector != null) {
      lazyVector.close();
      lazyVector = null;
    }
    allocator.close();
  }

  /** Creates a 1-element Arrow vector holding the constant value. */
  private ValueVector createOneElementVector() {
    if (isNull) {
      return new NullVector(arrowField.getName(), 1);
    }

    if (sparkType == DataTypes.BooleanType) {
      BitVector v = new BitVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Boolean) value ? 1 : 0);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.ByteType) {
      TinyIntVector v = new TinyIntVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Byte) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.ShortType) {
      SmallIntVector v = new SmallIntVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Short) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.IntegerType) {
      IntVector v = new IntVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Integer) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.LongType) {
      BigIntVector v = new BigIntVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Long) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.FloatType) {
      Float4Vector v = new Float4Vector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Float) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.DoubleType) {
      Float8Vector v = new Float8Vector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Double) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.StringType) {
      VarCharVector v = new VarCharVector(arrowField, allocator);
      byte[] bytes = ((UTF8String) value).getBytes();
      v.allocateNew((long) bytes.length, 1);
      v.set(0, bytes);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.BinaryType) {
      VarBinaryVector v = new VarBinaryVector(arrowField, allocator);
      byte[] bytes = (byte[]) value;
      v.allocateNew((long) bytes.length, 1);
      v.set(0, bytes);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.DateType) {
      DateDayVector v = new DateDayVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Integer) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType == DataTypes.TimestampType || sparkType == TimestampNTZType$.MODULE$) {
      TimeStampMicroTZVector v = new TimeStampMicroTZVector(arrowField, allocator);
      v.allocateNew(1);
      v.set(0, (Long) value);
      v.setValueCount(1);
      return v;
    } else if (sparkType instanceof DecimalType) {
      DecimalType dt = (DecimalType) sparkType;
      DecimalVector v =
          new DecimalVector(arrowField.getName(), allocator, dt.precision(), dt.scale());
      v.allocateNew(1);
      BigDecimal bigDecimal = ((Decimal) value).toJavaBigDecimal();
      v.set(0, bigDecimal);
      v.setValueCount(1);
      return v;
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + sparkType);
    }
  }
}

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

package org.apache.comet.parquet;

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

/**
 * A column reader that returns constant vectors using Arrow Java vectors directly (no native
 * mutable buffers). Used for partition columns and missing columns in the native_iceberg_compat
 * scan path.
 *
 * <p>The vector is filled with the constant value repeated for every row in the batch. This is
 * necessary because the underlying Arrow vector's buffers must be large enough to match the
 * reported value count — otherwise variable-width types (strings, binary) would have undersized
 * offset buffers, causing out-of-bounds reads on the native side.
 */
public class ArrowConstantColumnReader extends AbstractColumnReader {
  private final BufferAllocator allocator = new RootAllocator();

  private boolean isNull;
  private Object value;
  private FieldVector fieldVector;
  private CometPlainVector vector;
  private int currentSize;

  /** Constructor for missing columns (default values from schema). */
  ArrowConstantColumnReader(StructField field, int batchSize, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    this.value =
        ResolveDefaultColumns.getExistenceDefaultValues(new StructType(new StructField[] {field}))[
            0];
    initVector(value, batchSize);
  }

  /** Constructor for partition columns with values from a row. */
  ArrowConstantColumnReader(
      StructField field, int batchSize, InternalRow values, int index, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    Object v = values.get(index, field.dataType());
    this.value = v;
    initVector(v, batchSize);
  }

  @Override
  public void setBatchSize(int batchSize) {
    close();
    this.batchSize = batchSize;
    initVector(value, batchSize);
  }

  @Override
  public void readBatch(int total) {
    if (total != currentSize) {
      close();
      initVector(value, total);
    }
  }

  @Override
  public CometVector currentBatch() {
    return vector;
  }

  @Override
  public void close() {
    if (vector != null) {
      vector.close();
      vector = null;
    }
    if (fieldVector != null) {
      fieldVector.close();
      fieldVector = null;
    }
  }

  private void initVector(Object value, int count) {
    currentSize = count;
    if (value == null) {
      isNull = true;
      fieldVector = createNullVector(count);
    } else {
      isNull = false;
      fieldVector = createFilledVector(value, count);
    }
    vector = new CometPlainVector(fieldVector, useDecimal128, false, true);
  }

  /** Creates a vector of the correct type with {@code count} null values. */
  private FieldVector createNullVector(int count) {
    String name = "constant";
    FieldVector v;
    if (type == DataTypes.BooleanType) {
      v = new BitVector(name, allocator);
    } else if (type == DataTypes.ByteType) {
      v = new TinyIntVector(name, allocator);
    } else if (type == DataTypes.ShortType) {
      v = new SmallIntVector(name, allocator);
    } else if (type == DataTypes.IntegerType || type == DataTypes.DateType) {
      v = new IntVector(name, allocator);
    } else if (type == DataTypes.LongType
        || type == DataTypes.TimestampType
        || type == TimestampNTZType$.MODULE$) {
      v = new BigIntVector(name, allocator);
    } else if (type == DataTypes.FloatType) {
      v = new Float4Vector(name, allocator);
    } else if (type == DataTypes.DoubleType) {
      v = new Float8Vector(name, allocator);
    } else if (type == DataTypes.BinaryType) {
      v = new VarBinaryVector(name, allocator);
    } else if (type == DataTypes.StringType) {
      v = new VarCharVector(name, allocator);
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      if (!useDecimal128 && dt.precision() <= Decimal.MAX_INT_DIGITS()) {
        v = new IntVector(name, allocator);
      } else if (!useDecimal128 && dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
        v = new BigIntVector(name, allocator);
      } else {
        v = new DecimalVector(name, allocator, dt.precision(), dt.scale());
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
    v.setValueCount(count);
    return v;
  }

  /** Creates a vector filled with {@code count} copies of the given value. */
  private FieldVector createFilledVector(Object value, int count) {
    String name = "constant";
    if (type == DataTypes.BooleanType) {
      BitVector v = new BitVector(name, allocator);
      v.allocateNew(count);
      int bit = (boolean) value ? 1 : 0;
      for (int i = 0; i < count; i++) v.setSafe(i, bit);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.ByteType) {
      TinyIntVector v = new TinyIntVector(name, allocator);
      v.allocateNew(count);
      byte val = (byte) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.ShortType) {
      SmallIntVector v = new SmallIntVector(name, allocator);
      v.allocateNew(count);
      short val = (short) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.IntegerType || type == DataTypes.DateType) {
      IntVector v = new IntVector(name, allocator);
      v.allocateNew(count);
      int val = (int) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.LongType
        || type == DataTypes.TimestampType
        || type == TimestampNTZType$.MODULE$) {
      BigIntVector v = new BigIntVector(name, allocator);
      v.allocateNew(count);
      long val = (long) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.FloatType) {
      Float4Vector v = new Float4Vector(name, allocator);
      v.allocateNew(count);
      float val = (float) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.DoubleType) {
      Float8Vector v = new Float8Vector(name, allocator);
      v.allocateNew(count);
      double val = (double) value;
      for (int i = 0; i < count; i++) v.setSafe(i, val);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.BinaryType) {
      VarBinaryVector v = new VarBinaryVector(name, allocator);
      v.allocateNew(count);
      byte[] bytes = (byte[]) value;
      for (int i = 0; i < count; i++) v.setSafe(i, bytes, 0, bytes.length);
      v.setValueCount(count);
      return v;
    } else if (type == DataTypes.StringType) {
      VarCharVector v = new VarCharVector(name, allocator);
      v.allocateNew(count);
      byte[] bytes = ((UTF8String) value).getBytes();
      for (int i = 0; i < count; i++) v.setSafe(i, bytes, 0, bytes.length);
      v.setValueCount(count);
      return v;
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      Decimal d = (Decimal) value;
      if (!useDecimal128 && dt.precision() <= Decimal.MAX_INT_DIGITS()) {
        IntVector v = new IntVector(name, allocator);
        v.allocateNew(count);
        int val = (int) d.toUnscaledLong();
        for (int i = 0; i < count; i++) v.setSafe(i, val);
        v.setValueCount(count);
        return v;
      } else if (!useDecimal128 && dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
        BigIntVector v = new BigIntVector(name, allocator);
        v.allocateNew(count);
        long val = d.toUnscaledLong();
        for (int i = 0; i < count; i++) v.setSafe(i, val);
        v.setValueCount(count);
        return v;
      } else {
        DecimalVector v = new DecimalVector(name, allocator, dt.precision(), dt.scale());
        v.allocateNew(count);
        BigDecimal bd = d.toJavaBigDecimal();
        for (int i = 0; i < count; i++) v.setSafe(i, bd);
        v.setValueCount(count);
        return v;
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }
}

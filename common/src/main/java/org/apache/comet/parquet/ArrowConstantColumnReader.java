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
 * <p>Since the vector is constant, only a single element is stored and {@link CometPlainVector}
 * broadcasts it to all rows.
 */
public class ArrowConstantColumnReader extends AbstractColumnReader {
  private final BufferAllocator allocator = new RootAllocator();

  private boolean isNull;
  private Object value;
  private FieldVector fieldVector;
  private CometPlainVector vector;

  /** Constructor for missing columns (default values from schema). */
  ArrowConstantColumnReader(StructField field, int batchSize, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    this.value =
        ResolveDefaultColumns.getExistenceDefaultValues(new StructType(new StructField[] {field}))[
            0];
    initVector(value);
  }

  /** Constructor for partition columns with values from a row. */
  ArrowConstantColumnReader(
      StructField field, int batchSize, InternalRow values, int index, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    Object v = values.get(index, field.dataType());
    this.value = v;
    initVector(v);
  }

  @Override
  public void setBatchSize(int batchSize) {
    close();
    this.batchSize = batchSize;
    initVector(value);
  }

  @Override
  public void readBatch(int total) {
    vector.setNumValues(total);
    if (isNull) vector.setNumNulls(total);
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

  private void initVector(Object value) {
    if (value == null) {
      isNull = true;
      fieldVector = createTypedVector();
      fieldVector.setValueCount(1);
    } else {
      isNull = false;
      fieldVector = createAndSetVector(value);
    }
    vector = new CometPlainVector(fieldVector, useDecimal128, false, true);
  }

  private FieldVector createTypedVector() {
    String name = "constant";
    if (type == DataTypes.BooleanType) {
      return new BitVector(name, allocator);
    } else if (type == DataTypes.ByteType) {
      return new TinyIntVector(name, allocator);
    } else if (type == DataTypes.ShortType) {
      return new SmallIntVector(name, allocator);
    } else if (type == DataTypes.IntegerType || type == DataTypes.DateType) {
      return new IntVector(name, allocator);
    } else if (type == DataTypes.LongType
        || type == DataTypes.TimestampType
        || type == TimestampNTZType$.MODULE$) {
      return new BigIntVector(name, allocator);
    } else if (type == DataTypes.FloatType) {
      return new Float4Vector(name, allocator);
    } else if (type == DataTypes.DoubleType) {
      return new Float8Vector(name, allocator);
    } else if (type == DataTypes.BinaryType) {
      return new VarBinaryVector(name, allocator);
    } else if (type == DataTypes.StringType) {
      return new VarCharVector(name, allocator);
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      if (!useDecimal128 && dt.precision() <= Decimal.MAX_INT_DIGITS()) {
        return new IntVector(name, allocator);
      } else if (!useDecimal128 && dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
        return new BigIntVector(name, allocator);
      } else {
        return new DecimalVector(name, allocator, dt.precision(), dt.scale());
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }

  private FieldVector createAndSetVector(Object value) {
    String name = "constant";
    if (type == DataTypes.BooleanType) {
      BitVector v = new BitVector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (boolean) value ? 1 : 0);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.ByteType) {
      TinyIntVector v = new TinyIntVector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (byte) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.ShortType) {
      SmallIntVector v = new SmallIntVector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (short) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.IntegerType || type == DataTypes.DateType) {
      IntVector v = new IntVector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (int) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.LongType
        || type == DataTypes.TimestampType
        || type == TimestampNTZType$.MODULE$) {
      BigIntVector v = new BigIntVector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (long) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.FloatType) {
      Float4Vector v = new Float4Vector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (float) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.DoubleType) {
      Float8Vector v = new Float8Vector(name, allocator);
      v.allocateNew(1);
      v.setSafe(0, (double) value);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.BinaryType) {
      VarBinaryVector v = new VarBinaryVector(name, allocator);
      v.allocateNew(1);
      byte[] bytes = (byte[]) value;
      v.setSafe(0, bytes, 0, bytes.length);
      v.setValueCount(1);
      return v;
    } else if (type == DataTypes.StringType) {
      VarCharVector v = new VarCharVector(name, allocator);
      v.allocateNew(1);
      byte[] bytes = ((UTF8String) value).getBytes();
      v.setSafe(0, bytes, 0, bytes.length);
      v.setValueCount(1);
      return v;
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      Decimal d = (Decimal) value;
      if (!useDecimal128 && dt.precision() <= Decimal.MAX_INT_DIGITS()) {
        IntVector v = new IntVector(name, allocator);
        v.allocateNew(1);
        v.setSafe(0, (int) d.toUnscaledLong());
        v.setValueCount(1);
        return v;
      } else if (!useDecimal128 && dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
        BigIntVector v = new BigIntVector(name, allocator);
        v.allocateNew(1);
        v.setSafe(0, d.toUnscaledLong());
        v.setValueCount(1);
        return v;
      } else {
        DecimalVector v = new DecimalVector(name, allocator, dt.precision(), dt.scale());
        v.allocateNew(1);
        BigDecimal bd = d.toJavaBigDecimal();
        v.setSafe(0, bd);
        v.setValueCount(1);
        return v;
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }
}

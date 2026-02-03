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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

/**
 * A column reader that returns constant vectors without using native mutable buffers. This is used
 * for reading partition columns and missing columns in NativeBatchReader.
 *
 * <p>Unlike {@link ConstantColumnReader} which uses native Rust code with mutable buffers, this
 * implementation creates Arrow vectors directly in Java using Arrow's immutable buffer APIs.
 */
public class ImmutableConstantColumnReader extends AbstractColumnReader {

  /**
   * Checks if the given Spark DataType is supported by this reader. This is used at query planning
   * time to determine if NativeBatchReader can handle the partition schema or if it should fall
   * back to Spark.
   *
   * @param type the Spark DataType to check
   * @return true if the type is supported, false otherwise
   */
  public static boolean isTypeSupported(DataType type) {
    if (type == DataTypes.BooleanType
        || type == DataTypes.ByteType
        || type == DataTypes.ShortType
        || type == DataTypes.IntegerType
        || type == DataTypes.LongType
        || type == DataTypes.FloatType
        || type == DataTypes.DoubleType
        || type == DataTypes.StringType
        || type == DataTypes.BinaryType
        || type == DataTypes.DateType
        || type == DataTypes.TimestampType
        || type == TimestampNTZType$.MODULE$
        || type == DataTypes.NullType
        || type instanceof DecimalType) {
      return true;
    }
    // Complex types (StructType, ArrayType, MapType) and other types are not supported
    return false;
  }

  private final BufferAllocator allocator = new RootAllocator();

  /** Whether all the values in this constant column are nulls */
  private boolean isNull;

  /** The constant value */
  private Object value;

  /** The current vector */
  private CometVector vector;

  /** The Arrow field type for this column */
  private final Field arrowField;

  /** Constructor for missing columns with default values */
  ImmutableConstantColumnReader(StructField field, int batchSize, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    this.arrowField = toArrowField(field);
    this.value =
        ResolveDefaultColumns.getExistenceDefaultValues(new StructType(new StructField[] {field}))[
            0];
    this.isNull = (this.value == null);
  }

  /** Constructor for partition columns */
  ImmutableConstantColumnReader(
      StructField field, int batchSize, InternalRow values, int index, boolean useDecimal128) {
    super(field.dataType(), TypeUtil.convertToParquet(field), useDecimal128, false);
    this.batchSize = batchSize;
    this.arrowField = toArrowField(field);
    this.value = values.get(index, field.dataType());
    this.isNull = (this.value == null);
  }

  @Override
  public void setBatchSize(int batchSize) {
    close();
    this.batchSize = batchSize;
  }

  @Override
  public void readBatch(int total) {
    if (vector != null) {
      vector.close();
      vector = null;
    }
    vector = createConstantVector(total);
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
  }

  @Override
  protected void initNative() {
    // No native initialization needed - we create vectors purely in Java
    nativeHandle = 0;
  }

  /** Creates a constant Arrow vector with the specified number of rows. */
  private CometVector createConstantVector(int numRows) {
    ValueVector arrowVector = createArrowVector(numRows);
    return new CometPlainVector(arrowVector, useDecimal128);
  }

  /** Creates an Arrow vector filled with constant values. */
  private ValueVector createArrowVector(int numRows) {
    if (isNull) {
      return createNullVector(numRows);
    }

    if (type == DataTypes.BooleanType) {
      return createBooleanVector(numRows, (Boolean) value);
    } else if (type == DataTypes.ByteType) {
      return createByteVector(numRows, (Byte) value);
    } else if (type == DataTypes.ShortType) {
      return createShortVector(numRows, (Short) value);
    } else if (type == DataTypes.IntegerType) {
      return createIntVector(numRows, (Integer) value);
    } else if (type == DataTypes.LongType) {
      return createLongVector(numRows, (Long) value);
    } else if (type == DataTypes.FloatType) {
      return createFloatVector(numRows, (Float) value);
    } else if (type == DataTypes.DoubleType) {
      return createDoubleVector(numRows, (Double) value);
    } else if (type == DataTypes.StringType) {
      return createStringVector(numRows, (UTF8String) value);
    } else if (type == DataTypes.BinaryType) {
      return createBinaryVector(numRows, (byte[]) value);
    } else if (type == DataTypes.DateType) {
      return createDateVector(numRows, (Integer) value);
    } else if (type == DataTypes.TimestampType || type == TimestampNTZType$.MODULE$) {
      return createTimestampVector(numRows, (Long) value);
    } else if (type instanceof DecimalType) {
      return createDecimalVector(numRows, (Decimal) value, (DecimalType) type);
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }

  private ValueVector createNullVector(int numRows) {
    NullVector vector = new NullVector(arrowField.getName(), numRows);
    return vector;
  }

  private ValueVector createBooleanVector(int numRows, boolean value) {
    BitVector vector = new BitVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value ? 1 : 0);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createByteVector(int numRows, byte value) {
    TinyIntVector vector = new TinyIntVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createShortVector(int numRows, short value) {
    SmallIntVector vector = new SmallIntVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createIntVector(int numRows, int value) {
    IntVector vector = new IntVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createLongVector(int numRows, long value) {
    BigIntVector vector = new BigIntVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createFloatVector(int numRows, float value) {
    Float4Vector vector = new Float4Vector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createDoubleVector(int numRows, double value) {
    Float8Vector vector = new Float8Vector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createStringVector(int numRows, UTF8String value) {
    VarCharVector vector = new VarCharVector(arrowField, allocator);
    byte[] bytes = value.getBytes();
    vector.allocateNew((long) bytes.length * numRows, numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, bytes);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createBinaryVector(int numRows, byte[] value) {
    VarBinaryVector vector = new VarBinaryVector(arrowField, allocator);
    vector.allocateNew((long) value.length * numRows, numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createDateVector(int numRows, int value) {
    DateDayVector vector = new DateDayVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createTimestampVector(int numRows, long value) {
    TimeStampMicroTZVector vector = new TimeStampMicroTZVector(arrowField, allocator);
    vector.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      vector.set(i, value);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private ValueVector createDecimalVector(int numRows, Decimal value, DecimalType dt) {
    DecimalVector vector =
        new DecimalVector(arrowField.getName(), allocator, dt.precision(), dt.scale());
    vector.allocateNew(numRows);

    java.math.BigDecimal bigDecimal = value.toJavaBigDecimal();
    for (int i = 0; i < numRows; i++) {
      vector.set(i, bigDecimal);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  /** Converts a Spark StructField to an Arrow Field. */
  private Field toArrowField(StructField field) {
    ArrowType arrowType = toArrowType(field.dataType());
    FieldType fieldType = new FieldType(field.nullable(), arrowType, null);
    return new Field(field.name(), fieldType, null);
  }

  /** Converts a Spark DataType to an Arrow ArrowType. */
  private ArrowType toArrowType(DataType type) {
    if (type == DataTypes.BooleanType) {
      return ArrowType.Bool.INSTANCE;
    } else if (type == DataTypes.ByteType) {
      return new ArrowType.Int(8, true);
    } else if (type == DataTypes.ShortType) {
      return new ArrowType.Int(16, true);
    } else if (type == DataTypes.IntegerType) {
      return new ArrowType.Int(32, true);
    } else if (type == DataTypes.LongType) {
      return new ArrowType.Int(64, true);
    } else if (type == DataTypes.FloatType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (type == DataTypes.DoubleType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (type == DataTypes.StringType) {
      return ArrowType.Utf8.INSTANCE;
    } else if (type == DataTypes.BinaryType) {
      return ArrowType.Binary.INSTANCE;
    } else if (type == DataTypes.DateType) {
      return new ArrowType.Date(DateUnit.DAY);
    } else if (type == DataTypes.TimestampType) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    } else if (type == TimestampNTZType$.MODULE$) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      return new ArrowType.Decimal(dt.precision(), dt.scale(), 128);
    } else if (type == DataTypes.NullType) {
      return ArrowType.Null.INSTANCE;
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }
}

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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.types.*;

import org.apache.comet.vector.CometConstantVector;
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

  /** Creates a constant vector with the specified logical row count. */
  private CometVector createConstantVector(int numRows) {
    return new CometConstantVector(type, arrowField, useDecimal128, value, isNull, numRows);
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

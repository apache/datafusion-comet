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

import java.math.BigInteger;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column reader that always return constant vectors. Used for reading partition columns, for
 * instance.
 */
public class ConstantColumnReader extends MetadataColumnReader {
  /** Whether all the values in this constant column are nulls */
  private boolean isNull;

  /** The constant value in the format of Object that are used to initialize this column reader. */
  private Object value;

  ConstantColumnReader(StructField field, int batchSize, boolean useDecimal128) {
    this(field.dataType(), TypeUtil.convertToParquet(field), batchSize, useDecimal128);
    this.value =
        ResolveDefaultColumns.getExistenceDefaultValues(new StructType(new StructField[] {field}))[
            0];
    init(value);
  }

  ConstantColumnReader(
      StructField field, int batchSize, InternalRow values, int index, boolean useDecimal128) {
    this(field.dataType(), TypeUtil.convertToParquet(field), batchSize, useDecimal128);
    init(values, index);
  }

  // Used by Iceberg
  public ConstantColumnReader(
      DataType type, ParquetColumnSpec spec, Object value, boolean useDecimal128) {
    super(type, spec, useDecimal128, true);
    this.value = value;
  }

  ConstantColumnReader(
      DataType type, ColumnDescriptor descriptor, int batchSize, boolean useDecimal128) {
    super(type, descriptor, useDecimal128, true);
    this.batchSize = batchSize;
    initNative();
  }

  @Override
  public void setBatchSize(int batchSize) {
    super.setBatchSize(batchSize);
    init(value);
  }

  @Override
  public void readBatch(int total) {
    super.readBatch(total);
    if (isNull) setNumNulls(total);
  }

  private void init(InternalRow values, int index) {
    Object value = values.get(index, type);
    init(value);
  }

  private void init(Object value) {
    if (value == null) {
      Native.setNull(nativeHandle);
      isNull = true;
    } else if (type == DataTypes.BooleanType) {
      Native.setBoolean(nativeHandle, (boolean) value);
    } else if (type == DataTypes.ByteType) {
      Native.setByte(nativeHandle, (byte) value);
    } else if (type == DataTypes.ShortType) {
      Native.setShort(nativeHandle, (short) value);
    } else if (type == DataTypes.IntegerType) {
      Native.setInt(nativeHandle, (int) value);
    } else if (type == DataTypes.LongType) {
      Native.setLong(nativeHandle, (long) value);
    } else if (type == DataTypes.FloatType) {
      Native.setFloat(nativeHandle, (float) value);
    } else if (type == DataTypes.DoubleType) {
      Native.setDouble(nativeHandle, (double) value);
    } else if (type == DataTypes.BinaryType) {
      Native.setBinary(nativeHandle, (byte[]) value);
    } else if (type == DataTypes.StringType) {
      Native.setBinary(nativeHandle, ((UTF8String) value).getBytes());
    } else if (type == DataTypes.DateType) {
      Native.setInt(nativeHandle, (int) value);
    } else if (type == DataTypes.TimestampType || type == TimestampNTZType$.MODULE$) {
      Native.setLong(nativeHandle, (long) value);
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      Decimal d = (Decimal) value;
      if (!useDecimal128 && dt.precision() <= Decimal.MAX_INT_DIGITS()) {
        Native.setInt(nativeHandle, ((int) d.toUnscaledLong()));
      } else if (!useDecimal128 && dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
        Native.setLong(nativeHandle, d.toUnscaledLong());
      } else {
        final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
        byte[] bytes = integer.toByteArray();
        Native.setDecimal(nativeHandle, bytes);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + type);
    }
  }
}

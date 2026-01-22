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

package org.apache.comet.iceberg.api.parquet;

import org.junit.Test;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.types.*;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.TypeUtil;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the TypeUtil public API. */
public class TypeUtilApiTest extends AbstractApiTest {

  @Test
  public void testConvertBooleanType() {
    StructField field = new StructField("bool_col", DataTypes.BooleanType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPath()).containsExactly("bool_col");
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.BOOLEAN);
  }

  @Test
  public void testConvertIntegerType() {
    StructField field = new StructField("int_col", DataTypes.IntegerType, false, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPath()).containsExactly("int_col");
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
    assertThat(descriptor.getMaxDefinitionLevel()).isEqualTo(0); // required field
  }

  @Test
  public void testConvertLongType() {
    StructField field = new StructField("long_col", DataTypes.LongType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
    assertThat(descriptor.getMaxDefinitionLevel()).isEqualTo(1); // nullable field
  }

  @Test
  public void testConvertFloatType() {
    StructField field = new StructField("float_col", DataTypes.FloatType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.FLOAT);
  }

  @Test
  public void testConvertDoubleType() {
    StructField field = new StructField("double_col", DataTypes.DoubleType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.DOUBLE);
  }

  @Test
  public void testConvertStringType() {
    StructField field = new StructField("string_col", DataTypes.StringType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(descriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.StringLogicalTypeAnnotation.class);
  }

  @Test
  public void testConvertBinaryType() {
    StructField field = new StructField("binary_col", DataTypes.BinaryType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(descriptor.getPrimitiveType().getLogicalTypeAnnotation()).isNull();
  }

  @Test
  public void testConvertDateType() {
    StructField field = new StructField("date_col", DataTypes.DateType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
    assertThat(descriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.DateLogicalTypeAnnotation.class);
  }

  @Test
  public void testConvertTimestampType() {
    StructField field =
        new StructField("timestamp_col", DataTypes.TimestampType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
    assertThat(descriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class);

    LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsAnnotation =
        (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(tsAnnotation.isAdjustedToUTC()).isTrue();
    assertThat(tsAnnotation.getUnit()).isEqualTo(LogicalTypeAnnotation.TimeUnit.MICROS);
  }

  @Test
  public void testConvertByteType() {
    StructField field = new StructField("byte_col", DataTypes.ByteType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);

    LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
        (LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(intAnnotation.getBitWidth()).isEqualTo(8);
    assertThat(intAnnotation.isSigned()).isTrue();
  }

  @Test
  public void testConvertShortType() {
    StructField field = new StructField("short_col", DataTypes.ShortType, true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);

    LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
        (LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(intAnnotation.getBitWidth()).isEqualTo(16);
    assertThat(intAnnotation.isSigned()).isTrue();
  }

  @Test
  public void testConvertDecimalType() {
    StructField field =
        new StructField("decimal_col", new DecimalType(38, 10), true, Metadata.empty());
    ColumnDescriptor descriptor = TypeUtil.convertToParquet(field);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    assertThat(descriptor.getPrimitiveType().getTypeLength()).isEqualTo(16);

    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decAnnotation =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(decAnnotation.getPrecision()).isEqualTo(38);
    assertThat(decAnnotation.getScale()).isEqualTo(10);
  }

  @Test
  public void testConvertNullableVsRequired() {
    StructField nullableField =
        new StructField("nullable_col", DataTypes.IntegerType, true, Metadata.empty());
    ColumnDescriptor nullableDescriptor = TypeUtil.convertToParquet(nullableField);
    assertThat(nullableDescriptor.getMaxDefinitionLevel()).isEqualTo(1);

    StructField requiredField =
        new StructField("required_col", DataTypes.IntegerType, false, Metadata.empty());
    ColumnDescriptor requiredDescriptor = TypeUtil.convertToParquet(requiredField);
    assertThat(requiredDescriptor.getMaxDefinitionLevel()).isEqualTo(0);
  }
}

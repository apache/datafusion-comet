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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.ParquetColumnSpec;
import org.apache.comet.parquet.Utils;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the Utils public API. */
public class UtilsApiTest extends AbstractApiTest {

  @Test
  public void testBuildColumnDescriptorFromSimpleInt32Spec() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            1,
            new String[] {"id"},
            "INT32",
            0,
            false,
            0, // required
            0,
            null,
            null);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPath()).containsExactly("id");
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
    assertThat(descriptor.getMaxDefinitionLevel()).isEqualTo(0);
    assertThat(descriptor.getMaxRepetitionLevel()).isEqualTo(0);
  }

  @Test
  public void testBuildColumnDescriptorFromOptionalBinarySpec() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            2,
            new String[] {"name"},
            "BINARY",
            0,
            false,
            1, // optional
            0,
            "StringLogicalTypeAnnotation",
            new HashMap<>());

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPath()).containsExactly("name");
    assertThat(descriptor.getPrimitiveType().getPrimitiveTypeName())
        .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(descriptor.getMaxDefinitionLevel()).isEqualTo(1);
    assertThat(descriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.StringLogicalTypeAnnotation.class);
  }

  @Test
  public void testBuildColumnDescriptorFromDecimalSpec() {
    Map<String, String> params = new HashMap<>();
    params.put("precision", "38");
    params.put("scale", "10");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            3,
            new String[] {"amount"},
            "FIXED_LEN_BYTE_ARRAY",
            16,
            false,
            1,
            0,
            "DecimalLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

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
  public void testBuildColumnDescriptorFromTimestampSpec() {
    Map<String, String> params = new HashMap<>();
    params.put("isAdjustedToUTC", "true");
    params.put("unit", "MICROS");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            4,
            new String[] {"created_at"},
            "INT64",
            0,
            false,
            1,
            0,
            "TimestampLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

    assertThat(descriptor).isNotNull();
    LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsAnnotation =
        (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(tsAnnotation.isAdjustedToUTC()).isTrue();
    assertThat(tsAnnotation.getUnit()).isEqualTo(LogicalTypeAnnotation.TimeUnit.MICROS);
  }

  @Test
  public void testBuildColumnDescriptorFromIntLogicalTypeSpec() {
    Map<String, String> params = new HashMap<>();
    params.put("isSigned", "true");
    params.put("bitWidth", "16");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            5,
            new String[] {"small_int"},
            "INT32",
            0,
            false,
            1,
            0,
            "IntLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

    assertThat(descriptor).isNotNull();
    LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
        (LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(intAnnotation.isSigned()).isTrue();
    assertThat(intAnnotation.getBitWidth()).isEqualTo(16);
  }

  @Test
  public void testBuildColumnDescriptorFromRepeatedSpec() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            6, new String[] {"list", "element"}, "INT32", 0, true, 2, 1, null, null);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPath()).containsExactly("list", "element");
    assertThat(descriptor.getMaxDefinitionLevel()).isEqualTo(2);
    assertThat(descriptor.getMaxRepetitionLevel()).isEqualTo(1);
    assertThat(descriptor.getPrimitiveType().getRepetition()).isEqualTo(Type.Repetition.REPEATED);
  }

  @Test
  public void testDescriptorToParquetColumnSpec() {
    // Create a ColumnDescriptor
    PrimitiveType primitiveType =
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .id(42)
            .named("timestamp_col");

    ColumnDescriptor descriptor =
        new ColumnDescriptor(new String[] {"timestamp_col"}, primitiveType, 0, 1);

    ParquetColumnSpec spec = Utils.descriptorToParquetColumnSpec(descriptor);

    assertThat(spec).isNotNull();
    assertThat(spec.getFieldId()).isEqualTo(42);
    assertThat(spec.getPath()).containsExactly("timestamp_col");
    assertThat(spec.getPhysicalType()).isEqualTo("INT64");
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(1);
    assertThat(spec.getMaxRepetitionLevel()).isEqualTo(0);
    assertThat(spec.getLogicalTypeName()).isEqualTo("TimestampLogicalTypeAnnotation");
    assertThat(spec.getLogicalTypeParams()).containsEntry("isAdjustedToUTC", "true");
    assertThat(spec.getLogicalTypeParams()).containsEntry("unit", "MICROS");
  }

  @Test
  public void testDescriptorToParquetColumnSpecForDecimal() {
    PrimitiveType primitiveType =
        Types.primitive(
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
            .length(16)
            .as(LogicalTypeAnnotation.decimalType(10, 38))
            .id(10)
            .named("decimal_col");

    ColumnDescriptor descriptor =
        new ColumnDescriptor(new String[] {"decimal_col"}, primitiveType, 0, 1);

    ParquetColumnSpec spec = Utils.descriptorToParquetColumnSpec(descriptor);

    assertThat(spec).isNotNull();
    assertThat(spec.getPhysicalType()).isEqualTo("FIXED_LEN_BYTE_ARRAY");
    assertThat(spec.getTypeLength()).isEqualTo(16);
    assertThat(spec.getLogicalTypeName()).isEqualTo("DecimalLogicalTypeAnnotation");
    assertThat(spec.getLogicalTypeParams()).containsEntry("precision", "38");
    assertThat(spec.getLogicalTypeParams()).containsEntry("scale", "10");
  }

  @Test
  public void testRoundTripConversion() {
    // Create a ParquetColumnSpec
    Map<String, String> params = new HashMap<>();
    params.put("precision", "18");
    params.put("scale", "2");

    ParquetColumnSpec originalSpec =
        new ParquetColumnSpec(
            100,
            new String[] {"price"},
            "FIXED_LEN_BYTE_ARRAY",
            16,
            false,
            1,
            0,
            "DecimalLogicalTypeAnnotation",
            params);

    // Convert to ColumnDescriptor and back
    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(originalSpec);
    ParquetColumnSpec roundTrippedSpec = Utils.descriptorToParquetColumnSpec(descriptor);

    assertThat(roundTrippedSpec.getFieldId()).isEqualTo(originalSpec.getFieldId());
    assertThat(roundTrippedSpec.getPath()).isEqualTo(originalSpec.getPath());
    assertThat(roundTrippedSpec.getPhysicalType()).isEqualTo(originalSpec.getPhysicalType());
    assertThat(roundTrippedSpec.getTypeLength()).isEqualTo(originalSpec.getTypeLength());
    assertThat(roundTrippedSpec.getMaxDefinitionLevel())
        .isEqualTo(originalSpec.getMaxDefinitionLevel());
    assertThat(roundTrippedSpec.getMaxRepetitionLevel())
        .isEqualTo(originalSpec.getMaxRepetitionLevel());
    assertThat(roundTrippedSpec.getLogicalTypeName()).isEqualTo(originalSpec.getLogicalTypeName());
  }

  @Test
  public void testBuildColumnDescriptorForAllLogicalTypes() {
    // Test DATE
    ParquetColumnSpec dateSpec =
        new ParquetColumnSpec(
            1,
            new String[] {"date_col"},
            "INT32",
            0,
            false,
            1,
            0,
            "DateLogicalTypeAnnotation",
            null);
    ColumnDescriptor dateDescriptor = Utils.buildColumnDescriptor(dateSpec);
    assertThat(dateDescriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.DateLogicalTypeAnnotation.class);

    // Test ENUM
    ParquetColumnSpec enumSpec =
        new ParquetColumnSpec(
            2,
            new String[] {"enum_col"},
            "BINARY",
            0,
            false,
            1,
            0,
            "EnumLogicalTypeAnnotation",
            null);
    ColumnDescriptor enumDescriptor = Utils.buildColumnDescriptor(enumSpec);
    assertThat(enumDescriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.EnumLogicalTypeAnnotation.class);

    // Test JSON
    ParquetColumnSpec jsonSpec =
        new ParquetColumnSpec(
            3,
            new String[] {"json_col"},
            "BINARY",
            0,
            false,
            1,
            0,
            "JsonLogicalTypeAnnotation",
            null);
    ColumnDescriptor jsonDescriptor = Utils.buildColumnDescriptor(jsonSpec);
    assertThat(jsonDescriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.JsonLogicalTypeAnnotation.class);

    // Test UUID
    ParquetColumnSpec uuidSpec =
        new ParquetColumnSpec(
            4,
            new String[] {"uuid_col"},
            "FIXED_LEN_BYTE_ARRAY",
            16,
            false,
            1,
            0,
            "UUIDLogicalTypeAnnotation",
            null);
    ColumnDescriptor uuidDescriptor = Utils.buildColumnDescriptor(uuidSpec);
    assertThat(uuidDescriptor.getPrimitiveType().getLogicalTypeAnnotation())
        .isInstanceOf(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.class);

    // Note: LIST and MAP are group types, not applicable to primitive column descriptors
    // They would require a different API or nested structure to test properly
  }

  @Test
  public void testBuildColumnDescriptorForTimeLogicalType() {
    Map<String, String> params = new HashMap<>();
    params.put("isAdjustedToUTC", "false");
    params.put("unit", "NANOS");

    ParquetColumnSpec timeSpec =
        new ParquetColumnSpec(
            7,
            new String[] {"time_col"},
            "INT64",
            0,
            false,
            1,
            0,
            "TimeLogicalTypeAnnotation",
            params);
    ColumnDescriptor timeDescriptor = Utils.buildColumnDescriptor(timeSpec);

    LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnnotation =
        (LogicalTypeAnnotation.TimeLogicalTypeAnnotation)
            timeDescriptor.getPrimitiveType().getLogicalTypeAnnotation();
    assertThat(timeAnnotation.isAdjustedToUTC()).isFalse();
    assertThat(timeAnnotation.getUnit()).isEqualTo(LogicalTypeAnnotation.TimeUnit.NANOS);
  }
}

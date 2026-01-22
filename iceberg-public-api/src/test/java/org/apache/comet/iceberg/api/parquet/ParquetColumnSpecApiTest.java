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

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.ParquetColumnSpec;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the ParquetColumnSpec public API. */
public class ParquetColumnSpecApiTest extends AbstractApiTest {

  @Test
  public void testConstructorAndGetters() {
    int fieldId = 1;
    String[] path = new String[] {"parent", "child"};
    String physicalType = "INT64";
    int typeLength = 0;
    boolean isRepeated = false;
    int maxDefinitionLevel = 2;
    int maxRepetitionLevel = 1;
    String logicalTypeName = "TimestampLogicalTypeAnnotation";
    Map<String, String> logicalTypeParams = new HashMap<>();
    logicalTypeParams.put("isAdjustedToUTC", "true");
    logicalTypeParams.put("unit", "MICROS");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            fieldId,
            path,
            physicalType,
            typeLength,
            isRepeated,
            maxDefinitionLevel,
            maxRepetitionLevel,
            logicalTypeName,
            logicalTypeParams);

    assertThat(spec.getFieldId()).isEqualTo(fieldId);
    assertThat(spec.getPath()).isEqualTo(path);
    assertThat(spec.getPhysicalType()).isEqualTo(physicalType);
    assertThat(spec.getTypeLength()).isEqualTo(typeLength);
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(maxDefinitionLevel);
    assertThat(spec.getMaxRepetitionLevel()).isEqualTo(maxRepetitionLevel);
    assertThat(spec.getLogicalTypeName()).isEqualTo(logicalTypeName);
    assertThat(spec.getLogicalTypeParams()).isEqualTo(logicalTypeParams);
  }

  @Test
  public void testSimpleInt32Column() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            1,
            new String[] {"id"},
            "INT32",
            0,
            false,
            0, // required column
            0,
            null,
            null);

    assertThat(spec.getFieldId()).isEqualTo(1);
    assertThat(spec.getPath()).containsExactly("id");
    assertThat(spec.getPhysicalType()).isEqualTo("INT32");
    assertThat(spec.getTypeLength()).isEqualTo(0);
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(0);
    assertThat(spec.getMaxRepetitionLevel()).isEqualTo(0);
    assertThat(spec.getLogicalTypeName()).isNull();
    assertThat(spec.getLogicalTypeParams()).isNull();
  }

  @Test
  public void testOptionalStringColumn() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            2,
            new String[] {"name"},
            "BINARY",
            0,
            false,
            1, // optional column
            0,
            "StringLogicalTypeAnnotation",
            new HashMap<>());

    assertThat(spec.getFieldId()).isEqualTo(2);
    assertThat(spec.getPath()).containsExactly("name");
    assertThat(spec.getPhysicalType()).isEqualTo("BINARY");
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(1);
    assertThat(spec.getMaxRepetitionLevel()).isEqualTo(0);
    assertThat(spec.getLogicalTypeName()).isEqualTo("StringLogicalTypeAnnotation");
  }

  @Test
  public void testDecimalColumn() {
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

    assertThat(spec.getFieldId()).isEqualTo(3);
    assertThat(spec.getPath()).containsExactly("amount");
    assertThat(spec.getPhysicalType()).isEqualTo("FIXED_LEN_BYTE_ARRAY");
    assertThat(spec.getTypeLength()).isEqualTo(16);
    assertThat(spec.getLogicalTypeName()).isEqualTo("DecimalLogicalTypeAnnotation");
    assertThat(spec.getLogicalTypeParams()).containsEntry("precision", "38");
    assertThat(spec.getLogicalTypeParams()).containsEntry("scale", "10");
  }

  @Test
  public void testNestedColumn() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            4, new String[] {"struct", "nested", "field"}, "INT64", 0, false, 3, 0, null, null);

    assertThat(spec.getPath()).containsExactly("struct", "nested", "field");
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(3);
  }

  @Test
  public void testRepeatedColumn() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            5, new String[] {"list", "element"}, "INT32", 0, true, 2, 1, null, null);

    assertThat(spec.getPath()).containsExactly("list", "element");
    assertThat(spec.getMaxDefinitionLevel()).isEqualTo(2);
    assertThat(spec.getMaxRepetitionLevel()).isEqualTo(1);
  }

  @Test
  public void testTimestampColumn() {
    Map<String, String> params = new HashMap<>();
    params.put("isAdjustedToUTC", "true");
    params.put("unit", "MICROS");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            6,
            new String[] {"created_at"},
            "INT64",
            0,
            false,
            1,
            0,
            "TimestampLogicalTypeAnnotation",
            params);

    assertThat(spec.getLogicalTypeName()).isEqualTo("TimestampLogicalTypeAnnotation");
    assertThat(spec.getLogicalTypeParams()).containsEntry("isAdjustedToUTC", "true");
    assertThat(spec.getLogicalTypeParams()).containsEntry("unit", "MICROS");
  }

  @Test
  public void testIntLogicalTypeColumn() {
    Map<String, String> params = new HashMap<>();
    params.put("isSigned", "true");
    params.put("bitWidth", "16");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            7,
            new String[] {"small_int"},
            "INT32",
            0,
            false,
            1,
            0,
            "IntLogicalTypeAnnotation",
            params);

    assertThat(spec.getLogicalTypeName()).isEqualTo("IntLogicalTypeAnnotation");
    assertThat(spec.getLogicalTypeParams()).containsEntry("isSigned", "true");
    assertThat(spec.getLogicalTypeParams()).containsEntry("bitWidth", "16");
  }
}

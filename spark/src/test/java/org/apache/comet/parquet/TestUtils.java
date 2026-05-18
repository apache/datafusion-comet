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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import static org.junit.Assert.*;

public class TestUtils {

  @Test
  public void testBuildColumnDescriptorWithTimestamp() {
    Map<String, String> params = new HashMap<>();
    params.put("isAdjustedToUTC", "true");
    params.put("unit", "MICROS");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            10,
            new String[] {"event_time"},
            "INT64",
            0,
            false,
            0,
            0,
            "TimestampLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);
    assertNotNull(descriptor);

    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, primitiveType.getPrimitiveTypeName());
    assertTrue(
        primitiveType.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation);

    LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts =
        (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
            primitiveType.getLogicalTypeAnnotation();
    assertTrue(ts.isAdjustedToUTC());
    assertEquals(LogicalTypeAnnotation.TimeUnit.MICROS, ts.getUnit());
  }

  @Test
  public void testBuildColumnDescriptorWithDecimal() {
    Map<String, String> params = new HashMap<>();
    params.put("precision", "10");
    params.put("scale", "2");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            11,
            new String[] {"price"},
            "FIXED_LEN_BYTE_ARRAY",
            5,
            false,
            0,
            0,
            "DecimalLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);
    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    assertEquals(
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, primitiveType.getPrimitiveTypeName());

    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
            primitiveType.getLogicalTypeAnnotation();
    assertEquals(10, dec.getPrecision());
    assertEquals(2, dec.getScale());
  }

  @Test
  public void testBuildColumnDescriptorWithIntLogicalType() {
    Map<String, String> params = new HashMap<>();
    params.put("bitWidth", "32");
    params.put("isSigned", "true");

    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            12,
            new String[] {"count"},
            "INT32",
            0,
            false,
            0,
            0,
            "IntLogicalTypeAnnotation",
            params);

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);
    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, primitiveType.getPrimitiveTypeName());

    LogicalTypeAnnotation.IntLogicalTypeAnnotation ann =
        (LogicalTypeAnnotation.IntLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation();
    assertEquals(32, ann.getBitWidth());
    assertTrue(ann.isSigned());
  }

  @Test
  public void testBuildColumnDescriptorWithStringLogicalType() {
    ParquetColumnSpec spec =
        new ParquetColumnSpec(
            13,
            new String[] {"name"},
            "BINARY",
            0,
            false,
            0,
            0,
            "StringLogicalTypeAnnotation",
            Collections.emptyMap());

    ColumnDescriptor descriptor = Utils.buildColumnDescriptor(spec);
    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, primitiveType.getPrimitiveTypeName());
    assertTrue(
        primitiveType.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation);
  }
}

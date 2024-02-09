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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import scala.collection.JavaConverters;

import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class TestColumnReader {
  private static final int BATCH_SIZE = 1024;
  private static final List<DataType> TYPES =
      Arrays.asList(
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          BinaryType,
          DecimalType.apply(5, 2),
          DecimalType.apply(18, 10),
          DecimalType.apply(19, 5));
  private static final List<Object> VALUES =
      Arrays.asList(
          true,
          (byte) 42,
          (short) 100,
          1000,
          (long) 10000,
          (float) 3.14,
          3.1415926,
          new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
          Decimal.apply("123.45"),
          Decimal.apply("00.0123456789"),
          Decimal.apply("-001234.56789"));
  private static final List<BiFunction<CometVector, Integer, Object>> GETTERS =
      Arrays.asList(
          ColumnVector::getBoolean,
          ColumnVector::getByte,
          ColumnVector::getShort,
          ColumnVector::getInt,
          ColumnVector::getLong,
          ColumnVector::getFloat,
          ColumnVector::getDouble,
          ColumnVector::getBinary,
          (v, i) -> v.getDecimal(i, 5, 2),
          (v, i) -> v.getDecimal(i, 18, 10),
          (v, i) -> v.getDecimal(i, 19, 5));

  @Test
  public void testConstantVectors() {
    for (int i = 0; i < TYPES.size(); i++) {
      DataType type = TYPES.get(i);
      StructField field = StructField.apply("f", type, false, null);

      List<Object> values = Collections.singletonList(VALUES.get(i));
      InternalRow row = GenericInternalRow.apply(JavaConverters.asScalaBuffer(values).toSeq());
      ConstantColumnReader reader = new ConstantColumnReader(field, BATCH_SIZE, row, 0, true);
      reader.readBatch(BATCH_SIZE);
      CometVector vector = reader.currentBatch();
      assertEquals(BATCH_SIZE, vector.numValues());
      assertEquals(0, vector.numNulls());
      for (int j = 0; j < BATCH_SIZE; j++) {
        if (TYPES.get(i) == BinaryType || TYPES.get(i) == StringType) {
          assertArrayEquals((byte[]) VALUES.get(i), (byte[]) GETTERS.get(i).apply(vector, j));
        } else {
          assertEquals(VALUES.get(i), GETTERS.get(i).apply(vector, j));
        }
      }

      // Test null values too
      row.setNullAt(0);
      reader = new ConstantColumnReader(field, BATCH_SIZE, row, 0, true);
      reader.readBatch(BATCH_SIZE);
      vector = reader.currentBatch();
      assertEquals(BATCH_SIZE, vector.numValues());
      assertEquals(BATCH_SIZE, vector.numNulls());
      for (int j = 0; j < BATCH_SIZE; j++) {
        assertTrue(vector.isNullAt(j));
      }
    }

    if (org.apache.spark.package$.MODULE$.SPARK_VERSION_SHORT().compareTo("3.4") >= 0) {
      Metadata meta = new MetadataBuilder().putString("EXISTS_DEFAULT", "123").build();
      StructField field = StructField.apply("f", LongType, false, meta);
      ConstantColumnReader reader = new ConstantColumnReader(field, BATCH_SIZE, true);
      reader.readBatch(BATCH_SIZE);
      CometVector vector = reader.currentBatch();

      assertEquals(BATCH_SIZE, vector.numValues());
      assertEquals(0, vector.numNulls());
      for (int j = 0; j < BATCH_SIZE; j++) {
        assertEquals(123, vector.getLong(j));
      }
    }
  }

  @Test
  public void testRowIndexColumnVectors() {
    StructField field = StructField.apply("f", LongType, false, null);
    int bigBatchSize = BATCH_SIZE * 2;
    int step = 4;
    int batchSize = bigBatchSize / step;
    long[] indices = new long[step * 2];
    List<Long> expected = new ArrayList<>();

    long idx = 0, len = 0;
    for (int i = 0; i < step; i++) {
      idx = ThreadLocalRandom.current().nextLong(idx + len, Long.MAX_VALUE);
      indices[i * 2] = idx;
      len = ThreadLocalRandom.current().nextLong(Long.max(bigBatchSize - expected.size(), 0));
      indices[i * 2 + 1] = len;
      for (int j = 0; j < len; j++) {
        expected.add(idx + j);
      }
    }

    RowIndexColumnReader reader = new RowIndexColumnReader(field, BATCH_SIZE, indices);
    for (int i = 0; i < step; i++) {
      reader.readBatch(batchSize);
      CometVector vector = reader.currentBatch();
      assertEquals(
          Integer.min(batchSize, Integer.max(expected.size() - i * batchSize, 0)),
          vector.numValues());
      assertEquals(0, vector.numNulls());
      for (int j = 0; j < vector.numValues(); j++) {
        assertEquals((long) expected.get(i * batchSize + j), vector.getLong(j));
      }
    }

    reader.close();
  }

  @Test
  public void testIsFixedLength() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

    ValueVector vv = new IntVector("v1", allocator);
    CometVector vector = new CometPlainVector(vv, false);
    assertTrue(vector.isFixedLength());

    vv = new FixedSizeBinaryVector("v2", allocator, 12);
    vector = new CometPlainVector(vv, false);
    assertTrue(vector.isFixedLength());

    vv = new VarBinaryVector("v3", allocator);
    vector = new CometPlainVector(vv, false);
    assertFalse(vector.isFixedLength());
  }
}

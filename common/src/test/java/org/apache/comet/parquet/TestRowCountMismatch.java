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

import org.junit.Test;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import org.apache.comet.vector.CometVector;

import static org.junit.Assert.*;

/** Regression probes for apache/datafusion-comet#4211. */
public class TestRowCountMismatch {

  private static final StructField NULLABLE_INT =
      new StructField("c", DataTypes.IntegerType, true, Metadata.empty());

  @Test
  public void constantNullReaderRespectsBatchSize() throws Exception {
    int batchSize = 8192;
    try (ArrowConstantColumnReader reader =
        new ArrowConstantColumnReader(NULLABLE_INT, batchSize, false)) {
      reader.readBatch(batchSize);
      CometVector v = reader.currentBatch();
      assertNotNull(v);
      assertEquals(batchSize, v.getValueVector().getValueCount());
    }
  }

  @Test
  public void readBatchRejectsZeroToPreventRowCountMismatch() throws Exception {
    int batchSize = 8192;
    try (ArrowConstantColumnReader reader =
        new ArrowConstantColumnReader(NULLABLE_INT, batchSize, false)) {
      reader.readBatch(batchSize);
      assertEquals(batchSize, reader.currentBatch().getValueVector().getValueCount());

      try {
        reader.readBatch(0);
        fail("readBatch(0) should throw IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().contains("0"));
      }

      try {
        reader.readBatch(-1);
        fail("readBatch(-1) should throw IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().contains("-1"));
      }

      // Rejected calls must leave the previously-prepared vector intact.
      assertEquals(batchSize, reader.currentBatch().getValueVector().getValueCount());
    }
  }
}

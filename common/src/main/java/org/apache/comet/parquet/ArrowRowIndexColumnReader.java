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
import org.apache.arrow.vector.BigIntVector;
import org.apache.spark.sql.types.*;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

/**
 * A column reader that computes row indices in Java and creates Arrow BigIntVectors directly (no
 * native mutable buffers). Used for the row index metadata column in the native_iceberg_compat scan
 * path.
 *
 * <p>The {@code indices} array contains alternating pairs of (start_index, count) representing
 * ranges of sequential row indices within each row group.
 */
public class ArrowRowIndexColumnReader extends AbstractColumnReader {
  private final BufferAllocator allocator = new RootAllocator();

  /** Alternating (start_index, count) pairs from row groups. */
  private final long[] indices;

  /** Number of row indices consumed so far across batches. */
  private long offset;

  private BigIntVector fieldVector;
  private CometPlainVector vector;

  public ArrowRowIndexColumnReader(StructField field, int batchSize, long[] indices) {
    super(field.dataType(), TypeUtil.convertToParquet(field), false, false);
    this.indices = indices;
    this.batchSize = batchSize;
  }

  @Override
  public void setBatchSize(int batchSize) {
    close();
    this.batchSize = batchSize;
  }

  @Override
  public void readBatch(int total) {
    close();

    fieldVector = new BigIntVector("row_index", allocator);
    fieldVector.allocateNew(total);

    // Port of Rust set_indices: iterate (start, count) pairs, skip offset rows, fill up to total.
    long skipped = 0;
    int filled = 0;
    for (int i = 0; i < indices.length && filled < total; i += 2) {
      long index = indices[i];
      long count = indices[i + 1];
      long skip = Math.min(count, offset - skipped);
      skipped += skip;
      if (count == skip) {
        continue;
      }
      long remaining = Math.min(count - skip, total - filled);
      for (long j = 0; j < remaining; j++) {
        fieldVector.setSafe(filled, index + skip + j);
        filled++;
      }
    }
    offset += filled;

    fieldVector.setValueCount(filled);
    vector = new CometPlainVector(fieldVector, false, false, false);
    vector.setNumValues(filled);
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
}

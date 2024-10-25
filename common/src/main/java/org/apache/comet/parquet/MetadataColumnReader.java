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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.types.DataType;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

/** A metadata column reader that can be extended by {@link RowIndexColumnReader} etc. */
public class MetadataColumnReader extends AbstractColumnReader {
  private final BufferAllocator allocator = new RootAllocator();
  private CometVector vector;

  public MetadataColumnReader(DataType type, ColumnDescriptor descriptor, boolean useDecimal128) {
    // TODO: should we handle legacy dates & timestamps for metadata columns?
    super(type, descriptor, useDecimal128, false);
  }

  @Override
  public void setBatchSize(int batchSize) {
    close();
    super.setBatchSize(batchSize);
  }

  @Override
  public void readBatch(int total) {
    if (vector == null) {
      long[] addresses = Native.currentBatch(nativeHandle);
      try (ArrowArray array = ArrowArray.wrap(addresses[0]);
          ArrowSchema schema = ArrowSchema.wrap(addresses[1])) {
        FieldVector fieldVector = Data.importVector(allocator, array, schema, null);
        vector = new CometPlainVector(fieldVector, useDecimal128);
      }
    }
    vector.setNumValues(total);
  }

  void setNumNulls(int total) {
    vector.setNumNulls(total);
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
    super.close();
  }
}

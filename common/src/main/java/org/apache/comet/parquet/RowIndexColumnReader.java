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

import org.apache.spark.sql.types.*;

/**
 * A column reader that returns the row index vector. Used for reading row index metadata column for
 * Spark 3.4+. The row index can be accessed by {@code _tmp_metadata_row_index} column.
 */
public class RowIndexColumnReader extends MetadataColumnReader {
  /** The row indices that are used to initialize this column reader. */
  private final long[] indices;

  /** The current number of indices to skip reading from {@code indices}. */
  private long offset;

  public RowIndexColumnReader(StructField field, int batchSize, long[] indices) {
    super(field.dataType(), TypeUtil.convertToParquet(field), false);
    this.indices = indices;
    setBatchSize(batchSize);
  }

  @Override
  public void readBatch(int total) {
    Native.resetBatch(nativeHandle);
    int count = Native.setIndices(nativeHandle, offset, total, indices);
    offset += count;

    super.readBatch(count);
  }
}
